package filter_payment_format

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

var allowedPaymentFormats = map[string]bool{
	"Wire": true,
	"ACH":  true,
}

type FilterPaymentFormatConfig struct {
	ID                   int
	MomHost              string
	MomPort              int
	InputQueue           string
	InputExchangeName    string
	InputTopic           string
	OutputPrefix         string
	OutputAmount         int
	FilterAmount         int
	FilterPaymentControl string
	BatchSize            int
	Q5DateFilterAmount   int
}

// FilterPaymentFormat filters transactions by payment format (Wire, ACH) and
// routes approved transactions to the appropriate downstream exchange.
type FilterPaymentFormat struct {
	config          FilterPaymentFormatConfig
	inputQueue      middleware.Middleware
	outputExchanges []middleware.Middleware // one per downstream pod instance
	controlOutputs  []middleware.Middleware // one per peer pod
	controlInput    middleware.Middleware   // own control inbox
	eofCounter      map[int64]int           // tracks how many EOFs have been received per client_id
	mutex           sync.Mutex
	bufferByClient  map[int64]map[string][]transaction.PaymentRecord // client_id -> paymentFormat -> pending records
}

// getOutputIndex replicates the Python MD5-based routing:
// hash(payment_format) % outputAmount
func getOutputIndex(paymentFormat string, outputAmount int) int {
	sum := md5.Sum([]byte(paymentFormat))
	val := binary.BigEndian.Uint64(sum[:8])
	return int(val % uint64(outputAmount))
}

func NewFilterPaymentFormat(config FilterPaymentFormatConfig) (*FilterPaymentFormat, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	// Input queue — binds to the upstream transactions exchange
	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating input queue: %w", err)
	}
	if err = inputQueue.BindToTopics(config.InputExchangeName, config.InputTopic); err != nil {
		inputQueue.Close()
		return nil, fmt.Errorf("binding input queue: %w", err)
	}

	// One output exchange per downstream pod instance
	outputExchanges := make([]middleware.Middleware, config.OutputAmount)
	for i := 0; i < config.OutputAmount; i++ {
		key := fmt.Sprintf("%s_%d", config.OutputPrefix, i)
		exchange, err := middleware.CreateExchangeMiddleware(config.OutputPrefix, []string{key}, connSettings)
		if err != nil {
			inputQueue.Close()
			for j := 0; j < i; j++ {
				outputExchanges[j].Close()
			}
			return nil, fmt.Errorf("creating output exchange %d: %w", i, err)
		}
		outputExchanges[i] = exchange
	}

	// Control output exchanges — one per peer (all filter instances except self)
	var controlOutputs []middleware.Middleware
	for i := 0; i < config.FilterAmount; i++ {
		if i == config.ID {
			continue
		}
		key := fmt.Sprintf("%s_%d", config.FilterPaymentControl, i)
		exchange, err := middleware.CreateExchangeMiddleware(config.FilterPaymentControl, []string{key}, connSettings)
		if err != nil {
			inputQueue.Close()
			for _, outputExchange := range outputExchanges {
				outputExchange.Close()
			}
			for _, controlOutput := range controlOutputs {
				controlOutput.Close()
			}
			return nil, fmt.Errorf("creating control output exchange for peer %d: %w", i, err)
		}
		controlOutputs = append(controlOutputs, exchange)
	}

	// Own control input exchange — receives EOF notifications from peers
	myControlKey := fmt.Sprintf("%s_%d", config.FilterPaymentControl, config.ID)
	controlInput, err := middleware.CreateExchangeMiddleware(config.FilterPaymentControl, []string{myControlKey}, connSettings)
	if err != nil {
		inputQueue.Close()
		for _, outputExchange := range outputExchanges {
			outputExchange.Close()
		}
		for _, controlOutput := range controlOutputs {
			controlOutput.Close()
		}
		return nil, fmt.Errorf("creating control input exchange: %w", err)
	}

	return &FilterPaymentFormat{
		config:          config,
		inputQueue:      inputQueue,
		outputExchanges: outputExchanges,
		controlOutputs:  controlOutputs,
		controlInput:    controlInput,
		bufferByClient:  make(map[int64]map[string][]transaction.PaymentRecord),
		eofCounter:      make(map[int64]int),
	}, nil
}

// Run starts the worker. Returns once processing finishes or SIGTERM is received.
func (filter *FilterPaymentFormat) Run() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM)
	go func() {
		<-signalChannel
		slog.Info("SIGTERM received, stopping consumers")
		filter.inputQueue.StopConsuming()
		filter.controlInput.StopConsuming()
	}()

	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		filter.controlInput.StartConsuming(filter.handleControlMessage)
	}()

	filter.inputQueue.StartConsuming(filter.handleMessage)

	// Once the main input is done, stop the control consumer too
	filter.controlInput.StopConsuming()
	waitGroup.Wait()

	filter.close()
}

// handleMessage processes messages from the shared input queue.
func (filter *FilterPaymentFormat) handleMessage(msg middleware.Message, ack, nack func()) {
	clientID, transactions, isEof, err := inner.DeserializeRawTransactionsMessage(&msg)
	if err != nil {
		slog.Error("Deserializing input message", "err", err)
		nack()
		return
	}

	if isEof {
		slog.Info("EOF received, notifying peers and flushing", "client_id", clientID)
		if err := filter.sendControlEOF(clientID); err != nil {
			slog.Error("Sending control EOF to peers", "err", err, "client_id", clientID)
			nack()
			return
		}
		if err := filter.flushClient(clientID); err != nil {
			slog.Error("Flushing client on EOF", "err", err, "client_id", clientID)
			nack()
			return
		}
		ack()
		return
	}

	if err := filter.processData(clientID, transactions); err != nil {
		slog.Error("Processing data batch", "err", err, "client_id", clientID)
		nack()
		return
	}
	ack()
}

// processData filters transactions by payment format, accumulates records in
// bufferByClient[clientID][paymentFormat] and flushes full batches immediately.
func (filter *FilterPaymentFormat) processData(clientID int64, transactions []transaction.Transaction) error {
	filter.mutex.Lock()
	defer filter.mutex.Unlock()

	if filter.bufferByClient[clientID] == nil {
		slog.Info("New client arrived", "client_id", clientID)
		filter.bufferByClient[clientID] = make(map[string][]transaction.PaymentRecord)
		filter.eofCounter[clientID] = 0
	}

	for _, tx := range transactions {
		if !allowedPaymentFormats[tx.PaymentFormat] {
			slog.Debug("Discarding transaction — disallowed payment format",
				"client_id", clientID, "payment_format", tx.PaymentFormat)
			continue
		}
		filter.bufferByClient[clientID][tx.PaymentFormat] = append(
			filter.bufferByClient[clientID][tx.PaymentFormat],
			transaction.PaymentRecord{
				Timestamp:     tx.Timestamp,
				Amount:        tx.Amount,
				Currency:      tx.Currency,
				PaymentFormat: tx.PaymentFormat,
			},
		)
	}

	return filter.sendIfHaveBatchSize(clientID)
}

// sendIfHaveBatchSize sends complete batches (BatchSize) for all payment formats buffered for clientID.
// Must be called with filter.mutex held.
func (filter *FilterPaymentFormat) sendIfHaveBatchSize(clientID int64) error {
	for paymentFormat, buf := range filter.bufferByClient[clientID] {
		for len(buf) >= filter.config.BatchSize {
			if err := filter.sendBatch(clientID, paymentFormat, buf[:filter.config.BatchSize]); err != nil {
				return err
			}
			buf = buf[filter.config.BatchSize:]
		}
		filter.bufferByClient[clientID][paymentFormat] = buf
	}
	return nil
}

// sendBatch serializes and sends a slice of PaymentRecords to the output exchange
// determined by hashing paymentFormat.
func (filter *FilterPaymentFormat) sendBatch(clientID int64, paymentFormat string, records []transaction.PaymentRecord) error {
	outputIndex := getOutputIndex(paymentFormat, filter.config.OutputAmount)
	msg, err := inner.SerializePaymentRecordMessage(clientID, records)
	if err != nil {
		return fmt.Errorf("serializing batch for payment format %s: %w", paymentFormat, err)
	}
	if err := filter.outputExchanges[outputIndex].Send(*msg); err != nil {
		return fmt.Errorf("sending batch to output exchange %d: %w", outputIndex, err)
	}
	slog.Info("Sent batch", "client_id", clientID, "payment_format", paymentFormat, "count", len(records), "output_index", outputIndex)
	return nil
}

// flushClient sends any remaining buffered records for clientID and then sends the EOF marker.
func (filter *FilterPaymentFormat) flushClient(clientID int64) error {
	filter.mutex.Lock()
	filter.eofCounter[clientID] += 1
	if filter.eofCounter[clientID] != filter.config.Q5DateFilterAmount {
		filter.mutex.Unlock()
		slog.Info("Waiting for more EOFs...")
		return nil
	}
	shards := filter.bufferByClient[clientID]
	delete(filter.bufferByClient, clientID)
	delete(filter.eofCounter, clientID)
	filter.mutex.Unlock()

	for paymentFormat, remaining := range shards {
		if len(remaining) > 0 {
			if err := filter.sendBatch(clientID, paymentFormat, remaining); err != nil {
				return fmt.Errorf("flushing remaining records for payment format %s: %w", paymentFormat, err)
			}
		}
	}
	return filter.forwardEOF(clientID)
}

// forwardEOF sends an EOF marker to every output exchange.
func (filter *FilterPaymentFormat) forwardEOF(clientID int64) error {
	msg, err := inner.SerializePaymentRecordMessage(clientID, []transaction.PaymentRecord{})
	if err != nil {
		return fmt.Errorf("serializing EOF: %w", err)
	}
	for outputIndex, outputExchange := range filter.outputExchanges {
		if err := outputExchange.Send(*msg); err != nil {
			return fmt.Errorf("forwarding EOF to output exchange %d: %w", outputIndex, err)
		}
	}
	slog.Info("EOF forwarded to all output exchanges", "client_id", clientID)
	return nil
}

// sendControlEOF notifies all peer pods that this instance received an EOF for clientID.
func (filter *FilterPaymentFormat) sendControlEOF(clientID int64) error {
	msg, err := inner.SerializeMessage(clientID, []transaction.Transaction{})
	if err != nil {
		return fmt.Errorf("serializing control EOF: %w", err)
	}
	for peerIndex, controlOutput := range filter.controlOutputs {
		if err := controlOutput.Send(*msg); err != nil {
			return fmt.Errorf("sending control EOF to peer %d: %w", peerIndex, err)
		}
	}
	slog.Info("Control EOF sent to all peers", "client_id", clientID)
	return nil
}

// handleControlMessage processes EOF notifications from peer pods.
// When a peer sends an EOF, this pod must also flush and forward EOF to its output exchanges.
func (filter *FilterPaymentFormat) handleControlMessage(msg middleware.Message, ack, nack func()) {
	clientID, _, _, err := inner.DeserializeRawTransactionsMessage(&msg)
	if err != nil {
		slog.Error("Deserializing control message", "err", err)
		nack()
		return
	}
	slog.Info("Control EOF received from peer, flushing and forwarding EOF", "client_id", clientID)
	if err := filter.flushClient(clientID); err != nil {
		slog.Error("Flushing client from control message", "err", err, "client_id", clientID)
		nack()
		return
	}
	ack()
}

func (filter *FilterPaymentFormat) close() {
	filter.inputQueue.Close()
	filter.controlInput.Close()
	for _, outputExchange := range filter.outputExchanges {
		outputExchange.Close()
	}
	for _, controlOutput := range filter.controlOutputs {
		controlOutput.Close()
	}
}
