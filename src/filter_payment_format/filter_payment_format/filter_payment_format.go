package filter_payment_format

import (
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
	OutputQueue          string
	FilterAmount         int
	USDFilterAmount      int
	FilterPaymentControl string
	BatchSize            int
}

// FilterPaymentFormat filters transactions by payment format (Wire, ACH) and
// routes approved transactions to the appropriate downstream exchange.
type FilterPaymentFormat struct {
	config         FilterPaymentFormatConfig
	inputQueue     middleware.Middleware
	outputQueue    middleware.Middleware // cola compartida hacia currencies_cache
	controlOutputs []middleware.Middleware
	controlInput   middleware.Middleware

	mutex            sync.Mutex
	bufferByClient   map[int64]map[string][]transaction.PaymentRecord
	eofCountByClient map[int64]int
}

func NewFilterPaymentFormat(config FilterPaymentFormatConfig) (*FilterPaymentFormat, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating input queue: %w", err)
	}

	// Cola compartida de salida hacia currencies_cache (competing consumers)
	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, fmt.Errorf("creating output queue: %w", err)
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
			outputQueue.Close()
			for _, controlOutput := range controlOutputs {
				controlOutput.Close()
			}
			return nil, fmt.Errorf("creating control output exchange for peer %d: %w", i, err)
		}
		controlOutputs = append(controlOutputs, exchange)
	}

	myControlKey := fmt.Sprintf("%s_%d", config.FilterPaymentControl, config.ID)
	controlInput, err := middleware.CreateExchangeMiddleware(config.FilterPaymentControl, []string{myControlKey}, connSettings)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		for _, controlOutput := range controlOutputs {
			controlOutput.Close()
		}
		return nil, fmt.Errorf("creating control input exchange: %w", err)
	}

	return &FilterPaymentFormat{
		config:           config,
		inputQueue:       inputQueue,
		outputQueue:      outputQueue,
		controlOutputs:   controlOutputs,
		controlInput:     controlInput,
		bufferByClient:   make(map[int64]map[string][]transaction.PaymentRecord),
		eofCountByClient: make(map[int64]int),
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
		slog.Info("Direct EOF received, notifying peers", "client_id", clientID)
		if err := filter.sendControlEOF(clientID); err != nil {
			slog.Error("Sending control EOF to peers", "err", err, "client_id", clientID)
			nack()
			return
		}

		filter.mutex.Lock()
		filter.eofCountByClient[clientID]++
		count := filter.eofCountByClient[clientID]
		filter.mutex.Unlock()

		slog.Info("EOF accumulated", "client_id", clientID, "count", count, "expected", filter.config.USDFilterAmount)
		if count >= filter.config.USDFilterAmount {
			delete(filter.eofCountByClient, clientID)
			if err := filter.flushClient(clientID); err != nil {
				slog.Error("Flushing client on direct EOF", "err", err, "client_id", clientID)
				nack()
				return
			}
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
		filter.bufferByClient[clientID] = make(map[string][]transaction.PaymentRecord)
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

// sendBatch serializes and sends a slice of PaymentRecords to the shared output queue.
func (filter *FilterPaymentFormat) sendBatch(clientID int64, paymentFormat string, records []transaction.PaymentRecord) error {
	msg, err := inner.SerializePaymentRecordMessage(clientID, records)
	if err != nil {
		return fmt.Errorf("serializing batch for payment format %s: %w", paymentFormat, err)
	}
	if err := filter.outputQueue.Send(*msg); err != nil {
		return fmt.Errorf("sending batch for payment format %s: %w", paymentFormat, err)
	}
	//slog.Info("Sent batch", "client_id", clientID, "payment_format", paymentFormat, "count", len(records))
	return nil
}

// flushClient sends remaining buffered records for clientID and then the per-format EOFs.
func (filter *FilterPaymentFormat) flushClient(clientID int64) error {
	filter.mutex.Lock()
	shards := filter.bufferByClient[clientID]
	delete(filter.bufferByClient, clientID)
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

// forwardEOF sends a single EOF to the shared output queue signaling no more data for clientID.
func (filter *FilterPaymentFormat) forwardEOF(clientID int64) error {
	msg, err := inner.SerializePaymentRecordMessage(clientID, []transaction.PaymentRecord{})
	if err != nil {
		return fmt.Errorf("serializing EOF: %w", err)
	}
	if err := filter.outputQueue.Send(*msg); err != nil {
		return fmt.Errorf("forwarding EOF: %w", err)
	}
	slog.Info("EOF forwarded", "client_id", clientID)
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
// Acumula el EOF bajo el mutex y solo hace flush cuando se reciben todos los EOFs del upstream.
func (filter *FilterPaymentFormat) handleControlMessage(msg middleware.Message, ack, nack func()) {
	clientID, _, _, err := inner.DeserializeRawTransactionsMessage(&msg)
	if err != nil {
		slog.Error("Deserializing control message", "err", err)
		nack()
		return
	}
	slog.Info("Control EOF received from peer", "client_id", clientID)

	filter.mutex.Lock()
	filter.eofCountByClient[clientID]++
	count := filter.eofCountByClient[clientID]
	filter.mutex.Unlock()

	slog.Info("Control EOF accumulated", "client_id", clientID, "count", count, "expected", filter.config.USDFilterAmount)
	if count >= filter.config.USDFilterAmount {
		delete(filter.eofCountByClient, clientID)
		if err := filter.flushClient(clientID); err != nil {
			slog.Error("Flushing client on control EOF", "err", err, "client_id", clientID)
			nack()
			return
		}
	}
	ack()
}

func (filter *FilterPaymentFormat) close() {
	filter.inputQueue.Close()
	filter.outputQueue.Close()
	filter.controlInput.Close()
	for _, controlOutput := range filter.controlOutputs {
		controlOutput.Close()
	}
}
