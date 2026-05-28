package counter_q2

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

type CounterQ2Config struct {
	ID              int
	MomHost         string
	MomPort         int
	InputQueue      string
	InputExchange   string
	InputTopic      string
	OutputPrefix    string
	JoinAmount      int
	CounterAmount   int
	ControlExchange string
	BatchSize       int
}

type bankEntry struct {
	amount  float64
	account string
}

// CounterQ2 keeps the maximum-amount transaction per bank per client,
// then shards partial results to the downstream joiners.
type CounterQ2 struct {
	config          CounterQ2Config
	inputQueue      middleware.Middleware
	outputExchanges []middleware.Middleware
	controlOutputs  []middleware.Middleware // one per peer
	controlInput    middleware.Middleware

	mutex       sync.Mutex
	topByClient map[int64]map[int]bankEntry // client_id -> bankCode -> bankEntry{amount, account}
}

func getJoinerIndex(bank string, joinAmount int) int {
	sum := md5.Sum([]byte(bank))
	val := binary.BigEndian.Uint64(sum[:8])
	return int(val % uint64(joinAmount))
}

func NewCounterQ2(config CounterQ2Config) (*CounterQ2, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	// Input queue (shared/competing consumers)
	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating input queue: %w", err)
	}
	if err = inputQueue.BindToTopics(config.InputExchange, config.InputTopic); err != nil {
		inputQueue.Close()
		return nil, fmt.Errorf("binding input queue to exchange: %w", err)
	}

	// One output exchange per joiner
	outputExchanges := make([]middleware.Middleware, config.JoinAmount)
	for i := 0; i < config.JoinAmount; i++ {
		key := fmt.Sprintf("%s_%d", config.OutputPrefix, i)
		ex, err := middleware.CreateExchangeMiddleware(config.OutputPrefix, []string{key}, connSettings)
		if err != nil {
			inputQueue.Close()
			for j := 0; j < i; j++ {
				outputExchanges[j].Close()
			}
			return nil, fmt.Errorf("creating output exchange %d: %w", i, err)
		}
		outputExchanges[i] = ex
	}

	// Control output exchanges (one per peer)
	var controlOutputs []middleware.Middleware
	for i := 0; i < config.CounterAmount; i++ {
		if i == config.ID {
			continue
		}
		key := fmt.Sprintf("%s_%d", config.ControlExchange, i)
		ex, err := middleware.CreateExchangeMiddleware(config.ControlExchange, []string{key}, connSettings)
		if err != nil {
			inputQueue.Close()
			for _, o := range outputExchanges {
				o.Close()
			}
			for _, c := range controlOutputs {
				c.Close()
			}
			return nil, fmt.Errorf("creating control output exchange for peer %d: %w", i, err)
		}
		controlOutputs = append(controlOutputs, ex)
	}

	// Own control input exchange (exclusive queue, receives peer notifications)
	myKey := fmt.Sprintf("%s_%d", config.ControlExchange, config.ID)
	controlInput, err := middleware.CreateExchangeMiddleware(config.ControlExchange, []string{myKey}, connSettings)
	if err != nil {
		inputQueue.Close()
		for _, o := range outputExchanges {
			o.Close()
		}
		for _, c := range controlOutputs {
			c.Close()
		}
		return nil, fmt.Errorf("creating control input exchange: %w", err)
	}

	return &CounterQ2{
		config:          config,
		inputQueue:      inputQueue,
		outputExchanges: outputExchanges,
		controlOutputs:  controlOutputs,
		controlInput:    controlInput,
		topByClient:     make(map[int64]map[int]bankEntry),
	}, nil
}

// Run starts the worker. It returns when processing is complete or a signal is received.
func (counter *CounterQ2) Run() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM)
	go func() {
		<-signalChannel
		slog.Info("SIGTERM received, stopping consumers")
		counter.inputQueue.StopConsuming()
		counter.controlInput.StopConsuming()
	}()

	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		counter.controlInput.StartConsuming(counter.handleControlMessage)
	}()

	counter.inputQueue.StartConsuming(counter.handleMessage)

	// Stop control consumer once main consuming finishes
	counter.controlInput.StopConsuming()
	waitGroup.Wait()

	counter.close()
}

// handleMessage processes messages from the shared input queue.
func (counter *CounterQ2) handleMessage(middlewareMsg middleware.Message, ack, nack func()) {
	msg, err := inner.DeserializeMessage(&middlewareMsg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", msg.ClientID)
		nack()
		return
	}

	switch msg.MsgType {
	case inner.EndOfRecords:
		slog.Info("EOF received, notifying peers and flushing", "client_id", msg.ClientID)
		if err := counter.sendControlEOF(msg.ClientID); err != nil {
			slog.Error("Sending control EOF", "err", err, "client_id", msg.ClientID)
			nack()
			return
		}
		if err := counter.flushClient(msg.ClientID); err != nil {
			slog.Error("Flushing client", "err", err, "client_id", msg.ClientID)
			nack()
			return
		}
		ack()
		return
	case inner.TransactionBatch:
		transactions, err := inner.DeserializeTransactionBatch(msg.Data)
		if err != nil {
			slog.Error("While deserializing transactions from message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
		counter.processBatch(msg.ClientID, transactions)
		ack()

	}
}

// processBatch updates in-memory state: keeps max-amount entry per bank.
func (counter *CounterQ2) processBatch(clientID int64, transactions []transaction.Transaction) {
	counter.mutex.Lock()
	defer counter.mutex.Unlock()
	banks, ok := counter.topByClient[clientID]
	if !ok {
		banks = make(map[int]bankEntry)
		counter.topByClient[clientID] = banks
	}
	for _, tx := range transactions {
		prev, exists := banks[tx.FromBank]
		if !exists || tx.Amount > prev.amount {
			banks[tx.FromBank] = bankEntry{amount: tx.Amount, account: tx.FromAccount}
			slog.Info("New top", "client_id", clientID, "bank", tx.FromBank, "amount", tx.Amount, "account", tx.FromAccount)
		}
	}
}

// handleControlMessage processes EOF notifications from peer pods.
func (counter *CounterQ2) handleControlMessage(msg middleware.Message, ack, nack func()) {
	clientID, _, _, err := inner.DeserializeMaxBankTransactionMessage(&msg)
	if err != nil {
		slog.Error("Deserializing control message", "err", err)
		nack()
		return
	}
	slog.Info("Control EOF received from peer, flushing", "client_id", clientID)
	if err := counter.flushClient(clientID); err != nil {
		slog.Error("Flushing client from control", "err", err, "client_id", clientID)
		nack()
		return
	}
}

// sendControlEOF notifies all peer pods that this pod received an EOF for clientID.
func (counter *CounterQ2) sendControlEOF(clientID int64) error {
	msg, err := inner.SerializeMaxBankTransactionMessage(clientID, []transaction.MaxBankTransaction{})
	if err != nil {
		return err
	}
	for _, ex := range counter.controlOutputs {
		if err := ex.Send(*msg); err != nil {
			return fmt.Errorf("sending control EOF: %w", err)
		}
	}
	return nil
}

// flushClient pops partial state for clientID and sends it to the correct joiner(s).
func (counter *CounterQ2) flushClient(clientID int64) error {
	counter.mutex.Lock()
	banks := counter.topByClient[clientID]
	delete(counter.topByClient, clientID)
	counter.mutex.Unlock()

	if err := counter.sendData(clientID, banks); err != nil {
		return err
	}
	return counter.sendEOF(clientID)
}

// sendData shards the partial result by bank hash and sends each shard to the appropriate joiner.
func (counter *CounterQ2) sendData(clientID int64, banks map[int]bankEntry) error {
	// Group rows by joiner index (sharding by bank code)
	shards := make(map[int][]transaction.MaxBankTransaction)
	for bankCode, entry := range banks {
		idx := getJoinerIndex(fmt.Sprintf("%d", bankCode), counter.config.JoinAmount)
		shards[idx] = append(shards[idx], transaction.MaxBankTransaction{
			BankCode: bankCode,
			Account:  entry.account,
			Amount:   entry.amount,
		})
	}

	for idx, rows := range shards {
		for i := 0; i < len(rows); i += counter.config.BatchSize {
			end := i + counter.config.BatchSize
			if end > len(rows) {
				end = len(rows)
			}
			chunk := rows[i:end]
			msg, err := inner.SerializeMaxBankTransactionMessage(clientID, chunk)
			if err != nil {
				return fmt.Errorf("serializing data chunk for joiner %d: %w", idx, err)
			}
			if err := counter.outputExchanges[idx].Send(*msg); err != nil {
				return fmt.Errorf("sending data chunk to joiner %d: %w", idx, err)
			}
			slog.Info("Sent batch to joiner", "client_id", clientID, "joiner", idx, "count", len(chunk))
		}
	}
	return nil
}

// sendEOF forwards an EOF marker to every joiner exchange.
func (counter *CounterQ2) sendEOF(clientID int64) error {
	msg, err := inner.SerializeMaxBankTransactionMessage(clientID, []transaction.MaxBankTransaction{})
	if err != nil {
		return fmt.Errorf("serializing EOF: %w", err)
	}
	for i, ex := range counter.outputExchanges {
		if err := ex.Send(*msg); err != nil {
			return fmt.Errorf("sending EOF to joiner %d: %w", i, err)
		}
	}
	slog.Info("EOF forwarded to all joiners", "client_id", clientID)
	return nil
}

func (counter *CounterQ2) close() {
	counter.inputQueue.Close()
	counter.controlInput.Close()
	for _, ex := range counter.outputExchanges {
		ex.Close()
	}
	for _, ex := range counter.controlOutputs {
		ex.Close()
	}
}
