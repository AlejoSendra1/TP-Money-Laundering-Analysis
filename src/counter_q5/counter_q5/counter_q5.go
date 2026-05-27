package counter_q5

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

type CounterQ5Config struct {
	ID              int
	MomHost         string
	MomPort         int
	InputPrefix     string // exchange prefix from filter_payment_format — binds to InputPrefix_{ID}
	FilterAmount    int    // number of filter_payment_format instances (EOFs to collect before forwarding)
	ConversionQueue string // queue to currencies_cache input (send raw records)
	ConvertedPrefix string // prefix of queues from currencies_cache — reads from {ConvertedPrefix}_{ID}
	OutputQueue     string // final results queue
}

// CounterQ5 runs two goroutines:
//  1. Forwarder: reads raw PaymentRecords from filter_payment_format and sends them to currencies_cache.
//  2. Counter:   reads USD-converted PaymentRecords from currencies_cache, counts amount < 1 per
//     payment format, flushes results on EOF.
type CounterQ5 struct {
	config          CounterQ5Config
	inputExch       middleware.Middleware // from filter_payment_format (sharded exchange)
	conversionQueue middleware.Middleware // to currencies_cache
	convertedQueue  middleware.Middleware // from currencies_cache
	outputQueue     middleware.Middleware // final output

	countByClient    map[int64]map[string]int
	eofCountByClient map[int64]int // tracks EOFs received from filter_payment_format
}

func NewCounterQ5(config CounterQ5Config) (*CounterQ5, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	// Input exchange — sharded to this instance
	inputKey := fmt.Sprintf("%s_%d", config.InputPrefix, config.ID)
	inputExch, err := middleware.CreateExchangeMiddleware(config.InputPrefix, []string{inputKey}, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating input exchange: %w", err)
	}

	// Queue to currencies_cache (produce only — queue is also declared by currencies_cache)
	conversionQueue, err := middleware.CreateQueueMiddleware(config.ConversionQueue, connSettings)
	if err != nil {
		inputExch.Close()
		return nil, fmt.Errorf("creating conversion queue: %w", err)
	}

	// Queue from currencies_cache — dedicated to this instance: {ConvertedPrefix}_{ID}
	convertedQName := fmt.Sprintf("%s_%d", config.ConvertedPrefix, config.ID)
	convertedQueue, err := middleware.CreateQueueMiddleware(convertedQName, connSettings)
	if err != nil {
		inputExch.Close()
		conversionQueue.Close()
		return nil, fmt.Errorf("creating converted queue (%s): %w", convertedQName, err)
	}

	// Final output queue
	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputExch.Close()
		conversionQueue.Close()
		convertedQueue.Close()
		return nil, fmt.Errorf("creating output queue: %w", err)
	}

	return &CounterQ5{
		config:           config,
		inputExch:        inputExch,
		conversionQueue:  conversionQueue,
		convertedQueue:   convertedQueue,
		outputQueue:      outputQueue,
		countByClient:    make(map[int64]map[string]int),
		eofCountByClient: make(map[int64]int),
	}, nil
}

// Run starts both goroutines and waits for all work to finish.
func (counter *CounterQ5) Run() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	go func() {
		<-sigCh
		slog.Info("SIGTERM received, stopping consumers")
		counter.inputExch.StopConsuming()
		counter.convertedQueue.StopConsuming()
	}()

	var wg sync.WaitGroup

	// Goroutine 1 — forward raw records from filter_payment_format to currencies_cache
	wg.Add(1)
	go func() {
		defer wg.Done()
		counter.inputExch.StartConsuming(counter.handleForward)
	}()

	// Goroutine 2 — count USD-converted records received from currencies_cache
	counter.convertedQueue.StartConsuming(counter.handleConverted)

	// Once the counter goroutine finishes, also stop the forwarder
	counter.inputExch.StopConsuming()
	wg.Wait()
	counter.close()
}

// handleForward relays data messages to the currencies_cache input queue.
// EOFs are counted; only when all FilterAmount EOFs have been received
// a single consolidated EOF is forwarded to currencies_cache.
func (counter *CounterQ5) handleForward(msg middleware.Message, ack, nack func()) {
	clientID, _, isEof, err := inner.DeserializePaymentRecordMessage(&msg)
	if err != nil {
		slog.Error("Deserializing message in forwarder", "err", err)
		nack()
		return
	}

	if isEof {
		counter.eofCountByClient[clientID]++
		count := counter.eofCountByClient[clientID]
		slog.Info("EOF received from filter", "client_id", clientID, "count", count, "total", counter.config.FilterAmount)

		if count < counter.config.FilterAmount {
			ack()
			return
		}

		// All filters done: forward one consolidated EOF to currencies_cache.
		delete(counter.eofCountByClient, clientID)
		eofMsg, err := inner.SerializePaymentRecordMessage(clientID, []transaction.PaymentRecord{})
		if err != nil {
			slog.Error("Serializing consolidated EOF", "err", err, "client_id", clientID)
			nack()
			return
		}
		if err := counter.conversionQueue.Send(*eofMsg); err != nil {
			slog.Error("Forwarding consolidated EOF to currencies_cache", "err", err, "client_id", clientID)
			nack()
			return
		}
		slog.Info("Consolidated EOF forwarded to currencies_cache", "client_id", clientID)
		ack()
		return
	}

	if err := counter.conversionQueue.Send(msg); err != nil {
		slog.Error("Forwarding record to conversion queue", "err", err, "client_id", clientID)
		nack()
		return
	}
	ack()
}

// handleConverted processes USD-converted PaymentRecords from currencies_cache.
// Amounts are already in USD so only a simple < 1 check is needed.
func (counter *CounterQ5) handleConverted(msg middleware.Message, ack, nack func()) {
	clientID, records, isEof, err := inner.DeserializePaymentRecordMessage(&msg)
	if err != nil {
		slog.Error("Deserializing converted message", "err", err)
		nack()
		return
	}

	if isEof {
		slog.Info("EOF received from currencies_cache, flushing", "client_id", clientID)
		if err := counter.flushClient(clientID); err != nil {
			slog.Error("Flushing client", "err", err, "client_id", clientID)
			nack()
			return
		}
		ack()
		return
	}

	counter.countRecords(clientID, records)
	ack()
}

// countRecords increments pre-format counters for records with amount_usd < 1.
func (counter *CounterQ5) countRecords(clientID int64, records []transaction.PaymentRecord) {
	counts, ok := counter.countByClient[clientID]
	if !ok {
		counts = make(map[string]int)
		counter.countByClient[clientID] = counts
	}
	for _, r := range records {
		if r.Amount < 1 {
			counts[r.PaymentFormat]++
			slog.Debug("Counted transaction",
				"client_id", clientID, "payment_format", r.PaymentFormat,
				"amount_usd", r.Amount, "total", counts[r.PaymentFormat])
		}
	}
}

func (counter *CounterQ5) flushClient(clientID int64) error {
	counts := counter.countByClient[clientID]
	delete(counter.countByClient, clientID)

	if err := counter.sendData(clientID, counts); err != nil {
		return err
	}
	return counter.sendEOF(clientID)
}

func (counter *CounterQ5) sendData(clientID int64, counts map[string]int) error {
	records := make([]transaction.PaymentFormatCount, 0, len(counts))
	for pf, cnt := range counts {
		records = append(records, transaction.PaymentFormatCount{PaymentFormat: pf, Count: cnt})
	}
	if len(records) == 0 {
		return nil
	}
	queryResult := transaction.QueryResult{
		QueryID:      transaction.Query5,
		Transactions: records,
	}
	msg, err := inner.SerializeQueryResultMessage(clientID, queryResult)
	if err != nil {
		return fmt.Errorf("serializing count results: %w", err)
	}
	if err := counter.outputQueue.Send(*msg); err != nil {
		return fmt.Errorf("sending count results: %w", err)
	}
	slog.Info("Sent count results", "client_id", clientID, "payment_formats", len(records))
	return nil
}

func (counter *CounterQ5) sendEOF(clientID int64) error {
	queryResult := transaction.QueryResult{
		QueryID:      transaction.Query5,
		Transactions: []transaction.PaymentFormatCount{},
	}
	msg, err := inner.SerializeQueryResultMessage(clientID, queryResult)
	if err != nil {
		return fmt.Errorf("serializing EOF: %w", err)
	}
	if err := counter.outputQueue.Send(*msg); err != nil {
		return fmt.Errorf("sending EOF: %w", err)
	}
	slog.Info("EOF sent", "client_id", clientID)
	return nil
}

func (counter *CounterQ5) close() {
	counter.inputExch.Close()
	counter.conversionQueue.Close()
	counter.convertedQueue.Close()
	counter.outputQueue.Close()
}
