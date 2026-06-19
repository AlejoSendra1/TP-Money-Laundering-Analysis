package counter_q5

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"tp_distribuidos/common/batch_utils"

	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

type CounterQ5Config struct {
	ID                  int
	MomHost             string
	MomPort             int
	InputPrefix         string // queue prefix from currencies_cache — reads from {InputPrefix}_{ID}
	OutputQueue         string // final results queue
	CacheAmount         int    // total de EOFs esperados (uno por instancia de currencies_cache)
	InstanceAmount      int    // número de instancias de counter_q5 (para control entre peers)
	ControlExchangeName string // exchange para mensajes de control entre peers
}

// CounterQ5 reads USD-converted PaymentRecords from currencies_cache,
// counts records with amount < 1 per payment format, and flushes results on EOF.
type CounterQ5 struct {
	config         CounterQ5Config
	inputQueue     middleware.Middleware
	outputQueue    middleware.Middleware
	controlOutputs []middleware.Middleware
	controlInput   middleware.Middleware

	mu               sync.Mutex // protege eofCountByClient y countByClient
	countByClient    map[int64]map[string]int
	eofCountByClient map[int64]int
}

func NewCounterQ5(config CounterQ5Config) (*CounterQ5, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	qName := fmt.Sprintf("%s_%d", config.InputPrefix, config.ID)
	inputQueue, err := middleware.CreateQueueMiddleware(qName, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating input queue (%s): %w", qName, err)
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, fmt.Errorf("creating output queue: %w", err)
	}

	// Control output exchanges — uno por peer (todas las instancias excepto self)
	var controlOutputs []middleware.Middleware
	for i := 0; i < config.InstanceAmount; i++ {
		if i == config.ID {
			continue
		}
		key := fmt.Sprintf("%s_%d", config.ControlExchangeName, i)
		exchange, err := middleware.CreateExchangeMiddleware(config.ControlExchangeName, []string{key}, connSettings, "")
		if err != nil {
			inputQueue.Close()
			outputQueue.Close()
			for _, c := range controlOutputs {
				c.Close()
			}
			return nil, fmt.Errorf("creating control output exchange for peer %d: %w", i, err)
		}
		controlOutputs = append(controlOutputs, exchange)
	}

	// Control input exchange — recibe notificaciones EOF de los peers
	myControlKey := fmt.Sprintf("%s_%d", config.ControlExchangeName, config.ID)
	controlInput, err := middleware.CreateExchangeMiddleware(config.ControlExchangeName, []string{myControlKey}, connSettings, myControlKey)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		for _, c := range controlOutputs {
			c.Close()
		}
		return nil, fmt.Errorf("creating control input exchange: %w", err)
	}

	return &CounterQ5{
		config:           config,
		inputQueue:       inputQueue,
		outputQueue:      outputQueue,
		controlOutputs:   controlOutputs,
		controlInput:     controlInput,
		countByClient:    make(map[int64]map[string]int),
		eofCountByClient: make(map[int64]int),
	}, nil
}

// Run starts consuming. Returns once processing is done or SIGTERM is received.
func (counter *CounterQ5) Run() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	go func() {
		<-sigCh
		slog.Info("SIGTERM received, stopping consumer")
		counter.inputQueue.StopConsuming()
		counter.controlInput.StopConsuming()
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		counter.controlInput.StartConsuming(counter.handleControlMessage)
	}()

	counter.inputQueue.StartConsuming(counter.handleMessage)

	counter.controlInput.StopConsuming()
	wg.Wait()
	counter.close()
}

func (counter *CounterQ5) handleMessage(msg middleware.Message, ack, nack func()) {
	clientID, records, isEof, err := inner.DeserializePaymentRecordMessage(&msg)
	if err != nil {
		slog.Error("Deserializing message", "err", err)
		nack()
		return
	}

	if isEof {
		slog.Info("Direct EOF received from cache", "client_id", clientID)

		counter.mu.Lock()
		counter.eofCountByClient[clientID]++
		count := counter.eofCountByClient[clientID]
		counter.mu.Unlock()

		slog.Info("EOF accumulated", "client_id", clientID, "count", count, "expected", counter.config.CacheAmount)
		if count >= counter.config.CacheAmount {
			if err := counter.flushClient(clientID); err != nil {
				slog.Error("Flushing client", "err", err, "client_id", clientID)
				nack()
				return
			}
		}
		ack()
		return
	}

	// Proteger el conteo de records con mutex para evitar RC con EOFs de control
	counter.mu.Lock()
	counter.countRecords(clientID, records)
	counter.mu.Unlock()
	ack()
}

// sendControlEOF notifica a todos los peers que esta instancia recibió un EOF para clientID.
func (counter *CounterQ5) sendControlEOF(clientID int64) error {
	msg, err := inner.SerializePaymentRecordMessage(clientID, []transaction.PaymentRecord{})
	if err != nil {
		return fmt.Errorf("serializing control EOF: %w", err)
	}
	for i, controlOutput := range counter.controlOutputs {
		if err := controlOutput.Send(*msg); err != nil {
			return fmt.Errorf("sending control EOF to peer %d: %w", i, err)
		}
	}
	slog.Info("Control EOF sent to all peers", "client_id", clientID)
	return nil
}

// handleControlMessage: no-op — counter_q5 usa queues dedicadas, no necesita coordinación entre peers.
func (counter *CounterQ5) handleControlMessage(msg middleware.Message, ack, nack func()) {
	ack()
}

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
	counter.mu.Lock()
	counts := counter.countByClient[clientID]
	delete(counter.countByClient, clientID)
	delete(counter.eofCountByClient, clientID)
	counter.mu.Unlock()

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
	batch_utils.SortBatch(records, func(a, b transaction.PaymentFormatCount) bool {
		return a.Count < b.Count
	})
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
	//slog.Info("Sent count results", "client_id", clientID, "payment_formats", len(records), "data", records)
	return nil
}

func (counter *CounterQ5) sendEOF(clientID int64) error {
	msg, err := inner.SerializeQueryEOR(clientID, transaction.Query5, fmt.Sprintf("%d", counter.config.ID))
	if err != nil {
		return fmt.Errorf("serializing EOR: %w", err)
	}
	if err := counter.outputQueue.Send(*msg); err != nil {
		return fmt.Errorf("sending EOR: %w", err)
	}
	slog.Info("EOR sent", "client_id", clientID)
	return nil
}

func (counter *CounterQ5) close() {
	counter.inputQueue.Close()
	counter.outputQueue.Close()
	counter.controlInput.Close()
	for _, c := range counter.controlOutputs {
		c.Close()
	}
}
