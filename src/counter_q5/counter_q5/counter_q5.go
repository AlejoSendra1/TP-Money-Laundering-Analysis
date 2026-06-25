package counter_q5

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"tp_distribuidos/common/batch_utils"
	"tp_distribuidos/common/datasaver"
	"tp_distribuidos/common/heatbeat"

	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
	"tp_distribuidos/common/worker"
)

const LOGS_UNTIL_CHECKPOINT = 250

type CounterQ5Config struct {
	ID          int
	WorkerID    string
	MomHost     string
	MomPort     int
	InputPrefix string
	OutputQueue string
	CacheAmount int // EOFs esperados (uno por instancia de currencies_cache)
}

type CheckpointData struct {
	CountByClient    map[int64]map[string]int `json:"countByClient"`
	EofCountByClient map[int64]int            `json:"eofCountByClient"`
	FinishedClients  batch_utils.Set[int64]   `json:"finishedClients"`
}

// CounterQ5 reads USD-converted PaymentRecords from currencies_cache,
// counts records with amount < 1 per payment format, and flushes results on EOF.
type CounterQ5 struct {
	config      CounterQ5Config
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware
	heartbeat   *heatbeat.HeartbeatSender
	dataSaver   *datasaver.DataSaver

	countByClient    map[int64]map[string]int
	eofCountByClient map[int64]int
	finishedClients  batch_utils.Set[int64]
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

	hb, err := heatbeat.NewHeartbeatSender(config.WorkerID, connSettings)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		return nil, fmt.Errorf("creating heartbeat sender: %w", err)
	}

	ds, err := datasaver.NewDataSaver(fmt.Sprintf("/persistence/counter_q5_%d", config.ID), LOGS_UNTIL_CHECKPOINT)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		return nil, fmt.Errorf("creating data saver: %w", err)
	}

	return &CounterQ5{
		config:           config,
		inputQueue:       inputQueue,
		outputQueue:      outputQueue,
		heartbeat:        hb,
		dataSaver:        ds,
		countByClient:    make(map[int64]map[string]int),
		eofCountByClient: make(map[int64]int),
		finishedClients:  make(batch_utils.Set[int64]),
	}, nil
}

func (counter *CounterQ5) GetCheckpointData() any {
	return CheckpointData{
		CountByClient:    counter.countByClient,
		EofCountByClient: counter.eofCountByClient,
		FinishedClients:  counter.finishedClients,
	}
}

func (counter *CounterQ5) Restaurate() error {
	var checkpoint CheckpointData
	thereIsCheckpoint, err := counter.dataSaver.GetRestaurationCheckpoint(&checkpoint)
	if err != nil {
		return err
	}
	if thereIsCheckpoint {
		slog.Info("Restaurating counter_q5 from checkpoint")
		counter.countByClient = checkpoint.CountByClient
		counter.eofCountByClient = checkpoint.EofCountByClient
		counter.finishedClients = checkpoint.FinishedClients
	}

	var savedMsg middleware.Message
	for {
		hasLogs, err := counter.dataSaver.GetDataFromLogs(&savedMsg)
		if err != nil {
			return err
		}
		if !hasLogs {
			break
		}
		if err := worker.HandleMessageV2(&savedMsg, worker.MessageHandlerMap{
			inner.TransactionBatch: counter.handleTransactionBatch,
			inner.EndOfRecords:     counter.handleEOF,
		}); err != nil {
			return err
		}
	}
	return nil
}

// Run starts consuming. Returns once processing is done or SIGTERM is received.
func (counter *CounterQ5) Run() {
	go counter.handleSigterm()
	counter.heartbeat.Start()
	counter.inputQueue.StartConsuming(counter.handleMessage)
	counter.close()
}

func (counter *CounterQ5) handleSigterm() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	<-sigCh
	slog.Info("SIGTERM received, stopping consumer")
	counter.heartbeat.Stop()
	counter.inputQueue.StopConsuming()
}

func (counter *CounterQ5) handleMessage(msg middleware.Message, ack, nack func()) {
	err := worker.HandleMessageV2(
		&msg,
		worker.MessageHandlerMap{
			inner.TransactionBatch: counter.handleTransactionBatch,
			inner.EndOfRecords:     counter.handleEOF,
		},
	)
	if err != nil {
		slog.Error("Handling message", "err", err)
		nack()
		return
	}
	counter.dataSaver.Save(msg, counter)
	ack()
}

func (counter *CounterQ5) handleTransactionBatch(clientID int64, data []interface{}) error {
	records, err := inner.DeserializePaymentRecordBatch(data)
	if err != nil {
		slog.Error("Deserializing payment record batch", "err", err, "client_id", clientID)
		return err
	}
	counter.countRecords(clientID, records)
	return nil
}

func (counter *CounterQ5) handleEOF(clientID int64, _ []interface{}) error {
	slog.Info("EOF received from cache", "client_id", clientID)
	return counter.handleEOFLogic(clientID)
}

// handleEOFLogic incrementa el contador y hace flush cuando se recibieron todos los EOFs.
// Usado en handleMessage y durante Restaurate.
func (counter *CounterQ5) handleEOFLogic(clientID int64) error {
	if counter.finishedClients.Contains(clientID) {
		slog.Info("Client already finished, ignoring EOF", "client_id", clientID)
		return nil
	}

	counter.eofCountByClient[clientID]++
	eofCount := counter.eofCountByClient[clientID]
	slog.Info("EOF accumulated", "client_id", clientID, "eofCount", eofCount, "expected", counter.config.CacheAmount)
	if eofCount < counter.config.CacheAmount {
		return nil
	}

	counts := counter.countByClient[clientID]
	delete(counter.countByClient, clientID)
	delete(counter.eofCountByClient, clientID)
	counter.finishedClients.Add(clientID)

	if err := counter.sendData(clientID, counts); err != nil {
		return err
	}
	return counter.sendEOF(clientID)
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
}
