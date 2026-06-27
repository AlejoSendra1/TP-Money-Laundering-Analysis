package join_q2

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"tp_distribuidos/common/batch_utils"
	"tp_distribuidos/common/datasaver"
	"tp_distribuidos/common/heatbeat"
	"tp_distribuidos/common/worker"

	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

type JoinQ2Config struct {
	ID            int
	WorkerID      string
	MomHost       string
	MomPort       int
	InputPrefix   string
	CounterAmount int
	OutputQueue   string
	QueryID       transaction.QueryID
	BatchSize     int
}

// bankEntry holds the max-amount transaction data for a single bank.
type bankEntry struct {
	amount  float64
	account string
}

// JoinQ2 receives partial results from all counter_q2 instances (sharded by bank),
// keeps the max-amount entry per bank, and flushes to the output queue once all
// counter_q2 EOFs have arrived.
type JoinQ2 struct {
	config           JoinQ2Config
	inputExchange    middleware.Middleware
	outputQueue      middleware.Middleware
	topByClient      map[int64]map[int]bankEntry // client_id -> bankCode -> bankEntry{amount, account}
	eofCountByClient map[int64][]string          // tracks how many counter_q2 EOFs have arrived per client
	mssgHandlers     worker.MessageHandlerMap
	heartbeat        *heatbeat.HeartbeatSender
	datasaver        *datasaver.DataSaver
	flushHandler     *FlushHandler
}

func NewJoinQ2(config JoinQ2Config) (*JoinQ2, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	// Input exchange: this instance only receives messages sharded to its ID
	inputKey := fmt.Sprintf("%s_%d", config.InputPrefix, config.ID)
	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputPrefix, []string{inputKey}, connSettings, inputKey)
	if err != nil {
		return nil, fmt.Errorf("creating input exchange: %w", err)
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, fmt.Errorf("creating output queue: %w", err)
	}

	// para persistir la info ante posibles caidas
	//se podria agregar el nombre de del archivo de restauracion como var de entorno
	dataSaver, err := datasaver.NewDataSaver(fmt.Sprintf("/persistence/%s", config.WorkerID), LOGS_UNTIL_CHECKPOINT)
	if err != nil {
		return nil, err
	}

	hb, err := heatbeat.NewHeartbeatSender(config.WorkerID, connSettings)
	if err != nil {
		inputExchange.Close()
		outputQueue.Close()
		dataSaver.Close()
		return nil, fmt.Errorf("creating heartbeat sender: %w", err)
	}

	fh, err := NewFlushHandler(config.WorkerID)
	if err != nil {
		inputExchange.Close()
		outputQueue.Close()
		dataSaver.Close()
		return nil, fmt.Errorf("creating heartbeat sender: %w", err)
	}

	j := &JoinQ2{
		config:           config,
		inputExchange:    inputExchange,
		outputQueue:      outputQueue,
		topByClient:      make(map[int64]map[int]bankEntry),
		eofCountByClient: make(map[int64][]string),
		heartbeat:        hb,
		datasaver:        dataSaver,
		flushHandler:     fh,
	}

	j.mssgHandlers = worker.MessageHandlerMap{
		inner.EndOfRecords:     j.handleEOF,
		inner.TransactionBatch: j.processBatch,
	}

	return j, nil
}

func (joinQ2 *JoinQ2) Run() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM)
	go func() {
		<-signalChannel
		slog.Info("SIGTERM received, stopping consumer")
		joinQ2.heartbeat.Stop()
		joinQ2.inputExchange.StopConsuming()
	}()

	// Resume any flushes that were interrupted before a previous crash.
	// This must happen before we start consuming new messages so that
	// in-flight output is drained first.
	joinQ2.flushHandler.resumePendingFlushes(joinQ2.config.BatchSize, &joinQ2.outputQueue, joinQ2.sendEOF)

	joinQ2.heartbeat.Start()
	joinQ2.inputExchange.StartConsuming(joinQ2.handleMessage)

	joinQ2.inputExchange.Close()
	joinQ2.outputQueue.Close()
}

func (joinQ2 *JoinQ2) handleMessage(msg middleware.Message, ack, nack func()) {
	if err := worker.HandleMessageV2(&msg, joinQ2.mssgHandlers); err != nil {
		nack()
		return
	}

	datasaver.Crash(datasaver.CrashAfterLog)
	joinQ2.datasaver.Save(msg, joinQ2) // persistencia de datos
	ack()
}

// processBatch updates in-memory state: keeps max-amount entry per bank.
func (joinQ2 *JoinQ2) processBatch(clientID int64, data []interface{}) error {
	slog.Info("dato recibido", "val", data)

	records, err := inner.DeserializeMaxBankTransactionMessage(data)
	if err != nil {
		slog.Error("Deserializing input message", "err", err)
		return err
	}
	banks, ok := joinQ2.topByClient[clientID]
	if !ok {
		banks = make(map[int]bankEntry)
		joinQ2.topByClient[clientID] = banks
	}
	for _, record := range records {
		prev, exists := banks[record.BankCode]
		if !exists || record.Amount > prev.amount {
			banks[record.BankCode] = bankEntry{amount: record.Amount, account: record.Account}
		}
	}
	return nil
}

// ---------------------------- EOR ----------------------------

// handleEOF increments the EOF counter for the client. Once all counter_q2 instances
// have sent their EOF, flushes the accumulated result to the output queue.
func (joinQ2 *JoinQ2) handleEOF(clientID int64, data []interface{}) error {
	datasaver.Crash(datasaver.CrashBeforeEOF)

	_, sender, err := inner.DeserializeEOR(data) // no hace falta el bool dado que se utiliza otro canal para propagar
	if err != nil {
		slog.Error("While deserializing EOR msg", "err", err, "clientID", clientID)
		return err
	}

	if joinQ2.eofCountByClient[clientID] == nil {
		joinQ2.eofCountByClient[clientID] = make([]string, 0)
	}
	if slices.Contains(joinQ2.eofCountByClient[clientID], sender) {
		return nil
	}
	joinQ2.eofCountByClient[clientID] = append(joinQ2.eofCountByClient[clientID], sender)

	slog.Info("EOF received", "client_id", clientID, "count", len(joinQ2.eofCountByClient[clientID]), "total", joinQ2.config.CounterAmount)

	if len(joinQ2.eofCountByClient[clientID]) >= joinQ2.config.CounterAmount {
		return joinQ2.flushClient(clientID)
	}
	return nil
}

// flushClient pops the accumulated state for clientID, builds a deterministically
// sorted slice of all rows, persists the flush cursor, and then sends chunks to
// the output queue starting from the last committed offset.
//
// If the worker crashes mid-flush the persisted flushState lets the next
// startup resume from exactly the right chunk (via resumePendingFlushes).
func (joinQ2 *JoinQ2) flushClient(clientID int64) error {

	banks := joinQ2.topByClient[clientID]
	delete(joinQ2.topByClient, clientID)
	delete(joinQ2.eofCountByClient, clientID)

	// Build the full, deterministically-ordered row slice once.
	// Sorting here (rather than per-chunk) guarantees that chunk boundaries
	// are stable across restarts, which is required for offset-based resume.
	rows := make([]transaction.MaxBankTransaction, 0, len(banks))
	for bankCode, entry := range banks {
		rows = append(rows, transaction.MaxBankTransaction{
			BankCode: bankCode,
			Account:  entry.account,
			Amount:   entry.amount,
		})
	}
	batch_utils.SortBatch(rows, func(a, b transaction.MaxBankTransaction) bool {
		return a.Amount > b.Amount
	})

	// Register the flush intent before sending anything.  If we crash before
	// any chunk is sent nextChunk=0 means "start from the beginning", which
	// is exactly right.
	fs := &flushState{rows: rows, nextChunk: 0}
	// Persist the initial flushState so the DataSaver knows about it.
	joinQ2.flushHandler.SaveFlushState(clientID, fs)

	if err := joinQ2.flushHandler.SendChunksFrom(clientID, fs, joinQ2.config.BatchSize, &joinQ2.outputQueue); err != nil {
		return err
	}

	// All data chunks sent – remove the pending marker before sending EOF so
	// that a crash between the two still results in a correct resume (the
	// next restart will see no pendingFlush entry for this client and only
	// need to re-send the EOF, which is idempotent on the receiver side).
	joinQ2.flushHandler.Delete(clientID)
	return joinQ2.sendEOF(clientID)
}

// sendEOF sends an EOF marker to the output queue.
func (joinQ2 *JoinQ2) sendEOF(clientID int64) error {
	msg, err := inner.SerializeQueryEOR(clientID, transaction.Query2, fmt.Sprintf("%d", joinQ2.config.ID))
	if err != nil {
		return fmt.Errorf("serializing EOF: %w", err)
	}
	if err := joinQ2.outputQueue.Send(*msg); err != nil {
		return fmt.Errorf("sending EOF: %w", err)
	}
	slog.Info("EOF sent to output queue", "client_id", clientID)
	return nil
}
