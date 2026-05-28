package join_q2

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

type JoinQ2Config struct {
	ID            int
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
	mutex            sync.Mutex
	topByClient      map[int64]map[int]bankEntry // client_id -> bankCode -> bankEntry{amount, account}
	eofCountByClient map[int64]int               // tracks how many counter_q2 EOFs have arrived per client
}

func NewJoinQ2(config JoinQ2Config) (*JoinQ2, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	// Input exchange: this instance only receives messages sharded to its ID
	inputKey := fmt.Sprintf("%s_%d", config.InputPrefix, config.ID)
	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputPrefix, []string{inputKey}, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating input exchange: %w", err)
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, fmt.Errorf("creating output queue: %w", err)
	}

	return &JoinQ2{
		config:           config,
		inputExchange:    inputExchange,
		outputQueue:      outputQueue,
		topByClient:      make(map[int64]map[int]bankEntry),
		eofCountByClient: make(map[int64]int),
	}, nil
}

func (joinQ2 *JoinQ2) Run() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM)
	go func() {
		<-signalChannel
		slog.Info("SIGTERM received, stopping consumer")
		joinQ2.inputExchange.StopConsuming()
	}()

	joinQ2.inputExchange.StartConsuming(joinQ2.handleMessage)

	joinQ2.inputExchange.Close()
	joinQ2.outputQueue.Close()
}

func (joinQ2 *JoinQ2) handleMessage(msg middleware.Message, ack, nack func()) {
	clientID, records, isEof, err := inner.DeserializeMaxBankTransactionMessage(&msg)
	if err != nil {
		slog.Error("Deserializing input message", "err", err)
		nack()
		return
	}

	if isEof {
		if err := joinQ2.handleEOF(clientID); err != nil {
			slog.Error("Handling EOF", "err", err, "client_id", clientID)
			nack()
			return
		}
		ack()
		return
	}

	joinQ2.processBatch(clientID, records)
	ack()
}

// processBatch updates in-memory state: keeps max-amount entry per bank.
func (joinQ2 *JoinQ2) processBatch(clientID int64, records []transaction.MaxBankTransaction) {
	joinQ2.mutex.Lock()
	defer joinQ2.mutex.Unlock()
	banks, ok := joinQ2.topByClient[clientID]
	if !ok {
		banks = make(map[int]bankEntry)
		joinQ2.topByClient[clientID] = banks
	}
	for _, record := range records {
		prev, exists := banks[record.BankCode]
		if !exists || record.Amount > prev.amount {
			banks[record.BankCode] = bankEntry{amount: record.Amount, account: record.Account}
			//slog.Info("New top", "client_id", clientID, "bank", record.BankCode, "amount", record.Amount, "account", record.Account)
		}
	}
}

// handleEOF increments the EOF counter for the client. Once all counter_q2 instances
// have sent their EOF, flushes the accumulated result to the output queue.
func (joinQ2 *JoinQ2) handleEOF(clientID int64) error {
	joinQ2.mutex.Lock()
	joinQ2.eofCountByClient[clientID]++
	count := joinQ2.eofCountByClient[clientID]
	joinQ2.mutex.Unlock()

	slog.Info("EOF received", "client_id", clientID, "count", count, "total", joinQ2.config.CounterAmount)

	if count >= joinQ2.config.CounterAmount {
		return joinQ2.flushClient(clientID)
	}
	return nil
}

// flushClient pops the accumulated state for clientID and sends it to the output queue.
func (joinQ2 *JoinQ2) flushClient(clientID int64) error {
	joinQ2.mutex.Lock()
	banks := joinQ2.topByClient[clientID]
	slog.Info("Flushing client", "client_id", clientID, "banks", len(banks))
	delete(joinQ2.topByClient, clientID)
	delete(joinQ2.eofCountByClient, clientID)
	joinQ2.mutex.Unlock()

	if err := joinQ2.sendData(clientID, banks); err != nil {
		return err
	}
	return joinQ2.sendEOF(clientID)
}

// sendData converts the internal state to MaxBankTransaction records and sends them
// to the output queue wrapped as a QueryResult (Query2).
func (joinQ2 *JoinQ2) sendData(clientID int64, banks map[int]bankEntry) error {
	rows := make([]transaction.MaxBankTransaction, 0, len(banks))
	for bankCode, entry := range banks {
		rows = append(rows, transaction.MaxBankTransaction{
			BankCode: bankCode,
			Account:  entry.account,
			Amount:   entry.amount,
		})
	}

	for i := 0; i < len(rows); i += joinQ2.config.BatchSize {
		end := i + joinQ2.config.BatchSize
		if end > len(rows) {
			end = len(rows)
		}
		chunk := rows[i:end]
		msg, err := inner.SerializeQueryResultMessage(clientID, transaction.QueryResult{
			QueryID:      transaction.Query2,
			Transactions: chunk,
		})
		if err != nil {
			return fmt.Errorf("serializing data chunk: %w", err)
		}
		if err := joinQ2.outputQueue.Send(*msg); err != nil {
			return fmt.Errorf("sending data chunk: %w", err)
		}
		//slog.Info("Sent batch to output queue", "client_id", clientID, "count", len(chunk))
	}
	return nil
}

// sendEOF sends an EOF marker to the output queue.
func (joinQ2 *JoinQ2) sendEOF(clientID int64) error {

	msg, err := inner.SerializeQueryEOR(clientID, transaction.Query2)
	if err != nil {
		return fmt.Errorf("serializing EOF: %w", err)
	}
	if err := joinQ2.outputQueue.Send(*msg); err != nil {
		return fmt.Errorf("sending EOF: %w", err)
	}
	slog.Info("EOF sent to output queue", "client_id", clientID)
	return nil
}
