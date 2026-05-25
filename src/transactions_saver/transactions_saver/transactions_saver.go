package transactions_saver

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

type TransactionsSaverConfig struct {
	Id                       int
	MomHost                  string
	MomPort                  int
	StorageDir               string
	InputQueue               string
	InputExchangeName        string
	InputTopic               string
	OutputQueue              string
	NotificationExchangeName string
	NotificationTopic        string
	PromediatorAmount        int
}

type TransactionsSaver struct {
	inputQueue           middleware.Middleware
	outputQueue          middleware.Middleware
	notificationExchange middleware.Middleware
	config               TransactionsSaverConfig

	clientStates map[int64]*ClientState
	mu           sync.Mutex
}

type ClientState struct {
	filePath         string
	notificationEOFs int
	receivedFlowEOF  bool
	flushed          bool
}

func NewTransactionsSaver(config TransactionsSaverConfig) (*TransactionsSaver, error) {
	if err := os.MkdirAll(config.StorageDir, 0755); err != nil {
		return nil, fmt.Errorf("creating storage directory: %w", err)
	}

	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}
	err = inputQueue.BindToTopics(config.InputExchangeName, config.InputTopic)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}
	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	notificationExchange, err := middleware.CreateExchangeMiddleware(config.NotificationExchangeName, []string{config.NotificationTopic}, connSettings)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		return nil, err
	}
	return &TransactionsSaver{
		inputQueue:           inputQueue,
		outputQueue:          outputQueue,
		notificationExchange: notificationExchange,
		config:               config,
		clientStates:         make(map[int64]*ClientState),
	}, nil
}

func (transactionsSaver *TransactionsSaver) Run() {
	go transactionsSaver.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		transactionsSaver.handleMessage(&msg, ack, nack)
	})

	transactionsSaver.notificationExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		transactionsSaver.handleNotificationMessage(&msg, ack, nack)
	})
}

func (transactionsSaver *TransactionsSaver) handleMessage(msg *middleware.Message, ack, nack func()) {
	clientID, transactionRecords, isEof, err := inner.DeserializeRawTransactionsMessage(msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", clientID)
		nack()
		return
	}

	if isEof {
		if err := transactionsSaver.handleEndOfRecordMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err, "clientID", clientID)
			nack()
			return
		}
		ack()
		return
	}
	if err := transactionsSaver.handleDataMessage(transactionRecords, clientID); err != nil {
		slog.Error("While handling data message", "err", err, "clientID", clientID)
		nack()
		return
	}
	ack()
}
func (transactionsSaver *TransactionsSaver) handleEndOfRecordMessage(clientID int64) error {
	transactionsSaver.mu.Lock()
	defer transactionsSaver.mu.Unlock()

	state := transactionsSaver.getOrCreateClientState(clientID)
	state.receivedFlowEOF = true
	slog.Info("Received EOF from flow, try send EOF to output", "clientID", clientID)
	return transactionsSaver.tryFinalizeLocked(clientID, state)
}

func (transactionsSaver *TransactionsSaver) handleNotificationMessage(msg *middleware.Message, ack, nack func()) {
	clientID, _, isEof, err := inner.DeserializePaymentFormatAverageMessage(msg)
	if err != nil {
		slog.Error("While deserializing notification message", "err", err, "clientID", clientID)
		nack()
		return
	}
	if !isEof {
		ack()
		return
	}

	transactionsSaver.mu.Lock()
	defer transactionsSaver.mu.Unlock()

	state := transactionsSaver.getOrCreateClientState(clientID)
	if state.notificationEOFs < transactionsSaver.config.PromediatorAmount-1 {
		state.notificationEOFs++
		ack()
		return
	}
	if err := transactionsSaver.flushStoredTransactionsLocked(clientID, state); err != nil {
		slog.Error("While flushing transactions for client", "err", err, "clientID", clientID)
		nack()
		return
	}
	slog.Info("Received final EOF from notification, transactions flushed, try send EOF to output", "clientID", clientID)
	if err := transactionsSaver.tryFinalizeLocked(clientID, state); err != nil {
		slog.Error("While finalizing client after notification EOF", "err", err, "clientID", clientID)
		nack()
		return
	}
	ack()
}

func (transactionsSaver *TransactionsSaver) handleDataMessage(transactionRecords []transaction.Transaction, clientID int64) error {
	transactions := make([]transaction.ThresholdFilteredTransfer, 0, len(transactionRecords))
	for _, tx := range transactionRecords {
		transactions = append(transactions, transaction.ThresholdFilteredTransfer{
			FromBank:      tx.FromBank,
			FromAccount:   tx.FromAccount,
			PaymentFormat: tx.PaymentFormat,
			Amount:        tx.Amount,
		})
	}

	transactionsSaver.mu.Lock()
	defer transactionsSaver.mu.Unlock()

	state := transactionsSaver.getOrCreateClientState(clientID)
	if state.flushed {
		return transactionsSaver.sendToOutput(clientID, transactions)
	}
	if state.filePath == "" {
		state.filePath = filepath.Join(transactionsSaver.config.StorageDir,
			fmt.Sprintf("client_%d_instance_%d.jsonl", clientID, transactionsSaver.config.Id))
	}
	return transactionsSaver.storeTransactions(transactions, state.filePath)
}

func (transactionsSaver *TransactionsSaver) storeTransactions(transactions []transaction.ThresholdFilteredTransfer, storageFilePath string) error {
	file, err := os.OpenFile(storageFilePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0664)
	if err != nil {
		return fmt.Errorf("opening transaction file on disk: %w", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	for _, tx := range transactions {
		jsonData, err := json.Marshal(tx)
		if err != nil {
			return fmt.Errorf("marshaling transaction to json: %w", err)
		}
		if _, err := writer.Write(append(jsonData, '\n')); err != nil {
			return fmt.Errorf("writing transaction line to disk: %w", err)
		}
	}
	return nil
}

func (transactionsSaver *TransactionsSaver) flushStoredTransactionsLocked(clientID int64, state *ClientState) error {
	if state.flushed {
		return nil
	}
	if state.filePath == "" {
		state.flushed = true
		slog.Info("No transactions to flush from disk", "clientID", clientID)
		return nil
	}
	slog.Info("Flushing transactions from disk to output", "clientID", clientID, "filePath", state.filePath)
	file, err := os.Open(state.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			state.flushed = true
			return nil
		}
		return fmt.Errorf("opening file for flushing: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	txsToSend := make([]transaction.ThresholdFilteredTransfer, 0, 10)
	const networkBatchSize = 10 // TODO: No tendria que estar hardcodeado
	for scanner.Scan() {
		var tx transaction.ThresholdFilteredTransfer
		if err := json.Unmarshal(scanner.Bytes(), &tx); err != nil {
			return fmt.Errorf("unmarshaling transaction from disk stream: %w", err)
		}
		txsToSend = append(txsToSend, tx)
		if len(txsToSend) == networkBatchSize {
			if err := transactionsSaver.sendToOutput(clientID, txsToSend); err != nil {
				return err
			}
			txsToSend = txsToSend[:0]
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading transactions file stream: %w", err)
	}
	if len(txsToSend) > 0 {
		if err := transactionsSaver.sendToOutput(clientID, txsToSend); err != nil {
			return err
		}
	}

	state.flushed = true
	if err := os.Remove(state.filePath); err != nil && !os.IsNotExist(err) {
		slog.Error("Failed to delete temporary file from disk", "path", state.filePath, "err", err)
	} else if !os.IsNotExist(err) {
		slog.Debug("Temporary client file deleted from disk successfully", "clientID", clientID)
	}
	return nil
}

func (transactionsSaver *TransactionsSaver) tryFinalizeLocked(clientID int64, state *ClientState) error {
	if !state.receivedFlowEOF || !state.flushed {
		slog.Info("Too early to send EOF", "clientID", clientID, "receivedFlowEOF", state.receivedFlowEOF, "flushed", state.flushed)
		return nil
	}
	if err := transactionsSaver.sendToOutput(clientID, []transaction.ThresholdFilteredTransfer{}); err != nil {
		return err
	}

	transactionsSaver.cleanupClientState(clientID, state)
	slog.Info("Client processing completed - EOF sent and state cleaned", "clientID", clientID)
	return nil
}

func (transactionsSaver *TransactionsSaver) sendToOutput(clientID int64, txs []transaction.ThresholdFilteredTransfer) error {
	message, err := inner.SerializeThresholdFilteredTransferMessage(clientID, txs)
	if err != nil {
		slog.Error("While serializing message to output", "err", err, "clientID", clientID, "transactions", txs)
		return err
	}
	if err = transactionsSaver.outputQueue.Send(*message); err != nil {
		slog.Error("While sending message to output", "err", err, "clientID", clientID, "transactions", txs)
		return err
	}
	return nil
}

func (transactionsSaver *TransactionsSaver) getOrCreateClientState(clientID int64) *ClientState {
	if state, exists := transactionsSaver.clientStates[clientID]; exists {
		return state
	}
	state := &ClientState{}
	transactionsSaver.clientStates[clientID] = state
	return state
}

func (transactionsSaver *TransactionsSaver) cleanupClientState(clientID int64, state *ClientState) {
	if state.filePath != "" {
		if err := os.Remove(state.filePath); err != nil && !os.IsNotExist(err) {
			slog.Error("Failed to delete temporary file from disk", "path", state.filePath, "err", err)
		} else if !os.IsNotExist(err) {
			slog.Debug("Temporary client file deleted from disk successfully", "clientID", clientID)
		}
	}
	delete(transactionsSaver.clientStates, clientID)
}
