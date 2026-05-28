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
	"tp_distribuidos/common/messageprotocol/inner/control"
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
	ControlExchangeName      string
	ControlTopic             string
	Q3AmountFilterAmount     int
}

type TransactionsSaver struct {
	inputQueue           middleware.Middleware
	outputQueue          middleware.Middleware
	notificationExchange middleware.Middleware
	controlExchange      middleware.Middleware
	config               TransactionsSaverConfig
	clientStates         map[int64]*ClientState
	mu                   sync.Mutex // protege clientStates
}

type ClientState struct {
	mu               sync.Mutex // protege estado del cliente y el flujo a seguir
	notificationEOFs int
	receivedFlowEOF  bool
	qtyTx            int
	flushed          bool
	flushing         bool
	storage          *ClientStorage
}

type ClientStorage struct {
	mu       sync.Mutex // protege archivo
	filePath string
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
	controlExchange, err := middleware.CreateExchangeMiddleware(config.ControlExchangeName, []string{config.ControlTopic}, connSettings)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		return nil, err
	}
	notificationExchange, err := middleware.CreateExchangeMiddleware(config.NotificationExchangeName, []string{config.NotificationTopic}, connSettings)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		controlExchange.Close()
		return nil, err
	}
	return &TransactionsSaver{
		inputQueue:           inputQueue,
		outputQueue:          outputQueue,
		notificationExchange: notificationExchange,
		controlExchange:      controlExchange,
		config:               config,
		clientStates:         make(map[int64]*ClientState),
	}, nil
}

func (transactionsSaver *TransactionsSaver) Run() {
	go transactionsSaver.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		transactionsSaver.handleMessage(&msg, ack, nack)
	})

	go transactionsSaver.notificationExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		transactionsSaver.handleNotificationMessage(&msg, ack, nack)
	})

	transactionsSaver.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		transactionsSaver.handleControlMessage(&msg, ack, nack)
	})
}

func (transactionsSaver *TransactionsSaver) handleMessage(middlewareMsg *middleware.Message, ack, nack func()) {
	msg, err := inner.DeserializeMessage(middlewareMsg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", msg.ClientID)
		nack()
		return
	}

	slog.Info("Mensaje recibido", "data", msg.Data)
	switch msg.MsgType {
	case inner.EndOfRecords:
		if err := transactionsSaver.handleEndOfRecordMessage(msg.ClientID); err != nil {
			slog.Error("While handling end of record message", "err", err, "clientID", msg.ClientID)
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
		if err := transactionsSaver.handleDataMessage(transactions, msg.ClientID); err != nil {
			slog.Error("While handling data message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
		ack()
	default:
		slog.Warn("No function could handle this mesage", "err", err, "clientID", msg.ClientID)
	}
}
func (transactionsSaver *TransactionsSaver) handleEndOfRecordMessage(clientID int64) error {
	controlEOFMessage := control.ControlMessage{Type: control.TypeEOF, ClientID: clientID}
	message, err := control.SerializeControlMessage(controlEOFMessage)
	if err != nil {
		slog.Debug("While serializing control message", "err", err, "clientID", clientID)
		return err
	}
	if err := transactionsSaver.controlExchange.Send(*message); err != nil {
		slog.Debug("While sending control message", "err", err, "clientID", clientID)
		return err
	}
	return nil
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
	clientState := transactionsSaver.getOrCreateClientState(clientID)
	transactionsSaver.mu.Unlock()

	clientState.mu.Lock()
	clientState.notificationEOFs++
	if clientState.notificationEOFs != transactionsSaver.config.Q3AmountFilterAmount {
		clientState.mu.Unlock()
		ack()
		return
	}
	clientState.flushing = true
	storage := clientState.storage
	clientState.mu.Unlock()

	if err := transactionsSaver.flushClientTransactions(clientID, storage); err != nil {
		slog.Error("While flushing transactions for client", "err", err, "clientID", clientID)
		nack()
		return
	}

	clientState.mu.Lock()
	clientState.flushing = false
	clientState.flushed = true
	slog.Info("Received final EOF from notification, transactions flushed, try send EOF to output", "clientID", clientID)
	if err := transactionsSaver.tryFinalize(clientID, clientState); err != nil {
		slog.Error("While finalizing client after notification EOF", "err", err, "clientID", clientID)
		clientState.mu.Unlock()
		nack()
		return
	}
	shouldCleanup := clientState.receivedFlowEOF && clientState.flushed
	clientState.mu.Unlock()
	if shouldCleanup {
		transactionsSaver.cleanupClientState(clientID)
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
	clientState := transactionsSaver.getOrCreateClientState(clientID)
	transactionsSaver.mu.Unlock()

	clientState.mu.Lock()
	clientState.qtyTx += len(transactionRecords)
	if clientState.flushed || clientState.flushing {
		clientState.mu.Unlock()
		return transactionsSaver.sendToOutput(clientID, transactions)
	}
	storage := clientState.storage
	err := transactionsSaver.storeTransactions(transactions, storage)
	clientState.mu.Unlock()
	return err
}

func (transactionsSaver *TransactionsSaver) handleControlMessage(msg *middleware.Message, ack func(), nack func()) {
	controlMessage, err := control.DeserializeControlMessage(msg)
	if err != nil {
		slog.Error("While deserializing control message", "err", err)
		nack()
		return
	}
	transactionsSaver.mu.Lock()

	state := transactionsSaver.getOrCreateClientState(controlMessage.ClientID)
	state.mu.Lock()
	state.receivedFlowEOF = true
	slog.Info("Received EOF from instance, try send EOF to output", "clientID", controlMessage.ClientID)
	shouldCleanup := state.receivedFlowEOF && state.flushed
	if err = transactionsSaver.tryFinalize(controlMessage.ClientID, state); err != nil {
		state.mu.Unlock()
		transactionsSaver.mu.Unlock()
		nack()
		return
	}
	state.mu.Unlock()
	transactionsSaver.mu.Unlock()
	if shouldCleanup {
		transactionsSaver.cleanupClientState(controlMessage.ClientID)
	}
	ack()
}

func (transactionsSaver *TransactionsSaver) storeTransactions(transactions []transaction.ThresholdFilteredTransfer, storage *ClientStorage) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	file, err := os.OpenFile(storage.filePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0664)
	if err != nil {
		return fmt.Errorf("opening transaction file on disk: %w", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	jsonData, err := json.Marshal(transactions)
	if err != nil {
		return fmt.Errorf("marshaling transaction batch to json: %w", err)
	}
	if _, err := writer.Write(append(jsonData, '\n')); err != nil {
		return fmt.Errorf("writing transaction batch to disk: %w", err)
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flushing transaction batch buffer: %w", err)
	}
	return nil
}

func (transactionsSaver *TransactionsSaver) flushClientTransactions(clientID int64, storage *ClientStorage) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	if storage.filePath == "" {
		slog.Info("No transactions to flush from disk", "clientID", clientID)
		return nil
	}

	slog.Info("Flushing transactions from disk to output", "clientID", clientID, "filePath", storage.filePath)
	if err := transactionsSaver.readAndSendFromFile(clientID, storage.filePath); err != nil {
		return err
	}

	if err := os.Remove(storage.filePath); err != nil && !os.IsNotExist(err) {
		slog.Error("Failed to delete temporary file from disk", "path", storage.filePath, "err", err)
	} else if !os.IsNotExist(err) {
		slog.Debug("Temporary client file deleted from disk successfully", "clientID", clientID)
	}
	return nil
}

func (transactionsSaver *TransactionsSaver) readAndSendFromFile(clientID int64, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("opening file for flushing: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	// Ojo si el batch es muy grande, scanner puede fallar en ese caso.

	for scanner.Scan() {
		var txs []transaction.ThresholdFilteredTransfer
		if err := json.Unmarshal(scanner.Bytes(), &txs); err != nil {
			return fmt.Errorf("unmarshaling transaction batch from disk stream: %w", err)
		}
		if len(txs) == 0 {
			continue
		}
		if err := transactionsSaver.sendToOutput(clientID, txs); err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading transactions file stream: %w", err)
	}
	return nil
}

func (transactionsSaver *TransactionsSaver) tryFinalize(clientID int64, state *ClientState) error {
	if !state.receivedFlowEOF || !state.flushed {
		slog.Info("Too early to send EOF", "clientID", clientID, "receivedFlowEOF", state.receivedFlowEOF, "flushed", state.flushed)
		return nil
	}
	if err := transactionsSaver.sendToOutput(clientID, []transaction.ThresholdFilteredTransfer{}); err != nil {
		return err
	}
	slog.Info("Client processing completed - EOF sent", "clientID", clientID)
	slog.Info("Size transactions sent", "qty", state.qtyTx)
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
	filePath := filepath.Join(transactionsSaver.config.StorageDir,
		fmt.Sprintf("client_%d_instance_%d.jsonl", clientID, transactionsSaver.config.Id))
	state := &ClientState{
		mu:               sync.Mutex{},
		notificationEOFs: 0,
		receivedFlowEOF:  false,
		qtyTx:            0,
		flushed:          false,
		flushing:         false,
		storage: &ClientStorage{
			filePath: filePath,
		},
	}
	transactionsSaver.clientStates[clientID] = state
	return state
}

func (transactionsSaver *TransactionsSaver) cleanupClientState(clientID int64) {
	transactionsSaver.mu.Lock()
	defer transactionsSaver.mu.Unlock()

	state, exists := transactionsSaver.clientStates[clientID]
	if !exists {
		return
	}

	if state.storage.filePath != "" {
		if err := os.Remove(state.storage.filePath); err != nil && !os.IsNotExist(err) {
			slog.Error("Failed to delete temporary file from disk", "path", state.storage.filePath, "err", err)
		} else if !os.IsNotExist(err) {
			slog.Debug("Temporary client file deleted from disk successfully", "clientID", clientID)
		}
	}
	delete(transactionsSaver.clientStates, clientID)
	slog.Info("Cleanup client", "clientID", clientID)
}
