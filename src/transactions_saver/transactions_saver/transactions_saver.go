package transactions_saver

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
	"tp_distribuidos/common/batch_utils"
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
	ControlExchangeName      string
	ControlTopic             string
	Q3AmountFilterAmount     int
	DateFilterAmount         int
}

type TransactionsSaver struct {
	inputQueue           middleware.Middleware
	outputQueue          middleware.Middleware
	notificationExchange middleware.Middleware
	controlExchange      middleware.Middleware
	config               TransactionsSaverConfig
	clientStates         map[int64]*ClientState // Ahora mapea al nuevo tipo State
	eofCounter           map[int64]batch_utils.Set[string]
	mu                   sync.Mutex // Protege clientStates y eofCounter
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
	myKeyControl := fmt.Sprintf("%s_%d", config.ControlExchangeName, config.Id)
	controlExchange, err := middleware.CreateExchangeMiddleware(config.ControlExchangeName, []string{config.ControlTopic}, connSettings, myKeyControl)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		return nil, err
	}
	myKeyNotification := fmt.Sprintf("%s_%d", config.NotificationExchangeName, config.Id)
	notificationExchange, err := middleware.CreateExchangeMiddleware(config.NotificationExchangeName, []string{config.NotificationTopic}, connSettings, myKeyNotification)
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
		eofCounter:           make(map[int64]batch_utils.Set[string]),
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
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}

	var processErr error
	switch msg.MsgType {
	case inner.EndOfRecords:
		_, sender, err := inner.DeserializeEOR(msg.Data)
		if err == nil {
			processErr = transactionsSaver.handleEndOfRecordMessage(msg.ClientID, sender)
		} else {
			processErr = err
		}
	case inner.TransactionBatch:
		transactions, err := inner.DeserializeTransactionBatch(msg.Data)
		if err == nil {
			processErr = transactionsSaver.handleDataMessage(transactions, msg.ClientID)
		} else {
			processErr = err
		}
	default:
		processErr = fmt.Errorf("unexpected msg type received")
	}

	if processErr != nil {
		slog.Error("Failed processing message", "err", processErr, "clientID", msg.ClientID)
		nack()
		return
	}
	ack()
}

func (transactionsSaver *TransactionsSaver) handleEndOfRecordMessage(clientID int64, sender string) error {
	slog.Info("Received End Of Records message", "clientID", clientID)
	msg, err := inner.SerializeEOR(clientID, false, sender)
	if err != nil {
		slog.Info("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}
	if err := transactionsSaver.controlExchange.Send(*msg); err != nil {
		slog.Info("While sending EOF message to other instances", "err", err, "clientID", clientID)
		return err
	}
	return nil
}

func (transactionsSaver *TransactionsSaver) handleNotificationMessage(middlewareMsg *middleware.Message, ack, nack func()) {
	msg, err := inner.DeserializeMessage(middlewareMsg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}
	_, sender, err := inner.DeserializeEOR(msg.Data)
	if err != nil {
		slog.Error("While deserializing EOR msg", "err", err, "clientID", msg.ClientID)
		nack()
		return
	}

	slog.Info("Received notification message", "clientID", msg.ClientID)
	clientState := transactionsSaver.getOrCreateClientState(msg.ClientID)
	if !clientState.ShouldStartFlush(transactionsSaver.config.Q3AmountFilterAmount, sender) {
		slog.Info("Dont flush disk because still waiting for more averages notification",
			"clientID", msg.ClientID,
			"receivedNotifications", clientState.notificationEOFs,
			"expectedNotifications", transactionsSaver.config.Q3AmountFilterAmount,
		)
		ack()
		return
	}

	if err = clientState.Storage.FlushTransactions(msg.ClientID, transactionsSaver.sendToOutput); err != nil {
		slog.Error("While flushing transactions for client", "err", err, "clientID", msg.ClientID)
		nack()
		return
	}
	if clientState.MarkFlushAndCheckFinish() {
		if err = transactionsSaver.finishClient(msg.ClientID); err != nil {
			nack() // Manejar bien este caso...
			return
		}
	} else {
		slog.Info("Flushed disk, but cannot send client EOF because it hasnt been received yet")
	}
	ack()
}

func (transactionsSaver *TransactionsSaver) handleControlMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	msg, err := inner.DeserializeMessage(middlewareMsg)
	if err != nil {
		slog.Error("While deserializing control message", "err", err)
		nack()
		return
	}
	_, sender, err := inner.DeserializeEOR(msg.Data)
	if err != nil {
		slog.Error("While deserializing control message", "err", err, "clientID", msg.ClientID)
		nack()
		return
	}
	clientState := transactionsSaver.getOrCreateClientState(msg.ClientID)

	transactionsSaver.mu.Lock()
	transactionsSaver.eofCounter[msg.ClientID].Add(sender)
	if transactionsSaver.eofCounter[msg.ClientID].Size() != transactionsSaver.config.DateFilterAmount {
		slog.Info("Dont send EOF because still waiting for more EOFs",
			"clientID", msg.ClientID,
			"receivedEOFCount", transactionsSaver.eofCounter[msg.ClientID],
			"expectedEOFCount", transactionsSaver.config.DateFilterAmount)
		transactionsSaver.mu.Unlock()
		ack()
		return
	}
	transactionsSaver.mu.Unlock()

	if clientState.MarkEOFAndCheckFinish() {
		if err = transactionsSaver.finishClient(msg.ClientID); err != nil {
			nack() // Manejar bien este caso...
			return
		}
	} else {
		slog.Info("Received client EOF, but cannot send because disk has not been flushed yet")
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
			Timestamp:     tx.Timestamp,
		})
	}

	clientState := transactionsSaver.getOrCreateClientState(clientID)
	if clientState.ShouldBuffData(len(transactionRecords)) {
		return clientState.Storage.StoreTransactions(transactions)
	}
	return transactionsSaver.sendToOutput(clientID, transactions)
}

func (transactionsSaver *TransactionsSaver) sentEOF(clientID int64) error {
	msgToSend, err := inner.SerializeEOR(clientID, false, fmt.Sprintf("%d", transactionsSaver.config.Id))
	if err != nil {
		slog.Info("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}
	if err := transactionsSaver.outputQueue.Send(*msgToSend); err != nil {
		slog.Info("While sending EOF message to q3 amount filter", "err", err, "clientID", clientID)
		return err
	}
	slog.Info("Client processing completed - EOF sent", "clientID", clientID)
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
	transactionsSaver.mu.Lock()
	defer transactionsSaver.mu.Unlock()

	if state, exists := transactionsSaver.clientStates[clientID]; exists {
		return state
	}
	slog.Info("Client new arrived", "clientID", clientID)
	transactionsSaver.eofCounter[clientID] = batch_utils.NewSet[string]()
	fileName := fmt.Sprintf("client_%d_instance_%d.jsonl", clientID, transactionsSaver.config.Id)
	clientState := NewClientState(transactionsSaver.config.StorageDir, fileName)
	transactionsSaver.clientStates[clientID] = clientState
	return clientState
}

func (transactionsSaver *TransactionsSaver) cleanupClientState(clientID int64) error {
	transactionsSaver.mu.Lock()
	defer transactionsSaver.mu.Unlock()

	clientState, exists := transactionsSaver.clientStates[clientID]
	if !exists {
		return nil
	}
	err := clientState.Storage.RemoveFile()
	if err != nil {
		return err
	}
	delete(transactionsSaver.clientStates, clientID)
	delete(transactionsSaver.eofCounter, clientID)
	slog.Info("Cleanup client completed", "clientID", clientID)
	return nil
}

func (transactionsSaver *TransactionsSaver) finishClient(clientID int64) error {
	if err := transactionsSaver.sentEOF(clientID); err != nil {
		return err
	}
	return transactionsSaver.cleanupClientState(clientID)
}
