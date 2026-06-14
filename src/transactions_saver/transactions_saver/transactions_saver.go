package transactions_saver

import (
	"fmt"
	"log/slog"
	"os"
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
	DateFilterAmount         int
}

type TransactionsSaver struct {
	inputQueue           middleware.Middleware
	outputQueue          middleware.Middleware
	notificationExchange middleware.Middleware
	controlExchange      middleware.Middleware
	config               TransactionsSaverConfig
	clientStates         map[int64]*ClientState // Ahora mapea al nuevo tipo State
	eofCounter           map[int64]int
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
		eofCounter:           make(map[int64]int),
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
		slog.Error("While deserializing message", "err", err, "middlewareMsg", middlewareMsg)
		nack()
		return
	}

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
		slog.Info("Only received average notification message, not average data", "clientID", clientID)
		ack()
		return
	}

	slog.Info("Received averages notification message", "clientID", clientID)
	clientState := transactionsSaver.getOrCreateClientState(clientID)
	if !clientState.ShouldStartFlush(transactionsSaver.config.Q3AmountFilterAmount) {
		slog.Info("Dont flush disk because still waiting for more averages notification",
			"clientID", clientID,
			"receivedNotifications", clientState.notificationEOFs,
			"expectedNotifications", transactionsSaver.config.Q3AmountFilterAmount,
		)
		ack()
		return
	}

	if err = clientState.Storage.FlushTransactions(clientID, transactionsSaver.sendToOutput); err != nil {
		slog.Error("While flushing transactions for client", "err", err, "clientID", clientID)
		nack()
		return
	}
	if clientState.MarkFlushAndCheckFinish() {
		if err = transactionsSaver.finishClient(clientID); err != nil {
			nack() // Manejar bien este caso...
			return
		}
	} else {
		slog.Info("Flushed disk, but cannot send client EOF because it hasnt been received yet")
	}
	ack()
}

func (transactionsSaver *TransactionsSaver) handleControlMessage(msg *middleware.Message, ack func(), nack func()) {
	controlMessage, err := control.DeserializeControlMessage(msg)
	if err != nil {
		slog.Error("While deserializing control message", "err", err)
		nack()
		return
	}
	clientID := controlMessage.ClientID
	slog.Info("Received EOF message", "clientID", clientID)

	transactionsSaver.mu.Lock()
	transactionsSaver.eofCounter[clientID]++
	if transactionsSaver.eofCounter[clientID] != transactionsSaver.config.DateFilterAmount {
		slog.Info("Dont send EOF because still waiting for more EOFs",
			"clientID", clientID,
			"receivedEOFCount", transactionsSaver.eofCounter[clientID],
			"expectedEOFCount", transactionsSaver.config.DateFilterAmount)
		transactionsSaver.mu.Unlock()
		ack()
		return
	}
	transactionsSaver.mu.Unlock()

	clientState := transactionsSaver.getOrCreateClientState(clientID)
	if clientState.MarkEOFAndCheckFinish() {
		if err = transactionsSaver.finishClient(clientID); err != nil {
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
		})
	}

	clientState := transactionsSaver.getOrCreateClientState(clientID)
	if clientState.ShouldBuffData(len(transactionRecords)) {
		return clientState.Storage.StoreTransactions(transactions)
	}
	return transactionsSaver.sendToOutput(clientID, transactions)
}

func (transactionsSaver *TransactionsSaver) sentEOF(clientID int64) error {
	if err := transactionsSaver.sendToOutput(clientID, []transaction.ThresholdFilteredTransfer{}); err != nil {
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
	transactionsSaver.eofCounter[clientID] = 0
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
