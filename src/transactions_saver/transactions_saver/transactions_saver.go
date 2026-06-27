package transactions_saver

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"tp_distribuidos/common/batch_utils"
	"tp_distribuidos/common/datasaver"
	"tp_distribuidos/common/heatbeat"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
	"tp_distribuidos/common/worker"
)

const LogsUntilCheckpoint = 1 // Se reciben pocos EOFs y NotificationAVG

type TransactionsSaverConfig struct {
	Id                       int
	WorkerID                 string
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

type CheckpointData struct {
	ClientStates    map[int64]*ClientState            `json:"clientStates"`
	EofCounter      map[int64]batch_utils.Set[string] `json:"eofCounter"`
	FinishedClients batch_utils.Set[int64]            `json:"finishedClients"`
}

type TransactionsSaver struct {
	inputQueue           middleware.Middleware
	outputQueue          middleware.Middleware
	notificationExchange middleware.Middleware
	controlExchange      middleware.Middleware
	config               TransactionsSaverConfig
	clientStates         map[int64]*ClientState // Ahora mapea al nuevo tipo State
	eofCounter           map[int64]batch_utils.Set[string]
	finishedClients      batch_utils.Set[int64]
	mu                   sync.Mutex // Protege clientStates y eofCounter
	muDataSaver          sync.Mutex // Protege el acceso a dataSaver
	dataSaver            *datasaver.DataSaver
	heartbeat            *heatbeat.HeartbeatSender
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

	dataSaver, err := datasaver.NewDataSaver(fmt.Sprintf("/persistence/transactions_saver_%d", config.Id), LogsUntilCheckpoint)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		controlExchange.Close()
		notificationExchange.Close()
		return nil, err
	}

	hb, err := heatbeat.NewHeartbeatSender(config.WorkerID, connSettings)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		controlExchange.Close()
		notificationExchange.Close()
		return nil, fmt.Errorf("creating heartbeat sender: %w", err)
	}

	return &TransactionsSaver{
		inputQueue:           inputQueue,
		outputQueue:          outputQueue,
		notificationExchange: notificationExchange,
		controlExchange:      controlExchange,
		config:               config,
		clientStates:         make(map[int64]*ClientState),
		eofCounter:           make(map[int64]batch_utils.Set[string]),
		finishedClients:      make(batch_utils.Set[int64]),
		dataSaver:            dataSaver,
		heartbeat:            hb,
	}, nil
}

func (transactionsSaver *TransactionsSaver) GetCheckpointData() any {
	slog.Debug("State saved",
		"clientStates", transactionsSaver.clientStates,
		"eofCounter", transactionsSaver.eofCounter,
		"finishedClients", transactionsSaver.finishedClients,
	)
	return CheckpointData{
		ClientStates:    transactionsSaver.clientStates,
		EofCounter:      transactionsSaver.eofCounter,
		FinishedClients: transactionsSaver.finishedClients,
	}
}

func (transactionsSaver *TransactionsSaver) handleSigterm() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	<-sigCh
	slog.Info("SIGTERM received, stopping consumers")
	transactionsSaver.heartbeat.Stop()
	transactionsSaver.inputQueue.StopConsuming()
	transactionsSaver.notificationExchange.StopConsuming()
	transactionsSaver.controlExchange.StopConsuming()
}

func (transactionsSaver *TransactionsSaver) Run() {
	go transactionsSaver.handleSigterm()
	transactionsSaver.heartbeat.Start()
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
	transactionsSaver.mu.Lock()
	defer transactionsSaver.mu.Unlock()
	err := worker.HandleMessageV2(
		middlewareMsg,
		worker.MessageHandlerMap{
			inner.EndOfRecords:     transactionsSaver.handleEndOfRecordMessageWrapper,
			inner.TransactionBatch: transactionsSaver.handleDataMessageWrapper,
		})
	if err != nil {
		nack()
		return
	}
	ack()
}

func (transactionsSaver *TransactionsSaver) handleDataMessageWrapper(clientID int64, data []interface{}) error {
	transactions, err := inner.DeserializeTransactionBatch(data)
	var processErr error
	if err == nil {
		processErr = transactionsSaver.handleDataMessage(transactions, clientID)
	} else {
		processErr = err
	}
	return processErr
}

func (transactionsSaver *TransactionsSaver) handleEndOfRecordMessageWrapper(clientID int64, data []interface{}) error {
	_, sender, err := inner.DeserializeEOR(data)
	var processErr error

	if err == nil {
		processErr = transactionsSaver.handleEndOfRecordMessage(clientID, sender)
	} else {
		processErr = err
	}
	return processErr
}

func (transactionsSaver *TransactionsSaver) handleEndOfRecordMessage(clientID int64, sender string) error {
	slog.Info("Received End Of Records message", "clientID", clientID)
	msg, err := inner.SerializeEOR(clientID, false, sender)
	if err != nil {
		slog.Error("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}
	if err := transactionsSaver.controlExchange.Send(*msg); err != nil {
		slog.Error("While sending EOF message to other instances", "err", err, "clientID", clientID)
		return err
	}
	return nil
}

func (transactionsSaver *TransactionsSaver) handleNotificationMessage(middlewareMsg *middleware.Message, ack, nack func()) {
	transactionsSaver.mu.Lock()
	err := worker.HandleMessageV2(
		middlewareMsg,
		worker.MessageHandlerMap{
			inner.NotificationAverage: transactionsSaver.handleNotificationMessageWrapper,
		})
	if err != nil {
		nack()
		transactionsSaver.mu.Unlock()
		return
	}
	transactionsSaver.mu.Unlock()
	transactionsSaver.muDataSaver.Lock()
	transactionsSaver.dataSaver.Save(middlewareMsg, transactionsSaver)
	transactionsSaver.muDataSaver.Unlock()
	ack()
}

func (transactionsSaver *TransactionsSaver) handleNotificationMessageWrapper(clientID int64, data []interface{}) error {
	_, sender, err := inner.DeserializeEOR(data)
	if err != nil {
		slog.Error("While deserializing EOR msg", "err", err, "clientID", clientID)
		return err
	}

	slog.Debug("Received notification message", "clientID", clientID)
	clientState := transactionsSaver.getOrCreateClientState(clientID)
	if clientState == nil {
		// Clienta ya finalizado
		return nil
	}
	if !clientState.ShouldStartFlush(transactionsSaver.config.Q3AmountFilterAmount, sender) {
		slog.Info("Dont flush disk because still waiting for more averages notification",
			"clientID", clientID,
			"receivedNotifications", clientState.NotificationEOFs,
			"expectedNotifications", transactionsSaver.config.Q3AmountFilterAmount,
		)
		return nil
	}

	if err = clientState.Storage.FlushTransactions(clientID, transactionsSaver.sendToOutput); err != nil {
		slog.Error("While flushing transactions for client", "err", err, "clientID", clientID)
		return err
	}
	if clientState.MarkFlushAndCheckFinish() {
		if err = transactionsSaver.finishClient(clientID); err != nil {
			return err
		}
	} else {
		slog.Info("Flushed disk, but cannot send client EOF because it hasnt been received yet")
	}
	return nil
}

func (transactionsSaver *TransactionsSaver) handleControlMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	transactionsSaver.mu.Lock()
	err := worker.HandleMessageV2(
		middlewareMsg,
		worker.MessageHandlerMap{
			inner.EndOfRecords: transactionsSaver.handleControlMessageWrapper,
		})
	if err != nil {
		nack()
		transactionsSaver.mu.Unlock()
		return
	}
	transactionsSaver.mu.Unlock()
	transactionsSaver.muDataSaver.Lock()
	transactionsSaver.dataSaver.Save(middlewareMsg, transactionsSaver)
	transactionsSaver.muDataSaver.Unlock()
	ack()
}

func (transactionsSaver *TransactionsSaver) handleControlMessageWrapper(clientID int64, data []interface{}) error {
	_, sender, err := inner.DeserializeEOR(data)
	if err != nil {
		slog.Error("While deserializing control message", "err", err, "clientID", clientID)
		return err
	}

	clientState := transactionsSaver.getOrCreateClientState(clientID)
	if clientState == nil {
		// Clienta ya finalizado
		return nil
	}
	transactionsSaver.eofCounter[clientID].Add(sender)
	if transactionsSaver.eofCounter[clientID].Size() != transactionsSaver.config.DateFilterAmount {
		slog.Debug("Dont send EOF because still waiting for more EOFs",
			"clientID", clientID,
			"receivedEOFCount", transactionsSaver.eofCounter[clientID],
			"expectedEOFCount", transactionsSaver.config.DateFilterAmount)
		return nil
	}

	if clientState.MarkEOFAndCheckFinish() {
		if err = transactionsSaver.finishClient(clientID); err != nil {
			return err
		}
	} else {
		slog.Info("Received client EOF, but cannot send because disk has not been flushed yet")
	}
	return nil
}

func (transactionsSaver *TransactionsSaver) handleDataMessage(transactionRecords []transaction.Transaction, clientID int64) error {
	transactions := make([]transaction.ThresholdFilteredTransfer, 0, len(transactionRecords))
	for _, tx := range transactionRecords {
		transactions = append(transactions, transaction.ThresholdFilteredTransfer{
			FromBank:      tx.FromBank,
			FromAccount:   tx.FromAccount,
			PaymentFormat: tx.PaymentFormat,
			Amount:        tx.Amount,
			Timestamp:     tx.Timestamp.UnixNano(),
		})
	}
	batch_utils.SortBatch(transactions, func(a, b transaction.ThresholdFilteredTransfer) bool {
		if a.Timestamp != b.Timestamp {
			return a.Timestamp < b.Timestamp
		}
		return a.Amount > b.Amount
	})
	clientState := transactionsSaver.getOrCreateClientState(clientID)
	if clientState == nil {
		// Clienta ya finalizado
		return nil
	}
	if clientState.ShouldBuffData() {
		return clientState.Storage.StoreTransactions(transactions)
	}
	return transactionsSaver.sendToOutput(clientID, transactions)
}

func (transactionsSaver *TransactionsSaver) sentEOF(clientID int64) error {
	msgToSend, err := inner.SerializeEOR(clientID, false, fmt.Sprintf("%d", transactionsSaver.config.Id))
	if err != nil {
		slog.Error("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}
	if err := transactionsSaver.outputQueue.Send(*msgToSend); err != nil {
		slog.Error("While sending EOF message to q3 amount filter", "err", err, "clientID", clientID)
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

	if _, isDone := transactionsSaver.finishedClients[clientID]; isDone {
		slog.Info("Client has already done", "clientID", clientID)
		return nil
	}
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
	transactionsSaver.finishedClients.Add(clientID)
	slog.Info("Cleanup client completed", "clientID", clientID)
	return nil
}

func (transactionsSaver *TransactionsSaver) finishClient(clientID int64) error {
	if err := transactionsSaver.sentEOF(clientID); err != nil {
		return err
	}
	return transactionsSaver.cleanupClientState(clientID)
}

func (transactionsSaver *TransactionsSaver) Restaurate() error {
	var checkpoint CheckpointData
	thereIsCheckpoint, err := transactionsSaver.dataSaver.GetRestaurationCheckpoint(&checkpoint)
	if err != nil {
		return err
	}

	if thereIsCheckpoint {
		slog.Info("Cargando TransactionsSaver en base a checkpoint")

		transactionsSaver.eofCounter = checkpoint.EofCounter
		transactionsSaver.clientStates = checkpoint.ClientStates
		transactionsSaver.finishedClients = checkpoint.FinishedClients

		for _, state := range transactionsSaver.clientStates {
			state.Storage = NewStorage(state.StorageFilePath, state.StorageFileName)
		}
		slog.Debug("State restaured", "clientStates", checkpoint.ClientStates, "eof", checkpoint.EofCounter, "finishedClients", checkpoint.FinishedClients)
	}
	var savedDataVar middleware.Message
	var thereIsLogs bool
	for {
		thereIsLogs, err = transactionsSaver.dataSaver.GetDataFromLogs(&savedDataVar)
		if err != nil {
			return err
		}
		if !thereIsLogs {
			break
		}
		err = worker.HandleMessageV2(
			&savedDataVar,
			worker.MessageHandlerMap{
				inner.EndOfRecords:        transactionsSaver.handleControlMessageWrapper,
				inner.NotificationAverage: transactionsSaver.handleNotificationMessageWrapper,
			},
		)
		if err != nil {
			return err
		}
	}
	return nil
}
