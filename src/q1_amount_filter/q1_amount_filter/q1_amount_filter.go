package q1_amount_filter

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

const LogsUntilCheckpoint = 1

type Q1AmountFilterConfig struct {
	Id                int
	WorkerID          string
	MomHost           string
	MomPort           int
	InputQueue        string
	InputTopic        string
	InputExchangeName string
	OutputQueue       string
	ControlExchange   string
	ControlTopic      string
	USDFilterAmount   int
}

type Q1AmountFilter struct {
	inputQueue      middleware.Middleware
	outputQueue     middleware.Middleware
	controlExchange middleware.Middleware
	eofCounter      map[int64]batch_utils.Set[string]
	finishedClients batch_utils.Set[int64] // Sirve para no procesar un eof de un cliente que ya termino
	config          Q1AmountFilterConfig
	mu              sync.Mutex
	heartbeat       *heatbeat.HeartbeatSender
	dataSaver       *datasaver.DataSaver
}

type CheckpointData struct {
	EofCounter      map[int64]batch_utils.Set[string] `json:"eofCounter"`
	FinishedClients batch_utils.Set[int64]            `json:"finishedClients"`
}

func NewQ1AmountFilter(config Q1AmountFilterConfig) (*Q1AmountFilter, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}
	if err = inputQueue.BindToTopics(config.InputExchangeName, config.InputTopic); err != nil {
		inputQueue.Close()
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}
	controlQueueName := fmt.Sprintf("%s_%d", config.ControlExchange, config.Id)
	controlExchange, err := middleware.CreateExchangeMiddleware(config.ControlExchange, []string{config.ControlTopic}, connSettings, controlQueueName)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		return nil, err
	}

	hb, err := heatbeat.NewHeartbeatSender(config.WorkerID, connSettings)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		controlExchange.Close()
		return nil, fmt.Errorf("creating heartbeat sender: %w", err)
	}

	dataSaver, err := datasaver.NewDataSaver(fmt.Sprintf("/persistence_%s_%d", "q1_amount_filter", config.Id), LogsUntilCheckpoint)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		controlExchange.Close()
		return nil, err
	}
	return &Q1AmountFilter{
		inputQueue:      inputQueue,
		outputQueue:     outputQueue,
		controlExchange: controlExchange,
		config:          config,
		eofCounter:      make(map[int64]batch_utils.Set[string]),
		finishedClients: make(batch_utils.Set[int64]),
		dataSaver:       dataSaver,
		heartbeat:       hb,
	}, nil
}

func (q1AmountFilter *Q1AmountFilter) handleSigterm() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	<-sigCh
	slog.Info("SIGTERM received, stopping consumers")
	q1AmountFilter.heartbeat.Stop()
	q1AmountFilter.inputQueue.StopConsuming()
	q1AmountFilter.controlExchange.StopConsuming()
}

func (q1AmountFilter *Q1AmountFilter) GetCheckpointData() any {
	slog.Info("State saved",
		"eofCounter", q1AmountFilter.eofCounter,
		"finishedClients", q1AmountFilter.finishedClients)
	return CheckpointData{
		EofCounter:      q1AmountFilter.eofCounter,
		FinishedClients: q1AmountFilter.finishedClients,
	}
}

func (q1AmountFilter *Q1AmountFilter) Run() {
	go q1AmountFilter.handleSigterm()
	q1AmountFilter.heartbeat.Start()

	go q1AmountFilter.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		q1AmountFilter.handleControlMessage(&msg, ack, nack)
	})

	q1AmountFilter.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		q1AmountFilter.handleMessage(&msg, ack, nack)
	})
}

func (q1AmountFilter *Q1AmountFilter) handleMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	q1AmountFilter.mu.Lock()
	defer q1AmountFilter.mu.Unlock()
	err := worker.HandleMessageV2(
		middlewareMsg,
		worker.MessageHandlerMap{
			inner.EndOfRecords:     q1AmountFilter.handleEndOfRecordMessage,
			inner.TransactionBatch: q1AmountFilter.handleTransactionBatchMessage,
		},
	)
	if err != nil {
		nack()
		return
	}
	ack()
}

func (q1AmountFilter *Q1AmountFilter) handleTransactionBatchMessage(clientID int64, data []interface{}) error {
	transactions, err := inner.DeserializeTransactionBatch(data)
	if err != nil {
		slog.Error("While deserializing transactions from message", "err", err, "clientID", clientID)
		return err
	}
	if err = q1AmountFilter.handleDataMessage(transactions, clientID); err != nil {
		slog.Error("While handling data message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}

func (q1AmountFilter *Q1AmountFilter) handleEndOfRecordMessage(clientID int64, data []interface{}) error {
	slog.Info("Arrived EOF record message", "clientID", clientID)
	_, sender, err := inner.DeserializeEOR(data)
	if err != nil {
		slog.Error("While deserializing EOR message", "err", err, "clientID", clientID)
		return err
	}
	msg, err := inner.SerializeEOR(clientID, false, sender)
	if err != nil {
		slog.Info("While serializing EOF control message", "err", err, "clientID", clientID)
		return err
	}
	if err = q1AmountFilter.controlExchange.Send(*msg); err != nil {
		slog.Error("While sending EOF control message to other instances", "err", err, "clientID", clientID)
		return err
	}
	return nil
}

func (q1AmountFilter *Q1AmountFilter) handleDataMessage(transactionRecords []transaction.Transaction, clientID int64) error {
	if _, ok := q1AmountFilter.eofCounter[clientID]; !ok {
		slog.Info("New client arrived", "clientID", clientID)
		q1AmountFilter.eofCounter[clientID] = batch_utils.NewSet[string]()
	}
	transactions := []transaction.LowAmountTransfer{}
	for _, transactionRecord := range transactionRecords {
		if transactionRecord.Amount < 50.0 {
			transactions = append(transactions, transaction.LowAmountTransfer{
				FromBank:    transactionRecord.FromBank,
				FromAccount: transactionRecord.FromAccount,
				ToBank:      transactionRecord.ToBank,
				ToAccount:   transactionRecord.ToAccount,
				Amount:      transactionRecord.Amount,
				Timestamp:   transactionRecord.Timestamp,
			})
		}
	}

	if len(transactions) != 0 {
		batch_utils.SortBatch(transactions, func(a, b transaction.LowAmountTransfer) bool {
			if !a.Timestamp.Equal(b.Timestamp) {
				return a.Timestamp.Before(b.Timestamp) // Ascendente: Las más viejas primero
			}

			return a.Amount > b.Amount // Descendente: Las más caras primero
		})
		queryResult := transaction.QueryResult{
			QueryID:      transaction.Query1,
			Transactions: transactions,
		}
		if err := q1AmountFilter.sendOutput(clientID, queryResult); err != nil {
			return err
		}
	}
	return nil
}

func (q1AmountFilter *Q1AmountFilter) handleControlMessage(msg *middleware.Message, ack func(), nack func()) {
	q1AmountFilter.mu.Lock()
	defer q1AmountFilter.mu.Unlock()
	err := worker.HandleMessageV2(
		msg,
		worker.MessageHandlerMap{
			inner.EndOfRecords: q1AmountFilter.handleControlEndOfRecords,
		},
	)
	if err != nil {
		nack()
		return
	}
	q1AmountFilter.dataSaver.Save(msg, q1AmountFilter)
	ack()
}

func (q1AmountFilter *Q1AmountFilter) handleControlEndOfRecords(clientID int64, data []interface{}) error {
	_, sender, err := inner.DeserializeEOR(data)
	if err != nil {
		slog.Error("While deserializing control message", "err", err, "clientID", clientID)
		return err
	}
	if q1AmountFilter.finishedClients.Contains(clientID) {
		slog.Info("Client has already done", "clientID", clientID)
		return nil
	}

	if _, ok := q1AmountFilter.eofCounter[clientID]; !ok {
		slog.Info("EOF arrived before client data (or dont save client new arrived for crash), initializing...", "clientID", clientID)
		q1AmountFilter.eofCounter[clientID] = batch_utils.NewSet[string]()
	}

	q1AmountFilter.eofCounter[clientID].Add(sender)
	if q1AmountFilter.eofCounter[clientID].Size() != q1AmountFilter.config.USDFilterAmount {
		slog.Info("Received EOF from other instance, waiting for more...")
		return nil
	}
	msgEof, err := inner.SerializeQueryEOR(clientID, transaction.Query1, fmt.Sprintf("%d", q1AmountFilter.config.Id)) // TO DO agregar otra var de entorno y para group tmb
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}

	if err = q1AmountFilter.outputQueue.Send(*msgEof); err != nil {
		slog.Debug("While sending EOF message", "err", err, "clientID", clientID)
		return err
	}
	slog.Info("Sent EOF", "clientID", clientID)
	delete(q1AmountFilter.eofCounter, clientID)
	q1AmountFilter.finishedClients.Add(clientID)
	return nil
}

func (q1AmountFilter *Q1AmountFilter) sendOutput(clientID int64, queryResult transaction.QueryResult) error {
	message, err := inner.SerializeQueryResultMessage(clientID, queryResult)
	if err != nil {
		slog.Debug("While serializing data message", "err", err, "clientID", clientID)
		return err
	}
	if err := q1AmountFilter.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending data message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}

func (q1AmountFilter *Q1AmountFilter) Restaurate() error {
	var checkpoint CheckpointData
	thereIsCheckpoint, err := q1AmountFilter.dataSaver.GetRestaurationCheckpoint(&checkpoint)
	if err != nil {
		return err
	}

	if thereIsCheckpoint {
		slog.Info("Cargando Q1 Amount Filter en base a checkpoint")
		q1AmountFilter.eofCounter = checkpoint.EofCounter
		q1AmountFilter.finishedClients = checkpoint.FinishedClients
		slog.Info("State restaured", "eofCounter", checkpoint.EofCounter, "finishedClients", checkpoint.FinishedClients)
	}
	var savedDataVar middleware.Message
	var thereIsLogs bool
	for {
		thereIsLogs, err = q1AmountFilter.dataSaver.GetDataFromLogs(&savedDataVar)
		if err != nil {
			return err
		}
		if !thereIsLogs {
			break
		}
		err = worker.HandleMessageV2(
			&savedDataVar,
			worker.MessageHandlerMap{
				inner.EndOfRecords: q1AmountFilter.handleControlEndOfRecords,
			},
		)
		if err != nil {
			return err
		}
	}
	return nil
}
