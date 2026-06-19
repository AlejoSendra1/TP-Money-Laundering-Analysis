package q1_amount_filter

import (
	"fmt"
	"log/slog"
	"sync"
	"tp_distribuidos/common/batch_utils"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/messageprotocol/inner/control"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

type Q1AmountFilterConfig struct {
	Id                int
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
	eofCounter      map[int64]int
	config          Q1AmountFilterConfig
	mu              sync.Mutex
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

	return &Q1AmountFilter{
		inputQueue:      inputQueue,
		outputQueue:     outputQueue,
		controlExchange: controlExchange,
		config:          config,
		eofCounter:      make(map[int64]int),
	}, nil
}

func (q1AmountFilter *Q1AmountFilter) Run() {
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
	msg, err := inner.DeserializeMessage(middlewareMsg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", msg.ClientID)
		nack()
		return
	}

	switch msg.MsgType {
	case inner.EndOfRecords:
		if err := q1AmountFilter.handleEndOfRecordMessage(msg.ClientID); err != nil {
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
		if err := q1AmountFilter.handleDataMessage(transactions, msg.ClientID); err != nil {
			slog.Error("While handling data message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
		ack()
	}
}

func (q1AmountFilter *Q1AmountFilter) handleEndOfRecordMessage(clientID int64) error {
	slog.Info("Arrived EOF record message", "clientID", clientID)
	ctrlMsg, err := control.SerializeControlMessage(control.ControlMessage{Type: control.TypeEOF, ClientID: clientID})
	if err != nil {
		slog.Error("While serializing control message", "err", err)
		return err
	}
	if err = q1AmountFilter.controlExchange.Send(*ctrlMsg); err != nil {
		slog.Error("While sending control message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}

func (q1AmountFilter *Q1AmountFilter) handleDataMessage(transactionRecords []transaction.Transaction, clientID int64) error {
	if _, ok := q1AmountFilter.eofCounter[clientID]; !ok {
		q1AmountFilter.eofCounter[clientID] = 0
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

	slog.Info("Arrived control message", "msg", msg)
	controlMessage, err := control.DeserializeControlMessage(msg)
	if err != nil {
		slog.Error("While deserializing control message", "err", err)
		nack()
		return
	}

	clientID := controlMessage.ClientID
	q1AmountFilter.eofCounter[clientID] += 1
	if q1AmountFilter.eofCounter[clientID] != q1AmountFilter.config.USDFilterAmount {
		slog.Info("Received EOF from other instance, waiting for more...")
		ack()
		return
	}
	msgEof, err := inner.SerializeQueryEOR(clientID, transaction.Query1, fmt.Sprintf("%d", q1AmountFilter.config.Id)) // TO DO agregar otra var de entorno y para group tmb
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err, "clientID", clientID)
		nack()
		return
	}

	if err := q1AmountFilter.outputQueue.Send(*msgEof); err != nil {
		slog.Debug("While sending EOF message", "err", err, "clientID", clientID)
		nack()
		return
	}
	slog.Info("Sent EOF", "clientID", controlMessage.ClientID)
	delete(q1AmountFilter.eofCounter, clientID)
	ack()
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
