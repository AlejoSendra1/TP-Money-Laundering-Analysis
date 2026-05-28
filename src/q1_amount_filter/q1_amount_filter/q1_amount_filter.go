package q1_amount_filter

import (
	"log/slog"
	"sync"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/messageprotocol/inner/control"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

type Q1AmountFilterConfig struct {
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
	qtyTx           map[int64]int
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

	controlExchange, err := middleware.CreateExchangeMiddleware(config.ControlExchange, []string{config.ControlTopic}, connSettings)
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
		qtyTx:           make(map[int64]int),
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

func (q1AmountFilter *Q1AmountFilter) handleMessage(msg *middleware.Message, ack func(), nack func()) {
	q1AmountFilter.mu.Lock()
	defer q1AmountFilter.mu.Unlock()
	clientID, transactionRecords, isEof, err := inner.DeserializeRawTransactionsMessage(msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", clientID)
		nack()
		return
	}

	if isEof {
		if err := q1AmountFilter.handleEndOfRecordMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err, "clientID", clientID)
			nack()
			return
		}
		ack()
		return
	}

	if err := q1AmountFilter.handleDataMessage(transactionRecords, clientID); err != nil {
		slog.Error("While handling data message", "err", err, "clientID", clientID)
		nack()
		return
	}
	ack()
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
		q1AmountFilter.qtyTx[clientID] = 0
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
			})
		}
	}

	if len(transactions) != 0 {
		queryResult := transaction.QueryResult{
			QueryID:      transaction.Query1,
			Transactions: transactions,
		}
		q1AmountFilter.qtyTx[clientID] += len(transactions)
		if err := q1AmountFilter.sendOutput(queryResult, clientID); err != nil {
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

	queryResult := transaction.QueryResult{
		QueryID:      transaction.Query1,
		Transactions: []transaction.LowAmountTransfer{},
	}
	if err := q1AmountFilter.sendOutput(queryResult, clientID); err != nil {
		slog.Error("While sending EOF data message", "err", err, "clientID", clientID)
		nack()
		return
	}
	slog.Info("Sent EOF", "clientID", controlMessage.ClientID)
	slog.Info("size transactions", "qtyTx", q1AmountFilter.qtyTx[clientID])
	delete(q1AmountFilter.eofCounter, clientID)
	delete(q1AmountFilter.qtyTx, clientID)
	ack()
}

func (q1AmountFilter *Q1AmountFilter) sendOutput(queryResult transaction.QueryResult, clientID int64) error {
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
