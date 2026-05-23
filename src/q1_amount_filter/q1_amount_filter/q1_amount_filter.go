package q1_amount_filter

import (
	"log/slog"
	"tp_distribuidos/common/messageprotocol/inner"
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
}

type Q1AmountFilter struct {
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware
	config      Q1AmountFilterConfig
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

	return &Q1AmountFilter{
		inputQueue:  inputQueue,
		outputQueue: outputQueue,
		config:      config,
	}, nil
}

func (q1AmountFilter *Q1AmountFilter) Run() {
	q1AmountFilter.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		q1AmountFilter.handleMessage(&msg, ack, nack)
	})
}

func (q1AmountFilter *Q1AmountFilter) handleMessage(msg *middleware.Message, ack func(), nack func()) {
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
	queryResult := transaction.QueryResult{
		QueryID:      transaction.Query1,
		Transactions: []transaction.LowAmountTransfer{},
	}
	if err := q1AmountFilter.sendOutput(queryResult, clientID); err != nil {
		return err
	}
	slog.Info("Sent EOF record message", "clientID", clientID)
	return nil
}

func (q1AmountFilter *Q1AmountFilter) handleDataMessage(transactionRecords []transaction.Transaction, clientID int64) error {
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
		if err := q1AmountFilter.sendOutput(queryResult, clientID); err != nil {
			return err
		}
	}
	return nil
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
