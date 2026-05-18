package q1_amount_filter

import (
	"log/slog"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

type Q1AmountFilterConfig struct {
	MomHost            string
	MomPort            int
	InputQueue         string
	InputTopic         string
	InputExchangeName  string
	OutputExchangeName string
	OutputTopic        string
}

type Q1AmountFilter struct {
	inputExchange  middleware.Middleware
	outputExchange middleware.Middleware
	config         Q1AmountFilterConfig
}

func NewQ1AmountFilter(config Q1AmountFilterConfig) (*Q1AmountFilter, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputExchangeName, []string{config.InputTopic}, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.OutputExchangeName, []string{config.OutputTopic}, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}

	return &Q1AmountFilter{
		inputExchange:  inputExchange,
		outputExchange: outputExchange,
		config:         config,
	}, nil
}

func (q1AmountFilter *Q1AmountFilter) Run() {
	q1AmountFilter.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
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
	return q1AmountFilter.sendOutput([]transaction.TransactionResultQuery1{}, clientID)
}

func (q1AmountFilter *Q1AmountFilter) handleDataMessage(transactionRecords []transaction.Transaction, clientID int64) error {
	transactions := []transaction.TransactionResultQuery1{}
	for _, transactionRecord := range transactionRecords {
		if transactionRecord.Amount > 50.0 {
			transactions = append(transactions, transaction.TransactionResultQuery1{
				FromBank:    transactionRecord.FromBank,
				ToBank:      transactionRecord.ToBank,
				FromAccount: transactionRecord.FromAccount,
				ToAccount:   transactionRecord.ToAccount,
				Amount:      transactionRecord.Amount,
			})
		}
	}

	if len(transactions) != 0 {
		slog.Info("Sending output message", "clientID", clientID, "transactions", transactions)
		if err := q1AmountFilter.sendOutput(transactions, clientID); err != nil {
			return err
		}
	}
	return nil
}

func (q1AmountFilter *Q1AmountFilter) sendOutput(transactionRecords []transaction.TransactionResultQuery1, clientID int64) error {
	return nil
}
