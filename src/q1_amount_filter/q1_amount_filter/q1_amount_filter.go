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

func (q1AmountFilter *Q1AmountFilter) handleMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
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
	msg, err := inner.SerializeQueryEOR(clientID, transaction.Query1) // TO DO agregar otra var de entorno y para group tmb
	if err != nil {
		slog.Info("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}

	if err := q1AmountFilter.outputQueue.Send(*msg); err != nil {
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
		if err := q1AmountFilter.sendOutput(clientID, transactions); err != nil {
			return err
		}
	}
	return nil
}

func (q1AmountFilter *Q1AmountFilter) sendOutput(clientID int64, queryResult []transaction.LowAmountTransfer) error {
	message, err := inner.SerializeQuery1ResultMessage(clientID, queryResult)
	if err != nil {
		slog.Info("While serializing data message", "err", err, "clientID", clientID)
		return err
	}
	if err := q1AmountFilter.outputQueue.Send(*message); err != nil {
		slog.Info("While sending data message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}
