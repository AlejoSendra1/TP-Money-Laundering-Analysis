package q5_date_filter

import (
	"log/slog"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

type Q5DateFilterConfig struct {
	MomHost            string
	MomPort            int
	InputQueue         string
	InputExchangeName  string
	InputTopic         string
	OutputExchangeName string
	OutputTopic        string
}

type Q5DateFilter struct {
	inputExchange  middleware.Middleware
	outputExchange middleware.Middleware
	config         Q5DateFilterConfig
}

func NewQ5DateFilter(config Q5DateFilterConfig) (*Q5DateFilter, error) {
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

	return &Q5DateFilter{
		inputExchange:  inputExchange,
		outputExchange: outputExchange,
		config:         config,
	}, nil
}

func (q5DateFilter *Q5DateFilter) Run() {
	q5DateFilter.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		q5DateFilter.handleMessage(&msg, ack, nack)
	})
}

func (q5DateFilter *Q5DateFilter) handleMessage(msg *middleware.Message, ack func(), nack func()) {
	// TODO: Una vez que pase el filtro, el campo TimeStamp ya no es necesario
	clientID, transactionRecords, isEof, err := inner.DeserializeRawTransactionsMessage(msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", clientID)
		nack()
		return
	}

	if isEof {
		if err := q5DateFilter.handleEndOfRecordMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err, "clientID", clientID)
			nack()
			return
		}
		ack()
		return
	}
	if err := q5DateFilter.handleDataMessage(transactionRecords, clientID); err != nil {
		slog.Error("While handling data message", "err", err, "clientID", clientID)
		nack()
		return
	}
	ack()
}

func (q5DateFilter *Q5DateFilter) handleEndOfRecordMessage(clientID int64) error {
	slog.Info("Sending end of record message, clientID", clientID)
	if err := q5DateFilter.sendOutput([]transaction.Transaction{}, clientID); err != nil {
		return err
	}
	return nil
}

func (q5DateFilter *Q5DateFilter) handleDataMessage(transactionRecords []transaction.Transaction, clientID int64) error {
	transactions := []transaction.Transaction{}
	for _, transactionRecord := range transactionRecords {
		date := transactionRecord.Timestamp.UTC().Format("2006-01-02")
		if date >= "2022-09-01" && date <= "2022-09-05" {
			transactions = append(transactions, transactionRecord)
		}
	}

	if len(transactions) != 0 {
		slog.Info("Sending data message", "clientID", clientID, "transactions", transactions)
		if err := q5DateFilter.sendOutput(transactions, clientID); err != nil {
			return err
		}
	}

	return nil
}

func (q5DateFilter *Q5DateFilter) sendOutput(transactionRecords []transaction.Transaction, clientID int64) error {
	message, err := inner.SerializeMessage(clientID, transactionRecords)
	if err != nil {
		slog.Debug("While serializing data message", "err", err, "clientID", clientID)
		return err
	}

	if err = q5DateFilter.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending data message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}
