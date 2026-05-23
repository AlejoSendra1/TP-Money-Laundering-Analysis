package usd_filter

import (
	"log/slog"

	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

const USDCurrencyName = "US Dollar"

type USDFilterConfig struct {
	MomHost            string
	MomPort            int
	InputQueue         string // Con 1 replica no es necesario
	InputTopic         string
	InputExchangeName  string
	OutputExchangeName string
	OutputTopic        string
}

type USDFilter struct {
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware
	config         USDFilterConfig
}

func NewUSDFilter(config USDFilterConfig) (*USDFilter, error) {
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
	outputExchange, err := middleware.CreateExchangeMiddleware(config.OutputExchangeName, []string{config.OutputTopic}, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &USDFilter{
		inputQueue:     inputQueue,
		outputExchange: outputExchange,
		config:         config,
	}, nil
}

func (usdFilter *USDFilter) Run() {
	usdFilter.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		usdFilter.handleMessage(&msg, ack, nack)
	})
}

func (usdFilter *USDFilter) handleMessage(msg *middleware.Message, ack func(), nack func()) {
	// TODO: Una vez que pase el filtro, el campo Currency ya no es necesario
	clientID, transactionRecords, isEof, err := inner.DeserializeRawTransactionsMessage(msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", clientID)
		nack()
		return
	}

	if isEof {
		if err := usdFilter.handleEndOfRecordMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err, "clientID", clientID)
			nack()
			return
		}
		ack()
		return
	}
	if err := usdFilter.handleDataMessage(transactionRecords, clientID); err != nil {
		slog.Error("While handling data message", "err", err, "clientID", clientID)
		nack()
		return
	}
	ack()
}

func (usdFilter *USDFilter) handleEndOfRecordMessage(clientID int64) error {
	slog.Info("Sent EOF record message", "clientID", clientID)
	return usdFilter.sendOutput([]transaction.Transaction{}, clientID)
}

func (usdFilter *USDFilter) handleDataMessage(transactionRecords []transaction.Transaction, clientID int64) error {
	transactions := make([]transaction.Transaction, 0, len(transactionRecords))
	for _, tr := range transactionRecords {
		if tr.Currency == USDCurrencyName {
			transactions = append(transactions, tr)
		}
	}

	if len(transactions) != 0 {
		if err := usdFilter.sendOutput(transactions, clientID); err != nil {
			return err
		}
	}
	return nil
}

func (usdFilter *USDFilter) sendOutput(transactionRecords []transaction.Transaction, clientID int64) error {
	message, err := inner.SerializeMessage(clientID, transactionRecords)
	if err != nil {
		slog.Debug("While serializing data message", "err", err, "clientID", clientID)
		return err
	}
	if err := usdFilter.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending data message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}
