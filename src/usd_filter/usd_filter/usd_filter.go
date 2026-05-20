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
	inputExchange  middleware.Middleware
	outputExchange middleware.Middleware
	config         USDFilterConfig
}

func NewUSDFilter(config USDFilterConfig) (*USDFilter, error) {
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

	return &USDFilter{
		inputExchange:  inputExchange,
		outputExchange: outputExchange,
		config:         config,
	}, nil
}

func (usdFilter *USDFilter) Run() {
	usdFilter.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
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
	for _, transactionRecord := range transactionRecords {
		if transactionRecord.Currency == USDCurrencyName {
			if err := usdFilter.sendOutput([]transaction.Transaction{transactionRecord}, clientID); err != nil {
				return err
			}
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
