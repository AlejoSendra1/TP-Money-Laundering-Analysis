package usd_filter

import (
	"log/slog"

	"tp_distribuidos/src/common/messageprotocol/inner"
	"tp_distribuidos/src/common/middleware"
	"tp_distribuidos/src/common/transaction"
)

const USDCurrencyName = "US Dollar"

type USDFilterConfig struct {
	MomHost     string
	MomPort     int
	InputQueue  string
	InputTopic  string
	OutputTopic string
}

type USDFilter struct {
	inputExchange  middleware.Middleware
	outputExchange middleware.Middleware
	config         USDFilterConfig
}

func NewUSDFilter(config USDFilterConfig) (*USDFilter, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	// TODO: Aca el nombre del exchange tiene que coincidir con el de gateway
	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputTopic, []string{config.InputTopic}, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.OutputTopic, []string{config.OutputTopic}, connSettings)
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
		usdFilter.handleMessage(msg, ack, nack)
	})
}

func (usdFilter *USDFilter) handleMessage(msg middleware.Message, ack func(), nack func()) {
	// TODO: Actualizar DeserializeMessage con transaction en lugar de FruitItem
	transactionRecords, clientID, isEof, err := inner.DeserializeMessage(msg)
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

func (usdFilter *USDFilter) handleEndOfRecordMessage(clientID string) error {
	return usdFilter.sendOutput([]transaction.Transaction{}, clientID)
}

func (usdFilter *USDFilter) handleDataMessage(transactionRecords []transaction.Transaction, clientID string) error {
	for _, transactionRecord := range transactionRecords {
		if transactionRecord.Currency == USDCurrencyName {
			if err := usdFilter.sendOutput([]transaction.Transaction{transactionRecord}, clientID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (usdFilter *USDFilter) sendOutput(transactionRecords []transaction.Transaction, clientID string) error {
	// TODO: Actualizar SerializeMessage con transaction en lugar de FruitItem
	message, err := inner.SerializeMessage(transactionRecords, clientID)
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
