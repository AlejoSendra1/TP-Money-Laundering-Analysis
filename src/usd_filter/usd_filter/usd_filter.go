package usd_filter

import (
	"log/slog"
	"sync"
	"tp_distribuidos/common/messageprotocol/inner/control"

	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

const USDCurrencyName = "US Dollar"

type USDFilterConfig struct {
	MomHost             string
	MomPort             int
	InputQueue          string
	InputTopic          string
	InputExchangeName   string
	OutputExchangeName  string
	OutputTopic         string
	ControlExchangeName string
	ControlTopic        string
}

type USDFilter struct {
	inputQueue      middleware.Middleware
	outputExchange  middleware.Middleware
	controlExchange middleware.Middleware
	config          USDFilterConfig
	mu              sync.Mutex // mutex para sincronizar la llegada de EOF
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
	controlExchange, err := middleware.CreateExchangeMiddleware(config.ControlExchangeName, []string{config.ControlTopic}, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}
	return &USDFilter{
		inputQueue:      inputQueue,
		outputExchange:  outputExchange,
		controlExchange: controlExchange,
		config:          config,
	}, nil
}

func (usdFilter *USDFilter) Run() {
	go usdFilter.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		usdFilter.handleControlMessage(&msg, ack, nack)
	})

	usdFilter.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		usdFilter.handleMessage(&msg, ack, nack)
	})
}

func (usdFilter *USDFilter) handleMessage(msg *middleware.Message, ack func(), nack func()) {
	usdFilter.mu.Lock()
	defer usdFilter.mu.Unlock()
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
	slog.Info("Arrived EOF record message", "clientID", clientID)
	ctrlMsg, err := control.SerializeControlMessage(control.ControlMessage{Type: control.TypeEOF, ClientID: clientID})
	if err != nil {
		slog.Error("While serializing control message", "err", err)
		return err
	}
	if err = usdFilter.controlExchange.Send(*ctrlMsg); err != nil {
		slog.Error("While sending control message", "err", err, "clientID", clientID)
		return err
	}
	return nil
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

func (usdFilter *USDFilter) handleControlMessage(msg *middleware.Message, ack func(), nack func()) {
	usdFilter.mu.Lock()
	defer usdFilter.mu.Unlock()
	slog.Info("Arrived control message", "msg", msg)
	controlMessage, err := control.DeserializeControlMessage(msg)
	if err != nil {
		slog.Error("While deserializing control message", "err", err)
		nack()
		return
	}
	if err = usdFilter.sendOutput([]transaction.Transaction{}, controlMessage.ClientID); err != nil {
		slog.Error("While sending EOF message", "err", err, "clientID", controlMessage.ClientID)
		nack()
		return
	}
	slog.Info("Sent EOF", "clientID", controlMessage.ClientID)
	ack()
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
