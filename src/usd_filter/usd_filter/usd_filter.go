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
	qtyTx           map[int64]int
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
		qtyTx:           make(map[int64]int),
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

func (usdFilter *USDFilter) handleMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	usdFilter.mu.Lock()
	defer usdFilter.mu.Unlock()
	msg, err := inner.DeserializeMessage(middlewareMsg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", msg.ClientID)
		nack()
		return
	}

	switch msg.MsgType {
	case inner.EndOfRecords:
		if err := usdFilter.handleEndOfRecordMessage(msg.ClientID); err != nil {
			slog.Error("While handling end of record message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
		ack()
		return
	case inner.TransactionBatch:
		transactionRecords, err := inner.DeserializeTransactionBatch(msg.Data)
		if err != nil {
			slog.Error("While deserializing transactions", "err", err, "clientID", msg.ClientID, "content", middlewareMsg.Body)
			nack()
			return
		}
		if err := usdFilter.handleDataMessage(transactionRecords, msg.ClientID); err != nil {
			slog.Error("While handling data message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
		ack()
	default:
		slog.Error("Unexpected msg type received", "err", err, "clientID", msg.ClientID)
	}
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
	if _, ok := usdFilter.qtyTx[clientID]; !ok {
		slog.Info("New client arrived", "clientID", clientID)
		usdFilter.qtyTx[clientID] = 0
	}
	transactions := make([]transaction.Transaction, 0, len(transactionRecords))
	for _, tr := range transactionRecords {
		if tr.Currency == USDCurrencyName {
			transactions = append(transactions, tr)
		}
	}

	if len(transactions) != 0 {
		usdFilter.qtyTx[clientID] += len(transactions)
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
	msgEof, err := inner.SerializeEOF(controlMessage.ClientID, true, "usd_filter") // TO DO agregar otra var de entorno y para group tmb
	if err != nil {
		slog.Info("While serializing EOF message", "err", err, "clientID", controlMessage.ClientID)
		nack()
		return
	}
	if err = usdFilter.outputExchange.Send(*msgEof); err != nil {
		slog.Info("While sending EOF message", "err", err, "clientID")
	}
	slog.Info("size transactions sent:", "qtyTx", usdFilter.qtyTx[controlMessage.ClientID])
	delete(usdFilter.qtyTx, controlMessage.ClientID)
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
