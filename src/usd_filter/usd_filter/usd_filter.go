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
	approved       int // para Info
	batchesSent    int // para Info
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
		approved:       0,
		batchesSent:    0,
	}, nil
}

func (usdFilter *USDFilter) Run() {
	usdFilter.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		usdFilter.handleMessage(&msg, ack, nack)
	})
}

func (usdFilter *USDFilter) handleMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	// TODO: Una vez que pase el filtro, el campo Currency ya no es necesario
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
	slog.Info("Sent EOF record message, ", "clientID", clientID)
	slog.Info("Transactions approved", "Amount", usdFilter.approved)
	slog.Info("Batches sent", "Amount", usdFilter.batchesSent)

	msg, err := inner.SerializeEOF(clientID, true, "usd_filter") // TO DO agregar otra var de entorno y para group tmb
	if err != nil {
		slog.Info("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}
	return usdFilter.outputExchange.Send(*msg)
}

func (usdFilter *USDFilter) handleDataMessage(transactionRecords []transaction.Transaction, clientID int64) error {
	toSend := make([]transaction.Transaction, 0, 10)
	for _, transactionRecord := range transactionRecords {
		if transactionRecord.Currency == USDCurrencyName {
			toSend = append(toSend, transactionRecord)
		}
	}

	if len(toSend) == 0 {
		return nil
	}

	if err := usdFilter.sendOutput(toSend, clientID); err != nil {
		return err
	}

	return nil
}

func (usdFilter *USDFilter) sendOutput(transactionRecords []transaction.Transaction, clientID int64) error {
	message, err := inner.SerializeMessage(clientID, transactionRecords)
	if err != nil {
		slog.Info("While serializing data message", "err", err, "clientID", clientID)
		return err
	}
	if err := usdFilter.outputExchange.Send(*message); err != nil {
		slog.Info("While sending data message", "err", err, "clientID", clientID)
		return err
	}
	usdFilter.approved += len(transactionRecords)
	usdFilter.batchesSent += 1
	return nil
}

// TO DO
//2026/05/23 22:14:26 ERROR While deserializing message err="record 0: expected 8 fields, got 2 — contents: [true gateway]" clientID=0
