package q5_date_filter

import (
	"log/slog"
	"sync"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/messageprotocol/inner/control"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

type Q5DateFilterConfig struct {
	MomHost             string
	MomPort             int
	InputQueue          string
	InputExchangeName   string
	InputTopic          string
	OutputExchangeName  string
	OutputTopic         string
	ControlExchangeName string
	ControlTopic        string
}

type Q5DateFilter struct {
	inputQueue      middleware.Middleware
	outputExchange  middleware.Middleware
	controlExchange middleware.Middleware
	config          Q5DateFilterConfig
	mu              sync.Mutex
}

func NewQ5DateFilter(config Q5DateFilterConfig) (*Q5DateFilter, error) {
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
	return &Q5DateFilter{
		inputQueue:      inputQueue,
		outputExchange:  outputExchange,
		controlExchange: controlExchange,
		config:          config,
	}, nil
}

func (q5DateFilter *Q5DateFilter) Run() {
	go q5DateFilter.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		q5DateFilter.handleControlMessage(&msg, ack, nack)
	})

	q5DateFilter.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		q5DateFilter.handleMessage(&msg, ack, nack)
	})
}

func (q5DateFilter *Q5DateFilter) handleMessage(msg *middleware.Message, ack func(), nack func()) {
	q5DateFilter.mu.Lock()
	defer q5DateFilter.mu.Unlock()
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
	slog.Info("Arrived EOF record message", "clientID", clientID)
	ctrlMsg, err := control.SerializeControlMessage(control.ControlMessage{Type: control.TypeEOF, ClientID: clientID})
	if err != nil {
		slog.Error("While serializing control message", "err", err)
		return err
	}
	if err = q5DateFilter.controlExchange.Send(*ctrlMsg); err != nil {
		slog.Error("While sending control message", "err", err, "clientID", clientID)
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
		if err := q5DateFilter.sendOutput(transactions, clientID); err != nil {
			return err
		}
	}

	return nil
}

func (q5DateFilter *Q5DateFilter) handleControlMessage(msg *middleware.Message, ack func(), nack func()) {
	q5DateFilter.mu.Lock()
	defer q5DateFilter.mu.Unlock()
	slog.Info("Arrived control message", "msg", msg)
	controlMessage, err := control.DeserializeControlMessage(msg)
	if err != nil {
		slog.Error("While deserializing control message", "err", err)
		nack()
		return
	}
	if err = q5DateFilter.sendOutput([]transaction.Transaction{}, controlMessage.ClientID); err != nil {
		slog.Error("While sending EOF message", "err", err, "clientID", controlMessage.ClientID)
		nack()
		return
	}
	slog.Info("Sent EOF", "clientID", controlMessage.ClientID)
	ack()
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
