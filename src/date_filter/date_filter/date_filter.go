package date_filter

import (
	"fmt"
	"log/slog"
	"sync"
	"tp_distribuidos/common/batch_utils"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

// Segun el enunciado, early period es de [2022-09-01, 2022-09-05], pero en la notebook usa los de abajo...
const DateMinEarlyPeriod = "2022-09-01"
const DateMaxEarlyPeriod = "2022-09-06"

const DateMinLaterPeriod = "2022-09-06"
const DateMaxLaterPeriod = "2022-09-15"

type DateFilterConfig struct {
	Id                  int
	MomHost             string
	MomPort             int
	InputQueue          string
	InputExchangeName   string
	InputTopic          string
	OutputExchangeName  string
	OutputTopic1        string
	OutputTopic2        string
	ControlExchangeName string
	ControlTopic        string
	USDFilterAmount     int
}

type DateFilter struct {
	inputQueue      middleware.Middleware
	outputExchanges map[string]middleware.Middleware
	controlExchange middleware.Middleware
	eofCounter      map[int64]batch_utils.Set[string]
	config          DateFilterConfig
	mu              sync.Mutex
}

func NewDateFilter(config DateFilterConfig) (*DateFilter, error) {
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
	outputExchangeTopic1, err := middleware.CreateExchangeMiddleware(config.OutputExchangeName, []string{config.OutputTopic1}, connSettings, "")
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	outputExchangeTopic2, err := middleware.CreateExchangeMiddleware(config.OutputExchangeName, []string{config.OutputTopic2}, connSettings, "")
	if err != nil {
		inputQueue.Close()
		outputExchangeTopic1.Close()
		return nil, err
	}

	myKeyControl := fmt.Sprintf("%s_%d", config.ControlExchangeName, config.Id)
	controlExchange, err := middleware.CreateExchangeMiddleware(config.ControlExchangeName, []string{config.ControlTopic}, connSettings, myKeyControl)
	if err != nil {
		inputQueue.Close()
		outputExchangeTopic1.Close()
		outputExchangeTopic2.Close()
		return nil, err
	}
	outputExchanges := map[string]middleware.Middleware{
		config.OutputTopic1: outputExchangeTopic1,
		config.OutputTopic2: outputExchangeTopic2,
	}

	return &DateFilter{
		inputQueue:      inputQueue,
		outputExchanges: outputExchanges,
		controlExchange: controlExchange,
		eofCounter:      make(map[int64]batch_utils.Set[string]),
		config:          config,
	}, nil
}

func (dateFilter *DateFilter) Run() {
	go dateFilter.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		dateFilter.handleControlMessage(&msg, ack, nack)
	})

	dateFilter.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		dateFilter.handleMessage(&msg, ack, nack)
	})
}

func (dateFilter *DateFilter) handleMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	dateFilter.mu.Lock()
	defer dateFilter.mu.Unlock()
	msg, err := inner.DeserializeMessage(middlewareMsg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}

	switch msg.MsgType {
	case inner.EndOfRecords:
		_, sender, err := inner.DeserializeEOR(msg.Data)
		if err = dateFilter.handleEndOfRecordMessage(msg.ClientID, sender); err != nil {
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
		if err := dateFilter.handleDataMessage(transactionRecords, msg.ClientID); err != nil {
			slog.Error("While handling data message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
		ack()
		return
	default:
		slog.Info("Unexpected msg type received", "err", err, "clientID", msg.ClientID)
		nack()
	}
}

func (dateFilter *DateFilter) handleEndOfRecordMessage(clientID int64, sender string) error {
	slog.Info("Received End Of Records message", "clientID", clientID)
	msg, err := inner.SerializeEOR(clientID, false, sender)
	if err != nil {
		slog.Info("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}
	if err := dateFilter.controlExchange.Send(*msg); err != nil {
		slog.Info("While sending EOF message to other instances", "err", err, "clientID", clientID)
		return err
	}
	return nil
}

func (dateFilter *DateFilter) handleDataMessage(transactionRecords []transaction.Transaction, clientID int64) error {
	if _, ok := dateFilter.eofCounter[clientID]; !ok {
		slog.Info("New client arrived", "clientID", clientID)
		dateFilter.eofCounter[clientID] = batch_utils.NewSet[string]()
	}
	topics := map[string][]transaction.Transaction{
		dateFilter.config.OutputTopic1: {},
		dateFilter.config.OutputTopic2: {},
	}
	for _, transactionRecord := range transactionRecords {
		topic := dateFilter.getOutputTopic(transactionRecord)
		if topic == "" {
			continue
		}

		topics[topic] = append(topics[topic], transactionRecord)
	}

	for topic, transactions := range topics {
		if len(transactions) == 0 {
			continue
		}
		err := dateFilter.sendOutput(transactions, clientID, topic)
		if err != nil {
			return err
		}
	}

	return nil
}

func (dateFilter *DateFilter) handleControlMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	dateFilter.mu.Lock()
	defer dateFilter.mu.Unlock()

	slog.Info("Arrived control message", "msg", middlewareMsg)
	msg, err := inner.DeserializeMessage(middlewareMsg)
	if err != nil {
		slog.Error("While deserializing control message", "err", err)
		nack()
		return
	}
	_, sender, err := inner.DeserializeEOR(msg.Data)
	if err != nil {
		slog.Error("While deserializing control message", "err", err, "clientID", msg.ClientID)
		nack()
		return
	}

	clientID := msg.ClientID
	dateFilter.eofCounter[clientID].Add(sender)
	if dateFilter.eofCounter[clientID].Size() != dateFilter.config.USDFilterAmount {
		slog.Info("Received EOF from other instance, waiting for more...")
		ack()
		return
	}

	msgEOF, err := inner.SerializeEOR(clientID, true, fmt.Sprintf("%d", dateFilter.config.Id)) // TO DO agregar otra var de entorno y para group tmb

	if err != nil {
		slog.Info("While serializing EOF message", "err", err, "clientID", clientID)
		nack()
		return
	}

	for topic := range dateFilter.outputExchanges {
		err := dateFilter.outputExchanges[topic].Send(*msgEOF)
		if err != nil {
			slog.Info("While sending to topics", "topic", topic)
			nack()
			return
		}
	}
	slog.Info("Sent EOF", "clientID", clientID)

	delete(dateFilter.eofCounter, clientID)
	ack()
}

func (dateFilter *DateFilter) getOutputTopic(transaction transaction.Transaction) string {
	date := transaction.Timestamp.UTC().Format("2006-01-02")

	switch {
	case date >= DateMinEarlyPeriod && date < DateMaxEarlyPeriod:
		return dateFilter.config.OutputTopic1
	case date >= DateMinLaterPeriod && date < DateMaxLaterPeriod:
		return dateFilter.config.OutputTopic2
	default:
		return ""
	}
}

func (dateFilter *DateFilter) sendOutput(transactionRecords []transaction.Transaction, clientID int64, topic string) error {
	message, err := inner.SerializeMessage(clientID, transactionRecords)
	if err != nil {
		slog.Debug("While serializing data message", "err", err, "clientID", clientID)
		return err
	}
	outputExchange, _ := dateFilter.outputExchanges[topic]

	if err = outputExchange.Send(*message); err != nil {
		slog.Debug("While sending data message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}
