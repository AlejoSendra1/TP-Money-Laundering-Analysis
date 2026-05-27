package date_filter

import (
	"log/slog"
	"sync"
	"tp_distribuidos/common/messageprotocol/inner/control"

	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

type DateFilterConfig struct {
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
	eofCounter      map[int64]int
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
	outputExchangeTopic1, err := middleware.CreateExchangeMiddleware(config.OutputExchangeName, []string{config.OutputTopic1}, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	outputExchangeTopic2, err := middleware.CreateExchangeMiddleware(config.OutputExchangeName, []string{config.OutputTopic2}, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchangeTopic1.Close()
		return nil, err
	}

	controlExchange, err := middleware.CreateExchangeMiddleware(config.ControlExchangeName, []string{config.ControlTopic}, connSettings)
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
		eofCounter:      make(map[int64]int),
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

func (dateFilter *DateFilter) handleMessage(msg *middleware.Message, ack func(), nack func()) {
	dateFilter.mu.Lock()
	defer dateFilter.mu.Unlock()
	clientID, transactionRecords, isEof, err := inner.DeserializeRawTransactionsMessage(msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", clientID)
		nack()
		return
	}

	if isEof {
		if err := dateFilter.handleEndOfRecordMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err, "clientID", clientID)
			nack()
			return
		}
		ack()
		return
	}
	if err := dateFilter.handleDataMessage(transactionRecords, clientID); err != nil {
		slog.Error("While handling data message", "err", err, "clientID", clientID)
		nack()
		return
	}
	ack()
}

func (dateFilter *DateFilter) handleEndOfRecordMessage(clientID int64) error {
	slog.Info("Arrived EOF record message", "clientID", clientID)
	ctrlMsg, err := control.SerializeControlMessage(control.ControlMessage{Type: control.TypeEOF, ClientID: clientID})
	if err != nil {
		slog.Error("While serializing control message", "err", err)
		return err
	}
	if err = dateFilter.controlExchange.Send(*ctrlMsg); err != nil {
		slog.Error("While sending control message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}

func (dateFilter *DateFilter) handleDataMessage(transactionRecords []transaction.Transaction, clientID int64) error {
	if _, ok := dateFilter.eofCounter[clientID]; !ok {
		dateFilter.eofCounter[clientID] = 0
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

func (dateFilter *DateFilter) handleControlMessage(msg *middleware.Message, ack func(), nack func()) {
	dateFilter.mu.Lock()
	defer dateFilter.mu.Unlock()

	slog.Info("Arrived control message", "msg", msg)
	controlMessage, err := control.DeserializeControlMessage(msg)
	if err != nil {
		slog.Error("While deserializing control message", "err", err)
		nack()
		return
	}

	clientID := controlMessage.ClientID
	dateFilter.eofCounter[clientID] += 1
	if dateFilter.eofCounter[clientID] != dateFilter.config.USDFilterAmount {
		slog.Info("Received EOF from other instance, waiting for more...")
		ack()
		return
	}

	for topic := range dateFilter.outputExchanges {
		err := dateFilter.sendOutput([]transaction.Transaction{}, clientID, topic)
		if err != nil {
			return
		}
	}
	slog.Info("Sent EOF", "clientID", controlMessage.ClientID)
	delete(dateFilter.eofCounter, clientID)
	ack()
}

func (dateFilter *DateFilter) getOutputTopic(transaction transaction.Transaction) string {
	date := transaction.Timestamp.UTC().Format("2006-01-02")

	switch {
	case date >= "2022-09-01" && date <= "2022-09-05":
		return dateFilter.config.OutputTopic1
	case date >= "2022-09-06" && date <= "2022-09-15":
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
