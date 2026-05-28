package date_filter

import (
	"log/slog"

	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

type DateFilterConfig struct {
	MomHost            string
	MomPort            int
	InputQueue         string
	InputExchangeName  string
	InputTopic         string
	OutputExchangeName string
	OutputTopic1       string
	OutputTopic2       string
}

type DateFilter struct {
	inputQueue      middleware.Middleware
	outputExchanges map[string]middleware.Middleware
	config          DateFilterConfig
	// para debug
	received    int
	approved    int
	batchesSent int
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

	outputExchangeTopic2, err := middleware.CreateExchangeMiddleware(config.OutputExchangeName, []string{config.OutputTopic2}, connSettings) // solo para probar
	if err != nil {
		inputQueue.Close()
		outputExchangeTopic1.Close()
		return nil, err
	}

	outputExchanges := map[string]middleware.Middleware{
		config.OutputTopic1: outputExchangeTopic1,
		config.OutputTopic2: outputExchangeTopic2,
	}

	return &DateFilter{
		inputQueue:      inputQueue,
		outputExchanges: outputExchanges,
		config:          config,
		received:        0,
		approved:        0,
		batchesSent:     0,
	}, nil
}

func (dateFilter *DateFilter) Run() {
	dateFilter.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		dateFilter.handleMessage(&msg, ack, nack)
	})
}

func (dateFilter *DateFilter) handleMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	// TODO: Una vez que pase el filtro, el campo Timestamp ya no es necesario
	msg, err := inner.DeserializeMessage(middlewareMsg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", msg.ClientID)
		nack()
		return
	}

	switch msg.MsgType {
	case inner.EndOfRecords:
		if err := dateFilter.handleEndOfRecordMessage(msg.ClientID); err != nil {
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
	default:
		slog.Error("Unexpected msg type received", "err", err, "clientID", msg.ClientID)

	}

}

func (dateFilter *DateFilter) handleEndOfRecordMessage(clientID int64) error {
	slog.Info("Transactions received", "Amount", dateFilter.received)
	slog.Info("Transactions approved", "Amount", dateFilter.approved)
	slog.Info("Batches sent", "Amount", dateFilter.batchesSent)

	msg, err := inner.SerializeEOF(clientID, true, "date_filter") // TO DO agregar otra var de entorno y para group tmb
	if err != nil {
		slog.Info("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}

	for topic := range dateFilter.outputExchanges {
		err := dateFilter.outputExchanges[topic].Send(*msg)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dateFilter *DateFilter) handleDataMessage(transactionRecords []transaction.Transaction, clientID int64) error {
	topics := map[string][]transaction.Transaction{
		dateFilter.config.OutputTopic1: {},
		dateFilter.config.OutputTopic2: {},
	}
	for _, transactionRecord := range transactionRecords {
		dateFilter.received += 1
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
		//para testing -------
		if topic == dateFilter.config.OutputTopic1 {
			dateFilter.approved += len(transactions)
			dateFilter.batchesSent += 1
		}
		//fin testing --------
		if err != nil {
			return err
		}
	}

	return nil
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
		slog.Info("While serializing data message", "err", err, "clientID", clientID)
		return err
	}
	outputExchange, _ := dateFilter.outputExchanges[topic]

	if err = outputExchange.Send(*message); err != nil {
		slog.Info("While sending data message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}
