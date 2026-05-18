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
	inputExchange   middleware.Middleware
	outputExchanges map[string]middleware.Middleware
	config          DateFilterConfig
}

func NewDateFilter(config DateFilterConfig) (*DateFilter, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	// TODO: Aca el nombre del exchange tiene que coincidir con el de usd filter,
	// y cada instancia Date Filter debe usar la misma InputQueue
	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputExchangeName, []string{config.InputTopic}, connSettings)
	if err != nil {
		return nil, err
	}

	// TODO: Aca el nombre del exchange tiene que coincidir con el de group,
	// sum y transaction saver
	outputExchangeTopic1, err := middleware.CreateExchangeMiddleware(config.OutputExchangeName, []string{config.OutputTopic1}, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}

	outputExchangeTopic2, err := middleware.CreateExchangeMiddleware(config.OutputExchangeName, []string{config.OutputTopic2}, connSettings)
	if err != nil {
		inputExchange.Close()
		outputExchangeTopic1.Close()
		return nil, err
	}

	outputExchanges := map[string]middleware.Middleware{
		config.OutputTopic1: outputExchangeTopic1,
		config.OutputTopic2: outputExchangeTopic2,
	}

	return &DateFilter{
		inputExchange:   inputExchange,
		outputExchanges: outputExchanges,
		config:          config,
	}, nil
}

func (dateFilter *DateFilter) Run() {
	dateFilter.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		dateFilter.handleMessage(&msg, ack, nack)
	})
}

func (dateFilter *DateFilter) handleMessage(msg *middleware.Message, ack func(), nack func()) {
	// TODO: Una vez que pase el filtro, el campo Timestamp ya no es necesario
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
	err := dateFilter.sendOutput([]transaction.Transaction{}, clientID, dateFilter.config.OutputTopic1)
	if err != nil {
		return err
	}
	err = dateFilter.sendOutput([]transaction.Transaction{}, clientID, dateFilter.config.OutputTopic2)
	if err != nil {
		return err
	}
	return nil
}

func (dateFilter *DateFilter) handleDataMessage(transactionRecords []transaction.Transaction, clientID int64) error {
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
