package group

import (
	"fmt"
	"hash/fnv"
	"log/slog"

	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

type JoinConfig struct {
	MomHost               string
	MomPort               int
	InputQueue            string
	InputTopic            string
	InputExchangeName     string
	ControlExchange       string
	OutputExchangeName    string
	NextFaseWorkersAmount int
	NextFaseWorkersPrefix string
}

type Join struct {
	inputQueue      middleware.Middleware
	ControlExchange middleware.Middleware
	outputExchange  middleware.Middleware
	config          JoinConfig
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	inputQueue, err := middleware.NewQueueMiddleware(config.InputQueue, connSettings) // FALTA ASOCIARLA AL TOPIC
	if err != nil {
		return nil, err
	}
	inputQueue.BindToTopics(config.InputExchangeName, config.InputTopic)

	outputExchange, err := middleware.CreateExchangeMiddleware(config.OutputExchangeName, connSettings) // modifcar la forma en la que se manejan los topics
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:     inputQueue,
		outputExchange: outputExchange,
		config:         config,
	}, nil
}

func (joiner *Join) Run() {
	joiner.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		joiner.handleMessage(&msg, ack, nack)
	})
}

func (joiner *Join) handleMessage(msg *middleware.Message, ack func(), nack func()) {
	clientID, transactionRecords, isEof, err := inner.DeserializeRawTransactionsMessage(msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", clientID)
		nack()
		return
	}

	if isEof {
		if err := joiner.handleEndOfRecordMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err, "clientID", clientID)
			nack()
			return
		}
		ack()
		return
	}
	if err := joiner.handleDataMessage(transactionRecords, clientID); err != nil {
		slog.Error("While handling data message", "err", err, "clientID", clientID)
		nack()
		return
	}
	ack()
}

func (joiner *Join) handleEndOfRecordMessage(clientID int64) error {
	slog.Info("Sent EOF record message, clientID", clientID)
	// se debe propagar entre todos los group workers y estos a todos los brideges analizers
	return joiner.sendOutput([]transaction.Transaction{}, clientID)
}

func (joiner *Join) handleDataMessage(transactionRecords []transaction.Transaction, clientID int64) error {

	// agrupate the transactions based on the origin account
	workerBatches := make(map[int][]transaction.Transaction)

	for _, transaction := range transactionRecords {
		// get the hash for the given transaction
		hash := fnv.New32a()
		hash.Write([]byte(transaction.FromAccount))
		workerIndex := int(hash.Sum32()) % joiner.config.NextFaseWorkersAmount

		// acumulate the transactions
		workerBatches[workerIndex] = append(workerBatches[workerIndex], transaction)
	}

	// Send the batch to the corresponding bridgematcher
	for workerIndex, batch := range workerBatches {
		topic := fmt.Sprintf("%s_%d", joiner.config.NextFaseWorkersPrefix, workerIndex)
		if err := joiner.sendOutput(batch, clientID, topic); err != nil {
			return fmt.Errorf("error sending batch to worker %d: %w", workerIndex, err)
		}
	}

	return nil
}

func (joiner *Join) sendOutput(transactionRecords []transaction.Transaction, clientID int64, destinationTopic string) error {

	// modificar para que cada output se envie a un determinado topic / bridge
	message, err := inner.SerializeMessage(clientID, transactionRecords)
	if err != nil {
		slog.Debug("While serializing data message", "err", err, "clientID", clientID)
		return err
	}
	if err := joiner.outputExchange.Send(*message, destinationTopic); err != nil {
		slog.Debug("While sending data message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}
