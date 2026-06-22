package group

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"

	"tp_distribuidos/common/datasaver"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
	"tp_distribuidos/common/worker"
)

const FANOUT = ""

type GroupConfig struct {
	ID                    int
	WorkerPrefix          string
	MomHost               string
	MomPort               int
	InputQueue            string
	InputTopic            string
	InputExchangeName     string
	ControlExchangeName   string
	OutputExchangeName    string
	NextFaseWorkersAmount int
	NextFaseWorkersPrefix string
	DateFilterAmount      int
}

type Group struct {
	inputQueue          middleware.Middleware
	controlExchange     middleware.Middleware
	outputExchange      middleware.ExchangeMiddleware
	config              GroupConfig
	eofCounter          map[int64]map[string]int
	controlMutex        sync.Mutex
	precalculatedTopics []string
	datasaver           *datasaver.DataSaver
	handleFunctions     worker.MessageHandlerMap
}

func NewGroupWorker(config GroupConfig) (*Group, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	// input - transacciones
	inputQueue, err := middleware.NewQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputQueue.BindToTopics(config.InputExchangeName, config.InputTopic)
	myKeyControl := fmt.Sprintf("%s_%d", config.WorkerPrefix, config.ID)
	// input - control (particularmente EOF del cliente)
	controlExchange, err := middleware.NewExchangeMiddleware(config.ControlExchangeName, []string{FANOUT}, connSettings, myKeyControl) // control
	if err != nil {
		return nil, err
	}

	// output
	outputExchange, err := middleware.NewDinamicExchangeMiddleware(config.OutputExchangeName, connSettings) // modifcar la forma en la que se manejan los topics
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	// para optimizar un poco el tiempo
	precalculatedTopics := make([]string, config.NextFaseWorkersAmount)
	for i := 0; i < config.NextFaseWorkersAmount; i++ {
		precalculatedTopics[i] = fmt.Sprintf("%s_%d", config.NextFaseWorkersPrefix, i)
	}

	// para persistir la info ante posibles caidas
	//se podria agregar el nombre de del archivo de restauracion como var de entorno
	dataSaver, err := datasaver.NewDataSaver(fmt.Sprintf("/persistence/group_%d", config.ID), LOGS_UNTIL_CHECKPOINT)
	if err != nil {
		return nil, err
	}

	g := &Group{
		inputQueue:          inputQueue,
		outputExchange:      *outputExchange,
		controlExchange:     controlExchange,
		config:              config,
		eofCounter:          make(map[int64]map[string]int),
		controlMutex:        sync.Mutex{},
		precalculatedTopics: precalculatedTopics,
		datasaver:           dataSaver,
	}
	g.handleFunctions = worker.MessageHandlerMap{ // para no tener q crear el struct en cada recepcion de msg
		inner.EndOfRecords:     g.handleEndOfRecordMessage,
		inner.TransactionBatch: g.handleTransactionBatchMessage,
	}
	return g, nil
}

func (groupWorker *Group) Run() {
	done := make(chan struct{})

	go func() {
		groupWorker.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			groupWorker.controlMutex.Lock()
			groupWorker.handleMessage(&msg, ack, nack)
			groupWorker.controlMutex.Unlock()
		})
		close(done)
	}()

	groupWorker.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		groupWorker.handleMessage(&msg, ack, nack)
	})

	<-done
}

func (g *Group) handleMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	if err := worker.HandleMessageV2(middlewareMsg, g.handleFunctions); err != nil {
		nack()
		return
	}

	g.datasaver.Save(middlewareMsg, g) // persistencia de datos
	ack()
}

// --------------- TransactionBatch ---------------

func (groupWorker *Group) handleTransactionBatchMessage(clientID int64, data []interface{}) error {
	transactionRecords, err := inner.DeserializeTransactionBatch(data)
	if err != nil {
		slog.Error("While deserializing transactions from message", "err", err, "clientID", clientID)
		return err
	}

	// transacciones para cada worker de la proxima fase
	//slog.Info("Received Tansaction batch from ", "clientID", clientID)
	workerByBatches := make(map[int][]transaction.Transaction)

	hash := fnv.New32a()
	for _, transaction := range transactionRecords {
		if transaction.FromAccount == transaction.ToAccount && transaction.FromBank == transaction.ToBank {
			continue
		}

		hash.Reset()
		hash.Write([]byte(transaction.FromAccount))
		workerIndex := int(hash.Sum32()) % groupWorker.config.NextFaseWorkersAmount

		// vamos acumulando transacciones a pasar
		workerByBatches[workerIndex] = append(workerByBatches[workerIndex], transaction)
	}

	// enviamos cada batch al bridge correspondiente
	for workerIndex, batch := range workerByBatches {

		if err := groupWorker.sendTransactions(
			clientID,
			batch,
			groupWorker.precalculatedTopics[workerIndex]); err != nil {
			return fmt.Errorf("error sending batch to worker %d: %w", workerIndex, err)
		}
	}

	return nil
}

func (groupWorker *Group) sendTransactions(clientID int64, transactionRecords []transaction.Transaction, topic string) error {
	//slog.Info("Sending Tansactions from ", "clientID", clientID, " to topic: ", topic)

	message, err := inner.SerializeAccountsMessage(clientID, transactionRecords)
	if err != nil {
		slog.Info("While serializing data message", "err", err, "clientID", clientID)
		return err
	}
	if err := groupWorker.outputExchange.SendToTopic(*message, topic); err != nil {
		slog.Info("While sending data message", "err", err, "clientID", clientID)
		return err
	}
	//slog.Info("Batch enviado", "destinatario", topic)
	return nil
}

// --------------- EndOfRecords ---------------

func (groupWorker *Group) handleEndOfRecordMessage(clientID int64, data []interface{}) error {

	// se debe propagar entre todos los group workers y estos a todos los bridges analizers
	mustPropagate, sender, err := inner.DeserializeEOR(data)
	if err != nil {
		slog.Error("While deserializing EOR msg", "err", err, "clientID", clientID)
		return err
	}
	slog.Info("Received EOF record message from ", "clientID", clientID, "sender", sender)

	myName := fmt.Sprintf("%s_%d", "group", groupWorker.config.ID)

	groupWorker.controlMutex.Lock()
	if _, ok := groupWorker.eofCounter[clientID]; !ok {
		groupWorker.eofCounter[clientID] = make(map[string]int)
	}
	groupWorker.controlMutex.Unlock()

	if mustPropagate {
		// EOF viene del date_filter, reenviar por controlExchange sin propagación
		msg, err := inner.SerializeEOR(clientID, false, myName)
		if err != nil {
			slog.Info("While serializing EOF message", "err", err, "clientID", clientID)
			return err
		}

		slog.Info("Propagating EOF to other groups", "clientID", clientID, "messageSizeBytes", len(msg.Body))
		groupWorker.controlMutex.Lock()
		if err := groupWorker.controlExchange.Send(*msg); err != nil {
			groupWorker.controlMutex.Unlock()
			return err
		}
		groupWorker.controlMutex.Unlock()
		slog.Info("EOF propagated successfully", "clientID", clientID)
		return nil
	}

	if err = groupWorker.handleClientFinalization(clientID, sender); err != nil {
		return err
	}

	return nil
}

func (groupWorker *Group) handleClientFinalization(clientID int64, sender string) error {
	slog.Info("Received EOF from another group", "clientID", clientID)

	groupWorker.eofCounter[clientID][sender] = 1
	groupWorker.datasaver.Save(EORdata{CliID: clientID, Sender: sender}, groupWorker) // persistencia de datos

	currentEOFCount := len(groupWorker.eofCounter[clientID])
	if currentEOFCount < groupWorker.config.DateFilterAmount {
		slog.Info("Waiting for more EOFs", "clientID", clientID, "received", currentEOFCount, "expected", groupWorker.config.DateFilterAmount)
		return nil
	}

	slog.Info("EOF threshold reached, sending to output", "clientID", clientID)
	myName := fmt.Sprintf("%s_%d", "group", groupWorker.config.ID)
	msg, err := inner.SerializeEOR(clientID, false, myName)
	if err != nil {
		slog.Info("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}

	for i := range groupWorker.config.NextFaseWorkersAmount {
		topic := fmt.Sprintf("%s_%d", groupWorker.config.NextFaseWorkersPrefix, i)
		slog.Info("Sending EOF to next fase", "topic", topic, "messageSizeBytes", len(msg.Body))
		if err := groupWorker.outputExchange.SendToTopic(*msg, topic); err != nil {
			return err
		}
	}
	return nil
}
