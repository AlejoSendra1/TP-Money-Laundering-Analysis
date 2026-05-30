package group

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"

	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
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
	eofCounter          map[int64]int
	controlMutex        sync.Mutex
	precalculatedTopics []string
	counter             int
}

func NewGroupWorker(config GroupConfig) (*Group, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	// input - transacciones
	inputQueue, err := middleware.NewQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputQueue.BindToTopics(config.InputExchangeName, config.InputTopic)

	// input - control (particularmente EOF del cliente)
	controlExchange, err := middleware.NewExchangeMiddleware(config.ControlExchangeName, []string{FANOUT}, connSettings) // control
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

	return &Group{
		inputQueue:          inputQueue,
		outputExchange:      *outputExchange,
		controlExchange:     controlExchange,
		config:              config,
		eofCounter:          make(map[int64]int),
		controlMutex:        sync.Mutex{},
		precalculatedTopics: precalculatedTopics,
		counter:             0,
	}, nil
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

func (groupWorker *Group) handleMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	//slog.Info("Received msg", "body", middlewareMsg.Body)
	msg, err := inner.DeserializeMessage(middlewareMsg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", msg.ClientID)
		nack()
		return
	}

	switch msg.MsgType {
	case inner.EndOfRecords:
		mustPropagate, sender, err := inner.DeserializeEOR(msg.Data)
		if err != nil {
			slog.Error("While deserializing EOR msg", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
		slog.Info("Received EOF record message from ", "clientID", msg.ClientID, "sender", sender)
		if err := groupWorker.handleEndOfRecordMessage(msg.ClientID, mustPropagate); err != nil {
			slog.Error("While handling end of record message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}

	case inner.TransactionBatch:
		//obtenemos las transacciones
		//slog.Info("Received msg", "type", "tranasction batch")
		transactions, err := inner.DeserializeTransactionBatch(msg.Data)
		if err != nil {
			slog.Error("While deserializing transactions from message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}

		// hacemos lo q haya q hacer con las transa
		if err := groupWorker.handleTransactionBatchMessage(msg.ClientID, transactions); err != nil {
			slog.Error("While handling data message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
	default:
		slog.Error("Unexpected msg type received", "err", err, "clientID", msg.ClientID)
	}
	ack()
}

func (groupWorker *Group) handleEndOfRecordMessage(clientID int64, mustPropagate bool) error {

	// se debe propagar entre todos los group workers y estos a todos los bridges analizers
	slog.Info("Received EOF from input or control", "clientID", clientID, "mustPropagate", mustPropagate)

	senderName := fmt.Sprintf("%s_%d", "group", groupWorker.config.ID)

	if mustPropagate {
		// EOF viene del date_filter, reenviar por controlExchange sin propagación
		msg, err := inner.SerializeEOF(clientID, false, senderName)
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

	// mustPropagate == false: EOF viene del controlExchange de otro grupo, contar.
	//No necesito tomar el lock, ya esta tomado
	slog.Info("Received EOF from another group", "clientID", clientID)
	groupWorker.eofCounter[clientID] += 1
	currentEOFCount := groupWorker.eofCounter[clientID]

	slog.Info("EOF counter incremented", "clientID", clientID, "current", currentEOFCount, "expected", groupWorker.config.DateFilterAmount)

	if currentEOFCount < groupWorker.config.DateFilterAmount {
		slog.Info("Waiting for more EOFs", "clientID", clientID, "received", currentEOFCount, "expected", groupWorker.config.DateFilterAmount)
		return nil
	}

	slog.Info("EOF threshold reached, sending to output", "clientID", clientID)
	msg, err := inner.SerializeEOF(clientID, false, senderName)
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
		slog.Info("Cantidad de mensajes enviados al bridge", "val", groupWorker.counter)
	}

	return nil
}

func (groupWorker *Group) handleTransactionBatchMessage(clientID int64, transactionRecords []transaction.Transaction) error {
	groupWorker.controlMutex.Lock()
	if _, ok := groupWorker.eofCounter[clientID]; !ok {
		groupWorker.eofCounter[clientID] = 0
	}
	groupWorker.controlMutex.Unlock()

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
	groupWorker.counter += len(transactionRecords)
	message, err := inner.SerializeMessage(clientID, transactionRecords)
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
