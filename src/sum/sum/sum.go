package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"tp_distribuidos/common/batch_utils"
	"tp_distribuidos/common/datasaver"
	"tp_distribuidos/common/heatbeat"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
	"tp_distribuidos/common/worker"
)

const LogsUntilCheckpoint = 1 // Se reciben pocos EOFs

type SumConfig struct {
	Id                   int
	WorkerID             string
	MomHost              string
	MomPort              int
	InputQueue           string
	InputTopic           string
	InputExchangeName    string
	ControlExchangeName  string
	ControlExchangeTopic string
	OutputExchangeName   string
	PromediatorAmount    uint8
	PromedietorPrefix    string
	DateFilterAmount     uint8
}

type CheckpointData struct {
	EofCounter      map[int64]batch_utils.Set[string] `json:"eofCounter"`
	FinishedClients batch_utils.Set[int64]            `json:"finishedClients"`
}

type Sum struct {
	inputQueue      middleware.Middleware
	outputExchange  middleware.Middleware
	controlExchange middleware.Middleware
	eofCounter      map[int64]batch_utils.Set[string]
	finishedClients batch_utils.Set[int64] // Sirve para no procesar un eof de un cliente que ya termino
	config          SumConfig
	mu              sync.Mutex
	dataSaver       *datasaver.DataSaver
	heartbeat       *heatbeat.HeartbeatSender
}

func NewSum(config SumConfig) (*Sum, error) {
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

	keysOutput := []string{}
	for i := range config.PromediatorAmount {
		key := fmt.Sprintf("%s_%d", config.PromedietorPrefix, i)
		keysOutput = append(keysOutput, key)
	}
	outputExchange, err := middleware.CreateExchangeMiddleware(config.OutputExchangeName, keysOutput, connSettings, "") // No consumo, solo envio

	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	controlQueue := fmt.Sprintf("%s_%d", config.ControlExchangeName, config.Id)
	controlExchange, err := middleware.CreateExchangeMiddleware(config.ControlExchangeName, []string{config.ControlExchangeTopic}, connSettings, controlQueue)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}

	dataSaver, err := datasaver.NewDataSaver(fmt.Sprintf("/persistence/sum_%d", config.Id), LogsUntilCheckpoint)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		controlExchange.Close()
		return nil, err
	}

	hb, err := heatbeat.NewHeartbeatSender(config.WorkerID, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		controlExchange.Close()
		return nil, fmt.Errorf("creating heartbeat sender: %w", err)
	}

	return &Sum{
		inputQueue:      inputQueue,
		outputExchange:  outputExchange,
		controlExchange: controlExchange,
		config:          config,
		eofCounter:      make(map[int64]batch_utils.Set[string]),
		finishedClients: make(batch_utils.Set[int64]),
		dataSaver:       dataSaver,
		heartbeat:       hb,
	}, nil
}

func (sum *Sum) GetCheckpointData() any {
	slog.Debug("State saved",
		"eofCounter", sum.eofCounter,
		"finishedClients", sum.finishedClients)
	return CheckpointData{
		EofCounter:      sum.eofCounter,
		FinishedClients: sum.finishedClients,
	}
}

func (sum *Sum) handleSigterm() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	<-sigCh
	slog.Info("SIGTERM received, stopping consumers")
	sum.heartbeat.Stop()
	sum.inputQueue.StopConsuming()
	sum.controlExchange.StopConsuming()
}

func (sum *Sum) Run() {
	go sum.handleSigterm()
	sum.heartbeat.Start()
	go sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(&msg, ack, nack)
	})

	sum.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleControlMessage(&msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	sum.mu.Lock()
	defer sum.mu.Unlock()
	err := worker.HandleMessageV2(
		middlewareMsg,
		worker.MessageHandlerMap{
			inner.EndOfRecords:     sum.handleEndOfRecordsWrapper,
			inner.TransactionBatch: sum.handleTransactionBatchWrapper,
		})
	if err != nil {
		nack()
		return
	}
	ack()
}

func (sum *Sum) handleEndOfRecordsWrapper(clientID int64, data []interface{}) error {
	_, sender, err := inner.DeserializeEOR(data)
	var processErr error
	if err == nil {
		processErr = sum.handleEndOfRecordMessage(clientID, sender)
	} else {
		processErr = err
	}
	return processErr
}

func (sum *Sum) handleTransactionBatchWrapper(clientID int64, data []interface{}) error {
	var processErr error
	transactions, err := inner.DeserializeTransactionBatch(data)
	if err == nil {
		processErr = sum.handleDataMessage(transactions, clientID)
	} else {
		processErr = err
	}
	return processErr
}

func (sum *Sum) handleEndOfRecordMessage(clientID int64, sender string) error {
	slog.Debug("Received End Of Records message", "clientID", clientID)
	msg, err := inner.SerializeEOR(clientID, false, sender)
	if err != nil {
		slog.Error("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}
	if err := sum.controlExchange.Send(*msg); err != nil {
		slog.Error("While sending EOF message to other instances", "err", err, "clientID", clientID)
		return err
	}
	return nil
}

func (sum *Sum) handleDataMessage(transactionRecords []transaction.Transaction, clientID int64) error {
	transactionsByPaymentFormat := make(map[string][]transaction.Transaction)
	for _, tr := range transactionRecords {
		transactionsByPaymentFormat[tr.PaymentFormat] = append(transactionsByPaymentFormat[tr.PaymentFormat], tr)
	}

	// Verifico si es un nuevo cliente o no
	if _, exist := sum.eofCounter[clientID]; !exist {
		slog.Info("Client new arrived", "clientID", clientID)
		sum.eofCounter[clientID] = batch_utils.NewSet[string]()
	}

	// Envio al promediator
	for paymentFormat, transactions := range transactionsByPaymentFormat {
		key := sum.getKeyForExchange(clientID, paymentFormat)
		batch_utils.SortBatch(transactions, func(a, b transaction.Transaction) bool {
			return a.Amount < b.Amount
		})

		message, err := inner.SerializeMessage(clientID, transactions)
		if err != nil {
			slog.Error("While serializing transactions message", "err", err, "clientID", clientID)
			return err
		}
		if err := sum.sendToOutputExchange([]string{key}, message); err != nil {
			slog.Error("While sending transactions message to output exchange", "err", err, "clientID", clientID, "paymentFormat", paymentFormat)
			return err
		}
	}
	return nil
}

func (sum *Sum) handleControlMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	sum.mu.Lock()
	err := worker.HandleMessageV2(
		middlewareMsg,
		worker.MessageHandlerMap{
			inner.EndOfRecords: sum.handleControlMessageWrapper,
		})
	if err != nil {
		nack()
		sum.mu.Unlock()
		return
	}
	sum.mu.Unlock()
	sum.dataSaver.Save(middlewareMsg, sum)
	ack()
}

func (sum *Sum) handleControlMessageWrapper(clientID int64, data []interface{}) error {
	_, sender, err := inner.DeserializeEOR(data)
	if err != nil {
		slog.Error("While deserializing control message", "err", err, "clientID", clientID)
		return err
	}
	if sum.finishedClients.Contains(clientID) {
		slog.Info("Client has already done", "clientID", clientID)
		return nil
	}

	if _, ok := sum.eofCounter[clientID]; !ok {
		slog.Info("EOF arrived before client data (or dont save client new arrived for crash), initializing...", "clientID", clientID)
		sum.eofCounter[clientID] = batch_utils.NewSet[string]()
	}

	sum.eofCounter[clientID].Add(sender)
	if uint8(sum.eofCounter[clientID].Size()) != sum.config.DateFilterAmount {
		slog.Debug("Waiting for remaining EOFs")
		return nil
	}
	delete(sum.eofCounter, clientID)
	sum.finishedClients.Add(clientID)

	msgToSend, err := inner.SerializeEOR(clientID, false, fmt.Sprintf("%d", sum.config.Id))
	if err != nil {
		slog.Info("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}
	if err = sum.sendToOutputExchange([]string{}, msgToSend); err != nil {
		slog.Info("While sending EOF message to promediator", "err", err, "clientID", clientID)
		return err
	}
	slog.Info("Sent EOF", "clientID", clientID)
	return nil
}

func (sum *Sum) sendToOutputExchange(keys []string, message *middleware.Message) error {
	if len(keys) > 0 {
		return sum.outputExchange.SendWithKeys(keys, *message)
	}
	return sum.outputExchange.Send(*message)
}

func (sum *Sum) getKeyForExchange(clientID int64, paymentFormat string) string {
	hash := fnv.New32a()
	hash.Write([]byte(fmt.Sprintf("%d-%s", clientID, paymentFormat)))
	idx := uint8(hash.Sum32()) % sum.config.PromediatorAmount
	return fmt.Sprintf("%s_%d", sum.config.PromedietorPrefix, idx)
}

func (sum *Sum) Restaurate() error {
	var checkpoint CheckpointData
	thereIsCheckpoint, err := sum.dataSaver.GetRestaurationCheckpoint(&checkpoint)
	if err != nil {
		return err
	}

	if thereIsCheckpoint {
		slog.Info("Cargando Sum en base a checkpoint")
		sum.eofCounter = checkpoint.EofCounter
		sum.finishedClients = checkpoint.FinishedClients
		slog.Info("State restaured", "eofCounter", checkpoint.EofCounter, "finishedClients", sum.finishedClients)
	}
	var savedDataVar middleware.Message
	var thereIsLogs bool
	for {
		thereIsLogs, err = sum.dataSaver.GetDataFromLogs(&savedDataVar)
		if err != nil {
			return err
		}
		if !thereIsLogs {
			break
		}
		err = worker.HandleMessageV2(
			&savedDataVar,
			worker.MessageHandlerMap{
				inner.EndOfRecords: sum.handleControlMessageWrapper,
			},
		)
		if err != nil {
			return err
		}
	}
	return nil
}
