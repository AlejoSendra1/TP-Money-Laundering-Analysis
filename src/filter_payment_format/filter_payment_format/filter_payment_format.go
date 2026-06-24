package filter_payment_format

import (
	"fmt"
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

const LogsUntilCheckpoint = 250

var allowedPaymentFormats = map[string]bool{
	"Wire": true,
	"ACH":  true,
}

type FilterPaymentFormatConfig struct {
	ID                   int
	WorkerID             string
	MomHost              string
	MomPort              int
	InputQueue           string
	OutputQueue          string
	FilterAmount         int
	USDFilterAmount      int // cantidad de instancias q5_date_filter (upstream)
	FilterPaymentControl string
	ControlTopic         string
}

type CheckpointData struct {
	EofCounter      map[int64]batch_utils.Set[string] `json:"eofCounter"`
	FinishedClients batch_utils.Set[int64]            `json:"finishedClients"`
}

// FilterPaymentFormat filters transactions by payment format (Wire, ACH)
type FilterPaymentFormat struct {
	config          FilterPaymentFormatConfig
	inputQueue      middleware.Middleware
	outputQueue     middleware.Middleware
	controlExchange middleware.Middleware
	heartbeat       *heatbeat.HeartbeatSender
	mutex           sync.Mutex
	eofCounter      map[int64]batch_utils.Set[string]
	finishedClients batch_utils.Set[int64]
	dataSaver       *datasaver.DataSaver
}

func NewFilterPaymentFormat(config FilterPaymentFormatConfig) (*FilterPaymentFormat, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating input queue: %w", err)
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, fmt.Errorf("creating output queue: %w", err)
	}

	// Exchange broadcast: igual al patron de date_filter.
	// Todos los instances subscriben al mismo topic, incluyendo el que envia.
	myControlQueue := fmt.Sprintf("%s_%d", config.FilterPaymentControl, config.ID)
	controlExchange, err := middleware.CreateExchangeMiddleware(
		config.FilterPaymentControl,
		[]string{config.ControlTopic},
		connSettings,
		myControlQueue,
	)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		return nil, fmt.Errorf("creating control exchange: %w", err)
	}

	hb, err := heatbeat.NewHeartbeatSender(config.WorkerID, connSettings)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		controlExchange.Close()
		return nil, fmt.Errorf("creating heartbeat sender: %w", err)
	}

	dataSaver, err := datasaver.NewDataSaver(fmt.Sprintf("/persistence/filter_payment_format_%d", config.ID), LogsUntilCheckpoint)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		controlExchange.Close()
		return nil, fmt.Errorf("creating data saver: %w", err)
	}

	return &FilterPaymentFormat{
		config:          config,
		inputQueue:      inputQueue,
		outputQueue:     outputQueue,
		controlExchange: controlExchange,
		heartbeat:       hb,
		eofCounter:      make(map[int64]batch_utils.Set[string]),
		finishedClients: make(batch_utils.Set[int64]),
		dataSaver:       dataSaver,
	}, nil
}

func (filter *FilterPaymentFormat) GetCheckpointData() any {
	return CheckpointData{
		EofCounter:      filter.eofCounter,
		FinishedClients: filter.finishedClients,
	}
}

func (filter *FilterPaymentFormat) Restaurate() error {
	var checkpoint CheckpointData
	thereIsCheckpoint, err := filter.dataSaver.GetRestaurationCheckpoint(&checkpoint)
	if err != nil {
		return err
	}
	if thereIsCheckpoint {
		slog.Info("Restaurating filter_payment_format from checkpoint")
		filter.eofCounter = checkpoint.EofCounter
		filter.finishedClients = checkpoint.FinishedClients
	}

	var savedMsg middleware.Message
	for {
		hasLogs, err := filter.dataSaver.GetDataFromLogs(&savedMsg)
		if err != nil {
			return err
		}
		if !hasLogs {
			break
		}
		if err := worker.HandleMessageV2(&savedMsg, worker.MessageHandlerMap{
			inner.EndOfRecords: filter.handleEOFLogic,
		}); err != nil {
			return err
		}
	}
	return nil
}

// Run starts the worker. Returns once processing finishes or SIGTERM is received.
func (filter *FilterPaymentFormat) Run() {
	go filter.handleSigterm()
	filter.heartbeat.Start()

	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		filter.controlExchange.StartConsuming(filter.handleControlMessage)
	}()

	filter.inputQueue.StartConsuming(filter.handleMessage)

	filter.controlExchange.StopConsuming()
	waitGroup.Wait()
	filter.close()
}

func (filter *FilterPaymentFormat) handleSigterm() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM)
	<-signalChannel
	slog.Info("SIGTERM received, stopping consumers")
	filter.heartbeat.Stop()
	filter.inputQueue.StopConsuming()
	filter.controlExchange.StopConsuming()
}

// handleMessage processes messages from the shared input queue.
func (filter *FilterPaymentFormat) handleMessage(msg middleware.Message, ack, nack func()) {
	filter.mutex.Lock()
	defer filter.mutex.Unlock()
	err := worker.HandleMessageV2(
		&msg,
		worker.MessageHandlerMap{
			inner.EndOfRecords:     filter.handleEOF,
			inner.TransactionBatch: filter.handleTransactionBatch,
		},
	)
	if err != nil {
		nack()
		return
	}
	ack()
}

func (filter *FilterPaymentFormat) handleTransactionBatch(clientID int64, data []interface{}) error {
	transactions, err := inner.DeserializeTransactionBatch(data)
	if err != nil {
		slog.Error("Deserializing transaction batch", "err", err, "clientID", clientID)
		return err
	}
	return filter.processData(clientID, transactions)
}

func (filter *FilterPaymentFormat) handleEOF(clientID int64, data []interface{}) error {
	_, sender, err := inner.DeserializeEOR(data)
	if err != nil {
		slog.Error("Deserializing EOF", "err", err, "clientID", clientID)
		return err
	}
	slog.Info("EOF received from upstream, forwarding to control exchange", "client_id", clientID, "sender", sender)
	msg, err := inner.SerializeEOR(clientID, false, sender)
	if err != nil {
		return fmt.Errorf("serializing EOF for control: %w", err)
	}
	return filter.controlExchange.Send(*msg)
}

// handleEOFLogic adds the sender to the eofCounter and forwards EOF downstream when all received.
// Used both in normal flow and during Restaurate.
func (filter *FilterPaymentFormat) handleEOFLogic(clientID int64, data []interface{}) error {
	_, sender, err := inner.DeserializeEOR(data)
	if err != nil {
		slog.Error("Deserializing EOF in logic", "err", err, "clientID", clientID)
		return err
	}

	if filter.finishedClients.Contains(clientID) {
		slog.Info("Client already finished, ignoring EOF", "clientID", clientID, "sender", sender)
		return nil
	}

	if filter.eofCounter[clientID] == nil {
		filter.eofCounter[clientID] = batch_utils.NewSet[string]()
	}
	filter.eofCounter[clientID].Add(sender)

	slog.Info("EOF accumulated", "client_id", clientID, "sender", sender,
		"count", filter.eofCounter[clientID].Size(), "expected", filter.config.USDFilterAmount)

	if filter.eofCounter[clientID].Size() != filter.config.USDFilterAmount {
		return nil
	}

	delete(filter.eofCounter, clientID)
	filter.finishedClients.Add(clientID)
	return filter.forwardEOF(clientID)
}

// handleControlMessage processes EOF notifications from peer filter_payment_format instances.
func (filter *FilterPaymentFormat) handleControlMessage(msg middleware.Message, ack, nack func()) {
	filter.mutex.Lock()
	defer filter.mutex.Unlock()
	err := worker.HandleMessageV2(
		&msg,
		worker.MessageHandlerMap{
			inner.EndOfRecords: filter.handleEOFLogic,
		},
	)
	if err != nil {
		nack()
		return
	}
	filter.dataSaver.Save(&msg, filter)
	ack()
}

// processData filters transactions by payment format and sends one batch per format directly downstream.
func (filter *FilterPaymentFormat) processData(clientID int64, transactions []transaction.Transaction) error {
	byFormat := make(map[string][]transaction.PaymentRecord)
	for _, tx := range transactions {
		if !allowedPaymentFormats[tx.PaymentFormat] {
			slog.Debug("Discarding transaction — disallowed payment format",
				"client_id", clientID, "payment_format", tx.PaymentFormat)
			continue
		}
		byFormat[tx.PaymentFormat] = append(byFormat[tx.PaymentFormat], transaction.PaymentRecord{
			Timestamp:     tx.Timestamp,
			Amount:        tx.Amount,
			Currency:      tx.Currency,
			PaymentFormat: tx.PaymentFormat,
		})
	}
	for _, records := range byFormat {
		if err := filter.sendOutput(clientID, records); err != nil {
			return err
		}
	}
	return nil
}

func (filter *FilterPaymentFormat) sendOutput(clientID int64, records []transaction.PaymentRecord) error {
	msg, err := inner.SerializePaymentRecordMessage(clientID, records)
	if err != nil {
		return fmt.Errorf("serializing output batch: %w", err)
	}
	return filter.outputQueue.Send(*msg)
}

// forwardEOF sends an EOF marker downstream.
func (filter *FilterPaymentFormat) forwardEOF(clientID int64) error {
	msg, err := inner.SerializePaymentRecordMessage(clientID, []transaction.PaymentRecord{})
	if err != nil {
		return fmt.Errorf("serializing EOF: %w", err)
	}
	if err := filter.outputQueue.Send(*msg); err != nil {
		return fmt.Errorf("forwarding EOF: %w", err)
	}
	slog.Info("EOF forwarded downstream", "client_id", clientID)
	return nil
}

func (filter *FilterPaymentFormat) close() {
	filter.inputQueue.Close()
	filter.outputQueue.Close()
	filter.controlExchange.Close()
}
