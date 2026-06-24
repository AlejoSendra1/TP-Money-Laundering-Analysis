package q5_date_filter

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"tp_distribuidos/common/heatbeat"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
	"tp_distribuidos/common/worker"
)

// Segun el enunciado, early period es de [2022-09-01, 2022-09-05], pero en la notebook usa los de abajo...
const DateMinEarlyPeriod = "2022-09-01"
const DateMaxEarlyPeriod = "2022-09-06"

type Q5DateFilterConfig struct {
	ID                  int
	WorkerID            string
	MomHost             string
	MomPort             int
	InputQueue          string
	InputExchangeName   string
	InputTopic          string
	OutputQueue         string
	InstanceAmount      int
	ControlExchangeName string
}

type Q5DateFilter struct {
	inputQueue     middleware.Middleware
	outputQueue    middleware.Middleware
	controlOutputs []middleware.Middleware
	controlInput   middleware.Middleware
	config         Q5DateFilterConfig
	mu             sync.Mutex
	heartbeat      *heatbeat.HeartbeatSender
}

func NewQ5DateFilter(config Q5DateFilterConfig) (*Q5DateFilter, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}
	if err = inputQueue.BindToTopics(config.InputExchangeName, config.InputTopic); err != nil {
		inputQueue.Close()
		return nil, err
	}

	// Output: queue compartida hacia filter_payment_format
	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	// Control outputs — uno por peer
	var controlOutputs []middleware.Middleware
	for i := 0; i < config.InstanceAmount; i++ {
		if i == config.ID {
			continue
		}
		key := fmt.Sprintf("%s_%d", config.ControlExchangeName, i)
		exchange, err := middleware.CreateExchangeMiddleware(config.ControlExchangeName, []string{key}, connSettings, "") // No consumo, solo envio
		if err != nil {
			inputQueue.Close()
			outputQueue.Close()
			for _, c := range controlOutputs {
				c.Close()
			}
			return nil, fmt.Errorf("creating control output for peer %d: %w", i, err)
		}
		controlOutputs = append(controlOutputs, exchange)
	}

	// Control input
	myControlKey := fmt.Sprintf("%s_%d", config.ControlExchangeName, config.ID)
	controlInput, err := middleware.CreateExchangeMiddleware(config.ControlExchangeName, []string{myControlKey}, connSettings, myControlKey)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		for _, c := range controlOutputs {
			c.Close()
		}
		return nil, fmt.Errorf("creating control input: %w", err)
	}

	hb, err := heatbeat.NewHeartbeatSender(config.WorkerID, connSettings)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		controlInput.Close()
		for _, c := range controlOutputs {
			c.Close()
		}
		return nil, fmt.Errorf("creating heartbeat sender: %w", err)
	}

	return &Q5DateFilter{
		inputQueue:     inputQueue,
		outputQueue:    outputQueue,
		controlOutputs: controlOutputs,
		controlInput:   controlInput,
		config:         config,
		heartbeat:      hb,
	}, nil
}

func (f *Q5DateFilter) handleSigterm() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	<-sigCh
	slog.Info("SIGTERM received")
	f.heartbeat.Stop()
	f.inputQueue.StopConsuming()
	f.controlInput.StopConsuming()
}

func (f *Q5DateFilter) Run() {
	go f.handleSigterm()
	f.heartbeat.Start()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		f.controlInput.StartConsuming(f.handleControlMessage)
	}()

	f.inputQueue.StartConsuming(f.handleMessage)

	f.controlInput.StopConsuming()
	wg.Wait()
	f.close()
}

func (f *Q5DateFilter) handleMessage(middlewareMsg middleware.Message, ack, nack func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	err := worker.HandleMessageV2(
		&middlewareMsg,
		worker.MessageHandlerMap{
			inner.EndOfRecords:     f.handleEndOfRecords,
			inner.TransactionBatch: f.handleTransactionBatch,
		},
	)
	if err != nil {
		nack()
		return
	}
	ack()
}

func (f *Q5DateFilter) handleEndOfRecords(clientID int64, data []interface{}) error {
	slog.Info("EOF received from upstream, notifying peers and forwarding", "clientID", clientID)
	if err := f.sendControlEOF(clientID); err != nil {
		slog.Error("Sending control EOF to peers", "err", err)
		return err
	}
	return f.sendEOF(clientID)
}

func (f *Q5DateFilter) handleTransactionBatch(clientID int64, data []interface{}) error {
	transactions, err := inner.DeserializeTransactionBatch(data)
	if err != nil {
		slog.Error("Deserializing transactions", "err", err, "clientID", clientID)
		return err
	}
	return f.handleDataMessage(transactions, clientID)
}

// handleControlMessage: un peer recibió el EOF del upstream. Enviamos nuestro propio EOF.
func (f *Q5DateFilter) handleControlMessage(msg middleware.Message, ack, nack func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	err := worker.HandleMessageV2(
		&msg,
		worker.MessageHandlerMap{
			inner.EndOfRecords: f.handleControlEOF,
		},
	)
	if err != nil {
		nack()
		return
	}
	ack()
}

func (f *Q5DateFilter) handleControlEOF(clientID int64, data []interface{}) error {
	slog.Info("Control EOF from peer — sending own EOF", "clientID", clientID)
	return f.sendEOF(clientID)
}

func (f *Q5DateFilter) sendControlEOF(clientID int64) error {
	msg, err := inner.SerializeEOR(clientID, false, fmt.Sprintf("%d", f.config.ID))
	if err != nil {
		return fmt.Errorf("serializing control EOF: %w", err)
	}
	for i, ctrl := range f.controlOutputs {
		if err := ctrl.Send(*msg); err != nil {
			return fmt.Errorf("sending control EOF to peer %d: %w", i, err)
		}
	}
	slog.Info("Control EOF sent to all peers", "clientID", clientID)
	return nil
}

func (f *Q5DateFilter) handleDataMessage(records []transaction.Transaction, clientID int64) error {
	var filtered []transaction.Transaction
	for _, tx := range records {
		date := tx.Timestamp.UTC().Format("2006-01-02")
		if date >= DateMinEarlyPeriod && date < DateMaxEarlyPeriod {
			filtered = append(filtered, tx)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return f.sendOutput(filtered, clientID)
}

func (f *Q5DateFilter) sendOutput(records []transaction.Transaction, clientID int64) error {
	message, err := inner.SerializeMessage(clientID, records)
	if err != nil {
		return err
	}
	return f.outputQueue.Send(*message)
}

func (f *Q5DateFilter) sendEOF(clientID int64) error {
	message, err := inner.SerializeEOR(clientID, true, fmt.Sprintf("%d", f.config.ID))
	if err != nil {
		return err
	}
	return f.outputQueue.Send(*message)
}

func (f *Q5DateFilter) close() {
	f.inputQueue.Close()
	f.outputQueue.Close()
	f.controlInput.Close()
	for _, c := range f.controlOutputs {
		c.Close()
	}
}
