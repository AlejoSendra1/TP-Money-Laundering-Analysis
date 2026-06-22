package q3_amount_filter

import (
	"fmt"
	"log/slog"
	"sync"
	"tp_distribuidos/common/batch_utils"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
	"tp_distribuidos/common/worker"
)

type Q3AmountFilterConfig struct {
	Id                       int
	MomHost                  string
	MomPort                  int
	InputPromediatorExchange string
	InputPromediatorTopic    string
	InputQueue               string
	NotificationExchange     string
	NotificationTopic        string
	ControlExchange          string
	ControlTopic             string
	PromediatorAmount        int
	TransactionsSaverAmount  int
	OutputQueue              string
}

type CheckpointData struct {
	EofCounterAvg map[int64]batch_utils.Set[string] `json:"eofCounterAvg"`
	EofCounterTs  map[int64]batch_utils.Set[string] `json:"eofCounterTs"`
	Averages      map[int64]map[string]float64      `json:"averages"`
}

type Q3AmountFilter struct {
	promediatorExchange  middleware.Middleware
	inputQueue           middleware.Middleware
	outputQueue          middleware.Middleware
	notificationExchange middleware.Middleware
	controlExchange      middleware.Middleware
	eofCounterAvg        map[int64]batch_utils.Set[string]
	eofCounterTs         map[int64]batch_utils.Set[string]
	averages             map[int64]map[string]float64
	config               Q3AmountFilterConfig
	mu                   sync.Mutex
}

func NewQ3AmountFilter(config Q3AmountFilterConfig) (*Q3AmountFilter, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	myKeyInputPromediator := fmt.Sprintf("%s_%d", config.InputPromediatorExchange, config.Id)
	inputPromediator, err := middleware.CreateExchangeMiddleware(config.InputPromediatorExchange, []string{config.InputPromediatorTopic}, connSettings, myKeyInputPromediator)
	if err != nil {
		return nil, err
	}

	inputTransactionSaver, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		inputPromediator.Close()
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputPromediator.Close()
		inputTransactionSaver.Close()
		return nil, err
	}
	myKeyNotification := fmt.Sprintf("%s_%d", config.NotificationTopic, config.Id)
	notificationExchange, err := middleware.CreateExchangeMiddleware(config.NotificationExchange, []string{config.NotificationTopic}, connSettings, myKeyNotification)
	if err != nil {
		inputPromediator.Close()
		inputTransactionSaver.Close()
		outputQueue.Close()
		return nil, err
	}

	myKeyControl := fmt.Sprintf("%s_%d", config.ControlExchange, config.Id)
	controlExchange, err := middleware.CreateExchangeMiddleware(config.ControlExchange, []string{config.ControlTopic}, connSettings, myKeyControl)
	if err != nil {
		inputPromediator.Close()
		inputTransactionSaver.Close()
		outputQueue.Close()
		notificationExchange.Close()
		return nil, err
	}

	return &Q3AmountFilter{
		promediatorExchange:  inputPromediator,
		inputQueue:           inputTransactionSaver,
		outputQueue:          outputQueue,
		notificationExchange: notificationExchange,
		controlExchange:      controlExchange,
		averages:             make(map[int64]map[string]float64),
		eofCounterAvg:        make(map[int64]batch_utils.Set[string]),
		eofCounterTs:         make(map[int64]batch_utils.Set[string]),
		config:               config,
	}, nil
}

func (q3AmountFilter *Q3AmountFilter) GetCheckpointData() any {
	slog.Info("State saved",
		"eofCounterAvg", q3AmountFilter.eofCounterAvg,
		"eofCounterTs", q3AmountFilter.eofCounterTs,
		"averages", q3AmountFilter.averages)
	return CheckpointData{
		EofCounterAvg: q3AmountFilter.eofCounterAvg,
		EofCounterTs:  q3AmountFilter.eofCounterTs,
		Averages:      q3AmountFilter.averages,
	}
}

func (q3AmountFilter *Q3AmountFilter) Run() {
	go q3AmountFilter.promediatorExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		q3AmountFilter.handlePromediatorMessage(&msg, ack, nack)
	})

	go q3AmountFilter.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		q3AmountFilter.handleControlMessage(&msg, ack, nack)
	})

	q3AmountFilter.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		q3AmountFilter.handleTransactionSaverMessage(&msg, ack, nack)
	})
}

func (q3AmountFilter *Q3AmountFilter) handlePromediatorMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	err := worker.HandleMessageV2(
		middlewareMsg,
		worker.MessageHandlerMap{
			inner.EndOfRecords:         q3AmountFilter.handlePaymentEndOfRecordsWrapper,
			inner.PaymentFormatAverage: q3AmountFilter.handlePaymentFormatAverageWrapper,
		},
	)
	if err != nil {
		nack()
		return
	}
	ack()
}

func (q3AmountFilter *Q3AmountFilter) handleControlMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	err := worker.HandleMessageV2(
		middlewareMsg,
		worker.MessageHandlerMap{
			inner.EndOfRecords: q3AmountFilter.handleControlEndOfRecodsWrapper,
		},
	)
	if err != nil {
		nack()
		return
	}
	ack()
}

func (q3AmountFilter *Q3AmountFilter) handleTransactionSaverMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	err := worker.HandleMessageV2(
		middlewareMsg,
		worker.MessageHandlerMap{
			inner.EndOfRecords:              q3AmountFilter.handleTransactionsSaverEnfOfRecordsWrapper,
			inner.ThresholdFilteredTransfer: q3AmountFilter.handleThresholdFilteredTransferWrapper,
		},
	)
	if err != nil {
		nack()
		return
	}
	ack()
}

func (q3AmountFilter *Q3AmountFilter) handlePaymentEndOfRecordsWrapper(clientID int64, data []interface{}) error {
	_, sender, err := inner.DeserializeEOR(data)
	if err != nil {
		slog.Error("While deserializing EOR msg", "err", err, "clientID", clientID)
		return err
	}
	if err = q3AmountFilter.handlePromediatorEndOfRecordMessage(clientID, sender); err != nil {
		slog.Info("While handling end of record message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}

func (q3AmountFilter *Q3AmountFilter) handlePaymentFormatAverageWrapper(clientID int64, data []interface{}) error {
	paymentFormatAverageRecords, err := inner.DeserializePaymentFormatAverageMessage(data)
	if err != nil {
		slog.Info("While deserializing message", "err", err, "clientID", clientID)
		return err
	}
	q3AmountFilter.handlePromediatorDataMessage(paymentFormatAverageRecords, clientID)
	return nil
}

func (q3AmountFilter *Q3AmountFilter) handlePromediatorEndOfRecordMessage(clientID int64, sender string) error {
	slog.Info("Averages EOF arrived from promediator", "clientID", clientID)
	needNotify := false
	q3AmountFilter.mu.Lock()
	q3AmountFilter.eofCounterAvg[clientID].Add(sender)
	if q3AmountFilter.eofCounterAvg[clientID].Size() == q3AmountFilter.config.PromediatorAmount {
		needNotify = true
	}
	q3AmountFilter.mu.Unlock()

	if needNotify {
		// Envio la notificacion
		msgToSend, err := inner.SerializeNotificationAvg(clientID, false, fmt.Sprintf("%d", q3AmountFilter.config.Id))
		if err != nil {
			slog.Info("While serializing notification message", "err", err, "clientID", clientID)
			return err
		}
		if err := q3AmountFilter.notificationExchange.Send(*msgToSend); err != nil {
			slog.Info("While sending notification message", "err", err, "clientID", clientID)
			return err
		}
		slog.Info("Sent notification to transaction saver", "clientID", clientID)
		delete(q3AmountFilter.eofCounterAvg, clientID)
	} else {
		slog.Info("Waiting for more promediator EOFs", "clientID", clientID)
	}
	return nil
}

func (q3AmountFilter *Q3AmountFilter) handlePromediatorDataMessage(paymentFormatAverageRecords []transaction.PaymentFormatAverage, clientID int64) {
	// Ensure initialization happens under lock to avoid races
	q3AmountFilter.mu.Lock()
	if _, ok := q3AmountFilter.averages[clientID]; !ok {
		slog.Info("New average arrived from promediator", "clientID", clientID)
		q3AmountFilter.averages[clientID] = make(map[string]float64)
		q3AmountFilter.eofCounterAvg[clientID] = batch_utils.NewSet[string]()
		q3AmountFilter.eofCounterTs[clientID] = batch_utils.NewSet[string]()
	}
	for _, rec := range paymentFormatAverageRecords {
		q3AmountFilter.averages[clientID][rec.PaymentFormat] = rec.Average / 100.0
	}
	q3AmountFilter.mu.Unlock()
}

func (q3AmountFilter *Q3AmountFilter) handleControlEndOfRecodsWrapper(clientID int64, data []interface{}) error {
	_, sender, err := inner.DeserializeEOR(data)
	if err != nil {
		slog.Error("While deserializing control message", "err", err, "clientID", clientID)
		return err
	}
	q3AmountFilter.mu.Lock()
	_, ok := q3AmountFilter.eofCounterTs[clientID]
	q3AmountFilter.mu.Unlock()

	if !ok {
		// Si me llego un EOF de un cliente que no tengo registro, es porque me mando tarde la data
		slog.Warn("New client arrived from control message, but , wont process", "clientID", clientID)
		return nil
	}
	shouldSendEOF := false
	q3AmountFilter.mu.Lock()
	q3AmountFilter.eofCounterTs[clientID].Add(sender)
	if q3AmountFilter.eofCounterTs[clientID].Size() == q3AmountFilter.config.TransactionsSaverAmount {
		shouldSendEOF = true
	}
	q3AmountFilter.mu.Unlock()

	if shouldSendEOF {
		// para avisar q no hay mas nada
		msg, err := inner.SerializeQueryEOR(clientID, transaction.Query3, fmt.Sprintf("%d", q3AmountFilter.config.Id))
		if err != nil {
			slog.Error("serializing EOF", "error", err)
			return err
		}
		if err := q3AmountFilter.outputQueue.Send(*msg); err != nil {
			slog.Error("sending EOF", "error", err)
			return err
		}
		slog.Info("Sent EOF to gateway", "clientID", clientID)
		q3AmountFilter.cleanupClient(clientID)
	} else {
		slog.Info("Waiting for more transactions saver EOFs", "clientID", clientID)
	}
	return nil
}

func (q3AmountFilter *Q3AmountFilter) handleTransactionsSaverEnfOfRecordsWrapper(clientID int64, data []interface{}) error {
	_, sender, err := inner.DeserializeEOR(data)
	if err != nil {
		slog.Error("While deserializing EOR msg", "err", err, "clientID", clientID)
		return err
	}
	if err := q3AmountFilter.handleTransactionSaverEndOfRecordMessage(clientID, sender); err != nil {
		slog.Info("While handling transaction saver EOF", "err", err, "clientID", clientID)
		return err
	}
	return nil
}

func (q3AmountFilter *Q3AmountFilter) handleThresholdFilteredTransferWrapper(clientID int64, data []interface{}) error {
	transactionRecords, err := inner.DeserializeThresholdFilteredTransferMessage(data)
	if err != nil {
		slog.Info("While deserializing transaction saver message", "err", err, "clientID", clientID)
		return err
	}
	if err = q3AmountFilter.handleTransactionSaverDataMessage(transactionRecords, clientID); err != nil {
		slog.Error("While handling data message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}

func (q3AmountFilter *Q3AmountFilter) handleTransactionSaverEndOfRecordMessage(clientID int64, sender string) error {
	slog.Info("Received End Of Records message", "clientID", clientID)
	msg, err := inner.SerializeEOR(clientID, false, sender)
	if err != nil {
		slog.Info("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}
	if err := q3AmountFilter.controlExchange.Send(*msg); err != nil {
		slog.Info("While sending EOF message to other instances", "err", err, "clientID", clientID)
		return err
	}
	return nil
}

func (q3AmountFilter *Q3AmountFilter) handleTransactionSaverDataMessage(transactionRecords []transaction.ThresholdFilteredTransfer, clientID int64) error {
	q3AmountFilter.mu.Lock()
	_, ok := q3AmountFilter.averages[clientID]
	q3AmountFilter.mu.Unlock()

	if !ok {
		slog.Info("New client arrived from transaction saver without averages", "clientID", clientID)
		return fmt.Errorf("transactions arrived for client %d before receiving averages", clientID)
	}

	if err := q3AmountFilter.processTransactions(transactionRecords, clientID); err != nil {
		return err
	}
	return nil
}

func (q3AmountFilter *Q3AmountFilter) processTransactions(transactionsRecord []transaction.ThresholdFilteredTransfer, clientID int64) error {
	q3AmountFilter.mu.Lock()
	clientAverages, ok := q3AmountFilter.averages[clientID]
	if !ok {
		q3AmountFilter.mu.Unlock()
		slog.Info("No averages for client during processing", "clientID", clientID)
		return fmt.Errorf("no averages for client %d", clientID)
	}
	averagesCopy := make(map[string]float64, len(clientAverages))
	for k, v := range clientAverages {
		averagesCopy[k] = v
	}
	q3AmountFilter.mu.Unlock()

	transactions := make([]transaction.ThresholdFilteredTransfer, 0, len(transactionsRecord))
	for _, tx := range transactionsRecord {
		avg, exists := averagesCopy[tx.PaymentFormat]
		if !exists || avg == 0 {
			slog.Info("Average not found for payment format, skipping transaction", "clientID", clientID, "paymentFormat", tx.PaymentFormat)
			continue
		}
		if tx.Amount < avg {
			transactions = append(transactions, transaction.ThresholdFilteredTransfer{
				FromBank:      tx.FromBank,
				FromAccount:   tx.FromAccount,
				PaymentFormat: tx.PaymentFormat,
				Amount:        tx.Amount,
				Timestamp:     tx.Timestamp,
			})
		}
	}

	if len(transactions) > 0 {
		q3AmountFilter.mu.Lock()
		q3AmountFilter.mu.Unlock()
		batch_utils.SortBatch(transactions, func(a, b transaction.ThresholdFilteredTransfer) bool {
			if a.Timestamp != b.Timestamp {
				return a.Timestamp < b.Timestamp
			}
			return a.Amount > b.Amount
		})
		queryResult := transaction.QueryResult{
			QueryID:      transaction.Query3,
			Transactions: transactions,
		}
		if err := q3AmountFilter.sendOutput(queryResult, clientID); err != nil {
			return err
		}
	}
	return nil
}

func (q3AmountFilter *Q3AmountFilter) cleanupClient(clientID int64) {
	q3AmountFilter.mu.Lock()
	defer q3AmountFilter.mu.Unlock()
	delete(q3AmountFilter.averages, clientID)
	delete(q3AmountFilter.eofCounterAvg, clientID)
	delete(q3AmountFilter.eofCounterTs, clientID)
}

func (q3AmountFilter *Q3AmountFilter) sendOutput(queryResult transaction.QueryResult, clientID int64) error {
	message, err := inner.SerializeQueryResultMessage(clientID, queryResult)
	if err != nil {
		slog.Debug("While serializing data message", "err", err, "clientID", clientID)
		return err
	}
	if err := q3AmountFilter.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending data message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}
