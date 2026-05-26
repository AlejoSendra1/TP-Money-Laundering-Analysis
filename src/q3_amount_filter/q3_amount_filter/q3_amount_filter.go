package q3_amount_filter

import (
	"fmt"
	"log/slog"
	"sync"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/messageprotocol/inner/control"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

type Q3AmountFilterConfig struct {
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

type Q3AmountFilter struct {
	promediatorExchange  middleware.Middleware
	inputQueue           middleware.Middleware
	outputQueue          middleware.Middleware
	notificationExchange middleware.Middleware
	controlExchange      middleware.Middleware
	eofCounterAvg        map[int64]int
	eofCounterTs         map[int64]int
	averages             map[int64]map[string]float64
	qtyTx                map[int64]int
	config               Q3AmountFilterConfig
	mu                   sync.Mutex
}

func NewQ3AmountFilter(config Q3AmountFilterConfig) (*Q3AmountFilter, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputPromediator, err := middleware.CreateExchangeMiddleware(config.InputPromediatorExchange, []string{config.InputPromediatorTopic}, connSettings)
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

	notificationExchange, err := middleware.CreateExchangeMiddleware(config.NotificationExchange, []string{config.NotificationTopic}, connSettings)
	if err != nil {
		inputPromediator.Close()
		inputTransactionSaver.Close()
		outputQueue.Close()
		return nil, err
	}

	controlExchange, err := middleware.CreateExchangeMiddleware(config.ControlExchange, []string{config.ControlTopic}, connSettings)
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
		qtyTx:                make(map[int64]int), // Para debug
		eofCounterAvg:        make(map[int64]int),
		eofCounterTs:         make(map[int64]int),
		config:               config,
	}, nil
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

func (q3AmountFilter *Q3AmountFilter) handlePromediatorMessage(msg *middleware.Message, ack func(), nack func()) {
	clientID, paymentFormatAverageRecords, isEof, err := inner.DeserializePaymentFormatAverageMessage(msg)
	if err != nil {
		slog.Info("While deserializing message", "err", err, "clientID", clientID)
		nack()
		return
	}

	if isEof {
		if err := q3AmountFilter.handlePromediatorEndOfRecordMessage(clientID); err != nil {
			slog.Info("While handling end of record message", "err", err, "clientID", clientID)
			nack()
			return
		}
		ack()
		return
	}

	q3AmountFilter.handlePromediatorDataMessage(paymentFormatAverageRecords, clientID)
	ack()
}

func (q3AmountFilter *Q3AmountFilter) handlePromediatorEndOfRecordMessage(clientID int64) error {
	slog.Info("Averages EOF arrived from promediator", "clientID", clientID)
	needNotify := false
	q3AmountFilter.mu.Lock()
	q3AmountFilter.eofCounterAvg[clientID] += 1
	if q3AmountFilter.eofCounterAvg[clientID] == q3AmountFilter.config.PromediatorAmount {
		needNotify = true
	}
	q3AmountFilter.mu.Unlock()

	if needNotify {
		msg, err := inner.SerializePaymentFormatAverageMessage(clientID, []transaction.PaymentFormatAverage{})
		if err != nil {
			slog.Error("While serializing notification message", "err", err)
			return err
		}
		if err = q3AmountFilter.notificationExchange.Send(*msg); err != nil {
			slog.Error("While sending notification message", "err", err)
			return err
		}
		slog.Info("Sent notification to transaction saver", "clientID", clientID)
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
		q3AmountFilter.qtyTx[clientID] = 0
		q3AmountFilter.eofCounterAvg[clientID] = 0
		q3AmountFilter.eofCounterTs[clientID] = 0
	}
	for _, rec := range paymentFormatAverageRecords {
		q3AmountFilter.averages[clientID][rec.PaymentFormat] = rec.Average
	}
	q3AmountFilter.mu.Unlock()
}

func (q3AmountFilter *Q3AmountFilter) handleControlMessage(msg *middleware.Message, ack func(), nack func()) {
	controlMessage, err := control.DeserializeControlMessage(msg)
	if err != nil {
		slog.Info("While deserializing control message", "err", err)
		nack()
		return
	}

	slog.Info("Receive EOF from other instance")
	clientID := controlMessage.ClientID

	shouldSendEOF := false
	var qty int
	q3AmountFilter.mu.Lock()
	q3AmountFilter.eofCounterTs[clientID] += 1
	if q3AmountFilter.eofCounterTs[clientID] == q3AmountFilter.config.TransactionsSaverAmount {
		shouldSendEOF = true
		qty = q3AmountFilter.qtyTx[clientID]
	}
	q3AmountFilter.mu.Unlock()

	if shouldSendEOF {
		eofResult := transaction.QueryResult{
			QueryID:      transaction.Query3,
			Transactions: []transaction.ThresholdFilteredTransfer{},
		}
		if err := q3AmountFilter.sendOutput(eofResult, clientID); err != nil {
			return
		}
		slog.Info("Sent EOF to gateway", "clientID", clientID)
		slog.Info("Size transactions sent", "clientID", clientID, "qtyTx", qty)
		q3AmountFilter.cleanupClient(clientID)
	} else {
		slog.Info("Waiting for more transactions saver EOFs", "clientID", clientID)
	}
}

func (q3AmountFilter *Q3AmountFilter) handleTransactionSaverMessage(msg *middleware.Message, ack func(), nack func()) {
	clientID, transactionRecords, isEof, err := inner.DeserializeThresholdFilteredTransferMessage(msg)
	if err != nil {
		slog.Info("While deserializing transaction saver message", "err", err, "clientID", clientID)
		nack()
		return
	}

	if isEof {
		if err := q3AmountFilter.handleTransactionSaverEndOfRecordMessage(clientID); err != nil {
			slog.Info("While handling transaction saver EOF", "err", err, "clientID", clientID)
			nack()
			return
		}
		ack()
		return
	}

	if err = q3AmountFilter.handleTransactionSaverDataMessage(transactionRecords, clientID); err != nil {
		slog.Error("While handling data message", "err", err, "clientID", clientID)
		nack()
		return
	}
	ack()
}

func (q3AmountFilter *Q3AmountFilter) handleTransactionSaverEndOfRecordMessage(clientID int64) error {
	slog.Info("Received Transaction Saver EOF", "clientID", clientID)
	controlEOFMessage := control.ControlMessage{Type: control.TypeEOF, ClientID: clientID}
	message, err := control.SerializeControlMessage(controlEOFMessage)
	if err != nil {
		slog.Debug("While serializing control message", "err", err, "clientID", clientID)
		return err
	}
	if err := q3AmountFilter.controlExchange.Send(*message); err != nil {
		slog.Debug("While sending control message", "err", err, "clientID", clientID)
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
		if tx.Amount < (avg / 100.0) {
			transactions = append(transactions, transaction.ThresholdFilteredTransfer{
				FromBank:      tx.FromBank,
				FromAccount:   tx.FromAccount,
				PaymentFormat: tx.PaymentFormat,
				Amount:        tx.Amount,
			})
		}
	}

	if len(transactions) > 0 {
		q3AmountFilter.mu.Lock()
		q3AmountFilter.qtyTx[clientID] += len(transactions)
		q3AmountFilter.mu.Unlock()

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
	delete(q3AmountFilter.qtyTx, clientID)
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
