package q3_amount_filter

import (
	"fmt"
	"log/slog"
	"sync"
	"tp_distribuidos/common/messageprotocol/inner"
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
	PromediatorAmount        int
	TransactionsSaverAmount  int
	OutputQueue              string
}

type Q3AmountFilter struct {
	promediatorExchange  middleware.Middleware
	inputQueue           middleware.Middleware
	outputQueue          middleware.Middleware
	notificationExchange middleware.Middleware
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

	notificationExchange, err := middleware.NewExchangeMiddleware(config.NotificationExchange, []string{config.NotificationTopic}, connSettings)
	if err != nil {
		inputPromediator.Close()
		inputTransactionSaver.Close()
		outputQueue.Close()
		return nil, err
	}

	return &Q3AmountFilter{
		promediatorExchange:  inputPromediator,
		inputQueue:           inputTransactionSaver,
		outputQueue:          outputQueue,
		notificationExchange: notificationExchange,
		averages:             make(map[int64]map[string]float64),
		qtyTx:                make(map[int64]int),
		eofCounterAvg:        make(map[int64]int),
		eofCounterTs:         make(map[int64]int),
		config:               config,
	}, nil
}

func (q3AmountFilter *Q3AmountFilter) Run() {
	go q3AmountFilter.promediatorExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		q3AmountFilter.handlePromediatorMessage(&msg, ack, nack)
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

	q3AmountFilter.eofCounterAvg[clientID] += 1
	if q3AmountFilter.eofCounterAvg[clientID] == q3AmountFilter.config.PromediatorAmount {
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

	q3AmountFilter.eofCounterTs[clientID] += 1
	if q3AmountFilter.eofCounterTs[clientID] == q3AmountFilter.config.TransactionsSaverAmount {
		eofResult := transaction.QueryResult{
			QueryID:      transaction.Query3,
			Transactions: []transaction.ThresholdFilteredTransfer{},
		}
		if err := q3AmountFilter.sendOutput(eofResult, clientID); err != nil {
			return err
		}
		slog.Info("Sent EOF to gateway", "clientID", clientID)
		slog.Info("Size transactions sent", "clientID", clientID, "qtyTx", q3AmountFilter.qtyTx[clientID])
	} else {
		slog.Info("Waiting for more transactions saver EOFs", "clientID", clientID)
	}

	return nil
}

func (q3AmountFilter *Q3AmountFilter) handleTransactionSaverDataMessage(transactionRecords []transaction.ThresholdFilteredTransfer, clientID int64) error {
	if _, ok := q3AmountFilter.averages[clientID]; !ok {
		slog.Info("New client arrived from transaction saver", "clientID", clientID)
		q3AmountFilter.averages[clientID] = map[string]float64{}
		slog.Info("ESTA MAL; NO ME DEBERIA LLEGAR LAS TRANSACCIONES DE UN CLIENTE QUE AUN NO SE SU PROMEDIO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
		return fmt.Errorf("transactions arrived for client %d before receiving averages", clientID)
	}

	if err := q3AmountFilter.processTransactions(transactionRecords, clientID); err != nil {
		return err
	}
	return nil
}

func (q3AmountFilter *Q3AmountFilter) processTransactions(transactionsRecord []transaction.ThresholdFilteredTransfer, clientID int64) error {
	clientAverages := q3AmountFilter.averages[clientID]
	transactions := make([]transaction.ThresholdFilteredTransfer, 0, len(transactionsRecord))
	for _, tx := range transactionsRecord {
		avg, exists := clientAverages[tx.PaymentFormat]
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
	q3AmountFilter.qtyTx[clientID] += len(transactions)
	if len(transactions) != 0 {
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
