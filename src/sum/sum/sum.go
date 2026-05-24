package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/messageprotocol/inner/control"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

type SumConfig struct {
	MomHost              string
	MomPort              int
	InputQueue           string
	InputTopic           string
	InputExchangeName    string
	ControlExchangeName  string
	ControlExchangeTopic string
	OutputExchangeName   string
	PromediatorAmount    int
	PromedietorPrefix    string
}

type Sum struct {
	inputQueue         middleware.Middleware
	outputExchange     middleware.Middleware
	controlExchange    middleware.Middleware
	clientTransactions map[int64]map[string]transaction.PaymentFormatAverage
	config             SumConfig
	mu                 sync.Mutex
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
	outputExchange, err := middleware.CreateExchangeMiddleware(config.OutputExchangeName, keysOutput, connSettings)

	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	controlExchange, err := middleware.CreateExchangeMiddleware(config.ControlExchangeName, []string{config.ControlExchangeTopic}, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}
	return &Sum{
		inputQueue:         inputQueue,
		outputExchange:     outputExchange,
		controlExchange:    controlExchange,
		clientTransactions: make(map[int64]map[string]transaction.PaymentFormatAverage),
		config:             config,
	}, nil
}

func (sum *Sum) Run() {
	go sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(&msg, ack, nack)
	})

	sum.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleControlMessage(&msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg *middleware.Message, ack func(), nack func()) {
	clientID, transactionRecords, isEof, err := inner.DeserializeRawTransactionsMessage(msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", clientID)
		nack()
		return
	}

	if isEof {
		if err := sum.handleEndOfRecordMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err, "clientID", clientID)
			nack()
			return
		}
		ack()
		return
	}
	if err := sum.handleDataMessage(transactionRecords, clientID); err != nil {
		slog.Error("While handling data message", "err", err, "clientID", clientID)
		nack()
		return
	}
	ack()
}

func (sum *Sum) handleEndOfRecordMessage(clientID int64) error {
	slog.Info("Received End Of Records message", "clientID", clientID)
	controlEOFMessage := control.ControlMessage{Type: control.TypeEOF, ClientID: clientID}
	message, err := control.SerializeControlMessage(controlEOFMessage)
	if err != nil {
		slog.Debug("While serializing control message", "err", err, "clientID", clientID)
		return err
	}
	if err := sum.controlExchange.Send(*message); err != nil {
		slog.Debug("While sending control message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}

func (sum *Sum) handleDataMessage(transactionRecords []transaction.Transaction, clientID int64) error {
	transactionsByPaymentFormat := make(map[string][]transaction.PaymentFormatAverage)
	for _, tr := range transactionRecords {
		if _, ok := transactionsByPaymentFormat[tr.PaymentFormat]; !ok {
			transactionsByPaymentFormat[tr.PaymentFormat] = make([]transaction.PaymentFormatAverage, 0)
		}
		t := transaction.PaymentFormatAverage{PaymentFormat: tr.PaymentFormat, Average: tr.Amount, Count: 1}
		transactionsByPaymentFormat[tr.PaymentFormat] = append(transactionsByPaymentFormat[tr.PaymentFormat], t)
	}
	sum.mu.Lock()
	defer sum.mu.Unlock()
	if _, exist := sum.clientTransactions[clientID]; !exist {
		slog.Info("Client new arrived", "clientID", clientID)
		sum.clientTransactions[clientID] = make(map[string]transaction.PaymentFormatAverage)
	}
	// Acumulo amount y count para cada formato de pago del cliente
	for paymentFormat, transactions := range transactionsByPaymentFormat {
		if paymentFormatAverage, exist := sum.clientTransactions[clientID][paymentFormat]; !exist {
			aux := transaction.PaymentFormatAverage{PaymentFormat: paymentFormat, Average: 0, Count: 0}
			sum.clientTransactions[clientID][paymentFormat] = sum.addPaymentFormatAverage(aux, transactions)
		} else {
			sum.clientTransactions[clientID][paymentFormat] = sum.addPaymentFormatAverage(paymentFormatAverage, transactions)
		}
	}
	return nil
}

func (sum *Sum) handleControlMessage(msg *middleware.Message, ack func(), nack func()) {
	controlMessage, err := control.DeserializeControlMessage(msg)
	if err != nil {
		slog.Error("While deserializing control message", "err", err)
		nack()
		return
	}
	// Send data
	sum.mu.Lock()
	averages := sum.getPaymentFormats(controlMessage.ClientID)
	sum.mu.Unlock()
	if averages != nil {
		for _, average := range averages {
			key := sum.getKeyForExchange(controlMessage.ClientID, average.PaymentFormat)
			if err := sum.sendToOutputExchange([]string{key}, []transaction.PaymentFormatAverage{average}, controlMessage.ClientID); err != nil {
				slog.Error("While sending payment format average to output exchange", "err", err, "clientID", controlMessage.ClientID, "paymentFormat", average.PaymentFormat)
				nack()
				return
			}
		}
	}
	sum.mu.Lock()
	delete(sum.clientTransactions, controlMessage.ClientID)
	sum.mu.Unlock()

	// Send EOF
	eofMessage, err := inner.SerializePaymentFormatAverageMessage(controlMessage.ClientID, []transaction.PaymentFormatAverage{})
	if err != nil {
		slog.Error("While serializing EOF message", "err", err, "clientID", controlMessage.ClientID)
		nack()
		return
	}
	if err := sum.outputExchange.Send(*eofMessage); err != nil {
		slog.Error("While sending EOF message", "err", err, "clientID", controlMessage.ClientID)
		nack()
		return
	}
	ack()
}

func (sum *Sum) addPaymentFormatAverage(paymentFormatAverage transaction.PaymentFormatAverage, transactions []transaction.PaymentFormatAverage) transaction.PaymentFormatAverage {
	for _, t := range transactions {
		paymentFormatAverage.Average += t.Average
		paymentFormatAverage.Count += t.Count
	}
	return paymentFormatAverage
}

func (sum *Sum) sendToOutputExchange(keys []string, paymentFormatAverageRecords []transaction.PaymentFormatAverage, clientID int64) error {
	message, err := inner.SerializePaymentFormatAverageMessage(clientID, paymentFormatAverageRecords)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}
	if err := sum.outputExchange.SendWithKeys(keys, *message); err != nil {
		slog.Debug("While sending EOF message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}

func (sum *Sum) getKeyForExchange(clientID int64, paymentFormat string) string {
	hash := fnv.New32a()
	hash.Write([]byte(fmt.Sprintf("%d-%s", clientID, paymentFormat)))
	idx := int(hash.Sum32()) % sum.config.PromediatorAmount
	return fmt.Sprintf("%s_%d", sum.config.PromedietorPrefix, idx)
}

func (sum *Sum) getPaymentFormats(clientID int64) []transaction.PaymentFormatAverage {
	paymentFormats, ok := sum.clientTransactions[clientID]
	if !ok {
		return nil
	}
	result := make([]transaction.PaymentFormatAverage, 0, len(paymentFormats))
	for _, avg := range paymentFormats {
		result = append(result, avg)
	}
	return result
}
