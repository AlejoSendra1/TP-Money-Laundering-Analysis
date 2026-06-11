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
	DateFilterAmount     int
}

type Sum struct {
	inputQueue      middleware.Middleware
	outputExchange  middleware.Middleware
	controlExchange middleware.Middleware
	eofCounter      map[int64]int
	config          SumConfig
	mu              sync.Mutex
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
		inputQueue:      inputQueue,
		outputExchange:  outputExchange,
		controlExchange: controlExchange,
		config:          config,
		eofCounter:      make(map[int64]int),
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

func (sum *Sum) handleMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	sum.mu.Lock()
	defer sum.mu.Unlock()
	msg, err := inner.DeserializeMessage(middlewareMsg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", msg.ClientID)
		nack()
		return
	}

	switch msg.MsgType {
	case inner.EndOfRecords:
		if err := sum.handleEndOfRecordMessage(msg.ClientID); err != nil {
			slog.Error("While handling end of record message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
		ack()
		return

	case inner.TransactionBatch:
		transactions, err := inner.DeserializeTransactionBatch(msg.Data)
		if err != nil {
			slog.Error("While deserializing transactions from message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
		if err := sum.handleDataMessage(transactions, msg.ClientID); err != nil {
			slog.Error("While handling data message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
		ack()
	}
}

func (sum *Sum) handleEndOfRecordMessage(clientID int64) error {
	slog.Info("Received End Of Records message", "clientID", clientID)
	controlEOFMessage := control.ControlMessage{Type: control.TypeEOF, ClientID: clientID}
	message, err := control.SerializeControlMessage(controlEOFMessage)
	if err != nil {
		slog.Info("While serializing control message", "err", err, "clientID", clientID)
		return err
	}
	if err := sum.controlExchange.Send(*message); err != nil {
		slog.Info("While sending control message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}

func (sum *Sum) handleDataMessage(transactionRecords []transaction.Transaction, clientID int64) error {
	transactionsByPaymentFormat := make(map[string][]transaction.PaymentFormatAverage)
	for _, tr := range transactionRecords {
		if _, ok := transactionsByPaymentFormat[tr.PaymentFormat]; !ok {
			transactionsByPaymentFormat[tr.PaymentFormat] = make([]transaction.PaymentFormatAverage, 0)
		} else {
			t := transaction.PaymentFormatAverage{PaymentFormat: tr.PaymentFormat, Average: tr.Amount, Count: 1}
			transactionsByPaymentFormat[tr.PaymentFormat] = sum.addPaymentFormatAverage(t, transactionsByPaymentFormat[tr.PaymentFormat])
		}
	}
	if _, exist := sum.eofCounter[clientID]; !exist {
		slog.Info("Client new arrived", "clientID", clientID)
		sum.eofCounter[clientID] = 0
	}
	// Envio al promediator
	for paymentformat, transactions := range transactionsByPaymentFormat {
		keys := []string{sum.getKeyForExchange(clientID, paymentformat)}
		if err := sum.sendToOutputExchange(keys, transactions, clientID); err != nil {
			slog.Error("While sending payment format average to output exchange", "err", err, "clientID", clientID, "paymentFormat", paymentformat)
			return err
		}
	}
	return nil
}

func (sum *Sum) handleControlMessage(msg *middleware.Message, ack func(), nack func()) {
	sum.mu.Lock()
	defer sum.mu.Unlock()

	controlMessage, err := control.DeserializeControlMessage(msg)
	if err != nil {
		slog.Error("While deserializing control message", "err", err)
		nack()
		return
	}
	sum.eofCounter[controlMessage.ClientID] += 1
	if sum.eofCounter[controlMessage.ClientID] != sum.config.DateFilterAmount {
		slog.Info("Received EOF from other instance, waiting for more...")
		ack()
		return
	}
	delete(sum.eofCounter, controlMessage.ClientID)
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
	slog.Info("Sent EOF...", "clientID", controlMessage.ClientID)
	ack()
}

func (sum *Sum) addPaymentFormatAverage(paymentFormatAverage transaction.PaymentFormatAverage, transactions []transaction.PaymentFormatAverage) []transaction.PaymentFormatAverage {
	for _, t := range transactions {
		paymentFormatAverage.Average += t.Average
		paymentFormatAverage.Count += t.Count
	}
	return []transaction.PaymentFormatAverage{paymentFormatAverage}
}

func (sum *Sum) sendToOutputExchange(keys []string, paymentFormatAverageRecords []transaction.PaymentFormatAverage, clientID int64) error {
	message, err := inner.SerializePaymentFormatAverageMessage(clientID, paymentFormatAverageRecords)
	if err != nil {
		slog.Info("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}
	if err := sum.outputExchange.SendWithKeys(keys, *message); err != nil {
		slog.Info("While sending EOF message", "err", err, "clientID", clientID)
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
