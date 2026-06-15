package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"tp_distribuidos/common/batch_utils"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

type SumConfig struct {
	Id                   int
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

type Sum struct {
	inputQueue      middleware.Middleware
	outputExchange  middleware.Middleware
	controlExchange middleware.Middleware
	eofCounter      map[int64]batch_utils.Set[string]
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
	return &Sum{
		inputQueue:      inputQueue,
		outputExchange:  outputExchange,
		controlExchange: controlExchange,
		config:          config,
		eofCounter:      make(map[int64]batch_utils.Set[string]),
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
	msg, err := inner.DeserializeMessage(middlewareMsg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", msg.ClientID)
		nack()
		return
	}

	var processErr error // Variable para atrapar los errores del switch
	switch msg.MsgType {
	case inner.EndOfRecords:
		_, sender, err := inner.DeserializeEOR(msg.Data)
		if err == nil {
			processErr = sum.handleEndOfRecordMessage(msg.ClientID, sender)
		} else {
			processErr = err
		}
	case inner.TransactionBatch:
		transactions, err := inner.DeserializeTransactionBatch(msg.Data)
		if err == nil {
			processErr = sum.handleDataMessage(transactions, msg.ClientID)
		} else {
			processErr = err
		}
	default:
		processErr = fmt.Errorf("no function could handle this message type")
	}

	if processErr != nil {
		slog.Error("Failed to process message", "err", processErr, "clientID", msg.ClientID)
		nack()
		return
	}
	ack()
}

func (sum *Sum) handleEndOfRecordMessage(clientID int64, sender string) error {
	slog.Info("Received End Of Records message", "clientID", clientID)
	msg, err := inner.SerializeEOF(clientID, false, sender)
	if err != nil {
		slog.Info("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}
	if err := sum.controlExchange.Send(*msg); err != nil {
		slog.Info("While sending EOF message to other instances", "err", err, "clientID", clientID)
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
	sum.mu.Lock()
	if _, exist := sum.eofCounter[clientID]; !exist {
		slog.Info("Client new arrived", "clientID", clientID)
		sum.eofCounter[clientID] = batch_utils.NewSet[string]()
	}
	sum.mu.Unlock()

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
	msg, err := inner.DeserializeMessage(middlewareMsg)
	if err != nil {
		slog.Error("While deserializing control message", "err", err)
		nack()
		return
	}
	_, sender, err := inner.DeserializeEOR(msg.Data)
	if err != nil {
		slog.Error("While deserializing control message", "err", err, "clientID", msg.ClientID)
		nack()
		return
	}

	sum.mu.Lock()
	sum.eofCounter[msg.ClientID].Add(sender)
	if uint8(sum.eofCounter[msg.ClientID].Size()) != sum.config.DateFilterAmount {
		slog.Debug("Waiting for remaining EOFs")
		ack()
		sum.mu.Unlock()
		return
	}
	delete(sum.eofCounter, msg.ClientID)
	sum.mu.Unlock()

	msgToSend, err := inner.SerializeEOF(msg.ClientID, false, fmt.Sprintf("%d", sum.config.Id))
	if err != nil {
		slog.Info("While serializing EOF message", "err", err, "clientID", msg.ClientID)
		return
	}
	if err := sum.sendToOutputExchange([]string{}, msgToSend); err != nil {
		slog.Info("While sending EOF message to promediator", "err", err, "clientID", msg.ClientID)
		return
	}
	slog.Info("Sent EOF", "clientID", msg.ClientID)
	ack()
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
