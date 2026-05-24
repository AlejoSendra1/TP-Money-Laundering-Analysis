package promediator

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

type PromediatorConfig struct {
	Id                     int
	MomHost                string
	MomPort                int
	InputExchangeName      string
	OutputExchangeName     string
	SumAmount              int
	PromediatorPrefix      string
	Q3AmountFilterAmount   int
	Q3AmountFilterPrefix   string
	TransactionSaverPrefix string
}

type Promediator struct {
	inputExchange    middleware.Middleware
	outputExchange   middleware.Middleware
	paymentFormatAvg map[int64]map[string]transaction.PaymentFormatAverage
	eofCounter       map[int64]int
	config           PromediatorConfig
}

func NewPromediator(config PromediatorConfig) (*Promediator, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputExchangeName, []string{fmt.Sprintf("%s_%d", config.PromediatorPrefix, config.Id)}, connSettings)
	if err != nil {
		return nil, err
	}

	keysOutput := []string{}
	for i := range config.Q3AmountFilterAmount {
		key := fmt.Sprintf("%s_%d", config.Q3AmountFilterPrefix, i)
		keysOutput = append(keysOutput, key)
	}
	keysOutput = append(keysOutput, config.TransactionSaverPrefix) // Key que le sera util a los transaction saver como notificacion
	outputExchange, err := middleware.CreateExchangeMiddleware(config.OutputExchangeName, keysOutput, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}

	return &Promediator{
		inputExchange:    inputExchange,
		outputExchange:   outputExchange,
		paymentFormatAvg: make(map[int64]map[string]transaction.PaymentFormatAverage),
		eofCounter:       make(map[int64]int),
		config:           config,
	}, nil
}

func (promediator *Promediator) Run() {
	promediator.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		promediator.handleMessage(&msg, ack, nack)
	})
}

func (promediator *Promediator) handleMessage(msg *middleware.Message, ack func(), nack func()) {
	clientID, paymentFormatAverageRecords, isEof, err := inner.DeserializePaymentFormatAverageMessage(msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", clientID)
		nack()
		return
	}

	if isEof {
		if err := promediator.handleEndOfRecordMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err, "clientID", clientID)
			nack()
			return
		}
		ack()
		return
	}
	if err := promediator.handleDataMessage(paymentFormatAverageRecords, clientID); err != nil {
		slog.Error("While handling data message", "err", err, "clientID", clientID)
		nack()
		return
	}
	ack()
}

func (promediator *Promediator) handleEndOfRecordMessage(clientID int64) error {
	slog.Info("Received End Of Records message", "clientID", clientID)
	promediator.eofCounter[clientID]++
	if promediator.eofCounter[clientID] != promediator.config.SumAmount {
		return nil
	}

	slog.Info("All EOFs received, calculating average and sending", "clientID", clientID)
	averages := promediator.getPaymentFormats(clientID)
	if averages != nil {
		for _, average := range averages {
			avg := transaction.PaymentFormatAverage{
				PaymentFormat: average.PaymentFormat,
				Average:       average.Average / float64(average.Count),
				Count:         average.Count}
			key := promediator.getKeyForExchange(clientID, average.PaymentFormat)
			if err := promediator.sendToOutputExchange([]string{key}, []transaction.PaymentFormatAverage{avg}, clientID); err != nil {
				slog.Error("While sending payment format average to output exchange", "err", err, "clientID", clientID, "paymentFormat", average.PaymentFormat)
				return err
			}
			slog.Info("Send average", "clientID", clientID, "avg", avg)
		}
	} else {
		slog.Info("Dont send anything", "clientID", clientID)
	}

	eofMessage, err := inner.SerializePaymentFormatAverageMessage(clientID, []transaction.PaymentFormatAverage{})
	if err != nil {
		return err
	}
	if err = promediator.outputExchange.Send(*eofMessage); err != nil {
		return err
	}
	slog.Info("Sent eof message to q3 amount filter and transaction saver", "clientID", clientID)
	delete(promediator.paymentFormatAvg, clientID)
	delete(promediator.eofCounter, clientID)
	return nil
}

func (promediator *Promediator) handleDataMessage(paymentFormatAverageRecords []transaction.PaymentFormatAverage, clientID int64) error {
	transactionsByPaymentFormat := make(map[string][]transaction.PaymentFormatAverage)
	for _, tr := range paymentFormatAverageRecords {
		if _, ok := transactionsByPaymentFormat[tr.PaymentFormat]; !ok {
			transactionsByPaymentFormat[tr.PaymentFormat] = make([]transaction.PaymentFormatAverage, 0)
		}
		t := transaction.PaymentFormatAverage{PaymentFormat: tr.PaymentFormat, Average: tr.Average, Count: tr.Count}
		transactionsByPaymentFormat[tr.PaymentFormat] = append(transactionsByPaymentFormat[tr.PaymentFormat], t)
	}
	if _, exist := promediator.paymentFormatAvg[clientID]; !exist {
		slog.Info("Client new arrived", "clientID", clientID)
		promediator.paymentFormatAvg[clientID] = make(map[string]transaction.PaymentFormatAverage)
	}
	// Acumulo amount y count para cada formato de pago del cliente
	for paymentFormat, transactions := range transactionsByPaymentFormat {
		if paymentFormatAverage, exist := promediator.paymentFormatAvg[clientID][paymentFormat]; !exist {
			aux := transaction.PaymentFormatAverage{PaymentFormat: paymentFormat, Average: 0, Count: 0}
			promediator.paymentFormatAvg[clientID][paymentFormat] = promediator.addPaymentFormatAverage(aux, transactions)
		} else {
			promediator.paymentFormatAvg[clientID][paymentFormat] = promediator.addPaymentFormatAverage(paymentFormatAverage, transactions)
		}
	}
	return nil
}

func (promediator *Promediator) addPaymentFormatAverage(paymentFormatAverage transaction.PaymentFormatAverage, transactions []transaction.PaymentFormatAverage) transaction.PaymentFormatAverage {
	for _, t := range transactions {
		paymentFormatAverage.Average += t.Average
		paymentFormatAverage.Count += t.Count
	}
	return paymentFormatAverage
}

func (promediator *Promediator) sendToOutputExchange(keys []string, paymentFormatAverageRecords []transaction.PaymentFormatAverage, clientID int64) error {
	message, err := inner.SerializePaymentFormatAverageMessage(clientID, paymentFormatAverageRecords)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}
	if err := promediator.outputExchange.SendWithKeys(keys, *message); err != nil {
		slog.Debug("While sending EOF message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}

func (promediator *Promediator) getKeyForExchange(clientID int64, paymentFormat string) string {
	hash := fnv.New32a()
	hash.Write([]byte(fmt.Sprintf("%d-%s", clientID, paymentFormat)))
	idx := int(hash.Sum32()) % promediator.config.Q3AmountFilterAmount
	return fmt.Sprintf("%s_%d", promediator.config.Q3AmountFilterPrefix, idx)
}

func (promediator *Promediator) getPaymentFormats(clientID int64) []transaction.PaymentFormatAverage {
	paymentFormats, ok := promediator.paymentFormatAvg[clientID]
	if !ok {
		return nil
	}
	result := make([]transaction.PaymentFormatAverage, 0, len(paymentFormats))
	for _, avg := range paymentFormats {
		result = append(result, avg)
	}
	return result
}
