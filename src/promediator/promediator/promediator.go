package promediator

import (
	"fmt"
	"log/slog"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

type PromediatorConfig struct {
	Id                 int
	MomHost            string
	MomPort            int
	InputExchangeName  string
	OutputExchangeName string
	OutputTopic        string
	SumAmount          uint8
	PromediatorPrefix  string
}

type Promediator struct {
	inputExchange    middleware.Middleware
	outputExchange   middleware.Middleware
	paymentFormatAvg map[int64]map[string]transaction.PaymentFormatAverage
	eofCounter       map[int64]uint8
	config           PromediatorConfig
}

func NewPromediator(config PromediatorConfig) (*Promediator, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	myKey := fmt.Sprintf("%s_%d", config.PromediatorPrefix, config.Id)
	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputExchangeName, []string{myKey}, connSettings, myKey)
	if err != nil {
		return nil, err
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.OutputExchangeName, []string{config.OutputTopic}, connSettings, "") // No consumo, solo envio
	if err != nil {
		inputExchange.Close()
		return nil, err
	}

	return &Promediator{
		inputExchange:    inputExchange,
		outputExchange:   outputExchange,
		paymentFormatAvg: make(map[int64]map[string]transaction.PaymentFormatAverage),
		eofCounter:       make(map[int64]uint8),
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
	// Verifico si ya recibi todos los EOFs que faltaban
	slog.Info("Received End Of Records message", "clientID", clientID)
	promediator.eofCounter[clientID]++
	if promediator.eofCounter[clientID] != promediator.config.SumAmount {
		slog.Debug("Waiting for remaining EOFs")
		return nil
	}

	// Envio los promedios por cliente a q3 amount filter
	slog.Info("All EOFs received, calculating average and sending", "clientID", clientID)
	averages := promediator.getPaymentFormats(clientID)
	if averages != nil {
		for _, avg := range averages {
			avg.Average /= float64(avg.Count)
			if err := promediator.sendToOutputExchange([]transaction.PaymentFormatAverage{avg}, clientID); err != nil {
				slog.Error("While sending average message to output exchange", "clientID", clientID)
				return err
			}
			slog.Info("Sent average to q3 amount filter", "clientID", clientID, "paymentFormat", avg.PaymentFormat, "average", avg.Average)
		}
	} else {
		slog.Info("Dont send anything", "clientID", clientID)
	}

	// Envio el EOF
	if err := promediator.sendToOutputExchange([]transaction.PaymentFormatAverage{}, clientID); err != nil {
		slog.Error("While sending EOF message to output exchange", "clientID", clientID)
		return err
	}
	slog.Info("Sent EOF message to q3 amount filter", "clientID", clientID)
	delete(promediator.paymentFormatAvg, clientID)
	delete(promediator.eofCounter, clientID)
	return nil
}

func (promediator *Promediator) handleDataMessage(paymentFormatAverageRecords []transaction.PaymentFormatAverage, clientID int64) error {
	if _, exist := promediator.paymentFormatAvg[clientID]; !exist {
		slog.Info("Client new arrived", "clientID", clientID)
		promediator.paymentFormatAvg[clientID] = make(map[string]transaction.PaymentFormatAverage)
	}

	for _, tr := range paymentFormatAverageRecords {
		current := promediator.paymentFormatAvg[clientID][tr.PaymentFormat]
		current.PaymentFormat = tr.PaymentFormat
		current.Average += tr.Average
		current.Count += tr.Count
		promediator.paymentFormatAvg[clientID][tr.PaymentFormat] = current
	}
	return nil
}

func (promediator *Promediator) sendToOutputExchange(paymentFormatAverageRecords []transaction.PaymentFormatAverage, clientID int64) error {
	message, err := inner.SerializePaymentFormatAverageMessage(clientID, paymentFormatAverageRecords)
	if err != nil {
		return err
	}
	if err := promediator.outputExchange.Send(*message); err != nil {
		return err
	}
	return nil
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
