package promediator

import (
	"fmt"
	"log/slog"
	"tp_distribuidos/common/batch_utils"
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
	eofCounter       map[int64]batch_utils.Set[string]
	config           PromediatorConfig
	deduplicator     *batch_utils.MultiClientDeduplicator
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
		eofCounter:       make(map[int64]batch_utils.Set[string]),
		config:           config,
		deduplicator:     batch_utils.NewMultiClientDeduplicator(1000),
	}, nil
}

func (promediator *Promediator) Run() {
	promediator.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		promediator.handleMessage(&msg, ack, nack)
	})
}
func (promediator *Promediator) handleMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	msg, err := inner.DeserializeMessage(middlewareMsg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", msg.ClientID)
		nack()
		return
	}
	batchID := batch_utils.GenerateBatchID([]byte(middlewareMsg.Body))
	if promediator.deduplicator.IsDuplicate(int(msg.ClientID), batchID) {
		slog.Warn("Duplicate message detected", "clientID", msg.ClientID, "batchID", batchID)
		ack()
		return
	}
	switch msg.MsgType {
	case inner.EndOfRecords:
		slog.Info("Received msg", "type", "EOF")
		_, sender, err := inner.DeserializeEOR(msg.Data)
		if err != nil {
			slog.Error("While deserializing EOR msg", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}

		if err := promediator.handleEndOfRecordMessage(msg.ClientID, sender); err != nil {
			slog.Error("While handling end of record message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
		ack()
		return
	case inner.TransactionBatch:
		transactions, err := inner.DeserializeTransactionBatch(msg.Data)
		if err != nil {
			slog.Error("While deserializing message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
		if err := promediator.handleDataMessage(transactions, msg.ClientID); err != nil {
			slog.Error("While handling data message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
	default:
		slog.Error("Unexpected msg type received", "err", err, "clientID", msg.ClientID)
	}
	ack()
}

func (promediator *Promediator) handleEndOfRecordMessage(clientID int64, sender string) error {
	// Verifico si ya recibi todos los EOFs que faltaban
	slog.Info("Received End Of Records message", "clientID", clientID)
	promediator.eofCounter[clientID].Add(sender)
	if uint8(promediator.eofCounter[clientID].Size()) != promediator.config.SumAmount {
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
	msgToSend, err := inner.SerializeEOF(clientID, false, fmt.Sprintf("%s_%d", promediator.config.PromediatorPrefix, promediator.config.Id))
	if err != nil {
		slog.Info("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}
	if err := promediator.outputExchange.Send(*msgToSend); err != nil {
		slog.Info("While sending EOF message to promediator", "err", err, "clientID", clientID)
		return err
	}
	slog.Info("Sent EOF message to q3 amount filter", "clientID", clientID)
	delete(promediator.paymentFormatAvg, clientID)
	delete(promediator.eofCounter, clientID)
	promediator.deduplicator.RemoveClient(int(clientID))
	return nil
}

func (promediator *Promediator) handleDataMessage(paymentFormatAverageRecords []transaction.Transaction, clientID int64) error {
	if _, exist := promediator.paymentFormatAvg[clientID]; !exist {
		slog.Info("Client new arrived", "clientID", clientID)
		promediator.paymentFormatAvg[clientID] = make(map[string]transaction.PaymentFormatAverage)
		promediator.eofCounter[clientID] = batch_utils.NewSet[string]()
	}

	for _, tr := range paymentFormatAverageRecords {
		current := promediator.paymentFormatAvg[clientID][tr.PaymentFormat]
		current.PaymentFormat = tr.PaymentFormat
		current.Average += tr.Amount
		current.Count += 1
		promediator.paymentFormatAvg[clientID][tr.PaymentFormat] = current
	}
	return nil
}

func (promediator *Promediator) sendToOutputExchange(paymentFormatAverageRecords []transaction.PaymentFormatAverage, clientID int64) error {
	message, err := inner.SerializePaymentFormatAverageMessage(clientID, paymentFormatAverageRecords)
	if err != nil {
		return err
	}
	return promediator.outputExchange.Send(*message)
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
