package promediator

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"tp_distribuidos/common/batch_utils"
	"tp_distribuidos/common/datasaver"
	"tp_distribuidos/common/heatbeat"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
	"tp_distribuidos/common/worker"
)

const LogsUntilCheckpoint = 250
const MaxBatchSize = 10000

type PromediatorConfig struct {
	Id                 int
	WorkerID           string
	MomHost            string
	MomPort            int
	InputExchangeName  string
	OutputExchangeName string
	OutputTopic        string
	SumAmount          uint8
	PromediatorPrefix  string
}

type CheckpointData struct {
	PaymentFormatAverage map[int64]map[string]transaction.PaymentFormatAverage `json:"topByClient"`
	EofCounter           map[int64]batch_utils.Set[string]                     `json:"eofCounter"`
	Deduplicator         *batch_utils.MultiClientDeduplicator                  `json:"deduplicator"`
}

type Promediator struct {
	inputExchange    middleware.Middleware
	outputExchange   middleware.Middleware
	paymentFormatAvg map[int64]map[string]transaction.PaymentFormatAverage
	eofCounter       map[int64]batch_utils.Set[string]
	config           PromediatorConfig
	deduplicator     *batch_utils.MultiClientDeduplicator
	dataSaver        *datasaver.DataSaver
	heartbeat        *heatbeat.HeartbeatSender
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
	dataSaver, err := datasaver.NewDataSaver(fmt.Sprintf("/persistence_%s_%d", config.PromediatorPrefix, config.Id), LogsUntilCheckpoint)
	if err != nil {
		return nil, err
	}

	hb, err := heatbeat.NewHeartbeatSender(config.WorkerID, connSettings)
	if err != nil {
		inputExchange.Close()
		outputExchange.Close()
		return nil, fmt.Errorf("creating heartbeat sender: %w", err)
	}

	return &Promediator{
		inputExchange:    inputExchange,
		outputExchange:   outputExchange,
		paymentFormatAvg: make(map[int64]map[string]transaction.PaymentFormatAverage),
		eofCounter:       make(map[int64]batch_utils.Set[string]),
		config:           config,
		deduplicator:     batch_utils.NewMultiClientDeduplicator(MaxBatchSize),
		dataSaver:        dataSaver,
		heartbeat:        hb,
	}, nil
}

func (promediator *Promediator) GetCheckpointData() any {
	return CheckpointData{
		PaymentFormatAverage: promediator.paymentFormatAvg,
		EofCounter:           promediator.eofCounter,
		Deduplicator:         promediator.deduplicator,
	}
}

func (promediator *Promediator) handleSigterm() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	<-sigCh
	slog.Info("SIGTERM received, stopping consumer")
	promediator.heartbeat.Stop()
	promediator.inputExchange.StopConsuming()
}

func (promediator *Promediator) Run() {
	go promediator.handleSigterm()
	promediator.heartbeat.Start()

	promediator.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		promediator.handleMessage(&msg, ack, nack)
	})
}
func (promediator *Promediator) handleMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	err := worker.HandleMessageV3(
		middlewareMsg,
		worker.MessageHandlerMap{
			inner.EndOfRecords:     promediator.handleEndOfRecords,
			inner.TransactionBatch: promediator.handleTransactionBatch,
		},
		promediator.deduplicator,
	)
	if err != nil {
		nack()
		return
	}
	promediator.dataSaver.Save(middlewareMsg, promediator) // persistencia de datos
	ack()
}

func (promediator *Promediator) handleTransactionBatch(clientID int64, data []interface{}) error {
	transactions, err := inner.DeserializeTransactionBatch(data)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", clientID)
		return err
	}
	if err = promediator.handleDataMessage(transactions, clientID); err != nil {
		slog.Error("While handling data message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}

func (promediator *Promediator) handleEndOfRecords(clientID int64, data []interface{}) error {
	slog.Info("Received msg", "type", "EOF")
	_, sender, err := inner.DeserializeEOR(data)
	if err != nil {
		slog.Error("While deserializing EOR msg", "err", err, "clientID", clientID)
		return err
	}
	if err = promediator.handleEndOfRecordMessage(clientID, sender); err != nil {
		slog.Error("While handling end of record message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}

func (promediator *Promediator) handleEndOfRecordMessage(clientID int64, sender string) error {
	// Verifico si ya recibi todos los EOFs que faltaban
	slog.Info("Received End Of Records message", "clientID", clientID)
	if _, ok := promediator.eofCounter[clientID]; !ok {
		slog.Info(
			"Received EOF for unknown client; client may have already finished or never sent any data",
			"clientID", clientID,
		)
		return nil
	}
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

	// Envio la notificacion a q3 amount filter
	msgToSend, err := inner.SerializeNotificationAvg(clientID, false, fmt.Sprintf("%s_%d", promediator.config.PromediatorPrefix, promediator.config.Id))
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
	promediator.deduplicator.RemoveClient(clientID)
	slog.Info("Cleanup client and finished", "clientID", clientID)
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

func (promediator *Promediator) Restaurate() error {
	// primero restauramos el checkpoint
	var checkpoint CheckpointData

	thereIsCheckpoint, err := promediator.dataSaver.GetRestaurationCheckpoint(&checkpoint)
	if err != nil { // habria q agregar retrys?
		return err
	}
	if thereIsCheckpoint == true {
		slog.Info("cargando en base a checkpoint")
		promediator.paymentFormatAvg = checkpoint.PaymentFormatAverage
		promediator.eofCounter = checkpoint.EofCounter
		promediator.deduplicator = checkpoint.Deduplicator
	}

	var savedDataVar middleware.Message
	var thereIsLogs bool

	for {
		thereIsLogs, err = promediator.dataSaver.GetDataFromLogs(&savedDataVar)
		if err != nil { // habria q modificar para retrys
			return err
		}
		if !thereIsLogs {
			break
		}
		err = worker.HandleMessageV3(
			&savedDataVar,
			worker.MessageHandlerMap{
				inner.EndOfRecords:     promediator.handleEndOfRecords,
				inner.TransactionBatch: promediator.handleTransactionBatch,
			},
			promediator.deduplicator,
		)
		if err != nil {
			return err
		}
	}

	return nil
}
