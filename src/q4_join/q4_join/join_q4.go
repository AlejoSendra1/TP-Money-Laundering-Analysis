package q4_join

import (
	"fmt"
	"log/slog"
	"slices"

	"tp_distribuidos/common/datasaver"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
	"tp_distribuidos/common/worker"
)

const FANOUT = ""
const DestinationThreshold = 5

type JoinConfig struct {
	ID                    int
	WorkerPrefix          string
	MomHost               string
	MomPort               int
	InputExchangeName     string
	OutputQueueName       string
	PrevFaseWorkersAmount int
}

type Join struct {
	inputQueue            middleware.Middleware
	outputQueue           middleware.Middleware
	config                JoinConfig
	sourceSinkRegisters   map[int64]map[string]map[string][]string
	bridgeWorkersNotified map[int64][]string
	dataSaver             *datasaver.DataSaver
	mssgHandlers          worker.MessageHandlerMap
	restoring             bool
}

func NewJoinWorker(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	instance_name := fmt.Sprintf("%s_%d", config.WorkerPrefix, config.ID)
	// input - batches con transacciones con origenes que mapean a esta instancia de bridge matcher
	inputQueue, err := middleware.NewQueueMiddleware(instance_name, connSettings)
	if err != nil {
		return nil, err
	}
	inputQueue.BindToTopics(config.InputExchangeName, instance_name)

	// output
	outputQueue, err := middleware.NewQueueMiddleware(config.OutputQueueName, connSettings) // modifcar la forma en la que se manejan los topics
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	// para persistir la info ante posibles caidas
	dataSaver, err := datasaver.NewDataSaver(fmt.Sprintf("/persistence/q4_join_%d", config.ID), LOGS_UNTIL_CHECKPOINT)
	if err != nil {
		return nil, err
	}

	j := &Join{
		inputQueue:            inputQueue,
		outputQueue:           outputQueue,
		config:                config,
		sourceSinkRegisters:   make(map[int64]map[string]map[string][]string),
		bridgeWorkersNotified: make(map[int64][]string),
		dataSaver:             dataSaver,
		restoring:             false,
	}
	j.mssgHandlers = worker.MessageHandlerMap{
		inner.EndOfRecords:              j.handleEndOfRecordMessage,
		inner.PossibleFraudDestinations: j.handlePossibleFraudDestinationsMessage,
	}

	return j, nil
}

func (join *Join) Run() {
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(&msg, ack, nack)
	})
}

func (join *Join) handleMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	if err := worker.HandleMessageV2(middlewareMsg, join.mssgHandlers); err != nil {
		nack()
		return
	}

	join.dataSaver.Save(*middlewareMsg, join) // persistencia de datos
	ack()
}

func (join *Join) handleEndOfRecordMessage(clientID int64, data []interface{}) error {
	slog.Info("Received msg", "type", "EOF")
	_, sender, err := inner.DeserializeEOR(data)
	if err != nil {
		slog.Error("While deserializing EOR msg", "err", err, "clientID", clientID)
		return err
	}
	slog.Info("Received EOF record message from ", "clientID", clientID, "sender", sender)

	join.updateClientEORCondition(clientID, sender)
	if !join.assertClientEORCondition(clientID) {
		return nil
	}

	if join.restoring {
		// si este mensaje van a enviar EOR cuando este ya fue persistido, no enviamos nada
		// si esta persisitido ya fue enviado !!
		return nil
	}

	msg, err := inner.SerializeQueryEOR(clientID, transaction.Query4, fmt.Sprintf("%d", join.config.ID)) // TO DO agregar otra var de entorno y para group tmb
	if err != nil {
		slog.Info("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}

	slog.Info("Sending EOR message to output queue", "clientID", clientID)

	if err := join.outputQueue.Send(*msg); err != nil {
		return err
	}

	join.cleanupClient(clientID)
	return nil
}

func (join *Join) handlePossibleFraudDestinationsMessage(clientID int64, data []interface{}) error {
	source, possibleBridgesAndSinks, err := inner.DeserializePossibleFraudDestinations(data)
	if err != nil {
		slog.Info("While serializing data message", "err", err, "clientID", clientID)
		return err
	}

	// Actualizar y Verificar si se alcanzó el umbral
	for possibleBridge, sinks := range possibleBridgesAndSinks {
		join.updateOriginAccountCondition(clientID, source, possibleBridge, sinks)
	}
	return nil
}

func (join *Join) updateOriginAccountCondition(clientID int64, source string, bridge string, possibleSinks []string) {
	// creamos los hashes en caso de inexistencia
	if join.sourceSinkRegisters[clientID] == nil {
		join.sourceSinkRegisters[clientID] = make(map[string]map[string][]string)
	}
	if join.sourceSinkRegisters[clientID][source] == nil {
		join.sourceSinkRegisters[clientID][source] = make(map[string][]string)
	}

	// por cada destino actualizamos su listado de puentes con los sinks dados
	for _, possibleSink := range possibleSinks {
		if source == possibleSink {
			continue
		}

		if join.sourceSinkRegisters[clientID][source][possibleSink] == nil {
			join.sourceSinkRegisters[clientID][source][possibleSink] = make([]string, 0, 10)
		}
		if !slices.Contains(join.sourceSinkRegisters[clientID][source][possibleSink], bridge) {
			join.sourceSinkRegisters[clientID][source][possibleSink] = append(join.sourceSinkRegisters[clientID][source][possibleSink], bridge)
		}

		// !join.restoring evita el envio de cosas ya enviadas antes de la caida
		if len(join.sourceSinkRegisters[clientID][source][possibleSink]) == DestinationThreshold && !join.restoring {
			msg, _ := inner.SerializeQ4SinkAndSource(clientID, source, possibleSink)
			join.outputQueue.Send(*msg)
		}
	}
}

func (join *Join) updateClientEORCondition(clientID int64, worker string) {
	if join.bridgeWorkersNotified[clientID] == nil {
		join.bridgeWorkersNotified[clientID] = make([]string, 0)
	}
	if slices.Contains(join.bridgeWorkersNotified[clientID], worker) {
		return
	}
	join.bridgeWorkersNotified[clientID] = append(join.bridgeWorkersNotified[clientID], worker)
	slog.Info("EOR manejado", "client", clientID, "worker", worker)
}

func (join *Join) assertClientEORCondition(clientID int64) bool {
	return len(join.bridgeWorkersNotified[clientID]) == join.config.PrevFaseWorkersAmount
}

func (join *Join) cleanupClient(clientID int64) {
	delete(join.sourceSinkRegisters, clientID)
	delete(join.bridgeWorkersNotified, clientID)
	slog.Info("Cleaned up client state", "clientID", clientID)
}
