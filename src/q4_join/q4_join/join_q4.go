package q4_join

import (
	"fmt"
	"log/slog"
	"slices"

	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
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
	bridgeSourceRegisters map[int64]map[string]map[string]struct{} // modificar por una estructura TO DO
	bridgeSinkRegisters   map[int64]map[string]map[string]struct{} // modificar por una estructura TO DO
	sourceSinkRegisters   map[int64]map[string]map[string][]string
	bridgeWorkersNotified map[int64][]string
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

	return &Join{
		inputQueue:            inputQueue,
		outputQueue:           outputQueue,
		config:                config,
		bridgeSourceRegisters: make(map[int64]map[string]map[string]struct{}),
		bridgeSinkRegisters:   make(map[int64]map[string]map[string]struct{}),
		sourceSinkRegisters:   make(map[int64]map[string]map[string][]string),
		bridgeWorkersNotified: make(map[int64][]string),
	}, nil
}

func (join *Join) Run() {
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(&msg, ack, nack)
	})
}

func (join *Join) handleMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	//slog.Info("Received msg", "body", middlewareMsg.Body)
	msg, err := inner.DeserializeMessage(middlewareMsg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", msg.ClientID)
		nack()
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

		if err := join.handleEndOfRecordMessage(msg.ClientID, sender); err != nil {
			slog.Error("While handling end of record message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
	case inner.SuspiciousAccount:
		slog.Info("Sus account recibida", "client", msg.ClientID, "data", msg.Data)
		if err := join.handleSuspiciousAccountMessage(msg.ClientID, msg.Data); err != nil {
			slog.Error("While handling data message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
	case inner.PossibleFraudDestinations:
		slog.Info("Possible fraud recibido", "client", msg.ClientID, "data", msg.Data)
		if err := join.handlePossibleFraudDestinationsMessage(msg.ClientID, msg.Data); err != nil {
			slog.Error("While handling data message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
	default:
		slog.Error("Unexpected msg type received", "err", err, "clientID", msg.ClientID)
	}
	ack()
}

func (join *Join) handleEndOfRecordMessage(clientID int64, sender string) error {
	slog.Info("Received EOF record message from ", "clientID", clientID, "sender", sender)

	join.updateClientEORCondition(clientID, sender)
	if !join.assertClientEORCondition(clientID) {
		return nil
	}

	msg, err := inner.SerializeQueryEOR(clientID, transaction.Query4) // TO DO agregar otra var de entorno y para group tmb
	if err != nil {
		slog.Info("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}

	if err := join.outputQueue.Send(*msg); err != nil {
		return err
	}

	join.cleanupClient(clientID)
	return nil
}

func (join *Join) handleSuspiciousAccountMessage(clientID int64, data []interface{}) error {
	source, possibleBridges, err := inner.DeserializeSuspiciousMsgData(data)
	if err != nil {
		slog.Info("While serializing data message", "err", err, "clientID", clientID)
		return err
	}

	for _, possibleBridge := range possibleBridges {
		// Inicializar mapas si no existen
		if join.bridgeSourceRegisters[clientID] == nil {
			join.bridgeSourceRegisters[clientID] = make(map[string]map[string]struct{})
		}
		if join.bridgeSourceRegisters[clientID][possibleBridge] == nil {
			join.bridgeSourceRegisters[clientID][possibleBridge] = make(map[string]struct{})
		}

		// Agregar para cada puente la relacion con el sus
		join.bridgeSourceRegisters[clientID][possibleBridge][source] = struct{}{}
		//slog.Info("Vinculado bridge con source", "client", clientID, "source", source, "possible bridge", possibleBridge)
	}
	// con prefetch uno este msg siempre llega primero
	return nil
}

func (join *Join) handlePossibleFraudDestinationsMessage(clientID int64, data []interface{}) error {
	source, bridge, possibleSinks, err := inner.DeserializePossibleFraudDestinations(data)
	if err != nil {
		slog.Info("While serializing data message", "err", err, "clientID", clientID)
		return err
	}

	if join.bridgeSinkRegisters[clientID] == nil {
		join.bridgeSinkRegisters[clientID] = make(map[string]map[string]struct{})
	}

	for _, possibleSink := range possibleSinks {
		if join.bridgeSinkRegisters[clientID][bridge] == nil {
			join.bridgeSinkRegisters[clientID][bridge] = make(map[string]struct{})
		}
		if join.bridgeSourceRegisters[clientID][bridge] == nil {
			return fmt.Errorf("error in bridge: %s while handling PossibleFraudDestinationsMessage from client %d: %w", bridge, clientID, err)
		}

		join.bridgeSinkRegisters[clientID][bridge][possibleSink] = struct{}{}
		slog.Info("Vinculado bridge con sink", "client", clientID, "source", source, "possible sink", possibleSink)
	}
	// Verificar si se alcanzó el umbral
	join.updateOriginAccountCondition(clientID, source, bridge, possibleSinks)
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
		if join.sourceSinkRegisters[clientID][source][possibleSink] == nil {
			join.sourceSinkRegisters[clientID][source][possibleSink] = make([]string, 0, 10)
		}
		if !slices.Contains(join.sourceSinkRegisters[clientID][source][possibleSink], bridge) {
			join.sourceSinkRegisters[clientID][source][possibleSink] = append(join.sourceSinkRegisters[clientID][source][possibleSink], bridge)
		}
		if len(join.sourceSinkRegisters[clientID][source][possibleSink]) == DestinationThreshold {
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
	delete(join.bridgeSourceRegisters, clientID)
	delete(join.bridgeSinkRegisters, clientID)
	delete(join.sourceSinkRegisters, clientID)
	delete(join.bridgeWorkersNotified, clientID)
	slog.Info("Cleaned up client state", "clientID", clientID)
}
