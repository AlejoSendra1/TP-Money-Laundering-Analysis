package q4_join

import (
	"fmt"
	"log/slog"
	"slices"

	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
)

const FANOUT = ""
const DestinationThreshold = 5

type JoinConfig struct {
	MomHost               string
	MomPort               int
	InputQueue            string
	InputTopic            string
	InputExchangeName     string
	OutputQueueName       string
	ID                    int
	NextFaseWorkersAmount int
	NextFaseWorkersPrefix string
}

type Join struct {
	inputQueue            middleware.Middleware
	outputQueue           middleware.Middleware
	config                JoinConfig
	bridgeSourceRegisters map[int64]map[string]map[string]struct{} // modificar por una estructura TO DO
	bridgeSinkRegisters   map[int64]map[string]map[string]struct{} // modificar por una estructura TO DO
	sourceSinkRegisters   map[int64]map[string]map[string][]string
}

func NewJoinWorker(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	// input - batches con transacciones con origenes que mapean a esta instancia de bridge matcher
	inputQueue, err := middleware.NewQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}
	inputQueue.BindToTopics(config.InputExchangeName, config.InputTopic)

	// output
	outputQueue, err := middleware.NewQueueMiddleware(config.OutputQueueName, connSettings) // modifcar la forma en la que se manejan los topics
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:  inputQueue,
		outputQueue: outputQueue,
		config:      config,
	}, nil
}

func (join *Join) Run() {
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(&msg, ack, nack)
	})
}

func (join *Join) handleMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	msg, err := inner.DeserializeMessage(middlewareMsg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", msg.ClientID)
		nack()
		return
	}

	switch msg.MsgType {
	case inner.EndOfRecords:
		if err := join.handleEndOfRecordMessage(msg.ClientID, msg.Data[0].(bool)); err != nil {
			slog.Error("While handling end of record message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
		ack()
		return
	case inner.SuspiciousAccount: // a modificar TO DO
		if err := join.handleSuspiciousAccountMessage(msg.ClientID, msg.Data); err != nil {
			slog.Error("While handling data message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
	case inner.PossibleFraudDestinations: // a modificar TO DO
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

func (join *Join) handleEndOfRecordMessage(clientID int64, mustPropagate bool) error {
	slog.Info("Received EOF record message from ", "clientID", clientID)

	msg, err := inner.SerializeEOF(clientID, false, fmt.Sprintf("%s_%d", "join", join.config.ID)) // TO DO agregar otra var de entorno y para group tmb
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}
	join.outputQueue.Send(*msg) // Error sin handlear

	return nil
}

func (join *Join) handleSuspiciousAccountMessage(clientID int64, data []interface{}) error {
	source, possibleBridges, err := inner.DeserializeSuspiciousMsgData(data)
	if err != nil {
		slog.Debug("While serializing data message", "err", err, "clientID", clientID)
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

	}

	// con prefetch uno este msg siempre llega primero
	//verifyFraudCondition(clientID, source, possibleBridges)
	return nil
}

func (join *Join) handlePossibleFraudDestinationsMessage(clientID int64, data []interface{}) error {
	source, bridge, possibleSinks, err := inner.DeserializePossibleFraudDestinations(data)
	if err != nil {
		slog.Debug("While serializing data message", "err", err, "clientID", clientID)
		return err
	}

	for _, possibleSink := range possibleSinks {
		if join.bridgeSinkRegisters[clientID] == nil || join.bridgeSourceRegisters[clientID] == nil {
			return fmt.Errorf("error handling PossibleFraudDestinationsMessage from %d: %w", clientID, err)
		}
		if join.bridgeSinkRegisters[clientID][bridge] == nil {
			join.bridgeSinkRegisters[clientID][bridge] = make(map[string]struct{})
		}
		if join.bridgeSourceRegisters[clientID][bridge] == nil {
			return fmt.Errorf("error in bridge: %s while handling PossibleFraudDestinationsMessage from client %d: %w", bridge, clientID, err)
		}

		join.bridgeSourceRegisters[clientID][bridge][possibleSink] = struct{}{}
	}
	// Verificar si se alcanzó el umbral
	join.updateOriginAccountCondition(clientID, source, bridge, possibleSinks)
	return nil
}

func (join *Join) updateOriginAccountCondition(clientID int64, source string, bridge string, possibleSinks []string) {
	if join.sourceSinkRegisters[clientID] == nil {
		join.sourceSinkRegisters[clientID] = make(map[string]map[string][]string)
	}
	if join.sourceSinkRegisters[clientID][source] == nil {
		join.sourceSinkRegisters[clientID][source] = make(map[string][]string)
	}

	for _, possibleSink := range possibleSinks {
		if join.sourceSinkRegisters[clientID][source][possibleSink] == nil {
			join.sourceSinkRegisters[clientID][source][possibleSink] = make([]string, 5, 10)
		}
		if !slices.Contains(join.sourceSinkRegisters[clientID][source][possibleSink], possibleSink) {
			join.sourceSinkRegisters[clientID][source][possibleSink] = append(join.sourceSinkRegisters[clientID][source][possibleSink], possibleSink)
		}
		if len(join.sourceSinkRegisters[clientID][source][possibleSink]) == DestinationThreshold {
			msg, _ := inner.SerializeQ4SourceAccount(clientID, source)
			join.outputQueue.Send(*msg)
		}
	}
}
