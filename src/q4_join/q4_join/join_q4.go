package q4_join

import (
	"fmt"
	"hash/fnv"
	"log/slog"

	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

const FANOUT = ""
const DestinationThreshold = 5

type JoinConfig struct {
	MomHost               string
	MomPort               int
	InputQueue            string
	InputTopic            string
	InputExchangeName     string
	ControlExchangeName   string
	ControlTopic          string
	OutputExchangeName    string
	ID                    int
	NextFaseWorkersAmount int
	NextFaseWorkersPrefix string
}

type Join struct {
	inputQueue      middleware.Middleware
	controlExchange middleware.Middleware
	outputExchange  middleware.ExchangeMiddleware
	config          JoinConfig
	//structs no pueden ser keys directamente so:
	// 1er key: cliente , 2da key: cuenta_banco, 3er key: cuentaDest_bancoDest
	Registers map[int64]map[string]map[string]struct{}
}

func NewJoinWorker(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	// input - batches con transacciones con origenes que mapean a esta instancia de bridge matcher
	inputQueue, err := middleware.NewQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}
	inputQueue.BindToTopics(config.InputExchangeName, config.InputTopic)
	inputQueue.BindToTopics(config.ControlExchangeName, FANOUT)

	// input - control
	// (No para EOF, de eso se encargan de mandar todos los groups)
	// (Se envian los sospechosos y sus posibles puentes para que la instancia receptora envie aquellas transacciones realizadas por los puentes)
	controlExchange, err := middleware.NewExchangeMiddleware(config.ControlExchangeName, []string{FANOUT}, connSettings) // control
	if err != nil {
		return nil, err
	}

	// output
	outputExchange, err := middleware.NewDinamicExchangeMiddleware(config.OutputExchangeName, connSettings) // modifcar la forma en la que se manejan los topics
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:      inputQueue,
		outputExchange:  *outputExchange,
		controlExchange: controlExchange,
		config:          config,
	}, nil
}

func (bridgeMatcher *Join) Run() {
	bridgeMatcher.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		bridgeMatcher.handleMessage(&msg, ack, nack)
	})
}

func (bridgeMatcher *Join) handleMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	msg, err := inner.DeserializeMessage(middlewareMsg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", msg.ClientID)
		nack()
		return
	}

	switch msg.MsgType {
	case inner.EndOfRecords:
		if err := bridgeMatcher.handleEndOfRecordMessage(msg.ClientID, msg.Data[0].(bool)); err != nil {
			slog.Error("While handling end of record message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
		ack()
		return

	case inner.TransactionBatch: // a modificar TO DO
		//obtenemos las transacciones
		transactions, err := inner.DeserializeTransactionBatch(msg.Data)
		if err != nil {
			slog.Error("While deserializing transactions from message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}

		// hacemos lo q haya q hacer con las transa
		if err := bridgeMatcher.handleTransactionBatchMessage(msg.ClientID, transactions); err != nil {
			slog.Error("While handling data message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
	case inner.SuspiciousAccount: // a modificar TO DO
		if err := bridgeMatcher.handleSuspiciousAccountMessage(msg.ClientID, msg.Data); err != nil {
			slog.Error("While handling data message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
	default:
		slog.Error("Unexpected msg type received", "err", err, "clientID", msg.ClientID)
	}
	ack()
}

func (bridgeMatcher *Join) handleEndOfRecordMessage(clientID int64, mustPropagate bool) error {
	slog.Info("Received EOF record message from ", "clientID", clientID)
	// se considera que en este punto ya cominicó todo lo relevante

	msg, err := inner.SerializeEOF(clientID, false, fmt.Sprintf("%s_%d", "bridge_matcher", bridgeMatcher.config.ID)) // TO DO agregar otra var de entorno y para group tmb
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}

	for i := range bridgeMatcher.config.NextFaseWorkersAmount {
		topic := fmt.Sprintf("%s_%d", bridgeMatcher.config.NextFaseWorkersPrefix, i)
		bridgeMatcher.outputExchange.SendToTopic(*msg, topic) // Error sin handlear
	}

	return nil
}

func (bridgeMatcher *Join) handleTransactionBatchMessage(clientID int64, transactionRecords []transaction.Transaction) error {
	for _, transaction := range transactionRecords {
		origin := fmt.Sprintf("%s_%d", transaction.FromAccount, transaction.FromBank)
		destiny := fmt.Sprintf("%s_%d", transaction.ToAccount, transaction.ToBank)
		// Inicializar mapas si no existen
		if bridgeMatcher.Registers[clientID] == nil {
			bridgeMatcher.Registers[clientID] = make(map[string]map[string]struct{})
		}
		if bridgeMatcher.Registers[clientID][origin] == nil {
			bridgeMatcher.Registers[clientID][origin] = make(map[string]struct{})
		}

		// Agregar cuenta destino al set
		bridgeMatcher.Registers[clientID][origin][destiny] = struct{}{}

		// Verificar si se alcanzó el umbral
		possibleBridges := bridgeMatcher.Registers[clientID][origin]
		if len(possibleBridges) >= DestinationThreshold {
			if err := bridgeMatcher.notifAll(clientID, origin, possibleBridges); err != nil {
				return fmt.Errorf("error sending to control exchange for client %s , transaction %s: %w", clientID, transaction.FromAccount, err)
			}
		}
	}

	return nil
}

// Notifica a todos los pares/copias, y al join correspondiente, sobre una cuenta sospechosa
func (bridgeMatcher *Join) notifAll(clientID int64, susAccount string, possibleBridges map[string]struct{}) error {
	msg, err := inner.SerializeSuspiciousAccountInfo(clientID, susAccount, possibleBridges)
	if err != nil {
		slog.Debug("While serializing SuspiciousAccountInfo message", "err", err, "clientID", clientID)
		return err
	}

	// notificamos al join correspondiente
	hash := fnv.New32a()
	hash.Write([]byte(susAccount))
	workerIndex := int(hash.Sum32()) % bridgeMatcher.config.NextFaseWorkersAmount
	topic := fmt.Sprintf("%s_%d", bridgeMatcher.config.NextFaseWorkersPrefix, workerIndex)

	if err := bridgeMatcher.outputExchange.SendToTopic(*msg, topic); err != nil {
		slog.Debug("While sending data message", "err", err, "clientID", clientID)
		return err
	}

	// notificamos al resto de bridges
	bridgeMatcher.controlExchange.Send(*msg)
	return nil
}

func (bridgeMatcher *Join) handleSuspiciousAccountMessage(clientID int64, data []interface{}) error {
	origin, possibleBridges, err := inner.DeserializeSuspiciousMsgData(data)
	if err != nil {
		slog.Debug("While serializing data message", "err", err, "clientID", clientID)
		return err
	}

	// definimos el destino de todos los mensajes
	hash := fnv.New32a()
	hash.Write([]byte(origin))
	workerIndex := int(hash.Sum32()) % bridgeMatcher.config.NextFaseWorkersAmount
	topic := fmt.Sprintf("%s_%d", bridgeMatcher.config.NextFaseWorkersPrefix, workerIndex)

	for _, possibleBridge := range possibleBridges {
		// si no hay registros de ese cliente, corto, no tengo nada para mandar
		if bridgeMatcher.Registers[clientID] == nil {
			break
		}
		// si NO hay registros de ese puente salto
		if bridgeMatcher.Registers[clientID][possibleBridge] == nil {
			continue
		}
		// en caso contrario mando los que tenga, ruteando por la cuenta que origino la alerta
		possibleBridgeDestinations := bridgeMatcher.Registers[clientID][possibleBridge]

		msg, err := inner.SerializesPossibleFraudDestinations(clientID, origin, possibleBridge, possibleBridgeDestinations)
		if err != nil {
			slog.Debug("While serializing PossibleFraudDestinations message", "err", err, "clientID", clientID)
			return err
		}
		bridgeMatcher.outputExchange.SendToTopic(*msg, topic)

	}
	return nil
}
