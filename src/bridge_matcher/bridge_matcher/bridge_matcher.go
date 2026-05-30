package bridge_matcher

import (
	"hash"
	"hash/fnv"
	"log/slog"
	"slices"
	"strconv"

	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

const FANOUT = ""
const DestinationThreshold = 2
const SuspiciousAccountsBatchSize = 100

type BridgeMatcherConfig struct {
	ID                    int
	WorkerPrefix          string
	MomHost               string
	MomPort               int
	InputExchangeName     string
	ControlExchangeName   string
	OutputExchangeName    string
	WorkersAmount         int
	NextFaseWorkersAmount int
	NextFaseWorkersPrefix string
	PrevFaseWorkersAmount int
}

type BridgeMatcher struct {
	inputQueue      middleware.Middleware
	controlExchange middleware.Middleware
	outputExchange  middleware.ExchangeMiddleware
	config          BridgeMatcherConfig
	//structs no pueden ser keys directamente so:
	// 1er key: cliente , 2da key: cuenta_banco, 3er key: cuentaDest_bancoDest
	Registers            map[int64]map[string]map[string]struct{}
	groupWorkersNotified map[int64][]string
	bridgesReadyForEOR   map[int64][]int // tracks which peers sent ReadyForEOF
	hasher               hash.Hash32
}

//TODO LIMIPIAR DATOS DE LOS CLIENTES

func NewBridgeMatcherWorker(config BridgeMatcherConfig) (*BridgeMatcher, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	instance_name := makeKey(config.WorkerPrefix, config.ID)

	// input - batches con transacciones con origenes que mapean a esta instancia de bridge matcher
	inputQueue, err := middleware.NewQueueMiddleware(instance_name, connSettings)
	if err != nil {
		return nil, err
	}
	inputQueue.BindToTopics(config.InputExchangeName, instance_name)
	inputQueue.BindToTopics(config.ControlExchangeName, FANOUT)

	// input - control
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

	return &BridgeMatcher{
		inputQueue:           inputQueue,
		outputExchange:       *outputExchange,
		controlExchange:      controlExchange,
		Registers:            make(map[int64]map[string]map[string]struct{}),
		groupWorkersNotified: make(map[int64][]string),
		config:               config,
		bridgesReadyForEOR:   make(map[int64][]int),
		hasher:               fnv.New32a(),
	}, nil
}

func (bridgeMatcher *BridgeMatcher) Run() {
	bridgeMatcher.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		bridgeMatcher.handleMessage(&msg, ack, nack)
	})
}

func (bridgeMatcher *BridgeMatcher) handleMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	//slog.Info("Received msg", "body", middlewareMsg.Body)
	msg, err := inner.DeserializeMessage(middlewareMsg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", msg.ClientID)
		nack()
		return
	}

	switch msg.MsgType {
	case inner.EndOfRecords:
		_, sender, err := inner.DeserializeEOR(msg.Data)
		if err != nil {
			slog.Error("While deserializing EOR msg", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
		if err := bridgeMatcher.handleEndOfRecordMessage(msg.ClientID, sender); err != nil {
			slog.Error("While handling end of record message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}

	case inner.TransactionBatch:
		//obtenemos las transacciones
		//slog.Info("Received msg", "type", "tranasction batch")
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
	case inner.SuspiciousAccount:
		if err := bridgeMatcher.handleSuspiciousAccountMessage(msg.ClientID, msg.Data); err != nil {
			slog.Error("While handling data message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}
	case inner.ReadyForEOR:
		if err := bridgeMatcher.handleReadyForEOR(msg.ClientID, msg.Data); err != nil {
			slog.Error("While handling data message", "err", err, "clientID", msg.ClientID)
			nack()
			return
		}

	default:
		slog.Error("Unexpected msg type received", "err", err, "clientID", msg.ClientID)
	}
	ack()
}

func (bridgeMatcher *BridgeMatcher) handleEndOfRecordMessage(clientID int64, sender string) error {
	//slog.Info("Received EOF record message from ", "clientID", clientID, "sender", sender)
	// se considera que en este punto ya cominicó todo lo relevante
	bridgeMatcher.updateClientEORCondition(clientID, sender)
	if !bridgeMatcher.assertClientEORCondition(clientID) {
		return nil
	}

	// Por cada cliente va a enviar las cuentas que tenga, al menos "DestinationThreshold" cantidad de cuentas destino
	bridgeMatcher.sendSuspiciousAccounts(clientID)

	readyMsg, err := inner.SerializeReadyForEOR(clientID, bridgeMatcher.config.ID)
	if err != nil {
		return err
	}

	// broadcast to all peers (including self via fanout)
	bridgeMatcher.controlExchange.Send(*readyMsg)
	return nil
}

func (bridgeMatcher *BridgeMatcher) handleTransactionBatchMessage(clientID int64, transactionRecords []transaction.Transaction) error {
	if bridgeMatcher.Registers[clientID] == nil {
		bridgeMatcher.Registers[clientID] = make(map[string]map[string]struct{})
	}
	clientRegisters := bridgeMatcher.Registers[clientID]

	for _, transaction := range transactionRecords {
		origin := makeKey(transaction.FromAccount, transaction.FromBank)
		destiny := makeKey(transaction.ToAccount, transaction.ToBank)
		// Inicializar mapas si no existen
		if clientRegisters[origin] == nil {
			clientRegisters[origin] = make(map[string]struct{})
		}

		// Agregar cuenta destino al set
		//slog.Info("transaccion a guardar", "origen", origin, "destiny", destiny)
		clientRegisters[origin][destiny] = struct{}{}
	}

	return nil
}

func (bridgeMatcher *BridgeMatcher) handleSuspiciousAccountMessage(clientID int64, data []interface{}) error {
	if bridgeMatcher.Registers[clientID] == nil {
		return nil
	}

	entries, err := inner.DeserializeSuspiciousAccountBatch(data)
	if err != nil {
		slog.Error("While deserializing suspicious account batch", "err", err, "clientID", clientID)
		return err
	}

	for _, entry := range entries {
		if err := bridgeMatcher.processSuspiciousAccount(clientID, entry.Origin, entry.Bridges); err != nil {
			slog.Error("While processing suspicious account", "err", err, "clientID", clientID)
			return err
		}
	}
	return nil
}

func (bridgeMatcher *BridgeMatcher) processSuspiciousAccount(clientID int64, origin string, possibleBridges []string) error {
	// definimos el destino de todos los mensajes
	bridgeMatcher.hasher.Reset()
	bridgeMatcher.hasher.Write([]byte(origin))
	workerIndex := int(bridgeMatcher.hasher.Sum32()) % bridgeMatcher.config.NextFaseWorkersAmount

	topic := makeKey(bridgeMatcher.config.NextFaseWorkersPrefix, workerIndex)

	for _, possibleBridge := range possibleBridges {
		if bridgeMatcher.Registers[clientID][possibleBridge] == nil {
			continue
		}
		possibleBridgeDestinations := bridgeMatcher.Registers[clientID][possibleBridge]

		msg, err := inner.SerializesPossibleFraudDestinations(clientID, origin, possibleBridge, possibleBridgeDestinations)
		if err != nil {
			slog.Error("While serializing PossibleFraudDestinations message", "err", err, "clientID", clientID)
			return err
		}
		bridgeMatcher.outputExchange.SendToTopic(*msg, topic)
	}

	return nil
}

func (bridgeMatcher *BridgeMatcher) handleReadyForEOR(clientID int64, data []interface{}) error {
	senderID, err := inner.DeserializeReadyForEOR(data)
	if err != nil {
		return err
	}

	bridgeMatcher.updateBridgeReadyCondition(clientID, senderID)
	if !bridgeMatcher.assertAllBridgesReady(clientID) {
		return nil // still waiting for other bridge_matcher
	}

	// all peers are done sending suspects, safe to EOF downstream
	msg, err := inner.SerializeEOF(clientID, false,
		makeKey("bridge_matcher", bridgeMatcher.config.ID))
	if err != nil {
		return err
	}
	//slog.Info("Sending EOR", "Client", clientID)
	for i := range bridgeMatcher.config.NextFaseWorkersAmount {
		topic := makeKey(bridgeMatcher.config.NextFaseWorkersPrefix, i)
		bridgeMatcher.outputExchange.SendToTopic(*msg, topic)
	}

	// cleanup after all EOFs are sent
	bridgeMatcher.cleanupClient(clientID)
	return nil
}

// Notifica a todos los pares/copias sobre un batch de cuentas sospechosas
func (bridgeMatcher *BridgeMatcher) notifAllBatch(clientID int64, batch []inner.SuspiciousAccountBatchEntry) error {
	msg, err := inner.SerializeSuspiciousAccountBatch(clientID, batch)
	if err != nil {
		slog.Error("While serializing SuspiciousAccountBatch message", "err", err, "clientID", clientID)
		return err
	}
	bridgeMatcher.controlExchange.Send(*msg)
	return nil
}

func (bm *BridgeMatcher) sendSuspiciousAccounts(clientID int64) error {
	register, ok := bm.Registers[clientID]
	if !ok {
		return nil
	}

	batch := make([]inner.SuspiciousAccountBatchEntry, 0, SuspiciousAccountsBatchSize)
	for origin, destinos := range register {
		if len(destinos) < DestinationThreshold {
			continue
		}
		bridges := make([]string, 0, len(destinos))
		for dest := range destinos {
			bridges = append(bridges, dest)
		}
		batch = append(batch, inner.SuspiciousAccountBatchEntry{Origin: origin, Bridges: bridges})

		if len(batch) >= SuspiciousAccountsBatchSize {
			if err := bm.notifAllBatch(clientID, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := bm.notifAllBatch(clientID, batch); err != nil {
			return err
		}
	}
	return nil
}

func (bridgeMatcher *BridgeMatcher) updateClientEORCondition(clientID int64, worker string) {
	if bridgeMatcher.groupWorkersNotified[clientID] == nil {
		bridgeMatcher.groupWorkersNotified[clientID] = make([]string, 0)
	}
	if slices.Contains(bridgeMatcher.groupWorkersNotified[clientID], worker) {
		return
	}
	bridgeMatcher.groupWorkersNotified[clientID] = append(bridgeMatcher.groupWorkersNotified[clientID], worker)
}

func (bridgeMatcher *BridgeMatcher) updateBridgeReadyCondition(clientID int64, workerID int) {
	if bridgeMatcher.bridgesReadyForEOR[clientID] == nil {
		bridgeMatcher.bridgesReadyForEOR[clientID] = make([]int, 0)
	}
	if slices.Contains(bridgeMatcher.bridgesReadyForEOR[clientID], workerID) {
		return
	}
	bridgeMatcher.bridgesReadyForEOR[clientID] = append(bridgeMatcher.bridgesReadyForEOR[clientID], workerID)
}

func (bridgeMatcher *BridgeMatcher) assertClientEORCondition(clientID int64) bool {
	//slog.Info("Workers q notificaron", "lista", bridgeMatcher.groupWorkersNotified[clientID])
	return len(bridgeMatcher.groupWorkersNotified[clientID]) == bridgeMatcher.config.PrevFaseWorkersAmount
}

func (bridgeMatcher *BridgeMatcher) assertAllBridgesReady(clientID int64) bool {
	// WorkersAmount here is the number of bridge_matcher copies
	//slog.Info("bridges q notificaron", "lista", bridgeMatcher.bridgesReadyForEOR[clientID])
	return len(bridgeMatcher.bridgesReadyForEOR[clientID]) == bridgeMatcher.config.WorkersAmount
}

func (bridgeMatcher *BridgeMatcher) cleanupClient(clientID int64) {
	delete(bridgeMatcher.Registers, clientID)
	delete(bridgeMatcher.groupWorkersNotified, clientID)
	delete(bridgeMatcher.bridgesReadyForEOR, clientID)
	//slog.Info("Cleaned up client state", "clientID", clientID)
}

func makeKey(account string, bank int) string {
	return account + "_" + strconv.Itoa(bank)
}
