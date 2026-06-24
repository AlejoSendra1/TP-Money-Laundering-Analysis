package bridge_matcher

import (
	"fmt"
	"hash"
	"hash/fnv"
	"log/slog"
	"slices"
	"strconv"

	"tp_distribuidos/common/datasaver"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/worker"
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
	Registers            map[int64]map[string]map[string]int
	groupWorkersNotified map[int64][]string
	bridgesReadyForEOR   map[int64][]int // tracks which peers sent ReadyForEOF
	hasher               hash.Hash32
	mssgHandlers         worker.MessageHandlerMap // para optimizar
	dataSaver            *datasaver.DataSaver
	restoring            bool
}

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
	controlExchange, err := middleware.NewExchangeMiddleware(config.ControlExchangeName, []string{FANOUT}, connSettings, "") // control
	if err != nil {
		return nil, err
	}

	// output
	outputExchange, err := middleware.NewDinamicExchangeMiddleware(config.OutputExchangeName, connSettings) // modifcar la forma en la que se manejan los topics
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	// para persistir la info ante posibles caidas
	dataSaver, err := datasaver.NewDataSaver(fmt.Sprintf("/persistence/bridge_matcher_%d", config.ID), LOGS_UNTIL_CHECKPOINT)
	if err != nil {
		return nil, err
	}

	bm := &BridgeMatcher{
		inputQueue:           inputQueue,
		outputExchange:       *outputExchange,
		controlExchange:      controlExchange,
		Registers:            make(map[int64]map[string]map[string]int),
		config:               config,
		groupWorkersNotified: make(map[int64][]string),
		bridgesReadyForEOR:   make(map[int64][]int),
		hasher:               fnv.New32a(),
		dataSaver:            dataSaver,
		restoring:            false,
	}
	bm.mssgHandlers = worker.MessageHandlerMap{
		inner.EndOfRecords:      bm.handleEndOfRecordMessage,
		inner.TransactionBatch:  bm.handleTransactionBatchMessage,
		inner.SuspiciousAccount: bm.handleSuspiciousAccountMessage,
		inner.ReadyForEOR:       bm.handleReadyForEOR,
	}

	return bm, nil
}

func (bridgeMatcher *BridgeMatcher) Run() {
	bridgeMatcher.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		bridgeMatcher.handleMessage(&msg, ack, nack)
	})
}

func (bm *BridgeMatcher) handleMessage(middlewareMsg *middleware.Message, ack func(), nack func()) {
	if err := worker.HandleMessageV2(middlewareMsg, bm.mssgHandlers); err != nil {
		nack()
		return
	}

	bm.dataSaver.Save(*middlewareMsg, bm) // persistencia de datos
	ack()
}

// ------------------- EndOfRecords -------------------

func (bridgeMatcher *BridgeMatcher) handleEndOfRecordMessage(clientID int64, data []interface{}) error {
	//slog.Info("Received EOF record message from ", "clientID", clientID, "sender", sender)
	_, sender, err := inner.DeserializeEOR(data)
	if err != nil {
		slog.Error("While deserializing EOR msg", "err", err, "clientID", clientID)
		return err
	}
	slog.Info("Received EOF record message from ", "clientID", clientID, "sender", sender)
	bridgeMatcher.updateClientEORCondition(clientID, sender)
	if !bridgeMatcher.assertClientEORCondition(clientID) {
		return nil
	}

	if bridgeMatcher.restoring { // en caso de restauracion no envio nada dado que ya se ackio -> ya se hizo todo esto
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

// ------------------- TransactionBatch -------------------

func (bridgeMatcher *BridgeMatcher) handleTransactionBatchMessage(clientID int64, data []interface{}) error {
	records, err := inner.DeserializeAccountsMessage(data)
	if err != nil {
		return err
	}

	if bridgeMatcher.Registers[clientID] == nil {
		bridgeMatcher.Registers[clientID] = make(map[string]map[string]int)
	}
	clientRegisters := bridgeMatcher.Registers[clientID]

	for _, record := range records {
		origin := makeKey(record.FromAccount, record.FromBank)
		destiny := makeKey(record.ToAccount, record.ToBank)

		// Inicializar mapas si no existen
		if clientRegisters[origin] == nil {
			clientRegisters[origin] = make(map[string]int)
		}

		// Agregar cuenta destino al set
		//slog.Info("transaccion a guardar", "origen", origin, "destiny", destiny)
		clientRegisters[origin][destiny] = 1
	}

	return nil
}

// ------------------- SuspiciousAccount -------------------

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
	if bridgeMatcher.Registers[clientID] == nil {
		return nil
	}

	possibleBridgesAndSinks := make(map[string]map[string]int)

	for _, possibleBridge := range possibleBridges {

		// si NO hay registros de ese puente salto
		if bridgeMatcher.Registers[clientID][possibleBridge] == nil {
			continue
		}
		// sino
		// agrego el puente al listado
		// y agrego las cuentas a las que le haya enviado al listado
		possibleBridgesAndSinks[possibleBridge] = bridgeMatcher.Registers[clientID][possibleBridge]
	}

	if bridgeMatcher.restoring { // en caso de restauracion no envio nada dado que ya se ackio -> ya se hizo todo esto
		return nil
	}

	msg, err := inner.SerializesPossibleFraudDestinations(clientID, origin, possibleBridgesAndSinks)
	if err != nil {
		slog.Info("While serializing PossibleFraudDestinations message", "err", err, "clientID", clientID)
		return err
	}
	bridgeMatcher.outputExchange.SendToTopic(*msg, topic)
	return nil
}

// ------------------- ReadyForEOR -------------------

func (bridgeMatcher *BridgeMatcher) handleReadyForEOR(clientID int64, data []interface{}) error {
	senderID, err := inner.DeserializeReadyForEOR(data)

	if err != nil {
		return err
	}

	bridgeMatcher.updateBridgeReadyCondition(clientID, senderID)
	if !bridgeMatcher.assertAllBridgesReady(clientID) || bridgeMatcher.restoring {
		return nil // still waiting for other bridge_matcher
	}

	// all peers are done sending suspects, safe to EOF downstream
	msg, err := inner.SerializeEOR(clientID, false,
		makeKey("bridge_matcher", bridgeMatcher.config.ID))
	if err != nil {
		return err
	}

	for i := range bridgeMatcher.config.NextFaseWorkersAmount {
		topic := makeKey(bridgeMatcher.config.NextFaseWorkersPrefix, i)
		bridgeMatcher.outputExchange.SendToTopic(*msg, topic)
	}

	// cleanup after all EOFs are sent
	bridgeMatcher.cleanupClient(clientID)
	return nil
}

// -------------------------------------- Utils --------------------------------------

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
