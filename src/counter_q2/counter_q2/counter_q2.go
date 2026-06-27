package counter_q2

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"tp_distribuidos/common/heatbeat"

	"tp_distribuidos/common/datasaver"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
	"tp_distribuidos/common/worker"
)

const LOGS_UNTIL_CHECKPOINT = 250

//docker compose run -e RESTAURATE="TRUE" q2_counter_0

type CounterQ2Config struct {
	ID              int
	WorkerID        string
	MomHost         string
	MomPort         int
	InputQueue      string
	InputExchange   string
	InputTopic      string
	OutputPrefix    string
	JoinAmount      int
	CounterAmount   int
	ControlExchange string
	BatchSize       int
	USDFilterAmount int
}

type bankEntry struct {
	Amount  float64 `json:"amont"`
	Account string  `json:"account"`
}

// struct usado para el guardado de checkpoints y recuperacion de datos
type CheckpointData struct {
	TopByClient map[int64]map[int]bankEntry `json:"topByClient"`
	EofCounter  map[int64][]string          `json:"eofCounter"`
}

// CounterQ2 keeps the maximum-amount transaction per bank per client,
// then shards partial results to the downstream joiners.
type CounterQ2 struct {
	config          CounterQ2Config
	inputQueue      middleware.Middleware
	outputExchanges []middleware.Middleware
	controlOutputs  []middleware.Middleware // one per peer
	controlInput    middleware.Middleware
	eofCounter      map[int64][]string
	mutex           sync.Mutex
	topByClient     map[int64]map[int]bankEntry // client_id -> bankCode -> bankEntry{amount, account}
	// for data saving and restoration
	handleFunctions worker.MessageHandlerMap
	dataSaver       *datasaver.DataSaver
	heartbeat       *heatbeat.HeartbeatSender
}

func getJoinerIndex(bank string, joinAmount int) int {
	sum := md5.Sum([]byte(bank))
	val := binary.BigEndian.Uint64(sum[:8])
	return int(val % uint64(joinAmount))
}

func NewCounterQ2(config CounterQ2Config) (*CounterQ2, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	// Input queue (shared/competing consumers)
	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating input queue: %w", err)
	}
	if err = inputQueue.BindToTopics(config.InputExchange, config.InputTopic); err != nil {
		inputQueue.Close()
		return nil, fmt.Errorf("binding input queue to exchange: %w", err)
	}

	// One output exchange per joiner
	outputExchanges := make([]middleware.Middleware, config.JoinAmount)
	for i := 0; i < config.JoinAmount; i++ {
		key := fmt.Sprintf("%s_%d", config.OutputPrefix, i)
		ex, err := middleware.CreateExchangeMiddleware(config.OutputPrefix, []string{key}, connSettings, "") // No consume, solo envia
		if err != nil {
			inputQueue.Close()
			for j := 0; j < i; j++ {
				outputExchanges[j].Close()
			}
			return nil, fmt.Errorf("creating output exchange %d: %w", i, err)
		}
		outputExchanges[i] = ex
	}

	// Control output exchanges (one per peer)
	var controlOutputs []middleware.Middleware
	for i := 0; i < config.CounterAmount; i++ {
		if i == config.ID {
			continue
		}
		key := fmt.Sprintf("%s_%d", config.ControlExchange, i)
		ex, err := middleware.CreateExchangeMiddleware(config.ControlExchange, []string{key}, connSettings, "") // No consume, solo envia
		if err != nil {
			inputQueue.Close()
			for _, o := range outputExchanges {
				o.Close()
			}
			for _, c := range controlOutputs {
				c.Close()
			}
			return nil, fmt.Errorf("creating control output exchange for peer %d: %w", i, err)
		}
		controlOutputs = append(controlOutputs, ex)
	}

	// Own control input exchange (exclusive queue, receives peer notifications)
	myKey := fmt.Sprintf("%s_%d", config.ControlExchange, config.ID)
	controlInput, err := middleware.CreateExchangeMiddleware(config.ControlExchange, []string{myKey}, connSettings, myKey) // Nombre de queue igual a key, es unico
	if err != nil {
		inputQueue.Close()
		for _, o := range outputExchanges {
			o.Close()
		}
		for _, c := range controlOutputs {
			c.Close()
		}
		return nil, fmt.Errorf("creating control input exchange: %w", err)
	}

	// para q siempre se use la misma queue ---------------------- se deberia refactorisar el control
	controlQueueName := fmt.Sprintf("%s_control_%d", config.ControlExchange, config.ID)
	controlInput, err = middleware.CreateQueueMiddleware(controlQueueName, connSettings)
	if err != nil {
		inputQueue.Close()
		for _, o := range outputExchanges {
			o.Close()
		}
		for _, c := range controlOutputs {
			c.Close()
		}
		return nil, fmt.Errorf("creating control input queue: %w", err)
	}

	if err := controlInput.BindToTopics(config.ControlExchange, myKey); err != nil {
		inputQueue.Close()
		controlInput.Close()
		for _, o := range outputExchanges {
			o.Close()
		}
		for _, c := range controlOutputs {
			c.Close()
		}
		return nil, fmt.Errorf("binding control input queue: %w", err)
	}

	// para persistir la info ante posibles caidas
	//se podria agregar el nombre de del archivo de restauracion como var de entorno
	dataSaver, err := datasaver.NewDataSaver(fmt.Sprintf("/persistence/q2_counter_%d", config.ID), LOGS_UNTIL_CHECKPOINT)
	if err != nil {
		return nil, err
	}

	hb, err := heatbeat.NewHeartbeatSender(config.WorkerID, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating heartbeat sender: %w", err)
	}

	counter := &CounterQ2{
		config:          config,
		inputQueue:      inputQueue,
		outputExchanges: outputExchanges,
		controlOutputs:  controlOutputs,
		controlInput:    controlInput,
		topByClient:     make(map[int64]map[int]bankEntry),
		eofCounter:      make(map[int64][]string),
		dataSaver:       dataSaver,
		heartbeat:       hb,
	}
	counter.handleFunctions = worker.MessageHandlerMap{
		inner.EndOfRecords:     counter.handleEndOfRecordMessage,
		inner.TransactionBatch: counter.processBatch,
	}

	return counter, nil
}

// Restaurate restaura el estado del nodo al estado en el que se encontraba previo a su caida
func (c *CounterQ2) Restaurate() error {
	// primero restauramos el checkpoint
	var checkpoint CheckpointData

	thereIsCheckpoint, err := c.dataSaver.GetRestaurationCheckpoint(&checkpoint)
	if err != nil { // habria q agregar retrys?
		return err
	}
	if thereIsCheckpoint == true {
		slog.Info("cargando en base a checkpoint")
		c.topByClient = checkpoint.TopByClient
		c.eofCounter = checkpoint.EofCounter
	}

	var savedDataVar middleware.Message
	var thereIsLogs bool

	for {
		thereIsLogs, err = c.dataSaver.GetDataFromLogs(&savedDataVar)
		if err != nil { // habria q modificar para retrys
			return err
		}
		if !thereIsLogs {
			break
		}
		// gracias a que hay idempotencia y no se hacen envios hasta finalizar, no hay problema
		if err := worker.HandleMessageV2(&savedDataVar, c.handleFunctions); err != nil {
			return err
		}
	}

	return nil
}

// Run starts the worker. It returns when processing is complete or a signal is received.
func (counter *CounterQ2) Run() {
	go counter.handleSigterm()
	counter.heartbeat.Start()

	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		counter.controlInput.StartConsuming(counter.handleControlMessage)
	}()

	counter.inputQueue.StartConsuming(counter.handleMessage)

	// Stop control consumer once main consuming finishes
	counter.controlInput.StopConsuming()
	waitGroup.Wait()
	counter.close()
}

func (counter *CounterQ2) handleSigterm() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM)
	<-signalChannel
	slog.Info("SIGTERM received, stopping consumers")
	counter.heartbeat.Stop()
	counter.inputQueue.StopConsuming()
	counter.controlInput.StopConsuming()
}

func (c *CounterQ2) GetCheckpointData() any {
	return CheckpointData{
		TopByClient: c.topByClient,
		EofCounter:  c.eofCounter,
	}
	// agregar log counter asi evitamos que se acumulen mas logs que los indicados
	// dado q en caso de caida y vuelta se podrian acumular mas logs q los indicados < (guardados + limite) - a probar
}

// handleMessage processes messages from the shared input queue.
// And periodicaly stores checkpoints to keep the backup to date
func (c *CounterQ2) handleMessage(middlewareMsg middleware.Message, ack func(), nack func()) {
	if err := worker.HandleMessageV2(&middlewareMsg, c.handleFunctions); err != nil {
		nack()
		return
	}

	c.dataSaver.Save(middlewareMsg, c) // persistencia de datos
	ack()
}

func (c *CounterQ2) handleEndOfRecordMessage(clientID int64, data []interface{}) error {
	slog.Info("EOF received, notifying peers and flushing", "client_id", clientID)

	_, sender, err := inner.DeserializeEOR(data) // no hace falta el bool dado que se utiliza otro canal para propagar
	if err != nil {
		slog.Error("While deserializing EOR msg", "err", err, "clientID", clientID)
		return err
	}

	if err := c.sendControlEOF(clientID, sender); err != nil {
		slog.Error("Sending control EOF", "err", err, "client_id", clientID)
		return err
	}

	if err := c.flushClient(clientID, sender); err != nil {
		slog.Error("Flushing client", "err", err, "client_id", clientID)
		return err
	}
	return nil
}

// processBatch updates in-memory state: keeps max-amount entry per bank.
func (counter *CounterQ2) processBatch(clientID int64, data []interface{}) error {
	slog.Info("dato recibido", "val", data)
	transactions, err := inner.DeserializeTransactionBatch(data)
	if err != nil {
		slog.Error("While deserializing transactions from message", "err", err, "clientID", clientID)
		return err
	}

	counter.mutex.Lock()
	defer counter.mutex.Unlock()
	banks, ok := counter.topByClient[clientID]
	if !ok {
		banks = make(map[int]bankEntry)
		counter.topByClient[clientID] = banks
		counter.eofCounter[clientID] = make([]string, 0, 5)
	}
	for _, tx := range transactions {
		prev, exists := banks[tx.FromBank]
		if !exists || tx.Amount > prev.Amount {
			banks[tx.FromBank] = bankEntry{Amount: tx.Amount, Account: tx.FromAccount}
			//slog.Info("New top", "client_id", clientID, "bank", tx.FromBank, "amount", tx.Amount, "account", tx.FromAccount)
		}
	}

	return nil
}

// handleControlMessage processes EOF notifications from peer pods.
func (counter *CounterQ2) handleControlMessage(middlewareMsg middleware.Message, ack, nack func()) {
	msg, err := inner.DeserializeMessage(&middlewareMsg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}
	_, sender, err := inner.DeserializeEOR(msg.Data)
	if err != nil {
		slog.Error("Deserializing control message", "err", err)
		nack()
		return
	}
	slog.Info("Control EOF received from peer, flushing", "client_id", msg.ClientID)

	if err := counter.flushClient(msg.ClientID, sender); err != nil {
		slog.Error("Flushing client from control", "err", err, "client_id", msg.ClientID)
		nack()
		return
	}
	ack()
}

// sendControlEOF notifies all peer pods that this pod received an EOF for clientID.
func (counter *CounterQ2) sendControlEOF(clientID int64, sender string) error {
	msg, err := inner.SerializeEOR(clientID, false, sender)
	if err != nil {
		return err
	}
	for _, ex := range counter.controlOutputs {
		if err := ex.Send(*msg); err != nil {
			return fmt.Errorf("sending control EOF: %w", err)
		}
	}
	return nil
}

// flushClient pops partial state for clientID and sends it to the correct joiner(s).
func (counter *CounterQ2) flushClient(clientID int64, sender string) error {
	slog.Info("tomando lock")
	counter.mutex.Lock()
	slog.Info("lock tomado")

	if slices.Contains(counter.eofCounter[clientID], sender) {
		slog.Info("Sender de EOR repetido")
		return nil
	}

	counter.eofCounter[clientID] = append(counter.eofCounter[clientID], sender)

	if len(counter.eofCounter[clientID]) != counter.config.USDFilterAmount {
		slog.Info("Waiting for more EOFs from usd filter")
		counter.mutex.Unlock()
		return nil
	}
	banks := counter.topByClient[clientID]
	delete(counter.topByClient, clientID)
	delete(counter.eofCounter, clientID)
	counter.mutex.Unlock()

	if err := counter.sendData(clientID, banks); err != nil {
		return err
	}
	return counter.sendEOF(clientID)
}

// sendData shards the partial result by bank hash and sends each shard to the appropriate joiner.
func (counter *CounterQ2) sendData(clientID int64, banks map[int]bankEntry) error {
	// Group rows by joiner index (sharding by bank code)
	shards := make(map[int][]transaction.MaxBankTransaction)
	for bankCode, entry := range banks {
		idx := getJoinerIndex(fmt.Sprintf("%d", bankCode), counter.config.JoinAmount)
		shards[idx] = append(shards[idx], transaction.MaxBankTransaction{
			BankCode: bankCode,
			Account:  entry.Account,
			Amount:   entry.Amount,
		})
	}

	for idx, rows := range shards {
		for i := 0; i < len(rows); i += counter.config.BatchSize {
			end := i + counter.config.BatchSize
			if end > len(rows) {
				end = len(rows)
			}
			chunk := rows[i:end]
			msg, err := inner.SerializeMaxBankTransactionMessage(clientID, chunk)
			if err != nil {
				return fmt.Errorf("serializing data chunk for joiner %d: %w", idx, err)
			}
			if err := counter.outputExchanges[idx].Send(*msg); err != nil {
				return fmt.Errorf("sending data chunk to joiner %d: %w", idx, err)
			}
			//slog.Info("Sent batch to joiner", "client_id", clientID, "joiner", idx, "count", len(chunk))
		}
	}
	return nil
}

// sendEOF forwards an EOF marker to every joiner exchange.
func (counter *CounterQ2) sendEOF(clientID int64) error {
	msg, err := inner.SerializeEOR(clientID, true, "q2Counter_"+strconv.Itoa(counter.config.ID))
	if err != nil {
		return fmt.Errorf("serializing EOF: %w", err)
	}
	for i, ex := range counter.outputExchanges {
		if err := ex.Send(*msg); err != nil {
			return fmt.Errorf("sending EOF to joiner %d: %w", i, err)
		}
	}
	slog.Info("EOF forwarded to all joiners", "client_id", clientID)
	return nil
}

func (counter *CounterQ2) close() {
	counter.inputQueue.Close()
	counter.controlInput.Close()
	for _, ex := range counter.outputExchanges {
		ex.Close()
	}
	for _, ex := range counter.controlOutputs {
		ex.Close()
	}
}
