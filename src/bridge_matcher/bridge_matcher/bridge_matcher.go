package group

import (
	"log/slog"

	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

type BridgeMatcherConfig struct {
	MomHost               string
	MomPort               int
	InputQueue            string
	InputTopic            string
	InputExchangeName     string
	ControlExchange       string
	OutputExchangeName    string
	NextFaseWorkersAmount int
	NextFaseWorkersPrefix string
}

type BridgeMatcher struct {
	inputQueue     middleware.Middleware
	outputExchange middleware.ExchangeMiddleware
	config         BridgeMatcherConfig
}

func NewBridgeMatcher(config BridgeMatcherConfig) (*BridgeMatcher, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	inputQueue, err := middleware.NewQueueMiddleware(config.InputQueue, connSettings) // FALTA ASOCIARLA AL TOPIC
	if err != nil {
		return nil, err
	}
	inputQueue.BindToTopics(config.InputExchangeName, config.InputTopic)

	outputExchange, err := middleware.NewDinamicExchangeMiddleware(config.OutputExchangeName, connSettings) // modifcar la forma en la que se manejan los topics
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &BridgeMatcher{
		inputQueue:     inputQueue,
		outputExchange: *outputExchange,
		config:         config,
	}, nil
}

func (bridgeMatcher *BridgeMatcher) Run() {
	bridgeMatcher.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		bridgeMatcher.handleMessage(&msg, ack, nack)
	})
}

func (bridgeMatcher *BridgeMatcher) handleMessage(msg *middleware.Message, ack func(), nack func()) {
	clientID, transactionRecords, isEof, err := inner.DeserializeRawTransactionsMessage(msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", clientID)
		nack()
		return
	}

	if isEof {
		if err := bridgeMatcher.handleEndOfRecordMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err, "clientID", clientID)
			nack()
			return
		}
		ack()
		return
	}
	if err := bridgeMatcher.handleDataMessage(transactionRecords, clientID); err != nil {
		slog.Error("While handling data message", "err", err, "clientID", clientID)
		nack()
		return
	}
	ack()
}

func (bridgeMatcher *BridgeMatcher) handleEndOfRecordMessage(clientID int64) error {
	slog.Info("Sent EOF record message, clientID", clientID)
	// se debe propagar entre todos los group workers y estos a todos los brideges analizers
	return bridgeMatcher.sendOutput([]transaction.Transaction{}, clientID)
}

func (bridgeMatcher *BridgeMatcher) handleDataMessage(transactionRecords []transaction.Transaction, clientID int64) error {
	for _, transactionRecord := range transactionRecords {
		//if transactionRecord.Currency == USDCurrencyName {
		//	if err := usdFilter.sendOutput([]transaction.Transaction{transactionRecord}, clientID); err != nil {
		//		return err
		//	}
		//}

		// obtener el hash deterministico de la cuenta de origen
		// en base al numero obtenido crear el topic a utilizar con el formato "NextFaseWorkersPrefix + _ + numerito obtenido"
		// ir acumulando las transacciones a enviar a cada bridge en particular
		// una vez analisado el batch recibido se debe enviar correctamente cada batch al bridge correspondiente
	}
	return nil
}

func (bridgeMatcher *BridgeMatcher) sendOutput(transactionRecords []transaction.Transaction, clientID int64) error {

	// modificar para que cada output se envie a un determinado topic / bridge

	message, err := inner.SerializeMessage(clientID, transactionRecords)
	if err != nil {
		slog.Debug("While serializing data message", "err", err, "clientID", clientID)
		return err
	}
	if err := usdFilter.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending data message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}
