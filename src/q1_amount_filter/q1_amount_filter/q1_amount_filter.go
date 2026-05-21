package q1_amount_filter

import (
	"log/slog"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

type Q1AmountFilterConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	InputTopic        string
	InputExchangeName string
	OutputQueue       string
}

type Q1AmountFilter struct {
	inputExchange middleware.Middleware
	outputQueue   middleware.Middleware
	accumulated   map[int64][]transaction.LowAmountTransfer
	config        Q1AmountFilterConfig
}

func NewQ1AmountFilter(config Q1AmountFilterConfig) (*Q1AmountFilter, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputExchangeName, []string{config.InputTopic}, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}

	return &Q1AmountFilter{
		inputExchange: inputExchange,
		outputQueue:   outputQueue,
		accumulated:   make(map[int64][]transaction.LowAmountTransfer),
		config:        config,
	}, nil
}

func (q1AmountFilter *Q1AmountFilter) Run() {
	q1AmountFilter.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		q1AmountFilter.handleMessage(&msg, ack, nack)
	})
}

func (q1AmountFilter *Q1AmountFilter) handleMessage(msg *middleware.Message, ack func(), nack func()) {
	clientID, transactionRecords, isEof, err := inner.DeserializeRawTransactionsMessage(msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", clientID)
		nack()
		return
	}

	if isEof {
		if err := q1AmountFilter.handleEndOfRecordMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err, "clientID", clientID)
			nack()
			return
		}
		ack()
		return
	}

	q1AmountFilter.handleDataMessage(transactionRecords, clientID)
	ack()
}

func (q1AmountFilter *Q1AmountFilter) handleEndOfRecordMessage(clientID int64) error {
	transactions, exists := q1AmountFilter.accumulated[clientID]
	var queryResult transaction.QueryResult
	// Si nunca me envio nada, envio vacio
	if !exists {
		queryResult = transaction.QueryResult{
			QueryID:      transaction.Query1,
			Transactions: []transaction.LowAmountTransfer{},
		}
	} else { // Envio lo acumulado
		queryResult = transaction.QueryResult{
			QueryID:      transaction.Query1,
			Transactions: transactions,
		}
	}
	if err := q1AmountFilter.sendOutput(queryResult, clientID); err != nil {
		return err
	}

	// Envio EOF, deberia sincronizas con demas instancias de q1 amount filter
	queryResult.Transactions = []transaction.LowAmountTransfer{}
	if err := q1AmountFilter.sendOutput(queryResult, clientID); err != nil {
		return err
	}

	delete(q1AmountFilter.accumulated, clientID)
	// TODO: Crear un join intermedio para juntar la data del client y enviarla toda junta, porque sino el gateway se va a encargar
	// de juntarlo al haber mas isntancias de q1 amount filter
	slog.Info("All data sent", "clientID", clientID)
	return nil
}

func (q1AmountFilter *Q1AmountFilter) handleDataMessage(transactionRecords []transaction.Transaction, clientID int64) {
	if _, ok := q1AmountFilter.accumulated[clientID]; !ok {
		slog.Info("New client arrived", "clientID", clientID)
		q1AmountFilter.accumulated[clientID] = []transaction.LowAmountTransfer{}
	}

	for _, transactionRecord := range transactionRecords {
		if transactionRecord.Amount < 50.0 {
			q1AmountFilter.accumulated[clientID] = append(q1AmountFilter.accumulated[clientID], transaction.LowAmountTransfer{
				FromBank:    transactionRecord.FromBank,
				FromAccount: transactionRecord.FromAccount,
				ToBank:      transactionRecord.ToBank,
				ToAccount:   transactionRecord.ToAccount,
				Amount:      transactionRecord.Amount,
			})
		}
	}
}

func (q1AmountFilter *Q1AmountFilter) sendOutput(queryResult transaction.QueryResult, clientID int64) error {
	message, err := inner.SerializeQueryResultMessage(clientID, queryResult)
	if err != nil {
		slog.Debug("While serializing data message", "err", err, "clientID", clientID)
		return err
	}
	if err := q1AmountFilter.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending data message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}
