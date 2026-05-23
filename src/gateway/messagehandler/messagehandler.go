package messagehandler

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

type MessageHandler struct {
	userId                int64
	results               map[transaction.QueryID]transaction.QueryResult // Resultados de las querys
	completedQueryCounter int                                             // Contador de las cantidad de querys terminadas
}

type accumulatorFunc func(current, incoming any) any

func appendGeneric[T any](current, incoming any) any {
	currentSlice := current.([]T)
	incomingSlice := incoming.([]T)
	return append(currentSlice, incomingSlice...)
}

var queryAccumulators = map[transaction.QueryID]accumulatorFunc{
	transaction.Query1: appendGeneric[transaction.LowAmountTransfer]}

func NewMessageHandler() MessageHandler {
	n := rand.Int64()
	return MessageHandler{
		userId:                n,
		results:               make(map[transaction.QueryID]transaction.QueryResult),
		completedQueryCounter: 0,
	}
}

func (messageHandler *MessageHandler) SerializeDataMessage(transactionBatch []transaction.Transaction) (*middleware.Message, error) {
	return inner.SerializeMessage(messageHandler.userId, transactionBatch)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	data := []transaction.Transaction{}
	return inner.SerializeMessage(messageHandler.userId, data)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) (*transaction.QueriesResult, bool, error) {
	clientID, queryResult, isEOF, err := inner.DeserializeQueryResultMessage(message)
	if err != nil {
		return nil, false, err
	}
	if clientID != messageHandler.userId {
		// ClientID dismatch, skipping...
		return nil, false, nil
	}

	if isEOF {
		slog.Info("Receive EOF from query", "queryID", queryResult.QueryID, "clientID", clientID)
		messageHandler.completedQueryCounter++
		if messageHandler.completedQueryCounter == 1 { // Por ahora solo se resolvio la 1ra query (al final sera == 5)
			queriesResult := &transaction.QueriesResult{
				ClientID: messageHandler.userId,
				Results:  make(map[transaction.QueryID]transaction.QueryResult),
			}
			for queryID, res := range messageHandler.results {
				queriesResult.Results[queryID] = res
			}
			messageHandler.results = make(map[transaction.QueryID]transaction.QueryResult)
			messageHandler.completedQueryCounter = 0
			slog.Info("All result queries received, returning...", "clientID", clientID)
			return queriesResult, true, nil
		}
		slog.Info("Receive EOF from query, waiting for more...", "queryID", queryResult.QueryID, "clientID", clientID)
		return nil, true, nil
	}
	currentResult, exists := messageHandler.results[queryResult.QueryID]
	if !exists {
		slog.Info("First result query received, waiting for more...", "queryID", queryResult.QueryID, "clientID", clientID)
		messageHandler.results[queryResult.QueryID] = *queryResult // Es la primera vez que se guarda
		return nil, true, nil
	}

	accumulator, exists := queryAccumulators[queryResult.QueryID]
	if !exists {
		return nil, false, fmt.Errorf("unknown query id during accumulation: %d", queryResult.QueryID)
	}

	currentResult.Transactions = accumulator(currentResult.Transactions, queryResult.Transactions)
	messageHandler.results[queryResult.QueryID] = currentResult
	return nil, true, nil
}
