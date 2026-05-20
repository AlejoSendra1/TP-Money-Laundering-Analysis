package messagehandler

import (
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
	clientID, queryResult, err := inner.DeserializeQueryResultMessage(message)
	if err != nil {
		return nil, false, err
	}
	if clientID != messageHandler.userId {
		slog.Info("ClientID dismatch, skipping...", "clientID", clientID, "messageHandler.userId", messageHandler.userId)
		return nil, false, nil
	}

	messageHandler.results[queryResult.QueryID] = transaction.QueryResult{
		QueryID:      queryResult.QueryID,
		Transactions: queryResult.Transactions,
	}
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
	slog.Info("Result query received, waiting for more...", "clientID", clientID, "queryID", queryResult.QueryID)
	return nil, true, nil
}
