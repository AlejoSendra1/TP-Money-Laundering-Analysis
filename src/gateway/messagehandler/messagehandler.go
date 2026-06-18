package messagehandler

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

const QUERIES_AMOUNT = 5

type MessageHandler struct {
	UserId             int64
	eorCountByQuery    map[transaction.QueryID]int // EOFs recibidos por query
	eorExpectedByQuery map[transaction.QueryID]int // Cantidad de EOFs esperados por query
}

type accumulatorFunc func(current, incoming any) any

func NewMessageHandler(eofExpectedByQuery map[transaction.QueryID]int) MessageHandler {
	n := rand.Int64()
	slog.Info("UserId created", "value", n)

	eorCountByQuery := make(map[transaction.QueryID]int)
	eorCountByQuery[transaction.Query1] = 0
	eorCountByQuery[transaction.Query2] = 0
	eorCountByQuery[transaction.Query3] = 0
	eorCountByQuery[transaction.Query4] = 0
	eorCountByQuery[transaction.Query5] = 0

	return MessageHandler{
		UserId:             n,
		eorCountByQuery:    eorCountByQuery,
		eorExpectedByQuery: eofExpectedByQuery,
	}
}

func (messageHandler *MessageHandler) SerializeDataMessage(transactionBatch []transaction.Transaction) (*middleware.Message, error) {
	return inner.SerializeMessage(messageHandler.UserId, transactionBatch)
}

func (messageHandler *MessageHandler) SerializeEORMessage() (*middleware.Message, error) {
	return inner.SerializeEOR(messageHandler.UserId, true, "gateway")
}

func (messageHandler *MessageHandler) HandleQueryEOR(message *inner.MessageClient) (bool, error) {
	queryID, err := getQueryID(message.Data)
	if err != nil {
		return false, err
	}

	currentCount := messageHandler.eorCountByQuery[queryID]
	expectedCount := messageHandler.eorExpectedByQuery[queryID]

	if currentCount >= expectedCount {
		slog.Info("Unexpected EOR received: expected count already reached",
			"query", queryID-3,
			"current_count", currentCount,
			"expected_count", expectedCount,
		)
		return false, nil
	}
	messageHandler.eorCountByQuery[queryID]++
	slog.Info("EOR received for query",
		"query", queryID-3,
		"current_count", messageHandler.eorCountByQuery[queryID],
		"expected_count", expectedCount,
	)
	if messageHandler.assertClientEnd() {
		return true, nil
	}

	return false, nil
}

func (messageHandler *MessageHandler) assertClientEnd() bool {
	completedQueries := 0
	for queryID, expected := range messageHandler.eorExpectedByQuery {
		current := messageHandler.eorCountByQuery[queryID]
		if current == expected {
			slog.Info("All EORs received for specific query", "query", queryID-3)
			completedQueries++
		} else {
			slog.Info("Query still awaiting EORs",
				"query", queryID-3,
				"current_count", current,
				"expected_count", expected,
			)
		}
	}
	if completedQueries == QUERIES_AMOUNT {
		slog.Info("All queries successfully processed. Terminating client session",
			"user_id", messageHandler.UserId,
			"completed_queries", completedQueries,
		)
		return true
	}
	slog.Info("Client processing in progress: awaiting remaining queries",
		"completed_queries", completedQueries,
		"total_queries_expected", QUERIES_AMOUNT,
	)
	return false
}

func getQueryID(data []interface{}) (transaction.QueryID, error) {
	queryIDFloat, ok := data[0].(float64)
	if !ok {
		return 0, fmt.Errorf("expected float64 for queryID, got %T", data)
	}

	queryID := transaction.QueryID(int(queryIDFloat))

	return queryID, nil
}
