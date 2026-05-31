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
	UserId             int64
	eorCountByQuery    map[transaction.QueryID]int // EOFs recibidos por query
	eorExpectedByQuery map[transaction.QueryID]int // Cantidad de EOFs esperados por query
}

type accumulatorFunc func(current, incoming any) any

func NewMessageHandler() MessageHandler {
	n := rand.Int64()
	slog.Info("UserId created", "value", n)

	// A MODIFICAR CON VARIABLES DE ENTORNO - TO DO
	eofExpectedByQuery := make(map[transaction.QueryID]int)
	eofExpectedByQuery[transaction.Query1] = 0
	eofExpectedByQuery[transaction.Query2] = 0
	eofExpectedByQuery[transaction.Query3] = 0
	eofExpectedByQuery[transaction.Query4] = 3
	eofExpectedByQuery[transaction.Query5] = 0

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

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	return inner.SerializeEOF(messageHandler.UserId, true, "gateway")
}

func (messageHandler *MessageHandler) HandleQueryEOR(message *inner.MessageClient) (bool, error) {
	queryID, err := getQueryID(message.Data)
	if err != nil {
		return false, err
	}

	/*
		if messageHandler.eorCountByQuery[queryID] == messageHandler.eorExpectedByQuery[queryID] {
			slog.Info("esta situacion no deberia darse, hay alguna entidad de mas no declarada")
			return false, nil
		}
	*/
	messageHandler.eorCountByQuery[queryID] += 1

	if messageHandler.assertClientEnd() {
		return true, nil
	}

	return false, nil
}

func (messageHandler *MessageHandler) assertClientEnd() bool {
	for key, val := range messageHandler.eorCountByQuery {
		if messageHandler.eorExpectedByQuery[key] > val {
			return false
		}
	}
	return true
}

func getQueryID(data []interface{}) (transaction.QueryID, error) {
	queryIDFloat, ok := data[0].(float64)
	if !ok {
		return 0, fmt.Errorf("expected float64 for queryID, got %T", data)
	}

	queryID := transaction.QueryID(int(queryIDFloat))

	return queryID, nil
}
