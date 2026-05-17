package messagehandler

import (
	"math/rand/v2"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

type MessageHandler struct {
	userId int64
}

func NewMessageHandler() MessageHandler {
	n := rand.Int64()
	return MessageHandler{userId: n}
}

func (messageHandler *MessageHandler) SerializeDataMessage(transactionBatch []transaction.Transaction) (*middleware.Message, error) {
	return inner.SerializeMessage(messageHandler.userId, transactionBatch)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	data := []transaction.Transaction{}
	return inner.SerializeMessage(messageHandler.userId, data)
}

//func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]transaction.Transaction, error) {
//fruitRecords, err := inner.DeserializeMessage(message)
//if err != nil {
//	return nil, err
//}
//return fruitRecords, nil
//slog.Debug("a implementar la deserializacion de cada resultado, dependera de cada query dados los diferentes valores a recibir")
//}
