package messagehandler

import (
	"tp_distribuidos/src/common/messageprotocol/inner"
	"tp_distribuidos/src/common/middleware"
	"tp_distribuidos/src/common/transaction"
)

type MessageHandler struct {
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{}
}

func (messageHandler *MessageHandler) SerializeDataMessage(transactionBatch []transaction.Transaction) (*middleware.Message, error) {
	return inner.SerializeMessage(transactionBatch)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	data := []transaction.Transaction{}
	return inner.SerializeMessage(data)
}

//func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]transaction.Transaction, error) {
//fruitRecords, err := inner.DeserializeMessage(message)
//if err != nil {
//	return nil, err
//}
//return fruitRecords, nil
//slog.Debug("a implementar la deserializacion de cada resultado, dependera de cada query dados los diferentes valores a recibir")
//}
