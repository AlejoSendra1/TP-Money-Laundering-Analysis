package worker

import (
	"log/slog"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
)

// interface
type Worker interface {
	GetCheckpointData() any
}

// utils

type MessageHandlerMap map[inner.MsgType]func(clientID int64, data []interface{}) error

func HandleMessage(
	middlewareMsg *middleware.Message,
	ack func(),
	nack func(),
	handlers MessageHandlerMap,
) {
	msg, err := inner.DeserializeMessage(middlewareMsg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}

	handler, ok := handlers[msg.MsgType]
	if !ok {
		slog.Error("Unexpected msg type received", "msgType", msg.MsgType, "clientID", msg.ClientID)
		nack()
		return
	}
	if err := handler(msg.ClientID, msg.Data); err != nil {
		slog.Error("While handling data message", "err", err, "clientID", msg.ClientID)
		nack()
		return
	}

	ack()
}

func HandleMessageV2(
	middlewareMsg *middleware.Message,
	handlers MessageHandlerMap,
) error {
	msg, err := inner.DeserializeMessage(middlewareMsg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return err
	}

	handler, ok := handlers[msg.MsgType]
	if !ok {
		slog.Error("Unexpected msg type received", "msgType", msg.MsgType, "clientID", msg.ClientID)
		return err
	}
	if err := handler(msg.ClientID, msg.Data); err != nil {
		slog.Error("While handling data message", "err", err, "clientID", msg.ClientID)
		return err
	}

	return nil
}
