package worker

import (
	"fmt"
	"log/slog"
	"tp_distribuidos/common/batch_utils"
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

func HandleMessageV3(
	middlewareMsg *middleware.Message,
	handlers MessageHandlerMap,
	deduplicator *batch_utils.MultiClientDeduplicator,
) error {
	msg, err := inner.DeserializeMessage(middlewareMsg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return err
	}

	batchID := batch_utils.GenerateBatchID([]byte(middlewareMsg.Body))
	if deduplicator.IsDuplicate(int(msg.ClientID), batchID) {
		slog.Warn("Duplicate message detected", "clientID", msg.ClientID, "batchID", batchID)
		return nil
	}

	handler, ok := handlers[msg.MsgType]
	if !ok {
		slog.Error("Unexpected msg type received", "msgType", msg.MsgType, "clientID", msg.ClientID)
		return fmt.Errorf("unexpected msg type: %v", msg.MsgType)
	}
	if err = handler(msg.ClientID, msg.Data); err != nil {
		slog.Error("While handling data message", "err", err, "clientID", msg.ClientID)
		return err
	}
	return nil
}
