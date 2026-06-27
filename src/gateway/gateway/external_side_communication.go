package gateway

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"tp_distribuidos/clientregistry"
	"tp_distribuidos/common/messageprotocol/external"
	"tp_distribuidos/common/messageprotocol/external/safeio"
)

func (gateway *Gateway) handleClientConnection(socket net.Conn) (int64, error) {
	msgType, err := external.ReadMsgType(socket)
	if err != nil {
		slog.Error("While reading message type handling client connection", "err", err)
		return 0, err
	}

	var id int64
	switch msgType {

	case external.ConnectionMsg:
		slog.Info("New client received ")
		id = gateway.getNewClientId()

		err = external.WriteConnectionMsgResponse(socket, id)
		if err != nil {
			return 0, err
		}

		response, err := external.ReadMsgType(socket)
		if err != nil {
			return 0, err
		}
		if response != external.Ack {
			return 0, fmt.Errorf("While waiting for ack on reconnection msg")
		}

	case external.ReconnectionMsg:
		id, err = external.ReadReconnectionId(socket)
		err = external.WriteAck(socket)
		if err != nil {
			return 0, err
		}

	default:
		slog.Info("Read unexpected message type")
		return 0, fmt.Errorf("Read unexpected message type on client connection")
	}

	return id, nil
}

func (gateway *Gateway) getNewClientId() int64 {
	id := rand.Int64()

	// para asegurar que no se repitan los ids muy improbable pero bueno
	var clientExists bool
	gateway.registry.WithLock(func(clients map[int64]*clientregistry.ClientState) {
		if _, ok := clients[id]; ok {
			clientExists = ok
		}
	})
	if clientExists {
		return gateway.getNewClientId()
	}

	return id
}

// ------------------------  Handlers de msgs recibidos ---------------------------------------------------------------

func (gateway *Gateway) handleTransactionBatchMessage(client *clientregistry.ClientState) error {
	transactions, secNum, err := external.ReadTransactionBatch(client.Conn)
	if err != nil {
		slog.Error("While reading transaction batch", "err", err)
		return err
	}

	if !gateway.mustProcess(client, secNum) {
		return nil
	}

	message, err := client.Handler.SerializeDataMessage(*transactions)
	if err != nil {
		slog.Error("While serializing data message", "err", err)
		return err
	}
	if err := gateway.outputExchange.Send(*message); err != nil {
		slog.Error("While sending data message", "err", err)
		return err
	}
	gateway.registry.IncrementSequenceNumber(client.Handler.UserId)

	return nil
}

func (gateway *Gateway) handleEndOfRecordsMessage(client *clientregistry.ClientState) error {
	slog.Info("Received END_OF_RECORDS message")

	if gateway.registry.UserSentEOF(client.Handler.UserId) {
		slog.Info("No es necesario enviar el eor, ya fue enviado")
		return nil
	}

	message, err := client.Handler.SerializeEORMessage()
	if err != nil {
		slog.Error("While serializing END_OF_RECORDS message", "err", err)
		return err
	}
	if err := gateway.outputExchange.Send(*message); err != nil {
		slog.Error("While sending eof message", "err", err)
		return err
	}

	return nil
}

func (gateway *Gateway) sendResponse(socket net.Conn, data []byte) error {
	if err := safeio.WriteAll(socket, data); err != nil {
		slog.Error("While writing queries result message", "err", err)
		return fmt.Errorf("While writing queries result message: %w", err)
	}
	return nil
}

func (gateway *Gateway) mustProcess(client *clientregistry.ClientState, secNum int64) bool {
	secuenceNumberToReceive := gateway.registry.GetSecuenceNumber(client.Handler.UserId)
	if secNum != secuenceNumberToReceive {
		slog.Error("Un mensaje recibido se ha salteado uno o mas numeros de secuencia", "Recibido", secNum, "Esperado", secuenceNumberToReceive)
	}
	return secNum == secuenceNumberToReceive
}
