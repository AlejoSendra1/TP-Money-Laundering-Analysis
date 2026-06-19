package client

import (
	"fmt"
	"net"
	"tp_distribuidos/common/messageprotocol/external"
)

func sendReconnectMsg(socket net.Conn, previousID int64) error {
	external.WriteReconnectionMsg(socket, previousID)

	response, err := external.ReadMsgType(socket)
	if err != nil {
		return err
	}
	if response != external.Ack {
		return fmt.Errorf("While waiting for ack on reconnection msg")
	}

	return nil
}

func sendConnectMsg(socket net.Conn) (int64, error) {
	if err := external.WriteConnectionMsg(socket); err != nil {
		return 0, err
	}

	response, err := external.ReadMsgType(socket)
	if err != nil {
		return 0, err
	}

	if response != external.ConnectionResponseMsg {
		return 0, fmt.Errorf("While waiting for Connection Response Msg")
	}

	id, err := external.ReadConnectionMsgResponse(socket)
	if err != nil {
		return 0, err
	}

	return id, external.WriteAck(socket)

}
