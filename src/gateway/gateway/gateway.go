package gateway

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"tp_distribuidos/clientregistry"
	"tp_distribuidos/common/messageprotocol/external"
	"tp_distribuidos/common/messageprotocol/external/safeio"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/messageprotocol/serializer"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/messagehandler"
)

type GatewayConfig struct {
	InputQueueName     string
	OutputExchangeName string
	OutputTopic        string
	ServerHost         string
	ServerPort         string
	MomHost            string
	MomPort            int
}

type Gateway struct {
	batchCounter   int64 // para Info porposes
	registry       clientregistry.ClientRegistry
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware
	listener       net.Listener
	running        atomic.Bool
}

func NewGateway(config GatewayConfig) (*Gateway, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.OutputExchangeName, []string{config.OutputTopic}, connSettings)
	if err != nil {
		return nil, err
	}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueueName, connSettings)
	if err != nil {
		outputExchange.Close()
		return nil, err
	}

	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}

	gateway := &Gateway{batchCounter: 0, outputExchange: outputExchange, inputQueue: inputQueue, listener: listener}
	gateway.running.Store(true)
	return gateway, nil
}

func (gateway *Gateway) Run() error {
	defer gateway.listener.Close()

	go gateway.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		gateway.handleClientResponse(msg, ack, nack)
	})
	go gateway.handleSignals()

	slog.Info("Accepting connections...")

	for {
		conn, err := gateway.listener.Accept()
		if err != nil {
			if !gateway.running.Load() {
				break
			}
			return err
		}

		slog.Info("Client connected...")

		handler := messagehandler.NewMessageHandler()
		client := clientregistry.ClientState{Conn: conn, Handler: &handler}
		gateway.registry.Add(client)

		go gateway.handleClientRequest(client)
	}

	gateway.outputExchange.StopConsuming()
	gateway.registry.WithLock(func(clients []clientregistry.ClientState) {
		for _, client := range clients {
			client.Conn.Close()
		}
	})
	return nil
}

func (gateway *Gateway) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	gateway.running.Store(false)
	gateway.listener.Close()
}

func (gateway *Gateway) handleClientRequest(client clientregistry.ClientState) {
loop:
	for {
		msgType, err := external.ReadMsgType(client.Conn)
		if err != nil {
			slog.Error("While reading message type", "err", err)
			return
		}

		switch msgType {
		case external.TransactionBatch:
			if err := gateway.handleTransactionBatchMessage(client); err != nil {
				slog.Info("While handling record message", "err", err)
				return
			}

		case external.EndOfRecords:
			slog.Info("Received EOF msg", "client", client)
			if err := gateway.handleEndOfRecordsMessage(client); err != nil {
				slog.Info("While handling end of records message", "err", err)
				return
			}
			break loop

		default:
			slog.Info("Read unexpected message type")
			return
		}
	}
}

func (gateway *Gateway) handleClientResponse(middlewareMsg middleware.Message, ack func(), nack func()) {
	clientIndex := -1
	wasProcessed := false
	slog.Info("Received response msg", "body", middlewareMsg.Body)

	gateway.registry.WithLock(func(clients []clientregistry.ClientState) {
		for i, client := range clients {
			msg, err := inner.DeserializeMessage(&middlewareMsg)
			if err != nil {
				slog.Error("While deserializing message", "err", err, "clientID", msg.ClientID)
				nack()
				return
			}

			if client.Handler.UserId != msg.ClientID {
				slog.Info("Client can't process this message", "Current client", client.Handler.UserId, "Received client", msg.ClientID)
				continue
			}

			switch msg.MsgType {
			case inner.Query1Response, inner.Query2Response, inner.Query3Response, inner.Query4Response, inner.Query5Response:
				slog.Info("response received", "query", msg.MsgType, "Content", msg.Data)

				serialization, err := serializer.SerializeQueryResponse(uint32(msg.MsgType), *msg) // ojo aprovechamos el hecho de que ambos protocolos tienen mismo queryreslts
				if err != nil {
					slog.Error("While serealizing response", "err", err, "clientID", msg.ClientID, "content", msg.Data)
					nack()
					return
				}
				gateway.sendResponse(client.Conn, serialization)
				slog.Info("Enviando resultados", "query", msg.MsgType, "Content", msg.Data)
				if err != nil {
					slog.Error("While sending results to client", "err", err, "clientID", msg.ClientID, "content", msg.Data)
					nack()
					return
				}
			case inner.EndOfRecords:
				// una vez que se reciben todos los results se notifica al cliente y se borra
				clientEnded, err := client.Handler.HandleQueryEOR(msg)
				if err != nil {
					slog.Error("While handling query response", "err", err, "clientID", msg.ClientID, "content", msg.Data)
					nack()
					return
				}

				if clientEnded {
					response := serializer.SerializeEOR()
					gateway.sendResponse(client.Conn, response)
					clientIndex = i // seteamos el valor para eliminar al cliente
				}

			default:
				slog.Error("Unexpected msg type received", "err", err, "clientID", msg.ClientID, "content", msg.Data)
				return
			}
			wasProcessed = true
			break
		}
		if !wasProcessed {
			slog.Warn("No client handler could process this message", "message", middlewareMsg.Body)
		}

		ack()
	})

	if clientIndex >= 0 {
		slog.Info("Client received all result queries, removing from registry", "clientIndex", clientIndex)
		gateway.registry.Remove(clientIndex)
		return
	}
}

func (gateway *Gateway) handleTransactionBatchMessage(client clientregistry.ClientState) error {
	transactions, err := external.ReadTransactionBatch(client.Conn)
	if err != nil {
		slog.Info("While reading transaction batch", "err", err)
		return err
	}
	message, err := client.Handler.SerializeDataMessage(*transactions)
	if err != nil {
		slog.Info("While serializing data message", "err", err)
		return err
	}
	if err := gateway.outputExchange.Send(*message); err != nil {
		slog.Info("While sending data message", "err", err)
		return err
	}
	if err := external.WriteAck(client.Conn); err != nil {
		slog.Info("While writing ACK message", "err", err)
		return err
	}
	gateway.batchCounter += 1
	return nil
}

func (gateway *Gateway) handleEndOfRecordsMessage(client clientregistry.ClientState) error {
	slog.Info("Received END_OF_RECORDS message")
	str := fmt.Sprint("batches enviados: ", gateway.batchCounter)
	slog.Info(str)

	message, err := client.Handler.SerializeEOFMessage()
	if err != nil {
		slog.Info("While serializing END_OF_RECORDS message", "err", err)
		return err
	}
	if err := gateway.outputExchange.Send(*message); err != nil {
		slog.Info("While sending eof message", "err", err)
		return err
	}
	if err := external.WriteAck(client.Conn); err != nil {
		slog.Info("While writing ACK message", "err", err)
		return err
	}
	return nil
}

func (gateway *Gateway) sendResponse(socket net.Conn, data []byte) error {

	if err := safeio.WriteAll(socket, data); err != nil {
		slog.Error("While writing queries result message", "err", err)
		return fmt.Errorf("While writing queries result message: %w", err)
	}
	msgType, err := external.ReadMsgType(socket)
	if err != nil {
		slog.Error("While reading message type", "err", err)
		return fmt.Errorf("While reading queries result message: %w", err)
	}
	if msgType != external.Ack {
		slog.Info("Expected ACK message")
		return fmt.Errorf("Waiting for ack msg: %w", err)
	}
	return nil
}
