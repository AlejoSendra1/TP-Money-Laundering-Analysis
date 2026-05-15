package gateway

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"tp_distribuidos/src/common/messageprotocol/external"
	"tp_distribuidos/src/common/middleware"
	"tp_distribuidos/src/gateway/clientregistry"
	"tp_distribuidos/src/gateway/messagehandler"
)

type GatewayConfig struct {
	InputQueueName  string
	OutputQueueName string
	ServerHost      string
	ServerPort      string
	MomHost         string
	MomPort         int
}

type Gateway struct {
	registry    clientregistry.ClientRegistry
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware
	listener    net.Listener
	running     atomic.Bool
}

func NewGateway(config GatewayConfig) (*Gateway, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.InputQueueName, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		return nil, err
	}

	gateway := &Gateway{outputQueue: outputQueue, inputQueue: inputQueue, listener: listener}
	gateway.running.Store(true)
	return gateway, nil
}

func (gateway *Gateway) Run() error {
	defer gateway.listener.Close()

	go gateway.outputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
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

	gateway.outputQueue.StopConsuming()
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
			slog.Debug("While reading message type", "err", err)
			return
		}

		switch msgType {
		case external.TransactionBatch:
			if err := gateway.handleTransactionBatchMessage(client); err != nil {
				slog.Debug("While handling record message", "err", err)
				return
			}

		case external.EndOfRecords:
			if err := gateway.handleEndOfRecordsMessage(client); err != nil {
				slog.Debug("While handling end of records message", "err", err)
				return
			}
			break loop

		default:
			slog.Debug("Read unexpected message type")
			return
		}
	}
}

func (gateway *Gateway) handleClientResponse(msg middleware.Message, ack func(), nack func()) {
	clientIndex := -1

	gateway.registry.WithLock(func(clients []clientregistry.ClientState) {
		for i, client := range clients {
			slog.Debug("A implementar la lectura de las respuestas a enviar al client")
			slog.Debug("se removera al cliente en 20 s", i, client)
			time.Sleep(20 * time.Second)
			/*
				fruitTop, err := client.Handler.DeserializeResultMessage(&msg)
				if err != nil {
					slog.Debug("While reading from output queue", "err", err)
					nack()
					gateway.outputQueue.StopConsuming()
					return
				}

				// The message handler can't process this message
				if fruitTop == nil {
					continue
				}

				if err := external.WriteFruitTop(client.Conn, fruitTop); err != nil {
					slog.Debug("While writing FRUIT_TOP message", "err", err)
					return
				}
				msgType, err := external.ReadMsgType(client.Conn)
				if err != nil {
					slog.Debug("While reading message type", "err", err)
					return
				}
				if msgType != external.Ack {
					slog.Debug("Expected ACK message")
					return
				}
				clientIndex = i
				return
			*/
		}
		slog.Warn("No client handler could process this message")
		nack()
	})

	if clientIndex >= 0 {
		gateway.registry.Remove(clientIndex)
		ack()
		return
	}
}

func (gateway *Gateway) handleTransactionBatchMessage(client clientregistry.ClientState) error {
	transactions, err := external.ReadTransactionBatch(client.Conn)
	if err != nil {
		slog.Debug("While reading transaction batch", "err", err)
		return err
	}
	message, err := client.Handler.SerializeDataMessage(*transactions)
	if err != nil {
		slog.Debug("While serializing data message", "err", err)
		return err
	}
	if err := gateway.inputQueue.Send(*message); err != nil {
		slog.Debug("While sending data message", "err", err)
		return err
	}
	if err := external.WriteAck(client.Conn); err != nil {
		slog.Debug("While writing ACK message", "err", err)
		return err
	}
	return nil
}

func (gateway *Gateway) handleEndOfRecordsMessage(client clientregistry.ClientState) error {
	slog.Info("Received END_OF_RECORDS message")
	message, err := client.Handler.SerializeEOFMessage()
	if err != nil {
		slog.Debug("While serializing END_OF_RECORDS  message", "err", err)
		return err
	}
	if err := gateway.inputQueue.Send(*message); err != nil {
		slog.Debug("While sending eof message", "err", err)
		return err
	}
	if err := external.WriteAck(client.Conn); err != nil {
		slog.Debug("While writing ACK message", "err", err)
		return err
	}
	return nil
}
