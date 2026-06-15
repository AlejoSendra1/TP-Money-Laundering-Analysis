package gateway

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"tp_distribuidos/common/transaction"

	"tp_distribuidos/clientregistry"
	"tp_distribuidos/common/messageprotocol/external"
	"tp_distribuidos/common/messageprotocol/external/safeio"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/messageprotocol/serializer"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/messagehandler"
)

type GatewayConfig struct {
	InputQueueName      string
	OutputExchangeName  string
	OutputTopic         string
	ServerHost          string
	ServerPort          string
	MomHost             string
	MomPort             int
	EofExpectedByQuery1 int
	EofExpectedByQuery2 int
	EofExpectedByQuery3 int
	EofExpectedByQuery4 int
	EofExpectedByQuery5 int
}

type Gateway struct {
	batchCounter   int64 // para Info porposes
	registry       clientregistry.ClientRegistry
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware
	listener       net.Listener
	running        atomic.Bool
	config         GatewayConfig
}

func NewGateway(config GatewayConfig) (*Gateway, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.OutputExchangeName, []string{config.OutputTopic}, connSettings, "")
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

	gateway := &Gateway{batchCounter: 0, outputExchange: outputExchange, inputQueue: inputQueue, listener: listener, config: config}
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
	eorMap := map[transaction.QueryID]int{
		transaction.Query1: gateway.config.EofExpectedByQuery1,
		transaction.Query2: gateway.config.EofExpectedByQuery2,
		transaction.Query3: gateway.config.EofExpectedByQuery3,
		transaction.Query4: gateway.config.EofExpectedByQuery4,
		transaction.Query5: gateway.config.EofExpectedByQuery5,
	}
	for {
		conn, err := gateway.listener.Accept()
		if err != nil {
			if !gateway.running.Load() {
				break
			}
			return err
		}

		slog.Info("Client connected...")

		handler := messagehandler.NewMessageHandler(eorMap)
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
	var targetClient clientregistry.ClientState
	var clientIndex int = -1
	found := false

	// Deserializamos el mensaje de la cola antes de bloquear nada
	msg, err := inner.DeserializeMessage(&middlewareMsg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}

	// Lock corto: Solo buscamos el cliente idóneo en el registro
	gateway.registry.WithLock(func(clients []clientregistry.ClientState) {
		for i, client := range clients {
			if client.Handler.UserId == msg.ClientID {
				targetClient = client
				clientIndex = i
				found = true
				break
			}
		}
	})

	if !found {
		slog.Warn("No client handler could process this message", "clientID", msg.ClientID)
		ack() // Si el cliente se desconectó, descartamos/confirmamos para no trabar la cola
		return
	}

	// Procesamos y enviamos AFUERA del lock del registro
	switch msg.MsgType {
	case inner.Query1Response, inner.Query2Response, inner.Query3Response, inner.Query4Response, inner.Query5Response:

		serialization, err := serializer.SerializeQueryResponse(uint32(msg.MsgType), *msg)
		if err != nil {
			slog.Error("While serializing response", "err", err)
			nack()
			return
		}

		if err := gateway.sendResponse(targetClient.Conn, serialization); err != nil {
			slog.Error("While sending results to client", "err", err)
			nack()
			return
		}

	case inner.EndOfRecords:
		slog.Info("Response received from MOM", "query", msg.MsgType, "clientID", msg.ClientID)
		clientEnded, err := targetClient.Handler.HandleQueryEOR(msg)
		if err != nil {
			slog.Error("While handling query EOR", "err", err)
			nack()
			return
		}

		if clientEnded {
			response := serializer.SerializeEOR()
			_ = gateway.sendResponse(targetClient.Conn, response)

			slog.Info("Client received all result queries, removing from registry", "clientID", msg.ClientID)
			// Removemos usando el índice detectado previamente
			gateway.registry.Remove(clientIndex)
		}

	default:
		slog.Error("Unexpected msg type received", "msgType", msg.MsgType)
		nack()
		return
	}

	ack()
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

	message, err := client.Handler.SerializeEORMessage()
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
	return nil
}
