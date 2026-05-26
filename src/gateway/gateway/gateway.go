package gateway

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"tp_distribuidos/clientregistry"
	"tp_distribuidos/common/messageprotocol/external"
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
	EOFExpectedByQuery map[int]int
}

type Gateway struct {
	batchCounter   int64 // para debug porposes
	registry       clientregistry.ClientRegistry
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware
	listener       net.Listener
	running        atomic.Bool
	config         GatewayConfig
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

	for {
		conn, err := gateway.listener.Accept()
		if err != nil {
			if !gateway.running.Load() {
				break
			}
			return err
		}

		slog.Info("Client connected...")

		handler := messagehandler.NewMessageHandler(gateway.config.EOFExpectedByQuery)
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

/*
func (gateway *Gateway) handleClientResponse(msg middleware.Message, ack func(), nack func()) {
	clientIndex := -1

	gateway.registry.WithLock(func(clients []clientregistry.ClientState) {
		for i, client := range clients {
			QueriesResult, wasProcessed, err := client.Handler.DeserializeResultMessage(&msg)
			if err != nil {
				slog.Debug("While reading from output queue", "err", err)
				nack()
				gateway.outputExchange.StopConsuming()
				return
			}
			// Si el mensaje no pertenece al dueño, se skipea
			if QueriesResult == nil && !wasProcessed {
				continue
			}

			// Si devolvió datos pero no es isAllDone, significa que el handler ya guardó el lote
			// internamente en su mapa, pero todavía faltan más resultados de queries.
			if wasProcessed && QueriesResult == nil {
				clientIndex = -2 // Flag interno para saber que fue procesado pero no terminó
				return
			}

			// Si llego aca, significa es el cliente dueño del mensaje, y que ya tiene todas las queries resueltas
			if err := external.WriteQueriesResult(client.Conn, QueriesResult); err != nil {
				slog.Debug("While writing queries result message", "err", err)
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
		}
		slog.Warn("No client handler could process this message")
		nack()
	})

	// El mensaje fue procesado, pero no fue enviado al cliente
	if clientIndex == -2 {
		ack() // El dato ya fue procesado por el gateway
		return
	}

	if clientIndex >= 0 {
		slog.Info("Client received all result queries, removing from registry", "clientIndex", clientIndex)
		gateway.registry.Remove(clientIndex)
		ack()
		return
	}
}
*/

func (gateway *Gateway) handleClientResponse(msg middleware.Message, ack func(), nack func()) {
	gateway.registry.WithLock(func(clients []clientregistry.ClientState) {
		for _, client := range clients {
			queriesResult, wasProcessed, err := client.Handler.DeserializeResultMessage_2(&msg)
			if err != nil {
				slog.Error("While deserializing result message", "err", err)
				nack()
				return
			}
			if !wasProcessed {
				continue
			}
			if queriesResult != nil {
				if err := external.WriteQueriesResult(client.Conn, queriesResult); err != nil {
					slog.Error("While writing queries result to client", "err", err)
					nack()
					return
				}
			}
			ack()
			return
		}
		nack()
	})
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
	if err := gateway.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending data message", "err", err)
		return err
	}
	if err := external.WriteAck(client.Conn); err != nil {
		slog.Debug("While writing ACK message", "err", err)
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
		slog.Debug("While serializing END_OF_RECORDS message", "err", err)
		return err
	}
	if err := gateway.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending eof message", "err", err)
		return err
	}
	if err := external.WriteAck(client.Conn); err != nil {
		slog.Debug("While writing ACK message", "err", err)
		return err
	}
	return nil
}
