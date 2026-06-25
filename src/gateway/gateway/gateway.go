package gateway

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
	"tp_distribuidos/common/batch_utils"
	"tp_distribuidos/common/transaction"

	"tp_distribuidos/clientregistry"
	"tp_distribuidos/common/messageprotocol/external"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/messageprotocol/serializer"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/messagehandler"
)

// Tiempo máximo que el gateway espera a que el cliente confirme
// la recepción de una respuesta antes de nackear el mensaje del MOM.
const responseAckTimeout = 20 * time.Second

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

const MaxBatchSize = 200000

type Gateway struct {
	registry       clientregistry.ClientRegistry
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware

	listener     net.Listener
	running      atomic.Bool
	config       GatewayConfig
	deduplicator *batch_utils.MultiClientDeduplicator
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

	gateway := &Gateway{
		registry:       clientregistry.NewClientRegistry(),
		inputQueue:     inputQueue,
		outputExchange: outputExchange,
		listener:       listener,
		config:         config,
		deduplicator:   batch_utils.NewMultiClientDeduplicator(MaxBatchSize),
	}
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

		clientId, err := gateway.handleClientConnection(conn)
		slog.Info("UserId assigned", "value", clientId)
		if err != nil {
			slog.Error("While client connects")
			continue
		}
		// si el cliente se reconecto solo cambiamos el socket del registro para enviarle las respuestas
		isAnOldClient := false
		var client clientregistry.ClientState
		gateway.registry.WithLock(func(clients map[int64]*clientregistry.ClientState) {
			if c, ok := clients[clientId]; ok {
				c.Conn.Close()
				c.Conn = conn
				isAnOldClient = true
				client = *c
			}
		})

		if !isAnOldClient {
			handler := messagehandler.NewMessageHandler(clientId, eorMap)
			NewClient := clientregistry.ClientState{Conn: conn, Handler: &handler, AckCh: make(chan struct{}, 1)}
			gateway.registry.Add(clientId, NewClient)
			go gateway.handleClientRequest(NewClient)
		} else {
			go gateway.handleClientRequest(client)
		}

	}

	gateway.outputExchange.StopConsuming()
	gateway.registry.WithLock(func(clients map[int64]*clientregistry.ClientState) {
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
	for {
		msgType, err := external.ReadMsgType(client.Conn)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				slog.Info("Client disconnected gracefully", "client", client)
				return
			}
			slog.Error("While reading message type handling client request", "err", err)
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

		case external.ResponseAck:
			// El cliente está confirmando la recepción de una respuesta que le
			// mandamos por handleClientResponse. No es un mensaje "nuevo" que
			// nosotros debamos volver a ackear: solo despertamos a quien esté
			// esperando esta confirmación y seguimos leyendo el socket.
			select {
			case client.AckCh <- struct{}{}:
			default:
				// Nadie estaba esperando (timeout ya venció, o ack duplicado):
				// lo descartamos para no bloquear el loop de lectura.
			}
			continue

		default:
			slog.Info("Read unexpected message type")
			return
		}

		if err := external.WriteAck(client.Conn); err != nil {
			slog.Info("While writing ACK message", "err", err)
			return
		}

	}
}

// waitForClientAck bloquea hasta que el cliente confirme (via external.ResponseAck)
// que recibió la última respuesta que le mandamos, o hasta que se cumpla el timeout.
func (gateway *Gateway) waitForClientAck(client clientregistry.ClientState) error {
	select {
	case <-client.AckCh:
		return nil
	case <-time.After(responseAckTimeout):
		return os.ErrDeadlineExceeded
	}
}

func (gateway *Gateway) handleClientResponse(middlewareMsg middleware.Message, ack func(), nack func()) {
	var targetClient clientregistry.ClientState
	found := false

	// Deserializamos el mensaje de la cola antes de bloquear nada
	msg, err := inner.DeserializeMessage(&middlewareMsg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}

	// Lock corto: Solo buscamos el cliente idóneo en el registro
	gateway.registry.WithLock(func(clients map[int64]*clientregistry.ClientState) {
		if c, ok := clients[msg.ClientID]; ok {
			targetClient = *c
			found = true
		}
	})

	if !found {
		slog.Warn("No client handler could process this message", "clientID", msg.ClientID)
		ack()
		return
	}

	batchID := batch_utils.GenerateBatchID([]byte(middlewareMsg.Body))
	if gateway.deduplicator.IsDuplicateNoUpdate(msg.ClientID, batchID) {
		slog.Warn("Duplicate message detected", "clientID", msg.ClientID, "batchID", batchID, "msg", msg)
		ack()
		return
	}

	// Procesamos y enviamos AFUERA del lock del registro
	switch msg.MsgType {
	case inner.Query1Response, inner.Query2Response, inner.Query3Response, inner.Query4Response, inner.Query5Response:

		actualSecuenceNumber := gateway.registry.GetSecuenceNumberToSent(msg.ClientID)
		serialization, err := serializer.SerializeQueryResponse(uint32(msg.MsgType), actualSecuenceNumber, *msg)
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

		if err := gateway.waitForClientAck(targetClient); err != nil {
			slog.Error("Client never acked the response, nacking", "clientID", msg.ClientID, "err", err)
			nack()
			return
		}

		gateway.deduplicator.Load(msg.ClientID, batchID)
		gateway.registry.IncrementSequenceNumberToSent(msg.ClientID)

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
			if err := gateway.sendResponse(targetClient.Conn, response); err != nil {
				slog.Error("While sending EOR to client", "err", err)
				nack()
				return
			}

			if err := gateway.waitForClientAck(targetClient); err != nil {
				slog.Warn("Client never acked the response, nacking", "clientID", msg.ClientID, "err", err)
				nack()
				return
			}

			slog.Info("Client received all result queries, removing from registry", "clientID", msg.ClientID)
			// Removemos usando el índice detectado previamente
			gateway.registry.Remove(msg.ClientID)
			gateway.deduplicator.RemoveClient(msg.ClientID)
		}

	default:
		slog.Error("Unexpected msg type received", "msgType", msg.MsgType)
		nack()
		return
	}

	ack()
}
