package client

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"tp_distribuidos/common/datasaver"
	"tp_distribuidos/common/messageprotocol/external"
	"tp_distribuidos/common/messageprotocol/external/safeio"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/messageprotocol/serializer"
	"tp_distribuidos/common/transaction"
	"tp_distribuidos/csvwriter"
	"tp_distribuidos/transactionsfilereader"
)

const connectionAttempts = 3
const connectionAttemptsDelayMs = 300

type ClientConfig struct {
	ServerHost string
	ServerPort string
	InputFile  string
	OutputFile string
	Restorate  bool
	ID         int
}

type Client struct {
	mutex   sync.Mutex
	conn    net.Conn
	running atomic.Bool
	config  ClientConfig
	writer  csvwriter.CSVWriter
	ackChan chan struct{} // para asegurar la llegada de los batches
	// tolerance resistence data
	// sending phase
	dataSaver     *datasaver.DataSaver
	assignedID    int64
	BatchesSecNum int64

	// Tracks valid state for results
	resultsLogsSaver *datasaver.DataSaver
	processedQueries map[uint32]QueryState
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		return nil, err
	}

	writer, err := csvwriter.NewCSVWriter(config.OutputFile)
	if err != nil {
		return nil, err
	}

	dataSaverVar, err := datasaver.NewDataSaver(fmt.Sprintf("/persistence/client_%d", config.ID), LOGS_UNTIL_CHECKPOINT)
	if err != nil {
		return nil, err
	}
	resultsLogsSaver, err := datasaver.NewDataSaver(fmt.Sprintf("/persistence/results_client_%d", config.ID), LOGS_UNTIL_CHECKPOINT)
	if err != nil {
		return nil, err
	}

	client := &Client{
		conn:             conn,
		config:           config,
		writer:           *writer,
		dataSaver:        dataSaverVar,
		ackChan:          make(chan struct{}, 1),
		BatchesSecNum:    0,
		processedQueries: make(map[uint32]QueryState),
		resultsLogsSaver: resultsLogsSaver,
	}

	if config.Restorate {
		if err := client.restaurateState(); err != nil { // restauramos la fase de envio y el id
			return nil, err
		}

		if err := client.restaurateResultsState(); err != nil { // restauramos la fase de envio
			return nil, err
		}

		slog.Info("cargando en base a checkpoint")
		err = sendReconnectMsg(conn, client.assignedID)
		if err != nil {
			return nil, err
		}
		slog.Info("Connection was succefull", "Id recuperated", client.assignedID)
	} else {
		id, err := sendConnectMsg(conn) // para obtener el id en caso de requerir reconexion
		if err != nil {
			return nil, err
		}
		client.assignedID = id
		slog.Info("Connection was succefull", "Id assigned", client.assignedID)
	}

	client.dataSaver.SaveCheckpoint(client.GetCheckpointData()) // guardamos checkpoint de una para persistir el id
	client.running.Store(true)
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	var err error
	var conn net.Conn

	for range connectionAttempts {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			slog.Warn("Retrying connection...")
			time.Sleep(connectionAttemptsDelayMs * time.Millisecond)
			continue
		}
		break
	}

	return conn, err
}

func (client *Client) Run() error {
	defer client.conn.Close()
	defer client.writer.Close()
	defer client.dataSaver.Close()
	go client.handleSignals()

	// Canal para avisar a la goroutine principal que la lectura terminó (con o sin error)
	readDone := make(chan error, 1)

	// Lanzamos la goroutine encargada de LEER todo lo que venga del Gateway
	go func() {
		readDone <- client.recvManager()
	}()

	// La goroutine principal se encarga de ENVIAR las transacciones
	if err := client.sendTransactionRecords(); err != nil {
		if client.running.Load() {
			return fmt.Errorf("error enviando registros: %w", err)
		}
		return nil
	}

	// Esperamos a que la goroutine de lectura finalice por completo
	slog.Info("Solo queda esperar resultados...")
	if err := <-readDone; err != nil {
		if client.running.Load() {
			return fmt.Errorf("error en la rutina de lectura: %w", err)
		}
	}

	return nil
}

func (client *Client) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	client.running.Store(false)
	client.conn.Close()
}

func (client *Client) sendBatch(batch []transaction.Transaction) error {
	if len(batch) == 0 {
		return nil
	}

	if err := external.WriteTransactionBatch(client.conn, client.BatchesSecNum, batch); err != nil { // implementar esta func
		return err
	}

	return nil
}

func (client *Client) sendTransactionRecords() error {
	batchSizeAsString := os.Getenv("BATCH_SIZE")
	batchSize, err := strconv.Atoi(batchSizeAsString)
	if err != nil {
		slog.Info("Error reading batchSize from environment", "err", err)
		return err
	}

	transactionsReader, err := transactionsfilereader.NewTransactionsFileReader(client.config.InputFile, batchSize, client.BatchesSecNum)
	defer transactionsReader.Close()
	if err != nil {
		slog.Info("Error opening transactions file for reading", "err", err)
		return err
	}

	slog.Info("Comenzando el envio de transacciones...")
	for {
		records, err := transactionsReader.GetTransactionRecords()
		if err != nil {
			return err
		}

		if len(records) == 0 {
			break
		}
		if err := client.sendBatch(records); err != nil {
			slog.Info("Error while sending transaction batch", "err", err)
			return err
		}

		// wait for recvManager to signal the ACK arrived
		slog.Info("Esperando ack...")
		if _, ok := <-client.ackChan; !ok {
			return fmt.Errorf("ack channel closed unexpectedly")
		}
		slog.Info("Ack recibido")

		// por cada linea se va a representar un batch enviado,
		//  osea, batch_size transacciones enviadas de arriba para para abajo
		var b byte
		b = 1
		client.dataSaver.Save(b, client)
		client.BatchesSecNum++
	}

	if err := external.WriteEndOfRecords(client.conn); err != nil {
		return err
	}

	if _, ok := <-client.ackChan; !ok {
		return fmt.Errorf("ack channel closed unexpectedly")
	}

	slog.Info("Batches enviados", "val", client.BatchesSecNum)

	return nil
}

/*
	func (client *Client) handleQueryResponse(queryCode uint32) error {
		toRead, err := client.readUint32()
		if err != nil {
			return err
		}

		read, err := safeio.ReadAll(client.conn, toRead)
		if err != nil {
			return err
		}

		receivedResultSecuenceNumber, toWrite, err := serializer.DeserializeQueryResponse(read)
		if err != nil {
			return err
		}
		if receivedResultSecuenceNumber != client.ReceivingSecNum {
			slog.Warn("Se recibio un numero de secuencia diferente al esperado", "esperado", client.ReceivingSecNum, "recibido", receivedResultSecuenceNumber)
			return nil
		}

		return client.writer.WriteResult(queryCode, toWrite)
	}
*/
func (client *Client) handleQueryResponse(queryCode uint32) error {
	secNumBytes, err := safeio.ReadAll(client.conn, serializer.UINT64_SIZE) // numero de secuencia del msg
	if err != nil {
		return err
	}
	newSecNum := int64(serializer.DeserializeUint64(secNumBytes))

	toRead, err := client.readUint32()
	if err != nil {
		return err
	}

	read, err := safeio.ReadAll(client.conn, toRead)
	if err != nil {
		return err
	}

	toWrite, err := serializer.DeserializeQueryResponse(read)
	if err != nil {
		return err
	}

	// CHECKEO Q NO LO HAYA ESCRITO
	if state, exists := client.processedQueries[queryCode]; exists && newSecNum <= state.LastSecNum {
		slog.Warn("Ignoring duplicate message from gateway redelivery", "query", queryCode, "secNum", newSecNum)
		return nil
	}

	// ESCRIBO EN EL ARCHIVO Q CORRESPONDA
	if err := client.writer.WriteResult(queryCode, toWrite); err != nil {
		return err
	}

	qName := csvwriter.QueryCodeToName(queryCode)
	newOffset, err := client.writer.GetCurrentOffset(qName)
	if err != nil {
		return fmt.Errorf("failed to fetch current file size: %w", err)
	}

	// efectivisamos el cambio
	client.processedQueries[queryCode] = QueryState{
		LastSecNum: newSecNum,
	}

	client.resultsLogsSaver.Save(
		LogData{
			QueryCode:  queryCode,
			QueryState: QueryState{LastSecNum: newSecNum, LastWriteOffset: newOffset},
		},
		client,
	)

	return nil
}

func (client *Client) recvManager() error {
	slog.Info("Manager de lectura iniciado...")
	defer close(client.ackChan)

	for {
		msgType, err := external.ReadMsgType(client.conn)
		if err != nil {
			// Si cerramos la conexión por diseño, salimos limpio
			if !client.running.Load() {
				return nil
			}
			return fmt.Errorf("leyendo tipo de mensaje: %w", err)
		}

		switch inner.MsgType(msgType) {

		case inner.MsgType(external.Ack):
			slog.Info("Notificando a la otra go rutine de la llegada del ack...")
			client.ackChan <- struct{}{}
			// Avisamos al otro hilo que puede considerar el msg como recibido.
			slog.Info("Go rutine de envio notificada")
			continue

		case inner.Query1Response, inner.Query2Response, inner.Query3Response, inner.Query4Response, inner.Query5Response:

			if err := client.handleQueryResponse(uint32(msgType)); err != nil {
				return err
			}
			if err := client.sendResponseAck(); err != nil {
				return err
			}

		// 4. Fin de todo el procesamiento (El Gateway nos avisa que no hay más respuestas)
		case inner.EndOfRecords:
			slog.Info("End of records total recibido del Gateway. Finalizando cliente.")
			if err := client.sendResponseAck(); err != nil {
				return err
			}
			return nil

		default:
			return fmt.Errorf("tipo de mensaje inesperado recibido en el cliente: %d", msgType)
		}

	}
}

// ---- HELPERS ----

func (client *Client) readUint32() (uint32, error) {
	bytes, err := safeio.ReadAll(client.conn, serializer.UINT32_SIZE)
	if err != nil {
		return 0, err
	}
	return serializer.DeserializeUint32(bytes), nil
}
