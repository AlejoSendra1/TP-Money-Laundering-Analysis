package client

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
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

// agregar a vars de entorno
const LOGS_UNTIL_CHECKPOINT = 200

type ClientConfig struct {
	ServerHost string
	ServerPort string
	InputFile  string
	OutputFile string
	Restorate  bool
	ID         int
}

type Client struct {
	conn      net.Conn
	running   atomic.Bool
	config    ClientConfig
	writer    csvwriter.CSVWriter
	dataSaver *datasaver.DataSaver
	// tolerance resistence data
	assignedID    int64
	BatchesSecNum int64
	ackChan       chan struct{}
}

type CheckpointData struct {
	AssignedID    int64 `json:"assigned_id"`
	BatchesSecNum int64 `json:"batches_sent_amount"`
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

	dataSaver, err := datasaver.NewDataSaver(fmt.Sprintf("/persistence/client_%d", config.ID), LOGS_UNTIL_CHECKPOINT)
	if err != nil {
		return nil, err
	}

	client := &Client{
		conn:      conn,
		config:    config,
		writer:    *writer,
		dataSaver: dataSaver,
		ackChan:   make(chan struct{}, 1),
	}

	if config.Restorate {
		client.restaurateState()

		slog.Info("cargando en base a checkpoint")

		err = sendReconnectMsg(conn, client.assignedID) // aviso al server la reconexion

		if err != nil {
			return nil, err
		}
		slog.Info("Connection was succefull", "Id recuperated", client.assignedID)

	} else {
		client.BatchesSecNum = 0
		id, err := sendConnectMsg(conn) // para obtener un id en caso de desconexion
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

// ---- HELPERS ----

func (client *Client) readUint32() (uint32, error) {
	bytes, err := safeio.ReadAll(client.conn, serializer.UINT32_SIZE)
	if err != nil {
		return 0, err
	}
	return serializer.DeserializeUint32(bytes), nil
}

func (Client *Client) handleQueryResponse(queryCode uint32) error {
	toRead, err := Client.readUint32()
	if err != nil {
		return err
	}

	read, err := safeio.ReadAll(Client.conn, toRead)
	if err != nil {
		return err
	}

	toWrite, err := serializer.DeserializeQueryResponse(read)
	if err != nil {
		return err
	}

	Client.writer.WriteResult(queryCode, toWrite)
	return nil
}

// igual q arriba
func (client *Client) handleQueryResponseWithQueryResult(queryCode uint32) error {
	//slog.Info("Leyendo query resultado", "queryCode", queryCode)

	// leer length prefix
	toRead, err := client.readUint32()
	if err != nil {
		return fmt.Errorf("reading length for query %d: %w", queryCode, err)
	}

	raw, err := safeio.ReadAll(client.conn, toRead)
	if err != nil {
		return fmt.Errorf("reading body for query %d: %w", queryCode, err)
	}

	// deserializar el []interface{} de records
	var records []interface{}
	if err := json.Unmarshal(raw, &records); err != nil {
		return fmt.Errorf("unmarshaling query %d result: %w", queryCode, err)
	}

	return client.writer.WriteResult(queryCode, records)
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

		// 2. Respuestas de Queries con JSON estructurado
		case inner.Query2Response, inner.Query3Response, inner.Query5Response:
			if err := client.handleQueryResponseWithQueryResult(uint32(msgType)); err != nil {
				return err
			}

		// 3. Respuestas de Queries serializadas nativas
		case inner.Query1Response, inner.Query4Response:
			if err := client.handleQueryResponse(uint32(msgType)); err != nil {
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

		if err := client.sendResponseAck(); err != nil {
			return err
		}
	}
}

// --------------- para la restauracion del cliente ---------------

func (client *Client) GetCheckpointData() any {
	return CheckpointData{
		AssignedID:    client.assignedID,
		BatchesSecNum: client.BatchesSecNum,
	}
}

// Asigna al cliente los valores guardados en el checkpoint antes de la caida
func (client *Client) restaurateState() error {
	// restauracion checkpoint
	var checkpoint CheckpointData
	thereIsCheckpoint, err := client.dataSaver.GetRestaurationCheckpoint(&checkpoint)
	if err != nil || !thereIsCheckpoint { // habria q agregar retrys?
		return err
	}

	slog.Info("Checkpoint levantado", "values", checkpoint)
	client.assignedID = checkpoint.AssignedID
	client.BatchesSecNum = checkpoint.BatchesSecNum

	// restauracion logs
	// lo unico importante es que cada log representa un batch enviado, no se necesita mas info(no se haria nada con eso)
	var b byte
	for {
		thereIsLogs, err := client.dataSaver.GetDataFromLogs(&b)
		if err != nil { // habria q modificar para retrys
			return err
		}
		if !thereIsLogs {
			break
		}

		client.BatchesSecNum++
	}

	return nil
}
