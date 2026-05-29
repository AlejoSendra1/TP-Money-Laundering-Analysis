package client

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"tp_distribuidos/common/csvwriter"
	"tp_distribuidos/common/messageprotocol/external"
	"tp_distribuidos/common/messageprotocol/external/safeio"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/messageprotocol/serializer"
	"tp_distribuidos/common/transaction"
)

const (
	TIMESTAMP_COLUMN      = 0
	FROM_BANK_COLUMN      = 1
	FROM_ACCOUNT_COLUMN   = 2
	TO_BANK_COLUMN        = 3
	TO_ACCOUNT_COLUMN     = 4
	AMOUNT_COLUMN         = 7
	CURRENCY_COLUMN       = 8
	PAYMENT_FORMAT_COLUMN = 9
)
const connectionAttempts = 3
const connectionAttemptsDelayMs = 300

type ClientConfig struct {
	ServerHost string
	ServerPort string
	InputFile  string
	OutputFile string
}

type Client struct {
	transactionsSentCounter int64 // for Info
	conn                    net.Conn
	running                 atomic.Bool
	config                  ClientConfig
	writer                  csvwriter.CSVWriter
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

	client := &Client{
		transactionsSentCounter: 0,
		conn:                    conn,
		config:                  config,
		writer:                  *writer,
	}
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

func (client *Client) expectMsgType(expectedMsgType external.MsgType) error {
	msgType, err := external.ReadMsgType(client.conn)
	if err != nil {
		slog.Error("Error while reading message type", "err", err)
		return err
	}
	if msgType != expectedMsgType {
		return errors.New("Unexpected message type")
	}
	return nil
}

func parseTransaction(columns []string) (*transaction.Transaction, error) {
	timestamp, err := time.Parse("2006/01/02 15:04", columns[TIMESTAMP_COLUMN])
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp %q: %w", columns[TIMESTAMP_COLUMN], err)
	}

	fromBank, err := strconv.Atoi(columns[FROM_BANK_COLUMN])
	if err != nil {
		return nil, fmt.Errorf("invalid from_bank %q: %w", columns[FROM_BANK_COLUMN], err)
	}

	toBank, err := strconv.Atoi(columns[TO_BANK_COLUMN])
	if err != nil {
		return nil, fmt.Errorf("invalid to_bank %q: %w", columns[TO_BANK_COLUMN], err)
	}

	amountReceived, err := strconv.ParseFloat(columns[AMOUNT_COLUMN], 64)
	if err != nil {
		return nil, fmt.Errorf("invalid amount_received %q: %w", columns[AMOUNT_COLUMN], err)
	}

	return &transaction.Transaction{
		Timestamp:     timestamp,
		FromBank:      fromBank,
		ToBank:        toBank,
		FromAccount:   columns[FROM_ACCOUNT_COLUMN],
		ToAccount:     columns[TO_ACCOUNT_COLUMN],
		Amount:        amountReceived,
		Currency:      columns[CURRENCY_COLUMN],
		PaymentFormat: columns[PAYMENT_FORMAT_COLUMN],
	}, nil
}

func (client *Client) sendBatch(batch *[]transaction.Transaction) error {
	if len(*batch) == 0 {
		return nil
	}

	if err := external.WriteTransactionBatch(client.conn, batch); err != nil { // implementar esta func
		return err
	}

	*batch = (*batch)[:0]
	return nil
}

func (client *Client) sendTransactionRecords() error {
	file, err := os.Open(client.config.InputFile)
	if err != nil {
		slog.Info("Error while runninging input file", "err", err)
		return err
	}
	defer file.Close()

	batchSizeAsString := os.Getenv("BATCH_SIZE")
	batchSize, err := strconv.Atoi(batchSizeAsString)
	if err != nil {
		slog.Info("Error reading batchSize from environment", "err", err)
		return err
	}

	scanner := bufio.NewScanner(file)
	if scanner.Scan() {

	}
	batch := []transaction.Transaction{}

	//slog.Info("procesando transacciones")
	for scanner.Scan() {
		columns := strings.Split(scanner.Text(), ",")

		tx, err := parseTransaction(columns)
		if err != nil {
			slog.Info("Error while parsing transaction record", "err", err)
			return err
		}

		batch = append(batch, *tx)
		if len(batch) == batchSize {
			client.transactionsSentCounter += int64(len(batch))
			if err := client.sendBatch(&batch); err != nil {
				slog.Info("Error while sending transaction batch", "err", err)
				return err
			}
		}
	}

	if err := client.sendBatch(&batch); err != nil {
		return err
	}
	client.transactionsSentCounter += int64(len(batch))
	str := fmt.Sprint("transacciones enviadas: ", client.transactionsSentCounter)
	slog.Info(str)

	if err := external.WriteEndOfRecords(client.conn); err != nil {
		return err
	}

	return nil
}

// recvQueriesResult lee la respuesta de las queries
// Por ahora soporta solo Query1
// Formato: [MsgType][numRecords][records...][EOF]
// Ejemplo:
// [Query1Response][numRecords][records...]
// [Query2Response][numRecords][records...]
// [Query3Response][numRecords][records...]
// [Query4Response][numRecords][records...]
// [Query5Response][numRecords][records...]
// [EOF]
func (client *Client) recvQueriesResult() error {
	// MsgType
	slog.Info("Waiting for answers")
	msgType, err := external.ReadMsgType(client.conn)
	if err != nil {
		slog.Error("While reading message type for queries result", "err", err)
		return err
	}
	slog.Info("Read message type for queries result", "msgType", msgType)
	for inner.MsgType(msgType) != inner.EndOfRecords { // En el futuro sera 5...
		switch inner.MsgType(msgType) {
		case inner.Query2Response, inner.Query3Response, inner.Query5Response:
			if err := client.handleQueryResponseWithQueryResult(uint32(msgType)); err != nil {
				return err
			}
		case inner.Query1Response, inner.Query4Response:
			if err := client.handleQueryResponse(uint32(msgType)); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected message type while receiving queries result: %d", msgType)
		}

		if err := external.WriteAck(client.conn); err != nil {
			slog.Info("While writing ACK after queries result", "err", err)
			return err
		}

		msgType, err = external.ReadMsgType(client.conn)
		if err != nil {
			return err
		}

	}

	slog.Info("End of records received, closing")
	if err := external.WriteAck(client.conn); err != nil {
		slog.Info("While writing ACK after queries result", "err", err)
		return err
	}

	return nil
}

func (client *Client) readQuery1Records() ([]transaction.LowAmountTransfer, error) {
	count, err := client.readUint32()
	if err != nil {
		slog.Info("While reading query1 records count", "err", err)
		return nil, err
	}
	slog.Info("Read query1 records count", "count", count)

	records := make([]transaction.LowAmountTransfer, 0, int(count))
	for i := 0; i < int(count); i++ {
		record, err := client.readLowAmountTransferRecord()
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func (client *Client) readLowAmountTransferRecord() (transaction.LowAmountTransfer, error) {
	fromBank, err := client.readUint64AsInt("FromBank")
	if err != nil {
		return transaction.LowAmountTransfer{}, err
	}
	fromAccount, err := client.readLengthPrefixedString("FromAccount")
	if err != nil {
		return transaction.LowAmountTransfer{}, err
	}
	toBank, err := client.readUint64AsInt("ToBank")
	if err != nil {
		return transaction.LowAmountTransfer{}, err
	}
	toAccount, err := client.readLengthPrefixedString("ToAccount")
	if err != nil {
		return transaction.LowAmountTransfer{}, err
	}
	amount, err := client.readFloat64("Amount")
	if err != nil {
		return transaction.LowAmountTransfer{}, err
	}
	return transaction.LowAmountTransfer{
		FromBank:    fromBank,
		FromAccount: fromAccount,
		ToBank:      toBank,
		ToAccount:   toAccount,
		Amount:      amount,
	}, nil
}

func (client *Client) expectEndOfRecords() error {
	msgType, err := external.ReadMsgType(client.conn)
	if err != nil {
		slog.Error("While reading message type for queries result EOF", "err", err)
		return err
	}
	if msgType != external.EndOfRecords {
		slog.Info("Expected EndOfRecords message type after reading queries result, got", "msgType", msgType)
		return fmt.Errorf("expected EndOfRecords message type after reading queries result, got %d", msgType)
	}
	return nil
}

func (client *Client) writeQuery1CSV(records []transaction.LowAmountTransfer) error {
	outputFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		slog.Info("Error while creating output file", "err", err)
		return err
	}
	defer outputFile.Close()

	writer := csv.NewWriter(outputFile)
	defer writer.Flush()

	for _, r := range records {
		line := []string{strconv.Itoa(r.FromBank), r.FromAccount, strconv.Itoa(r.ToBank), r.ToAccount, fmt.Sprintf("%.2f", r.Amount)}
		if err := writer.Write(line); err != nil {
			slog.Info("Error while writing CSV line", "err", err)
			return err
		}
	}

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

func (client *Client) readUint64AsInt(fieldName string) (int, error) {
	bytes, err := safeio.ReadAll(client.conn, serializer.UINT64_SIZE)
	if err != nil {
		slog.Info("While reading field", "field", fieldName, "err", err)
		return 0, err
	}
	return int(serializer.DeserializeUint64(bytes)), nil
}

func (client *Client) readLengthPrefixedString(fieldName string) (string, error) {
	length, err := client.readUint32()
	if err != nil {
		slog.Info("While reading string length", "field", fieldName, "err", err)
		return "", err
	}
	bytes, err := safeio.ReadAll(client.conn, length)
	if err != nil {
		slog.Info("While reading string bytes", "field", fieldName, "err", err)
		return "", err
	}
	return serializer.DeserializeString(bytes), nil
}

func (client *Client) readFloat64(fieldName string) (float64, error) {
	bytes, err := safeio.ReadAll(client.conn, serializer.UINT64_SIZE)
	if err != nil {
		slog.Info("While reading field", "field", fieldName, "err", err)
		return 0, err
	}
	return serializer.DeserializeFloat64(bytes), nil
}

func (Client *Client) handleQueryResponse(queryCode uint32) error {
	//slog.Info("Leyendo query")

	toRead, err := Client.readUint32()
	if err != nil {
		return err
	}
	//slog.Info("Bytes a leer", "value", toRead)

	read, err := safeio.ReadAll(Client.conn, toRead)
	if err != nil {
		return err
	}

	toWrite, err := serializer.DeserializeQueryResponse(read)
	//slog.Info("Data obtenida", "value", toWrite)
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

		// 1. El Gateway nos devuelve el ACK de un batch que enviamos
		case inner.MsgType(external.Ack): // Asegúrate de mapear bien tus constantes de protocolo
			slog.Debug("ACK de batch recibido en el cliente")
			// No hacemos nada, el Gateway procesó el lote exitosamente.

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
			return nil

		default:
			return fmt.Errorf("tipo de mensaje inesperado recibido en el cliente: %d", msgType)
		}
	}
}
