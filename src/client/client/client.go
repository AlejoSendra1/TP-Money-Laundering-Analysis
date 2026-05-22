package client

import (
	"bufio"
	"encoding/csv"
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

	"tp_distribuidos/common/messageprotocol/external"
	"tp_distribuidos/common/messageprotocol/external/safeio"
	"tp_distribuidos/common/messageprotocol/serializer"
	"tp_distribuidos/common/transaction"
)

const (
	TIMESTAMP_COLUMN    = 1
	FROM_BANK_COLUMN    = 2
	FROM_ACCOUNT_COLUMN = 3
	TO_BANK_COLUMN      = 4
	TO_ACCOUNT_COLUMN   = 5
	AMOUNT_COLUMN       = 8
	CURRENCY_COLUMN     = 9

	PAYMENT_FORMAT_COLUMN = 10

	EXPECTED_COLUMNS = 12
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
	transactionsSentCounter int64 // for debug
	conn                    net.Conn
	running                 atomic.Bool
	config                  ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		return nil, err
	}

	client := &Client{transactionsSentCounter: 0, conn: conn, config: config}
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
	go client.handleSignals()

	if err := client.sendTransactionRecords(); err != nil {
		if client.running.Load() {
			return err
		}
		return nil
	}

	if err := client.recvQueriesResult(); err != nil {
		if client.running.Load() {
			return err
		}
		return nil
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
		slog.Debug("Error while reading message type", "err", err)
		return err
	}
	if msgType != expectedMsgType {
		return errors.New("Unexpected message type")
	}
	return nil
}

func parseTransaction(columns []string) (*transaction.Transaction, error) {
	if len(columns) < EXPECTED_COLUMNS {
		return nil, fmt.Errorf("expected %d columns, got %d", EXPECTED_COLUMNS, len(columns))
	}

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

	if err := client.expectMsgType(external.Ack); err != nil {
		return err
	}

	*batch = (*batch)[:0]
	return nil
}

func (client *Client) sendTransactionRecords() error {
	file, err := os.Open(client.config.InputFile)
	if err != nil {
		slog.Debug("Error while runninging input file", "err", err)
		return err
	}
	defer file.Close()

	batchSizeAsString := os.Getenv("BATCH_SIZE")
	batchSize, err := strconv.Atoi(batchSizeAsString)
	if err != nil {
		slog.Debug("Error reading batchSize from environment", "err", err)
		return err
	}

	scanner := bufio.NewScanner(file)
	batch := []transaction.Transaction{}

	for scanner.Scan() {
		columns := strings.Split(scanner.Text(), ",")

		tx, err := parseTransaction(columns)
		if err != nil {
			slog.Debug("Error while parsing transaction record", "err", err)
			return err
		}

		batch = append(batch, *tx)
		if len(batch) == batchSize {
			client.transactionsSentCounter += int64(len(batch))
			if err := client.sendBatch(&batch); err != nil {
				slog.Debug("Error while sending transaction batch", "err", err)
				return err
			}
		}
	}

	if err := client.sendBatch(&batch); err != nil {
		return err
	}
	str := fmt.Sprint("transacciones enviadas: ", client.transactionsSentCounter)
	slog.Info(str)

	if err := external.WriteEndOfRecords(client.conn); err != nil {
		return err
	}
	if err := client.expectMsgType(external.Ack); err != nil {
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
	msgType, err := external.ReadMsgType(client.conn)
	if err != nil {
		slog.Debug("While reading message type for queries result", "err", err)
		return err
	}
	slog.Info("Read message type for queries result", "msgType", msgType)

	for queriesRead := 0; queriesRead < 1; queriesRead++ { // En el futuro sera 5...
		switch msgType {
		case external.Query1Response:
			records, err := client.readQuery1Records()
			if err != nil {
				return err
			}
			if err = client.writeQuery1CSV(records); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected message type while receiving queries result: %d", msgType)
		}
	}
	if err := client.expectEndOfRecords(); err != nil {
		return err
	}
	if err := external.WriteAck(client.conn); err != nil {
		slog.Debug("While writing ACK after queries result", "err", err)
		return err
	}
	return nil
}

func (client *Client) readQuery1Records() ([]transaction.LowAmountTransfer, error) {
	count, err := client.readUint32()
	if err != nil {
		slog.Debug("While reading query1 records count", "err", err)
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
		slog.Debug("While reading message type for queries result EOF", "err", err)
		return err
	}
	if msgType != external.EndOfRecords {
		slog.Debug("Expected EndOfRecords message type after reading queries result, got", "msgType", msgType)
		return fmt.Errorf("expected EndOfRecords message type after reading queries result, got %d", msgType)
	}
	return nil
}

func (client *Client) writeQuery1CSV(records []transaction.LowAmountTransfer) error {
	outputFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		slog.Debug("Error while creating output file", "err", err)
		return err
	}
	defer outputFile.Close()

	writer := csv.NewWriter(outputFile)
	defer writer.Flush()

	if err := writer.Write([]string{"From Bank", "Account", "To Bank", "Account.1", "Amount Paid"}); err != nil {
		slog.Debug("Error while writing CSV header", "err", err)
		return err
	}

	for _, r := range records {
		line := []string{strconv.Itoa(r.FromBank), r.FromAccount, strconv.Itoa(r.ToBank), r.ToAccount, fmt.Sprintf("%.2f", r.Amount)}
		if err := writer.Write(line); err != nil {
			slog.Debug("Error while writing CSV line", "err", err)
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
		slog.Debug("While reading field", "field", fieldName, "err", err)
		return 0, err
	}
	return int(serializer.DeserializeUint64(bytes)), nil
}

func (client *Client) readLengthPrefixedString(fieldName string) (string, error) {
	length, err := client.readUint32()
	if err != nil {
		slog.Debug("While reading string length", "field", fieldName, "err", err)
		return "", err
	}
	bytes, err := safeio.ReadAll(client.conn, length)
	if err != nil {
		slog.Debug("While reading string bytes", "field", fieldName, "err", err)
		return "", err
	}
	return serializer.DeserializeString(bytes), nil
}

func (client *Client) readFloat64(fieldName string) (float64, error) {
	bytes, err := safeio.ReadAll(client.conn, serializer.UINT64_SIZE)
	if err != nil {
		slog.Debug("While reading field", "field", fieldName, "err", err)
		return 0, err
	}
	return serializer.DeserializeFloat64(bytes), nil
}
