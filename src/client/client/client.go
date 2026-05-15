package client

import (
	"bufio"
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

	"tp_distribuidos/src/common/messageprotocol/external"
	"tp_distribuidos/src/common/transaction"
)

const (
	ID_COLUMN                 = 0
	TIMESTAMP_COLUMN          = 1
	FROM_BANK_COLUMN          = 2
	ACCOUNT_COLUMN            = 3
	TO_BANK_COLUMN            = 4
	ACCOUNT_1_COLUMN          = 5
	AMOUNT_RECEIVED_COLUMN    = 6
	RECEIVING_CURRENCY_COLUMN = 7
	AMOUNT_PAID_COLUMN        = 8
	PAYMENT_CURRENCY_COLUMN   = 9
	PAYMENT_FORMAT_COLUMN     = 10

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
	conn    net.Conn
	running atomic.Bool
	config  ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		return nil, err
	}

	client := &Client{conn: conn, config: config}
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
	/*
		if err := client.recvFruitTop(); err != nil {
			if client.running.Load() {
				return err
			}
			return nil
		}
	*/
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

	id, err := strconv.ParseUint(columns[ID_COLUMN], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid id %q: %w", columns[ID_COLUMN], err)
	}

	timestamp, err := time.Parse("2006-01-02 15:04:05", columns[TIMESTAMP_COLUMN])
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp %q: %w", columns[TIMESTAMP_COLUMN], err)
	}

	fromBank, err := strconv.ParseUint(columns[FROM_BANK_COLUMN], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid from_bank %q: %w", columns[FROM_BANK_COLUMN], err)
	}

	toBank, err := strconv.ParseUint(columns[TO_BANK_COLUMN], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid to_bank %q: %w", columns[TO_BANK_COLUMN], err)
	}

	amountReceived, err := strconv.ParseFloat(columns[AMOUNT_RECEIVED_COLUMN], 64)
	if err != nil {
		return nil, fmt.Errorf("invalid amount_received %q: %w", columns[AMOUNT_RECEIVED_COLUMN], err)
	}

	amountPaid, err := strconv.ParseFloat(columns[AMOUNT_PAID_COLUMN], 64)
	if err != nil {
		return nil, fmt.Errorf("invalid amount_paid %q: %w", columns[AMOUNT_PAID_COLUMN], err)
	}

	return &transaction.Transaction{
		Id:                 id,
		Timestamp:          timestamp,
		From_Bank:          fromBank,
		Account:            columns[ACCOUNT_COLUMN],
		To_Bank:            toBank,
		Account_1:          columns[ACCOUNT_1_COLUMN],
		Amount_Received:    amountReceived,
		Receiving_Currency: columns[RECEIVING_CURRENCY_COLUMN],
		Amount_Paid:        amountPaid,
		Payment_Currency:   columns[PAYMENT_CURRENCY_COLUMN],
		Payment_Format:     columns[PAYMENT_FORMAT_COLUMN],
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

		transaction, err := parseTransaction(columns)
		if err != nil {
			slog.Debug("Error while parsing transaction record", "err", err)
			return err
		}

		batch = append(batch, *transaction)
		if len(batch) == batchSize {
			if err := client.sendBatch(&batch); err != nil {
				return err
			}
		}
	}

	if err := client.sendBatch(&batch); err != nil {
		return err
	}
	if err := external.WriteEndOfRecords(client.conn); err != nil {
		return err
	}
	if err := client.expectMsgType(external.Ack); err != nil {
		return err
	}

	return nil
}

/*
func (client *Client) recvFruitTop() error {
	if err := client.expectMsgType(external.FruitTop); err != nil {
		return err
	}

	fruitTop, err := external.ReadFruitTop(client.conn)
	if err != nil {
		slog.Debug("Error while reading FruitTop message", "err", err)
		return err
	}
	if err := external.WriteAck(client.conn); err != nil {
		slog.Debug("Error while writing ack message", "err", err)
		return err
	}

	outputFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		slog.Debug("Error while creating output file", "err", err)
		return err
	}
	outputFileWriter := csv.NewWriter(outputFile)

	for _, fruitRecord := range fruitTop {
		line := []string{fruitRecord.Fruit, strconv.Itoa(int(fruitRecord.Amount))}
		if err := outputFileWriter.Write(line); err != nil {
			slog.Debug("Error while writing output file", "err", err)
			return err
		}
	}
	outputFileWriter.Flush()

	return nil
}
*/
