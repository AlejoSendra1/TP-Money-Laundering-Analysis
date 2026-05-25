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

	"tp_distribuidos/common/messageprotocol/external"
	"tp_distribuidos/common/transaction"
)

const (
	TIMESTAMP_COLUMN      = 0
	FROM_BANK_COLUMN      = 1
	FROM_ACCOUNT_COLUMN   = 2
	TO_BANK_COLUMN        = 3
	TO_ACCOUNT_COLUMN     = 4
	AMOUNT_COLUMN         = 5
	CURRENCY_COLUMN       = 6
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

	slog.Info("procesando transacciones")
	for scanner.Scan() {
		columns := strings.Split(scanner.Text(), ",")

		transaction, err := parseTransaction(columns)
		if err != nil {
			slog.Debug("Error while parsing transaction record", "err", err)
			return err
		}

		batch = append(batch, *transaction)
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
