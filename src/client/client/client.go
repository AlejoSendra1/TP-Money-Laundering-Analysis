package client

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"tp_distribuidos/common/datasaver"
	"tp_distribuidos/csvwriter"
)

const connectionAttempts = 3
const connectionAttemptsDelayMs = 300

type ClientConfig struct {
	ServerHost string
	ServerPort string
	InputFile  string
	OutputFile string
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

	if err := client.restaurateState(); err != nil { // restauramos la fase de envio y el id
		return nil, err
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
