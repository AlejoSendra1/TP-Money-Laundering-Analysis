package filewriterworker

import (
	"fmt"
	"log"
	"log/slog"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
)

type FileWriterWorkerConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	InputExchangeName string
	InputTopic        string
}

type FileWriterWorker struct {
	csvWriter     CSVWriter
	inputExchange middleware.Middleware
	config        FileWriterWorkerConfig
}

func NewFileWriterWorker(config FileWriterWorkerConfig) (*FileWriterWorker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.NewQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}
	err = inputQueue.BindToTopics(config.InputExchangeName, config.InputTopic)
	if err != nil {
		return nil, err
	}

	csvWriter, err := NewCSVWriter("filewriter_results/al_final_del_flujo.csv")
	if err != nil {
		log.Fatalf("creating csv writer: %v", err)
	}

	return &FileWriterWorker{
		inputExchange: inputQueue,
		csvWriter:     *csvWriter,
		config:        config,
	}, nil
}
func (fileWriterWorker *FileWriterWorker) Close() {
	fileWriterWorker.csvWriter.Close()
}

func (fileWriterWorker *FileWriterWorker) Run() {
	fileWriterWorker.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		//usdFilter.handleMessage(msg, ack, nack)
		fileWriterWorker.handleMessage_de_prueba(&msg, ack, nack)
	})
}
func (fileWriterWorker *FileWriterWorker) handleMessage_de_prueba(msg *middleware.Message, ack func(), nack func()) {
	// On every message received:
	clientId, transactions, _, err := inner.DeserializeRawTransactionsMessage(msg)
	if len(transactions) == 0 {
		slog.Info("Received EOF")
		str := fmt.Sprint("transacciones enviadas: ", fileWriterWorker.csvWriter.getCount())
		slog.Info(str)
	}

	if err != nil {
		log.Printf("Error: deserializing message from client %d: %v", clientId, err)
		nack()
	} else {
		if err := fileWriterWorker.csvWriter.WriteBatch(transactions); err != nil {
			log.Printf("writing batch from client %d: %v", clientId, err)
		}
		ack()
	}

}
