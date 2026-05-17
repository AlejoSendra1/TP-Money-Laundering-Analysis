package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
	"tp_distribuidos/filewriterworker"
)

func loadConfig() (filewriterworker.FileWriterWorkerConfig, error) {
	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return filewriterworker.FileWriterWorkerConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return filewriterworker.FileWriterWorkerConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return filewriterworker.FileWriterWorkerConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	inputExchange := os.Getenv("INPUT_EXCHANGE_NAME")
	if inputExchange == "" {
		return filewriterworker.FileWriterWorkerConfig{}, errors.New("INPUT_TOPIC environment variable is required")
	}

	inputTopic := os.Getenv("INPUT_TOPIC")
	if inputTopic == "" {
		return filewriterworker.FileWriterWorkerConfig{}, errors.New("INPUT_TOPIC environment variable is required")
	}

	return filewriterworker.FileWriterWorkerConfig{
		MomHost:           momHost,
		MomPort:           momPort,
		InputQueue:        inputQueue,
		InputExchangeName: inputExchange,
		InputTopic:        inputTopic,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	worker, err := filewriterworker.NewFileWriterWorker(config)
	if err != nil {
		slog.Error("While initializing file writer worker", "err", err)
		return 1
	}
	defer worker.Close()

	worker.Run()
	return 0
}

func main() {
	os.Exit(run())
}
