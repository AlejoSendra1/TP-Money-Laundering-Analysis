package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
	"tp_distribuidos/q4_join/q4_join"
)

func loadConfig() (q4_join.JoinConfig, error) {
	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return q4_join.JoinConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return q4_join.JoinConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return q4_join.JoinConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	inputTopic := os.Getenv("INPUT_TOPIC")
	if inputTopic == "" {
		return q4_join.JoinConfig{}, errors.New("INPUT_TOPIC environment variable is required")
	}

	inputExchangeName := os.Getenv("INPUT_EXCHANGE_NAME")
	if inputExchangeName == "" {
		return q4_join.JoinConfig{}, errors.New("INPUT_EXCHANGE_NAME environment variable is required")
	}

	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return q4_join.JoinConfig{}, errors.New("ID environment variable is required")
	}

	outputQueueName := os.Getenv("OUTPUT_QUEUE_NAME")
	if outputQueueName == "" {
		return q4_join.JoinConfig{}, errors.New("OUTPUT_EXCHANGE_NAME environment variable is required")
	}

	prevFaseWorkersAmount, err := strconv.Atoi(os.Getenv("PREV_FASE_WORKERS_AMOUNT"))
	if err != nil {
		return q4_join.JoinConfig{}, errors.New("PREV_FASE_WORKERS_AMOUNT environment variable is required")
	}

	return q4_join.JoinConfig{
		ID:                    id,
		MomHost:               momHost,
		MomPort:               momPort,
		InputQueue:            inputQueue,
		InputTopic:            inputTopic,
		InputExchangeName:     inputExchangeName,
		OutputQueueName:       outputQueueName,
		PrevFaseWorkersAmount: prevFaseWorkersAmount,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := q4_join.NewJoinWorker(config)
	if err != nil {
		slog.Error("While initializing", "err", err)
		return 1
	}

	server.Run()
	return 0
}

func main() {
	os.Exit(run())
}
