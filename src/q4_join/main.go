package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
	"tp_distribuidos/q4_join"
)

func loadConfig() (q4_join.JoinConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return q4_join.JoinConfig{}, errors.New("ID environment variable is required")
	}

	workerPrefix := os.Getenv("WORKER_PREFIX")
	if workerPrefix == "" {
		return q4_join.JoinConfig{}, errors.New("WORKER_PREFIX environment variable is required")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return q4_join.JoinConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return q4_join.JoinConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	inputExchangeName := os.Getenv("INPUT_EXCHANGE_NAME")
	if inputExchangeName == "" {
		return q4_join.JoinConfig{}, errors.New("INPUT_EXCHANGE_NAME environment variable is required")
	}

	outputQueueName := os.Getenv("OUTPUT_QUEUE_NAME")
	if outputQueueName == "" {
		return q4_join.JoinConfig{}, errors.New("OUTPUT_EXCHANGE_NAME environment variable is required")
	}

	prevFaseWorkersAmount, err := strconv.Atoi(os.Getenv("PREV_FASE_WORKERS_AMOUNT"))
	if err != nil {
		return q4_join.JoinConfig{}, errors.New("PREV_FASE_WORKERS_AMOUNT environment variable is required")
	}

	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		return q4_join.JoinConfig{}, errors.New("WORKER_ID environment variable is required")
	}

	return q4_join.JoinConfig{
		ID:                    id,
		WorkerID:              workerID,
		WorkerPrefix:          workerPrefix,
		MomHost:               momHost,
		MomPort:               momPort,
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

	join, err := q4_join.NewJoinWorker(config)
	if err != nil {
		slog.Error("While initializing", "err", err)
		return 1
	}

	slog.Info("restaurando")
	if err := join.Restaurate(); err != nil {
		slog.Error("While restoring state", "err", err)
		return 1
	}

	join.Run()
	return 0
}

func main() {
	os.Exit(run())
}
