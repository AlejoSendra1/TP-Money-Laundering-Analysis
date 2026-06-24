package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
	"tp_distribuidos/group"
)

func loadConfig() (group.GroupConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return group.GroupConfig{}, errors.New("ID environment variable is required")
	}

	workerPrefix := os.Getenv("WORKER_PREFIX")
	if workerPrefix == "" {
		return group.GroupConfig{}, errors.New("WORKER_PREFIX environment variable is required")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return group.GroupConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return group.GroupConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return group.GroupConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	inputTopic := os.Getenv("INPUT_TOPIC")
	if inputTopic == "" {
		return group.GroupConfig{}, errors.New("INPUT_TOPIC environment variable is required")
	}

	inputExchangeName := os.Getenv("INPUT_EXCHANGE_NAME")
	if inputExchangeName == "" {
		return group.GroupConfig{}, errors.New("INPUT_EXCHANGE_NAME environment variable is required")
	}

	controlExchange := os.Getenv("CONTROL_EXCHANGE_NAME")
	if controlExchange == "" {
		return group.GroupConfig{}, errors.New("CONTROL_EXCHANGE_NAME environment variable is required")
	}

	outputExchangeName := os.Getenv("OUTPUT_EXCHANGE_NAME")
	if outputExchangeName == "" {
		return group.GroupConfig{}, errors.New("OUTPUT_EXCHANGE_NAME environment variable is required")
	}

	nextFaseWorkersAmount, err := strconv.Atoi(os.Getenv("NEXT_FASE_WORKERS_AMOUNT"))
	if err != nil {
		return group.GroupConfig{}, errors.New("NEXT_FASE_WORKERS_AMOUNT environment variable is required")
	}

	nextFaseWorkersPrefix := os.Getenv("NEXT_FASE_WORKERS_PREFIX")
	if nextFaseWorkersPrefix == "" {
		return group.GroupConfig{}, errors.New("NEXT_FASE_WORKERS_PREFIX environment variable is required")
	}

	dateAmountFilter, err := strconv.Atoi(os.Getenv("DATE_FILTER_AMOUNT"))
	if err != nil {
		return group.GroupConfig{}, errors.New("DATE_FILTER_AMOUNT environment variable is required")
	}

	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		return group.GroupConfig{}, errors.New("WORKER_ID environment variable is required")
	}

	return group.GroupConfig{
		ID:                    id,
		WorkerID:              workerID,
		WorkerPrefix:          workerPrefix,
		MomHost:               momHost,
		MomPort:               momPort,
		InputQueue:            inputQueue,
		InputTopic:            inputTopic,
		InputExchangeName:     inputExchangeName,
		ControlExchangeName:   controlExchange,
		OutputExchangeName:    outputExchangeName,
		NextFaseWorkersAmount: nextFaseWorkersAmount,
		NextFaseWorkersPrefix: nextFaseWorkersPrefix,
		DateFilterAmount:      dateAmountFilter,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	g, err := group.NewGroupWorker(config)
	if err != nil {
		slog.Error("While initializing", "err", err)
		return 1
	}

	slog.Info("leyendo flag restaurate", "value", os.Getenv("RESTAURATE"))
	if os.Getenv("RESTAURATE") == "TRUE" {
		slog.Info("restaurando")
		if err := g.Restaurate(); err != nil {
			slog.Error("While restoring state", "err", err)
			return 1
		}
		slog.Info("restaurado todo piola")
	}

	g.Run()
	return 0
}

func main() {
	os.Exit(run())
}
