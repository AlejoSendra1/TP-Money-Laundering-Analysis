package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
	"tp_distribuidos/group"
)

func loadConfig() (group.GroupConfig, error) {
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

	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return group.GroupConfig{}, errors.New("ID environment variable is required")
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

	return group.GroupConfig{
		ID:                    id,
		MomHost:               momHost,
		MomPort:               momPort,
		InputQueue:            inputQueue,
		InputTopic:            inputTopic,
		InputExchangeName:     inputExchangeName,
		ControlExchange:       controlExchange,
		OutputExchangeName:    outputExchangeName,
		NextFaseWorkersAmount: nextFaseWorkersAmount,
		NextFaseWorkersPrefix: nextFaseWorkersPrefix,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := group.NewGroupWorker(config)
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
