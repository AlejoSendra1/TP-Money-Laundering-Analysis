package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
	"tp_distribuidos/sum"
)

func loadConfig() (sum.SumConfig, error) {
	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return sum.SumConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return sum.SumConfig{}, errors.New("ID environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return sum.SumConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		return sum.SumConfig{}, errors.New("WORKER_ID environment variable is required")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return sum.SumConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	inputExchangeName := os.Getenv("INPUT_EXCHANGE_NAME")
	if inputExchangeName == "" {
		return sum.SumConfig{}, errors.New("INPUT_EXCHANGE_NAME environment variable is required")
	}

	inputTopic := os.Getenv("INPUT_TOPIC")
	if inputTopic == "" {
		return sum.SumConfig{}, errors.New("INPUT_TOPIC environment variable is required")
	}

	controlExchangeName := os.Getenv("CONTROL_EXCHANGE_NAME")
	if controlExchangeName == "" {
		return sum.SumConfig{}, errors.New("CONTROL_EXCHANGE_NAME environment variable is required")
	}

	controlExchangeTopic := os.Getenv("CONTROL_EXCHANGE_TOPIC")
	if controlExchangeTopic == "" {
		return sum.SumConfig{}, errors.New("CONTROL_EXCHANGE_TOPIC environment variable is required")
	}

	outputExchangeName := os.Getenv("OUTPUT_EXCHANGE_NAME")
	if outputExchangeName == "" {
		return sum.SumConfig{}, errors.New("OUTPUT_EXCHANGE_NAME environment variable is required")
	}

	promediatorAmount, err := strconv.Atoi(os.Getenv("PROMEDIATOR_AMOUNT"))
	if err != nil {
		return sum.SumConfig{}, errors.New("PROMEDIATOR_AMOUNT environment variable is required and must be a number")
	}

	promedietorPrefix := os.Getenv("PROMEDIATOR_PREFIX")
	if promedietorPrefix == "" {
		return sum.SumConfig{}, errors.New("PROMEDIATOR_PREFIX environment variable is required")
	}

	DateFilterAmount, err := strconv.Atoi(os.Getenv("DATE_FILTER_AMOUNT"))
	if err != nil {
		return sum.SumConfig{}, errors.New("DATE_FILTER_AMOUNT environment variable is required and must be a number")
	}

	return sum.SumConfig{
		Id:                   id,
		WorkerID:             workerID,
		MomHost:              momHost,
		MomPort:              momPort,
		InputQueue:           inputQueue,
		InputTopic:           inputTopic,
		InputExchangeName:    inputExchangeName,
		ControlExchangeName:  controlExchangeName,
		ControlExchangeTopic: controlExchangeTopic,
		OutputExchangeName:   outputExchangeName,
		PromediatorAmount:    uint8(promediatorAmount),
		PromedietorPrefix:    promedietorPrefix,
		DateFilterAmount:     uint8(DateFilterAmount),
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := sum.NewSum(config)
	if err != nil {
		slog.Error("While initializing sum", "err", err)
		return 1
	}
	if err = server.Restaurate(); err != nil {
		slog.Error("While restoring state", "err", err)
		return 1
	}
	server.Run()
	return 0
}

func main() {
	os.Exit(run())
}
