package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
	"tp_distribuidos/q1_amount_filter"
)

func loadConfig() (q1_amount_filter.Q1AmountFilterConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return q1_amount_filter.Q1AmountFilterConfig{}, errors.New("ID environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return q1_amount_filter.Q1AmountFilterConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return q1_amount_filter.Q1AmountFilterConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return q1_amount_filter.Q1AmountFilterConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	inputExchangeName := os.Getenv("INPUT_EXCHANGE_NAME")
	if inputExchangeName == "" {
		return q1_amount_filter.Q1AmountFilterConfig{}, errors.New("INPUT_EXCHANGE_NAME environment variable is required")
	}

	inputTopic := os.Getenv("INPUT_TOPIC")
	if inputTopic == "" {
		return q1_amount_filter.Q1AmountFilterConfig{}, errors.New("INPUT_TOPIC environment variable is required")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return q1_amount_filter.Q1AmountFilterConfig{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	controlExchangeName := os.Getenv("CONTROL_EXCHANGE_NAME")
	if controlExchangeName == "" {
		return q1_amount_filter.Q1AmountFilterConfig{}, errors.New("CONTROL_EXCHANGE_NAME environment variable is required")
	}

	controlTopic := os.Getenv("CONTROL_TOPIC")
	if controlTopic == "" {
		return q1_amount_filter.Q1AmountFilterConfig{}, errors.New("CONTROL_TOPIC environment variable is required")
	}

	usdFilterAmount, err := strconv.Atoi(os.Getenv("USD_FILTER_AMOUNT"))
	if err != nil {
		return q1_amount_filter.Q1AmountFilterConfig{}, errors.New("USD_FILTER_AMOUNT environment variable is required and must be a number")
	}

	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		return q1_amount_filter.Q1AmountFilterConfig{}, errors.New("WORKER_ID environment variable is required")
	}

	return q1_amount_filter.Q1AmountFilterConfig{
		Id:                id,
		WorkerID:          workerID,
		MomHost:           momHost,
		MomPort:           momPort,
		InputQueue:        inputQueue,
		InputExchangeName: inputExchangeName,
		InputTopic:        inputTopic,
		OutputQueue:       outputQueue,
		ControlExchange:   controlExchangeName,
		ControlTopic:      controlTopic,
		USDFilterAmount:   usdFilterAmount,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := q1_amount_filter.NewQ1AmountFilter(config)
	if err != nil {
		slog.Error("While initializing q1 amount filter", "err", err)
		return 1
	}

	server.Run()
	return 0
}

func main() {
	os.Exit(run())
}
