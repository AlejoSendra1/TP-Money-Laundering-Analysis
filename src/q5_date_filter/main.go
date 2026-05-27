package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
	"tp_distribuidos/q5_date_filter"
)

func loadConfig() (q5_date_filter.Q5DateFilterConfig, error) {
	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return q5_date_filter.Q5DateFilterConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return q5_date_filter.Q5DateFilterConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return q5_date_filter.Q5DateFilterConfig{}, errors.New("INPUT_ QUEUE environment variable is required")
	}

	inputExchangeName := os.Getenv("INPUT_EXCHANGE_NAME")
	if inputExchangeName == "" {
		return q5_date_filter.Q5DateFilterConfig{}, errors.New("INPUT_EXCHANGE_NAME environment variable is required")
	}

	inputTopic := os.Getenv("INPUT_TOPIC")
	if inputTopic == "" {
		return q5_date_filter.Q5DateFilterConfig{}, errors.New("INPUT_TOPIC environment variable is required")
	}

	outputExchangeName := os.Getenv("OUTPUT_EXCHANGE_NAME")
	if outputExchangeName == "" {
		return q5_date_filter.Q5DateFilterConfig{}, errors.New("OUTPUT_EXCHANGE_NAME environment variable is required")
	}

	outputTopic := os.Getenv("OUTPUT_TOPIC")
	if outputTopic == "" {
		return q5_date_filter.Q5DateFilterConfig{}, errors.New("OUTPUT_TOPIC environment variable is required")
	}

	controlExchangeName := os.Getenv("CONTROL_EXCHANGE_NAME")
	if controlExchangeName == "" {
		return q5_date_filter.Q5DateFilterConfig{}, errors.New("CONTROL_EXCHANGE_NAME environment variable is required")
	}

	controlTopic := os.Getenv("CONTROL_TOPIC")
	if controlTopic == "" {
		return q5_date_filter.Q5DateFilterConfig{}, errors.New("CONTROL_TOPIC environment variable is required")
	}

	return q5_date_filter.Q5DateFilterConfig{
		MomHost:             momHost,
		MomPort:             momPort,
		InputQueue:          inputQueue,
		InputExchangeName:   inputExchangeName,
		InputTopic:          inputTopic,
		OutputExchangeName:  outputExchangeName,
		OutputTopic:         outputTopic,
		ControlExchangeName: controlExchangeName,
		ControlTopic:        controlTopic,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := q5_date_filter.NewQ5DateFilter(config)
	if err != nil {
		slog.Error("While initializing q5 date filter", "err", err)
		return 1
	}

	server.Run()
	return 0
}

func main() {
	os.Exit(run())
}
