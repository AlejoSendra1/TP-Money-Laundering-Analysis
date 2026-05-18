package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	"tp_distribuidos/usd_filter/usd_filter"
)

func loadConfig() (usd_filter.USDFilterConfig, error) {
	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return usd_filter.USDFilterConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return usd_filter.USDFilterConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return usd_filter.USDFilterConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	inputExchangeName := os.Getenv("INPUT_EXCHANGE_NAME")
	if inputExchangeName == "" {
		return usd_filter.USDFilterConfig{}, errors.New("INPUT_EXCHANGE_NAME environment variable is required")
	}

	inputTopic := os.Getenv("INPUT_TOPIC")
	if inputTopic == "" {
		return usd_filter.USDFilterConfig{}, errors.New("INPUT_TOPIC environment variable is required")
	}

	outputExchangeName := os.Getenv("OUTPUT_EXCHANGE_NAME")
	if outputExchangeName == "" {
		return usd_filter.USDFilterConfig{}, errors.New("OUTPUT_EXCHANGE_NAME environment variable is required")
	}

	outputTopic := os.Getenv("OUTPUT_TOPIC")
	if outputTopic == "" {
		return usd_filter.USDFilterConfig{}, errors.New("OUTPUT_TOPIC environment variable is required")
	}

	return usd_filter.USDFilterConfig{
		MomHost:            momHost,
		MomPort:            momPort,
		InputQueue:         inputQueue,
		InputExchangeName:  inputExchangeName,
		InputTopic:         inputTopic,
		OutputExchangeName: outputExchangeName,
		OutputTopic:        outputTopic,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := usd_filter.NewUSDFilter(config)
	if err != nil {
		slog.Error("While initializing usd filter", "err", err)
		return 1
	}

	server.Run()
	return 0
}

func main() {
	os.Exit(run())
}
