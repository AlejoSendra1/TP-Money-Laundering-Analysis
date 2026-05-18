package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	"tp_distribuidos/date_filter/date_filter"
)

/*
- INPUT_QUEUE=date_filter_queue
- INPUT_EXCHANGE_NAME=usd_exchange
- INPUT_TOPIC=usd_transactions_topic
- OUTPUT_EXCHANGE_NAME=date_exchange
- OUTPUT_TOPIC_1=usd_early_period_transactions_topic
- OUTPUT_TOPIC_2=usd_later_period_transactions_topic
*/
func loadConfig() (date_filter.DateFilterConfig, error) {
	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return date_filter.DateFilterConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return date_filter.DateFilterConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return date_filter.DateFilterConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	inputExchangeName := os.Getenv("INPUT_EXCHANGE_NAME")
	if inputExchangeName == "" {
		return date_filter.DateFilterConfig{}, errors.New("INPUT_EXCHANGE_NAME environment variable is required")
	}

	inputTopic := os.Getenv("INPUT_TOPIC")
	if inputTopic == "" {
		return date_filter.DateFilterConfig{}, errors.New("INPUT_TOPIC environment variable is required")
	}

	outputExchangeName := os.Getenv("OUTPUT_EXCHANGE_NAME")
	if outputExchangeName == "" {
		return date_filter.DateFilterConfig{}, errors.New("OUTPUT_EXCHANGE_NAME environment variable is required")
	}

	outputTopic1 := os.Getenv("OUTPUT_TOPIC_1")
	if outputTopic1 == "" {
		return date_filter.DateFilterConfig{}, errors.New("OUTPUT_TOPIC_1 environment variable is required")
	}
	outputTopic2 := os.Getenv("OUTPUT_TOPIC_2")
	if outputTopic2 == "" {
		return date_filter.DateFilterConfig{}, errors.New("OUTPUT_TOPIC_2 environment variable is required")

	}

	return date_filter.DateFilterConfig{
		MomHost:            momHost,
		MomPort:            momPort,
		InputQueue:         inputQueue,
		InputExchangeName:  inputExchangeName,
		InputTopic:         inputTopic,
		OutputExchangeName: outputExchangeName,
		OutputTopic1:       outputTopic1,
		OutputTopic2:       outputTopic2,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := DateFilter.NewDateFilter(config)
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
