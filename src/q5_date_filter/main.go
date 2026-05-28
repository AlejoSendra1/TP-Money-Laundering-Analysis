package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
	"tp_distribuidos/q5_date_filter"
)

func loadConfig() (q5_date_filter.Q5DateFilterConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return q5_date_filter.Q5DateFilterConfig{}, errors.New("ID environment variable is required and must be a number")
	}

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
		return q5_date_filter.Q5DateFilterConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	inputExchangeName := os.Getenv("INPUT_EXCHANGE_NAME")
	if inputExchangeName == "" {
		return q5_date_filter.Q5DateFilterConfig{}, errors.New("INPUT_EXCHANGE_NAME environment variable is required")
	}

	inputTopic := os.Getenv("INPUT_TOPIC")
	if inputTopic == "" {
		return q5_date_filter.Q5DateFilterConfig{}, errors.New("INPUT_TOPIC environment variable is required")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return q5_date_filter.Q5DateFilterConfig{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	instanceAmount, err := strconv.Atoi(os.Getenv("INSTANCE_AMOUNT"))
	if err != nil {
		return q5_date_filter.Q5DateFilterConfig{}, errors.New("INSTANCE_AMOUNT environment variable is required and must be a number")
	}

	controlExchangeName := os.Getenv("CONTROL_EXCHANGE_NAME")
	if controlExchangeName == "" {
		return q5_date_filter.Q5DateFilterConfig{}, errors.New("CONTROL_EXCHANGE_NAME environment variable is required")
	}

	return q5_date_filter.Q5DateFilterConfig{
		ID:                  id,
		MomHost:             momHost,
		MomPort:             momPort,
		InputQueue:          inputQueue,
		InputExchangeName:   inputExchangeName,
		InputTopic:          inputTopic,
		OutputQueue:         outputQueue,
		InstanceAmount:      instanceAmount,
		ControlExchangeName: controlExchangeName,
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
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))
	os.Exit(run())
}
