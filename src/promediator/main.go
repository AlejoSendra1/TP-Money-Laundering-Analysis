package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
	"tp_distribuidos/promediator"
)

func loadConfig() (promediator.PromediatorConfig, error) {
	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return promediator.PromediatorConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return promediator.PromediatorConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return promediator.PromediatorConfig{}, errors.New("ID environment variable is required and must be a number")
	}
	inputExchangeName := os.Getenv("INPUT_EXCHANGE_NAME")
	if inputExchangeName == "" {
		return promediator.PromediatorConfig{}, errors.New("INPUT_EXCHANGE_NAME environment variable is required")
	}
	outputExchangeName := os.Getenv("OUTPUT_EXCHANGE_NAME")
	if outputExchangeName == "" {
		return promediator.PromediatorConfig{}, errors.New("OUTPUT_EXCHANGE_NAME environment variable is required")
	}
	sumAmount, err := strconv.Atoi(os.Getenv("SUM_AMOUNT"))
	if err != nil {
		return promediator.PromediatorConfig{}, errors.New("SUM_AMOUNT environment variable is required")
	}

	promediatorPrefix := os.Getenv("PROMEDIATOR_PREFIX")
	if promediatorPrefix == "" {
		return promediator.PromediatorConfig{}, errors.New("PROMEDIATOR_PREFIX environment variable is required")
	}
	outputTopic := os.Getenv("OUTPUT_TOPIC_NAME")
	if outputTopic == "" {
		return promediator.PromediatorConfig{}, errors.New("OUTPUT_TOPIC_NAME environment variable is required")
	}

	return promediator.PromediatorConfig{
		Id:                 id,
		MomHost:            momHost,
		MomPort:            momPort,
		InputExchangeName:  inputExchangeName,
		OutputExchangeName: outputExchangeName,
		OutputTopic:        outputTopic,
		SumAmount:          sumAmount,
		PromediatorPrefix:  promediatorPrefix,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := promediator.NewPromediator(config)
	if err != nil {
		slog.Error("While initializing Promediator", "err", err)
		return 1
	}

	server.Run()
	return 0
}

func main() {
	os.Exit(run())
}
