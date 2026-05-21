package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
	"tp_distribuidos/bridge_matcher/bridge_matcher"
)

func loadConfig() (bridge_matcher.BridgeMatcherConfig, error) {
	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return bridge_matcher.BridgeMatcherConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return bridge_matcher.BridgeMatcherConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return bridge_matcher.BridgeMatcherConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	inputTopic := os.Getenv("INPUT_TOPIC")
	if inputTopic == "" {
		return bridge_matcher.BridgeMatcherConfig{}, errors.New("INPUT_TOPIC environment variable is required")
	}

	inputExchangeName := os.Getenv("INPUT_EXCHANGE_NAME")
	if inputExchangeName == "" {
		return bridge_matcher.BridgeMatcherConfig{}, errors.New("INPUT_EXCHANGE_NAME environment variable is required")
	}

	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return bridge_matcher.BridgeMatcherConfig{}, errors.New("ID environment variable is required")
	}

	controlExchange := os.Getenv("CONTROL_EXCHANGE_NAME")
	if controlExchange == "" {
		return bridge_matcher.BridgeMatcherConfig{}, errors.New("CONTROL_EXCHANGE_NAME environment variable is required")
	}

	outputExchangeName := os.Getenv("OUTPUT_EXCHANGE_NAME")
	if outputExchangeName == "" {
		return bridge_matcher.BridgeMatcherConfig{}, errors.New("OUTPUT_EXCHANGE_NAME environment variable is required")
	}

	nextFaseWorkersAmount, err := strconv.Atoi(os.Getenv("NEXT_FASE_WORKERS_AMOUNT"))
	if err != nil {
		return bridge_matcher.BridgeMatcherConfig{}, errors.New("NEXT_FASE_WORKERS_AMOUNT environment variable is required")
	}

	nextFaseWorkersPrefix := os.Getenv("NEXT_FASE_WORKERS_PREFIX")
	if nextFaseWorkersPrefix == "" {
		return bridge_matcher.BridgeMatcherConfig{}, errors.New("NEXT_FASE_WORKERS_PREFIX environment variable is required")
	}

	return bridge_matcher.BridgeMatcherConfig{
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

	server, err := bridge_matcher.NewBridgeMatcherWorker(config)
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
