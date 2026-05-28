package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	counter_q5 "tp_distribuidos/counter_q5"
)

func loadConfig() (counter_q5.CounterQ5Config, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return counter_q5.CounterQ5Config{}, errors.New("ID is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return counter_q5.CounterQ5Config{}, errors.New("MOM_HOST is required")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return counter_q5.CounterQ5Config{}, errors.New("MOM_PORT is required and must be a number")
	}

	inputPrefix := os.Getenv("INPUT_PREFIX")
	if inputPrefix == "" {
		return counter_q5.CounterQ5Config{}, errors.New("INPUT_PREFIX is required")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return counter_q5.CounterQ5Config{}, errors.New("OUTPUT_QUEUE is required")
	}

	cacheAmount, err := strconv.Atoi(os.Getenv("CACHE_AMOUNT"))
	if err != nil {
		return counter_q5.CounterQ5Config{}, errors.New("CACHE_AMOUNT is required and must be a number")
	}

	instanceAmount, err := strconv.Atoi(os.Getenv("INSTANCE_AMOUNT"))
	if err != nil {
		return counter_q5.CounterQ5Config{}, errors.New("INSTANCE_AMOUNT is required and must be a number")
	}

	controlExchangeName := os.Getenv("CONTROL_EXCHANGE_NAME")
	if controlExchangeName == "" {
		return counter_q5.CounterQ5Config{}, errors.New("CONTROL_EXCHANGE_NAME is required")
	}

	return counter_q5.CounterQ5Config{
		ID:                  id,
		MomHost:             momHost,
		MomPort:             momPort,
		InputPrefix:         inputPrefix,
		OutputQueue:         outputQueue,
		CacheAmount:         cacheAmount,
		InstanceAmount:      instanceAmount,
		ControlExchangeName: controlExchangeName,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("Loading config", "err", err)
		return 1
	}

	worker, err := counter_q5.NewCounterQ5(config)
	if err != nil {
		slog.Error("Initializing counter_q5", "err", err)
		return 1
	}

	worker.Run()
	return 0
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))
	os.Exit(run())
}
