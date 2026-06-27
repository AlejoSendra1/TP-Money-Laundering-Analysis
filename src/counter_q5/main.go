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

	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		return counter_q5.CounterQ5Config{}, errors.New("WORKER_ID is required")
	}

	return counter_q5.CounterQ5Config{
		ID:          id,
		WorkerID:    workerID,
		MomHost:     momHost,
		MomPort:     momPort,
		InputPrefix: inputPrefix,
		OutputQueue: outputQueue,
		CacheAmount: cacheAmount,
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

	if err = worker.Restaurate(); err != nil {
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
