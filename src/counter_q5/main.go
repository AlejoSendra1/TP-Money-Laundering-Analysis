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

	filterAmount, err := strconv.Atoi(os.Getenv("FILTER_AMOUNT"))
	if err != nil {
		return counter_q5.CounterQ5Config{}, errors.New("FILTER_AMOUNT is required and must be a number")
	}

	conversionQueue := os.Getenv("CONVERSION_QUEUE")
	if conversionQueue == "" {
		return counter_q5.CounterQ5Config{}, errors.New("CONVERSION_QUEUE is required")
	}

	convertedPrefix := os.Getenv("CONVERTED_PREFIX")
	if convertedPrefix == "" {
		return counter_q5.CounterQ5Config{}, errors.New("CONVERTED_PREFIX is required")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return counter_q5.CounterQ5Config{}, errors.New("OUTPUT_QUEUE is required")
	}

	return counter_q5.CounterQ5Config{
		ID:              id,
		MomHost:         momHost,
		MomPort:         momPort,
		InputPrefix:     inputPrefix,
		FilterAmount:    filterAmount,
		ConversionQueue: conversionQueue,
		ConvertedPrefix: convertedPrefix,
		OutputQueue:     outputQueue,
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
