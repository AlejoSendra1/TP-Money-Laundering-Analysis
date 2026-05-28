package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	filter_payment_format "tp_distribuidos/filter_payment_format"
)

func loadConfig() (filter_payment_format.FilterPaymentFormatConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return filter_payment_format.FilterPaymentFormatConfig{}, errors.New("ID environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return filter_payment_format.FilterPaymentFormatConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return filter_payment_format.FilterPaymentFormatConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return filter_payment_format.FilterPaymentFormatConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	inputExchangeName := os.Getenv("INPUT_EXCHANGE_NAME")
	if inputExchangeName == "" {
		return filter_payment_format.FilterPaymentFormatConfig{}, errors.New("INPUT_EXCHANGE_NAME environment variable is required")
	}

	inputTopic := os.Getenv("INPUT_TOPIC")
	if inputTopic == "" {
		return filter_payment_format.FilterPaymentFormatConfig{}, errors.New("INPUT_TOPIC environment variable is required")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return filter_payment_format.FilterPaymentFormatConfig{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	filterAmount, err := strconv.Atoi(os.Getenv("FILTER_AMOUNT"))
	if err != nil {
		return filter_payment_format.FilterPaymentFormatConfig{}, errors.New("FILTER_AMOUNT environment variable is required and must be a number")
	}

	filterPaymentControl := os.Getenv("FILTER_PAYMENT_CONTROL")
	if filterPaymentControl == "" {
		return filter_payment_format.FilterPaymentFormatConfig{}, errors.New("FILTER_PAYMENT_CONTROL environment variable is required")
	}

	batchSize := 100
	if bs := os.Getenv("BATCH_SIZE"); bs != "" {
		if v, err := strconv.Atoi(bs); err == nil {
			batchSize = v
		}
	}

	return filter_payment_format.FilterPaymentFormatConfig{
		ID:                   id,
		MomHost:              momHost,
		MomPort:              momPort,
		InputQueue:           inputQueue,
		InputExchangeName:    inputExchangeName,
		InputTopic:           inputTopic,
		OutputQueue:          outputQueue,
		FilterAmount:         filterAmount,
		FilterPaymentControl: filterPaymentControl,
		BatchSize:            batchSize,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	worker, err := filter_payment_format.NewFilterPaymentFormat(config)
	if err != nil {
		slog.Error("While initializing filter_payment_format", "err", err)
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
