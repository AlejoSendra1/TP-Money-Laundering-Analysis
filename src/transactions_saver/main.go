package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
	"tp_distribuidos/transactions_saver"
)

func loadConfig() (transactions_saver.TransactionsSaverConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return transactions_saver.TransactionsSaverConfig{}, errors.New("ID environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return transactions_saver.TransactionsSaverConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return transactions_saver.TransactionsSaverConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	storageDir := os.Getenv("STORAGE_DIR")
	if storageDir == "" {
		return transactions_saver.TransactionsSaverConfig{}, errors.New("STORAGE_DIR environment variable is required")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return transactions_saver.TransactionsSaverConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	inputExchangeName := os.Getenv("INPUT_EXCHANGE_NAME")
	if inputExchangeName == "" {
		return transactions_saver.TransactionsSaverConfig{}, errors.New("INPUT_EXCHANGE_NAME environment variable is required")
	}

	inputTopic := os.Getenv("INPUT_TOPIC")
	if inputTopic == "" {
		return transactions_saver.TransactionsSaverConfig{}, errors.New("INPUT_TOPIC environment variable is required")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return transactions_saver.TransactionsSaverConfig{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	notificationExchangeName := os.Getenv("NOTIFICATION_EXCHANGE_NAME")
	if notificationExchangeName == "" {
		return transactions_saver.TransactionsSaverConfig{}, errors.New("NOTIFICATION_EXCHANGE_NAME environment variable is required")
	}

	q3AmountFilterAmount, err := strconv.Atoi(os.Getenv("Q3_AMOUNT_FILTER_AMOUNT"))
	if err != nil {
		return transactions_saver.TransactionsSaverConfig{}, errors.New("Q3_AMOUNT_FILTER_AMOUNT environment variable is required and must be a number")
	}
	notificationTopic := os.Getenv("NOTIFICATION_TOPIC_NAME")
	if notificationTopic == "" {
		return transactions_saver.TransactionsSaverConfig{}, errors.New("NOTIFICATION_TOPIC_NAME environment variable is required")
	}

	controlExchangeName := os.Getenv("CONTROL_EXCHANGE_NAME")
	if controlExchangeName == "" {
		return transactions_saver.TransactionsSaverConfig{}, errors.New("CONTROL_EXCHANGE_NAME environment variable is required")
	}
	controlTopic := os.Getenv("CONTROL_TOPIC_NAME")
	if controlTopic == "" {
		return transactions_saver.TransactionsSaverConfig{}, errors.New("CONTROL_TOPIC_NAME environment variable is required")
	}

	dateFilterAmount, err := strconv.Atoi(os.Getenv("DATE_FILTER_AMOUNT"))
	if err != nil {
		return transactions_saver.TransactionsSaverConfig{}, errors.New("DATE_FILTER_AMOUNT environment variable is required and must be a number")

	}

	return transactions_saver.TransactionsSaverConfig{
		Id:                       id,
		MomHost:                  momHost,
		MomPort:                  momPort,
		StorageDir:               storageDir,
		InputQueue:               inputQueue,
		InputExchangeName:        inputExchangeName,
		InputTopic:               inputTopic,
		OutputQueue:              outputQueue,
		NotificationExchangeName: notificationExchangeName,
		NotificationTopic:        notificationTopic,
		ControlExchangeName:      controlExchangeName,
		ControlTopic:             controlTopic,
		Q3AmountFilterAmount:     q3AmountFilterAmount,
		DateFilterAmount:         dateFilterAmount,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := transactions_saver.NewTransactionsSaver(config)
	if err != nil {
		slog.Error("While initializing transactions saver", "err", err)
		return 1
	}
	if err = server.Restaurate(); err != nil {
		slog.Error("While restoring state", "err", err)
		return 1
	}
	server.Run()
	return 0
}

func main() {
	os.Exit(run())
}
