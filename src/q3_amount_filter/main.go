package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
	"tp_distribuidos/q3_amount_filter"
)

func loadConfig() (q3_amount_filter.Q3AmountFilterConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return q3_amount_filter.Q3AmountFilterConfig{}, errors.New("ID environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return q3_amount_filter.Q3AmountFilterConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return q3_amount_filter.Q3AmountFilterConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	inputPromediatorExchange := os.Getenv("INPUT_PROMEDIATOR_EXCHANGE")
	if inputPromediatorExchange == "" {
		return q3_amount_filter.Q3AmountFilterConfig{}, errors.New("INPUT_PROMEDIATOR_EXCHANGE environment variable is required")
	}

	inputPromediatorTopic := os.Getenv("INPUT_PROMEDIATOR_TOPIC")
	if inputPromediatorTopic == "" {
		return q3_amount_filter.Q3AmountFilterConfig{}, errors.New("INPUT_PROMEDIATOR_TOPIC environment variable is required")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return q3_amount_filter.Q3AmountFilterConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return q3_amount_filter.Q3AmountFilterConfig{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	promediatorAmount, err := strconv.Atoi(os.Getenv("PROMEDIATOR_AMOUNT"))
	if err != nil {
		return q3_amount_filter.Q3AmountFilterConfig{}, errors.New("PROMEDIATOR_AMOUNT environment variable is required and must be a number")
	}

	notificationExchange := os.Getenv("NOTIFICATION_EXCHANGE_NAME")
	if notificationExchange == "" {
		return q3_amount_filter.Q3AmountFilterConfig{}, errors.New("NOTIFICATION_EXCHANGE_NAME environment variable is required")
	}

	notificationTopic := os.Getenv("NOTIFICATION_TOPIC_NAME")
	if notificationTopic == "" {
		return q3_amount_filter.Q3AmountFilterConfig{}, errors.New("NOTIFICATION_TOPIC_NAME environment variable is required")
	}

	transactionsSaverAmount, err := strconv.Atoi(os.Getenv("TRANSACTIONS_SAVER_AMOUNT"))
	if err != nil {
		return q3_amount_filter.Q3AmountFilterConfig{}, errors.New("TRANSACTIONS_SAVER_AMOUNT environment variable is required and must be a number")
	}

	controlExchange := os.Getenv("CONTROL_EXCHANGE_NAME")
	if controlExchange == "" {
		return q3_amount_filter.Q3AmountFilterConfig{}, errors.New("CONTROL_EXCHANGE_NAME environment variable is required")
	}

	controlTopic := os.Getenv("CONTROL_TOPIC_NAME")
	if controlTopic == "" {
		return q3_amount_filter.Q3AmountFilterConfig{}, errors.New("CONTROL_TOPIC_NAME environment variable is required")
	}

	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		return q3_amount_filter.Q3AmountFilterConfig{}, errors.New("WORKER_ID environment variable is required")
	}

	return q3_amount_filter.Q3AmountFilterConfig{
		Id:                       id,
		WorkerID:                 workerID,
		MomHost:                  momHost,
		MomPort:                  momPort,
		InputPromediatorExchange: inputPromediatorExchange,
		InputPromediatorTopic:    inputPromediatorTopic,
		InputQueue:               inputQueue,
		OutputQueue:              outputQueue,
		PromediatorAmount:        promediatorAmount,
		NotificationExchange:     notificationExchange,
		NotificationTopic:        notificationTopic,
		ControlExchange:          controlExchange,
		ControlTopic:             controlTopic,
		TransactionsSaverAmount:  transactionsSaverAmount,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := q3_amount_filter.NewQ3AmountFilter(config)
	if err != nil {
		slog.Error("While initializing q5 date filter", "err", err)
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
