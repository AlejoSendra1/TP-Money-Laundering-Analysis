package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	"tp_distribuidos/common/transaction"
	join_q2 "tp_distribuidos/join_q2"
)

func loadConfig() (join_q2.JoinQ2Config, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return join_q2.JoinQ2Config{}, errors.New("ID environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return join_q2.JoinQ2Config{}, errors.New("MOM_HOST environment variable is required")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return join_q2.JoinQ2Config{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	inputPrefix := os.Getenv("INPUT_PREFIX")
	if inputPrefix == "" {
		return join_q2.JoinQ2Config{}, errors.New("INPUT_PREFIX environment variable is required")
	}

	counterAmount, err := strconv.Atoi(os.Getenv("COUNTER_AMOUNT"))
	if err != nil {
		return join_q2.JoinQ2Config{}, errors.New("COUNTER_AMOUNT environment variable is required and must be a number")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return join_q2.JoinQ2Config{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	queryIDInt, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return join_q2.JoinQ2Config{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	batchSize := 100
	if bs := os.Getenv("BATCH_SIZE"); bs != "" {
		if v, err := strconv.Atoi(bs); err == nil {
			batchSize = v
		}
	}

	return join_q2.JoinQ2Config{
		ID:            id,
		MomHost:       momHost,
		MomPort:       momPort,
		InputPrefix:   inputPrefix,
		CounterAmount: counterAmount,
		OutputQueue:   outputQueue,
		QueryID:       transaction.QueryID(queryIDInt),
		BatchSize:     batchSize,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	worker, err := join_q2.NewJoinQ2(config)
	if err != nil {
		slog.Error("While initializing join_q2", "err", err)
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
