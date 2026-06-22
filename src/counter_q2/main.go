package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	counter_q2 "tp_distribuidos/counter_q2"
)

// para levantar al caido
// docker run -e RESTORE_ON_START=true <container_name>

func loadConfig() (counter_q2.CounterQ2Config, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return counter_q2.CounterQ2Config{}, errors.New("ID environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return counter_q2.CounterQ2Config{}, errors.New("MOM_HOST environment variable is required")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return counter_q2.CounterQ2Config{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return counter_q2.CounterQ2Config{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	inputExchange := os.Getenv("INPUT_EXCHANGE_NAME")
	if inputExchange == "" {
		return counter_q2.CounterQ2Config{}, errors.New("INPUT_EXCHANGE_NAME environment variable is required")
	}

	inputTopic := os.Getenv("INPUT_TOPIC")
	if inputTopic == "" {
		return counter_q2.CounterQ2Config{}, errors.New("INPUT_TOPIC environment variable is required")
	}

	outputPrefix := os.Getenv("OUTPUT_PREFIX")
	if outputPrefix == "" {
		return counter_q2.CounterQ2Config{}, errors.New("OUTPUT_PREFIX environment variable is required")
	}

	joinAmount, err := strconv.Atoi(os.Getenv("JOIN_AMOUNT"))
	if err != nil {
		return counter_q2.CounterQ2Config{}, errors.New("JOIN_AMOUNT environment variable is required and must be a number")
	}

	counterAmount, err := strconv.Atoi(os.Getenv("COUNTER_AMOUNT"))
	if err != nil {
		return counter_q2.CounterQ2Config{}, errors.New("COUNTER_AMOUNT environment variable is required and must be a number")
	}

	controlExchange := os.Getenv("COUNTER_Q2_CONTROL")
	if controlExchange == "" {
		return counter_q2.CounterQ2Config{}, errors.New("COUNTER_Q2_CONTROL environment variable is required")
	}

	batchSize := 100
	if bs := os.Getenv("BATCH_SIZE"); bs != "" {
		if v, err := strconv.Atoi(bs); err == nil {
			batchSize = v
		}
	}

	usdFilterAmount, err := strconv.Atoi(os.Getenv("USD_FILTER_AMOUNT"))
	if err != nil {
		return counter_q2.CounterQ2Config{}, errors.New("USD_FILTER_AMOUNT environment variable is required and must be a number")
	}
	return counter_q2.CounterQ2Config{
		ID:              id,
		MomHost:         momHost,
		MomPort:         momPort,
		InputQueue:      inputQueue,
		InputExchange:   inputExchange,
		InputTopic:      inputTopic,
		OutputPrefix:    outputPrefix,
		JoinAmount:      joinAmount,
		CounterAmount:   counterAmount,
		ControlExchange: controlExchange,
		BatchSize:       batchSize,
		USDFilterAmount: usdFilterAmount,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	worker, err := counter_q2.NewCounterQ2(config)
	if err != nil {
		slog.Error("While initializing counter_q2", "err", err)
		return 1
	}

	// agregado para la restauracion
	slog.Info("leyendo flag restaurate", "value", os.Getenv("RESTAURATE"))
	if os.Getenv("RESTAURATE") == "TRUE" {
		slog.Info("restaurando")
		if err := worker.Restaurate(); err != nil {
			slog.Error("While restoring state", "err", err)
			return 1
		}
		slog.Info("restaurado todo piola")
	}
	// -----------------------------

	worker.Run()
	return 0
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))
	os.Exit(run())
}
