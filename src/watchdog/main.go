package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"

	watchdogworker "tp_distribuidos/watchdog"
)

func loadConfig() (watchdogworker.WatchdogConfig, error) {
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return watchdogworker.WatchdogConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	momPort := 5672
	if p := os.Getenv("MOM_PORT"); p != "" {
		if _, err := fmt.Sscanf(p, "%d", &momPort); err != nil {
			return watchdogworker.WatchdogConfig{}, errors.New("MOM_PORT must be a number")
		}
	}

	id := 0
	if v := os.Getenv("ID"); v != "" {
		if _, err := fmt.Sscanf(v, "%d", &id); err != nil {
			return watchdogworker.WatchdogConfig{}, errors.New("ID must be a number")
		}
	}


	electionExchange := os.Getenv("ELECTION_EXCHANGE")
	if electionExchange == "" {
		return watchdogworker.WatchdogConfig{}, errors.New("ELECTION_EXCHANGE environment variable is required")
	}

	return watchdogworker.WatchdogConfig{
		ID:      id,
		MomHost: momHost,
		MomPort: momPort,
		ID:               id,
		MomHost:          momHost,
		MomPort:          momPort,
		ElectionExchange: electionExchange,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("loading config", "err", err)
		return 1
	}

	w, err := watchdogworker.NewWatchdog(config)
	if err != nil {
		slog.Error("initializing heatbeat", "err", err)
		return 1
	}

	w.Run()
	return 0
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))
	os.Exit(run())
}
