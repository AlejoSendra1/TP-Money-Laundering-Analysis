package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
	"tp_distribuidos/client"
)

func loadConfig() (client.ClientConfig, error) {
	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return client.ClientConfig{}, errors.New("SERVER_HOST environment variable is required")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return client.ClientConfig{}, errors.New("SERVER_PORT environment variable is required")
	}

	inputFile := os.Getenv("INPUT_FILE")
	if inputFile == "" {
		return client.ClientConfig{}, errors.New("INPUT_FILE environment variable is required")
	}

	outputFile := os.Getenv("OUTPUT_FILE")
	if outputFile == "" {
		return client.ClientConfig{}, errors.New("OUTPUT_FILE environment variable is required")
	}

	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return client.ClientConfig{}, errors.New("ID environment variable is required and must be a number")
	}

	var restorate bool
	restorateStr := os.Getenv("RESTAURATE")
	slog.Info("valor obtenido en restaurate", "val", restorateStr)
	if restorateStr == "TRUE" {
		restorate = true
	} else {
		restorate = false
	}

	return client.ClientConfig{
		ServerHost: serverHost,
		ServerPort: serverPort,
		InputFile:  inputFile,
		OutputFile: outputFile,
		Restorate:  restorate,
		ID:         id,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := client.NewClient(config)
	if err != nil {
		slog.Error("While connecting to server", "err", err)
		return 1
	}

	if err := server.Run(); err != nil {
		slog.Error("Client stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
