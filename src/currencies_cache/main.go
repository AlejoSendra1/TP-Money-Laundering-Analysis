package main

import (
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strconv"

	currencies_cache "tp_distribuidos/currencies_cache"
)

func loadCurrencyCodes(filePath string) (map[string]string, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	var codes map[string]string
	if err := json.Unmarshal(data, &codes); err != nil {
		return nil, err
	}
	return codes, nil
}

func loadFallbackRates(filePath string) (map[string]map[string]float64, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	var rates map[string]map[string]float64
	if err := json.Unmarshal(data, &rates); err != nil {
		return nil, err
	}
	return rates, nil
}

func loadConfig() (currencies_cache.CurrenciesCacheConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return currencies_cache.CurrenciesCacheConfig{}, errors.New("ID is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return currencies_cache.CurrenciesCacheConfig{}, errors.New("MOM_HOST is required")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return currencies_cache.CurrenciesCacheConfig{}, errors.New("MOM_PORT is required and must be a number")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return currencies_cache.CurrenciesCacheConfig{}, errors.New("INPUT_QUEUE is required")
	}

	outputPrefix := os.Getenv("OUTPUT_PREFIX")
	if outputPrefix == "" {
		return currencies_cache.CurrenciesCacheConfig{}, errors.New("OUTPUT_PREFIX is required")
	}

	counterAmount, err := strconv.Atoi(os.Getenv("COUNTER_AMOUNT"))
	if err != nil {
		return currencies_cache.CurrenciesCacheConfig{}, errors.New("COUNTER_AMOUNT is required and must be a number")
	}

	filterAmount, err := strconv.Atoi(os.Getenv("FILTER_AMOUNT"))
	if err != nil {
		return currencies_cache.CurrenciesCacheConfig{}, errors.New("FILTER_AMOUNT is required and must be a number")
	}

	instanceAmount, err := strconv.Atoi(os.Getenv("INSTANCE_AMOUNT"))
	if err != nil {
		return currencies_cache.CurrenciesCacheConfig{}, errors.New("INSTANCE_AMOUNT is required and must be a number")
	}

	controlExchangeName := os.Getenv("CONTROL_EXCHANGE_NAME")
	if controlExchangeName == "" {
		return currencies_cache.CurrenciesCacheConfig{}, errors.New("CONTROL_EXCHANGE_NAME is required")
	}

	apiURL := os.Getenv("EXCHANGE_RATE_API_URL")
	if apiURL == "" {
		apiURL = "https://api.frankfurter.dev/v2/rates"
	}

	currencyCodesFile := os.Getenv("CURRENCY_CODES_FILE")
	if currencyCodesFile == "" {
		currencyCodesFile = "currency_codes.json"
	}

	currencyNameToCode, err := loadCurrencyCodes(currencyCodesFile)
	if err != nil {
		return currencies_cache.CurrenciesCacheConfig{}, errors.New("loading currency codes file: " + err.Error())
	}

	// Load Bitcoin USD exchange rates
	fallbackRatesFile := os.Getenv("FALLBACK_RATES_FILE")
	if fallbackRatesFile == "" {
		fallbackRatesFile = "bitcoin_usd_rates.json"
	}

	fallbackRates, err := loadFallbackRates(fallbackRatesFile)
	if err != nil {
		return currencies_cache.CurrenciesCacheConfig{}, errors.New("loading fallback rates file: " + err.Error())
	}

	// Optional: exchange binding for the input queue
	inputExchange := os.Getenv("INPUT_EXCHANGE_NAME")
	inputTopic := os.Getenv("INPUT_TOPIC")

	return currencies_cache.CurrenciesCacheConfig{
		ID:                  id,
		MomHost:             momHost,
		MomPort:             momPort,
		InputQueue:          inputQueue,
		InputExchangeName:   inputExchange,
		InputTopic:          inputTopic,
		OutputPrefix:        outputPrefix,
		CounterAmount:       counterAmount,
		FilterAmount:        filterAmount,
		InstanceAmount:      instanceAmount,
		ControlExchangeName: controlExchangeName,
		ExchangeRateAPIURL:  apiURL,
		CurrencyNameToCode:  currencyNameToCode,
		FallbackRates:       fallbackRates,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("Loading config", "err", err)
		return 1
	}

	worker, err := currencies_cache.NewCurrenciesCache(config)
	if err != nil {
		slog.Error("Initializing currencies_cache", "err", err)
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
