package currencies_cache

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

// getOutputIndex shards by payment_format using the same MD5-based routing as filter_payment_format.
func getOutputIndex(paymentFormat string, counterAmount int) int {
	sum := md5.Sum([]byte(paymentFormat))
	val := binary.BigEndian.Uint64(sum[:8])
	return int(val % uint64(counterAmount))
}

// frankfurterAPIRate represents one exchange rate entry from the Frankfurter API.
type frankfurterAPIRate struct {
	Date  string  `json:"date"`
	Base  string  `json:"base"`
	Quote string  `json:"quote"`
	Rate  float64 `json:"rate"`
}

type CurrenciesCacheConfig struct {
	MomHost            string
	MomPort            int
	InputQueue         string
	InputExchangeName  string
	InputTopic         string
	OutputPrefix       string // queue name prefix; actual queues: {OutputPrefix}_0, _1, ...
	CounterAmount      int    // number of counter_q5 instances (= number of output queues)
	ExchangeRateAPIURL string
	CurrencyNameToCode map[string]string             // maps full currency names to ISO 4217 codes
	FallbackRates      map[string]map[string]float64 // date -> ISO code -> rate (hardcoded Bitcoin rates)
}

// CurrenciesCache converts PaymentRecord amounts to USD using live exchange rates,
// then forwards the converted records sharded by payment_format to the correct counter_q5 queue.
type CurrenciesCache struct {
	config             CurrenciesCacheConfig
	inputQueue         middleware.Middleware
	outputQueues       []middleware.Middleware // one per counter_q5 instance
	currencyNameToCode map[string]string       // full currency name -> ISO code
	// Hardcoded Bitcoin rates: date (YYYY-MM-DD) -> rate (BTC per 1 USD)
	// Read-only after initialization, no mutex needed
	bitcoinRates map[string]float64
	// API rates cache: date (YYYY-MM-DD) -> ISO code -> rate (units of that currency per 1 USD)
	// Modified concurrently, requires mutex protection
	apiRatesByDate map[string]map[string]float64
	apiMutex       sync.RWMutex
}

// fetchRatesForDate fetches exchange rates from Frankfurter API for a specific date.
// API URL format: https://api.frankfurter.dev/v2/rates?base=USD&date=YYYY-MM-DD
// Returns a map of ISO code -> rate.
func fetchRatesForDate(apiURLPrefix string, date string) (map[string]float64, error) {
	url := fmt.Sprintf("%s?base=USD&date=%s", apiURLPrefix, date)
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("HTTP GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	var apiResp []frankfurterAPIRate
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, fmt.Errorf("parsing exchange rate response: %w", err)
	}

	// Convert array to map[ISO code -> rate]
	rates := make(map[string]float64, len(apiResp))
	for _, entry := range apiResp {
		rates[entry.Quote] = entry.Rate
	}
	return rates, nil
}

func NewCurrenciesCache(config CurrenciesCacheConfig) (*CurrenciesCache, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	// Shared input queue — supports competing consumers
	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating input queue: %w", err)
	}
	if config.InputExchangeName != "" {
		if err = inputQueue.BindToTopics(config.InputExchangeName, config.InputTopic); err != nil {
			inputQueue.Close()
			return nil, fmt.Errorf("binding input queue: %w", err)
		}
	}

	// One output queue per counter_q5 instance: {OutputPrefix}_0, _1, ...
	outputQueues := make([]middleware.Middleware, config.CounterAmount)
	for i := 0; i < config.CounterAmount; i++ {
		qName := fmt.Sprintf("%s_%d", config.OutputPrefix, i)
		q, err := middleware.CreateQueueMiddleware(qName, connSettings)
		if err != nil {
			inputQueue.Close()
			for j := 0; j < i; j++ {
				outputQueues[j].Close()
			}
			return nil, fmt.Errorf("creating output queue %d (%s): %w", i, qName, err)
		}
		outputQueues[i] = q
	}

	// Initialize Bitcoin rates from hardcoded data (bitcoin_usd_rates.json)
	bitcoinRates := make(map[string]float64)
	for date, rates := range config.FallbackRates {
		bitcoinRates[date] = rates["BTC"]
	}

	return &CurrenciesCache{
		config:             config,
		inputQueue:         inputQueue,
		outputQueues:       outputQueues,
		currencyNameToCode: config.CurrencyNameToCode,
		bitcoinRates:       bitcoinRates,
		apiRatesByDate:     make(map[string]map[string]float64),
	}, nil
}

// Run starts consuming. Returns once processing is done or SIGTERM is received.
func (currencyCache *CurrenciesCache) Run() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	go func() {
		<-sigCh
		slog.Info("SIGTERM received, stopping consumer")
		currencyCache.inputQueue.StopConsuming()
	}()

	currencyCache.inputQueue.StartConsuming(currencyCache.handleMessage)
	currencyCache.close()
}

func (currencyCache *CurrenciesCache) handleMessage(msg middleware.Message, ack, nack func()) {
	clientID, records, isEof, err := inner.DeserializePaymentRecordMessage(&msg)
	if err != nil {
		slog.Error("Deserializing payment record message", "err", err)
		nack()
		return
	}

	if isEof {
		slog.Info("EOF received, forwarding", "client_id", clientID)
		if err := currencyCache.forwardEOF(clientID); err != nil {
			slog.Error("Forwarding EOF", "err", err, "client_id", clientID)
			nack()
			return
		}
		ack()
		return
	}

	converted := currencyCache.convertBatch(clientID, records)
	if err := currencyCache.sendConvertedRecords(clientID, converted); err != nil {
		slog.Error("Sending converted records", "err", err, "client_id", clientID)
		nack()
		return
	}
	ack()
}

// sendConvertedRecords shards converted records by payment_format and sends each shard
// to the appropriate counter_q5 output queue.
func (currencyCache *CurrenciesCache) sendConvertedRecords(clientID int64, converted []transaction.PaymentRecord) error {
	// Group converted records by target counter shard
	shards := make(map[int][]transaction.PaymentRecord, currencyCache.config.CounterAmount)
	for _, r := range converted {
		idx := getOutputIndex(r.PaymentFormat, currencyCache.config.CounterAmount)
		shards[idx] = append(shards[idx], r)
	}

	// Send each shard to its corresponding counter
	for idx, batch := range shards {
		outMsg, err := inner.SerializePaymentRecordMessage(clientID, batch)
		if err != nil {
			return fmt.Errorf("serializing batch for shard %d: %w", idx, err)
		}
		if err := currencyCache.outputQueues[idx].Send(*outMsg); err != nil {
			return fmt.Errorf("sending to output queue %d: %w", idx, err)
		}
	}
	return nil
}

// convertBatch converts every record in the batch to USD.
// Records with unknown currencies are skipped with a warning.
func (currencyCache *CurrenciesCache) convertBatch(clientID int64, records []transaction.PaymentRecord) []transaction.PaymentRecord {
	result := make([]transaction.PaymentRecord, 0, len(records))
	for _, r := range records {
		amountUSD, err := currencyCache.toUSD(r.Currency, r.Amount, r.Timestamp)
		if err != nil {
			slog.Warn("Skipping record with unknown currency",
				"client_id", clientID, "currency", r.Currency, "err", err)
			continue
		}
		result = append(result, transaction.PaymentRecord{
			Timestamp:     r.Timestamp,
			Amount:        amountUSD,
			Currency:      "US Dollar",
			PaymentFormat: r.PaymentFormat,
		})
	}
	return result
}

// getBitcoinRate returns the hardcoded Bitcoin exchange rate for a specific date.
// Returns BTC per 1 USD from the fallback data.
func (currencyCache *CurrenciesCache) getBitcoinRate(date string) (float64, error) {
	rate, exists := currencyCache.bitcoinRates[date]
	if !exists || rate == 0 {
		return 0, fmt.Errorf("no hardcoded Bitcoin rate for %s", date)
	}
	return rate, nil
}

// getRatesForDate returns exchange rates from API for a specific date.
// Only used for non-Bitcoin currencies. Bitcoin uses hardcoded rates.
func (currencyCache *CurrenciesCache) getRatesForDate(date string) (map[string]float64, error) {
	// Check API cache with read lock
	currencyCache.apiMutex.RLock()
	rates, exists := currencyCache.apiRatesByDate[date]
	currencyCache.apiMutex.RUnlock()

	if exists {
		// Already cached from API
		return rates, nil
	}

	// Cache miss — fetch from API
	slog.Info("Fetching exchange rates from API", "date", date)
	apiRates, err := fetchRatesForDate(currencyCache.config.ExchangeRateAPIURL, date)
	if err != nil {
		return nil, fmt.Errorf("fetching rates from API: %w", err)
	}

	// Store in API cache
	currencyCache.apiMutex.Lock()
	currencyCache.apiRatesByDate[date] = apiRates
	currencyCache.apiMutex.Unlock()

	slog.Info("Exchange rates cached from API", "date", date)
	return apiRates, nil
}

// getAPICurrencyRate fetches the exchange rate for a specific currency from the API.
// Returns units of that currency per 1 USD.
func (currencyCache *CurrenciesCache) getAPICurrencyRate(code, currencyName, date string) (float64, error) {
	rates, err := currencyCache.getRatesForDate(date)
	if err != nil {
		return 0, fmt.Errorf("getting rates for %s: %w", date, err)
	}

	rate, ok := rates[code]
	if !ok || rate == 0 {
		return 0, fmt.Errorf("no rate for %q (%s) on %s", currencyName, code, date)
	}
	return rate, nil
}

// toUSD converts amount from a full currency name to USD using rates for the transaction's date.
// Bitcoin uses hardcoded rates from bitcoin_usd_rates.json (never from API).
// Other currencies use API rates (fetched on demand and cached).
// rates[code] = units of that currency per 1 USD → amountUSD = amount / rate
func (currencyCache *CurrenciesCache) toUSD(currencyName string, amount float64, timestamp time.Time) (float64, error) {
	if currencyName == "US Dollar" {
		return amount, nil
	}

	code, ok := currencyCache.currencyNameToCode[currencyName]
	if !ok {
		return 0, fmt.Errorf("unknown currency name %q", currencyName)
	}

	dateKey := timestamp.Format("2006-01-02")

	var rate float64
	var err error

	if code == "BTC" {
		rate, err = currencyCache.getBitcoinRate(dateKey)
	} else {
		rate, err = currencyCache.getAPICurrencyRate(code, currencyName, dateKey)
	}

	if err != nil {
		return 0, err
	}

	return amount / rate, nil
}

func (currencyCache *CurrenciesCache) forwardEOF(clientID int64) error {
	msg, err := inner.SerializePaymentRecordMessage(clientID, []transaction.PaymentRecord{})
	if err != nil {
		return err
	}
	for i, q := range currencyCache.outputQueues {
		if err := q.Send(*msg); err != nil {
			return fmt.Errorf("sending EOF to output queue %d: %w", i, err)
		}
	}
	return nil
}

func (currencyCache *CurrenciesCache) close() {
	currencyCache.inputQueue.Close()
	for _, q := range currencyCache.outputQueues {
		q.Close()
	}
}
