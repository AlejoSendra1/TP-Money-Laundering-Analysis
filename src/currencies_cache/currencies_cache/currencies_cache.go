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

	"tp_distribuidos/common/batch_utils"
	"tp_distribuidos/common/datasaver"
	"tp_distribuidos/common/heatbeat"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
	"tp_distribuidos/common/worker"
)

const LogsUntilCheckpoint = 250

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
	ID                  int
	WorkerID            string
	MomHost             string
	MomPort             int
	InputQueue          string
	InputExchangeName   string
	InputTopic          string
	OutputPrefix        string
	CounterAmount       int
	FilterAmount        int
	ControlExchangeName string
	ControlTopic        string
	ExchangeRateAPIURL  string
	CurrencyNameToCode  map[string]string
	FallbackRates       map[string]map[string]float64
}

type CheckpointData struct {
	EofCountByClient map[int64]batch_utils.Set[string] `json:"eofCountByClient"`
	FinishedClients  batch_utils.Set[int64]            `json:"finishedClients"`
}

// CurrenciesCache converts PaymentRecord amounts to USD using live exchange rates,
// then forwards the converted records sharded by payment_format to the correct counter_q5 queue.
type CurrenciesCache struct {
	config             CurrenciesCacheConfig
	inputQueue         middleware.Middleware
	outputQueues       []middleware.Middleware
	controlExchange    middleware.Middleware
	currencyNameToCode map[string]string
	bitcoinRates       map[string]float64
	apiRatesByDate     map[string]map[string]float64
	apiMutex           sync.RWMutex
	mu                 sync.Mutex
	eofCountByClient   map[int64]batch_utils.Set[string]
	finishedClients    batch_utils.Set[int64]
	heartbeat          *heatbeat.HeartbeatSender
	dataSaver          *datasaver.DataSaver
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

	// Exchange broadcast: igual al patron de date_filter / filter_payment_format.
	// Todos los instances subscriben al mismo topic, incluyendo el que envia.
	myControlQueue := fmt.Sprintf("%s_%d", config.ControlExchangeName, config.ID)
	controlExchange, err := middleware.CreateExchangeMiddleware(
		config.ControlExchangeName,
		[]string{config.ControlTopic},
		connSettings,
		myControlQueue,
	)
	if err != nil {
		inputQueue.Close()
		for _, q := range outputQueues {
			q.Close()
		}
		return nil, fmt.Errorf("creating control exchange: %w", err)
	}

	hb, err := heatbeat.NewHeartbeatSender(config.WorkerID, connSettings)
	if err != nil {
		inputQueue.Close()
		for _, q := range outputQueues {
			q.Close()
		}
		controlExchange.Close()
		return nil, fmt.Errorf("creating heartbeat sender: %w", err)
	}

	dataSaver, err := datasaver.NewDataSaver(fmt.Sprintf("/persistence/currencies_cache_%d", config.ID), LogsUntilCheckpoint)
	if err != nil {
		inputQueue.Close()
		for _, q := range outputQueues {
			q.Close()
		}
		controlExchange.Close()
		return nil, fmt.Errorf("creating data saver: %w", err)
	}

	bitcoinRates := make(map[string]float64)
	for date, rates := range config.FallbackRates {
		bitcoinRates[date] = rates["BTC"]
	}

	return &CurrenciesCache{
		config:             config,
		inputQueue:         inputQueue,
		outputQueues:       outputQueues,
		controlExchange:    controlExchange,
		currencyNameToCode: config.CurrencyNameToCode,
		bitcoinRates:       bitcoinRates,
		apiRatesByDate:     make(map[string]map[string]float64),
		eofCountByClient:   make(map[int64]batch_utils.Set[string]),
		finishedClients:    make(batch_utils.Set[int64]),
		heartbeat:          hb,
		dataSaver:          dataSaver,
	}, nil
}

func (currencyCache *CurrenciesCache) GetCheckpointData() any {
	return CheckpointData{
		EofCountByClient: currencyCache.eofCountByClient,
		FinishedClients:  currencyCache.finishedClients,
	}
}

func (currencyCache *CurrenciesCache) Restaurate() error {
	var checkpoint CheckpointData
	thereIsCheckpoint, err := currencyCache.dataSaver.GetRestaurationCheckpoint(&checkpoint)
	if err != nil {
		return err
	}
	if thereIsCheckpoint {
		slog.Info("Restaurating currencies_cache from checkpoint")
		currencyCache.eofCountByClient = checkpoint.EofCountByClient
		currencyCache.finishedClients = checkpoint.FinishedClients
		slog.Info("Currencies cache restored",
			"eofCountByClient", currencyCache.eofCountByClient,
			"finishedClients", currencyCache.finishedClients,
		)
	}

	var savedMsg middleware.Message
	for {
		hasLogs, err := currencyCache.dataSaver.GetDataFromLogs(&savedMsg)
		if err != nil {
			return err
		}
		if !hasLogs {
			break
		}
		if err := worker.HandleMessageV2(&savedMsg, worker.MessageHandlerMap{
			inner.EndOfRecords: currencyCache.handleEOFLogic,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (currencyCache *CurrenciesCache) Run() {
	go currencyCache.handleSigterm()
	currencyCache.heartbeat.Start()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		currencyCache.controlExchange.StartConsuming(currencyCache.handleControlMessage)
	}()

	currencyCache.inputQueue.StartConsuming(currencyCache.handleMessage)

	currencyCache.controlExchange.StopConsuming()
	wg.Wait()
	currencyCache.close()
}

func (currencyCache *CurrenciesCache) handleSigterm() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	<-sigCh
	slog.Info("SIGTERM received, stopping consumer")
	currencyCache.heartbeat.Stop()
	currencyCache.inputQueue.StopConsuming()
	currencyCache.controlExchange.StopConsuming()
}

func (currencyCache *CurrenciesCache) handleMessage(msg middleware.Message, ack, nack func()) {
	err := worker.HandleMessageV2(
		&msg,
		worker.MessageHandlerMap{
			inner.TransactionBatch: currencyCache.handleTransactionBatch,
			inner.EndOfRecords:     currencyCache.handleEOFFromUpstream,
		},
	)
	if err != nil {
		slog.Error("Handling message", "err", err)
		nack()
		return
	}
	ack()
}

func (currencyCache *CurrenciesCache) handleTransactionBatch(clientID int64, data []interface{}) error {
	records, err := inner.DeserializePaymentRecordBatch(data)
	if err != nil {
		slog.Error("Deserializing payment record batch", "err", err, "client_id", clientID)
		return err
	}
	currencyCache.mu.Lock()
	defer currencyCache.mu.Unlock()
	converted := currencyCache.convertBatch(clientID, records)
	return currencyCache.sendConvertedRecords(clientID, converted)
}

func (currencyCache *CurrenciesCache) handleEOFFromUpstream(clientID int64, data []interface{}) error {
	_, sender, err := inner.DeserializeEOR(data)
	if err != nil {
		slog.Error("Deserializing EOR", "err", err, "client_id", clientID)
		return err
	}
	slog.Info("EOF received from upstream, broadcasting to control", "client_id", clientID)
	return currencyCache.sendControlEOF(clientID, sender)
}

func (currencyCache *CurrenciesCache) sendControlEOF(clientID int64, sender string) error {
	msg, err := inner.SerializeEOR(clientID, false, sender)
	if err != nil {
		return fmt.Errorf("serializing control EOF: %w", err)
	}
	return currencyCache.controlExchange.Send(*msg)
}

func (currencyCache *CurrenciesCache) handleEOFLogic(clientID int64, data []interface{}) error {
	_, sender, err := inner.DeserializeEOR(data)
	if err != nil {
		slog.Error("While deserializing control message", "err", err, "clientID", clientID)
		return err
	}
	if currencyCache.finishedClients.Contains(clientID) {
		slog.Info("Client already finished, ignoring EOF", "client_id", clientID)
		return nil
	}
	if currencyCache.eofCountByClient[clientID] == nil {
		currencyCache.eofCountByClient[clientID] = batch_utils.NewSet[string]()
	}
	currencyCache.eofCountByClient[clientID].Add(sender)
	count := currencyCache.eofCountByClient[clientID].Size()
	slog.Info("EOF accumulated", "client_id", clientID, "count", count, "expected", currencyCache.config.FilterAmount)
	if count < currencyCache.config.FilterAmount {
		return nil
	}
	delete(currencyCache.eofCountByClient, clientID)
	currencyCache.finishedClients.Add(clientID)
	return currencyCache.forwardEOF(clientID)
}

func (currencyCache *CurrenciesCache) handleControlMessage(msg middleware.Message, ack, nack func()) {
	currencyCache.mu.Lock()
	defer currencyCache.mu.Unlock()
	err := worker.HandleMessageV2(
		&msg,
		worker.MessageHandlerMap{
			inner.EndOfRecords: currencyCache.handleEOFLogic,
		},
	)
	if err != nil {
		slog.Error("Deserializing control message", "err", err)
		nack()
		return
	}
	currencyCache.dataSaver.Save(&msg, currencyCache)
	ack()
}

func (currencyCache *CurrenciesCache) sendConvertedRecords(clientID int64, converted []transaction.PaymentRecord) error {
	shards := make(map[int][]transaction.PaymentRecord, currencyCache.config.CounterAmount)
	for _, r := range converted {
		idx := getOutputIndex(r.PaymentFormat, currencyCache.config.CounterAmount)
		shards[idx] = append(shards[idx], r)
	}
	for idx, batch := range shards {
		batch_utils.SortBatch(batch, func(a, b transaction.PaymentRecord) bool {
			if !a.Timestamp.Equal(b.Timestamp) {
				return a.Timestamp.Before(b.Timestamp)
			}
			return a.Amount > b.Amount
		})
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
	for i, q := range currencyCache.outputQueues {
		msg, err := inner.SerializeEOR(clientID, false, fmt.Sprintf("%d", currencyCache.config.ID))
		if err != nil {
			return fmt.Errorf("serializing EOF for queue %d: %w", i, err)
		}
		if err := q.Send(*msg); err != nil {
			return fmt.Errorf("sending EOF to output queue %d: %w", i, err)
		}
	}
	slog.Info("EOF forwarded to all counters", "client_id", clientID)
	return nil
}

func (currencyCache *CurrenciesCache) close() {
	currencyCache.inputQueue.Close()
	currencyCache.controlExchange.Close()
	for _, q := range currencyCache.outputQueues {
		q.Close()
	}
}
