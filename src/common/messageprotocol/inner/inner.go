package inner

import (
	"encoding/json"
	"fmt"
	"time"

	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

type MessageClient struct {
	ClientID int64         `json:"client_id"`
	Data     []interface{} `json:"data"`
}

type querySerializer func(transaction.QueryResult) ([]interface{}, error)
type queryDeserializer func([]interface{}) (interface{}, error)

var querySerializers = map[transaction.QueryID]querySerializer{
	transaction.Query1: serializeQuery1,
	transaction.Query3: serializeQuery3,
}
var queryDeserializers = map[transaction.QueryID]queryDeserializer{
	transaction.Query1: deserializeQuery1,
	// transaction.Query2: deserializeQuery2,
	transaction.Query3: deserializeQuery3,
}

func serializeJson(messageClient MessageClient) ([]byte, error) {
	return json.Marshal(messageClient)
}

func deserializeJson(message []byte) ([]interface{}, error) {
	var data []interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func SerializeQueryResultMessage(clientId int64, queryResult transaction.QueryResult) (*middleware.Message, error) {
	serializer, ok := querySerializers[queryResult.QueryID]
	if !ok {
		return nil, fmt.Errorf("type of query not supported for serialize: %d", queryResult.QueryID)
	}

	serializedTransactions, err := serializer(queryResult)
	if err != nil {
		return nil, err
	}

	data := []interface{}{
		int(queryResult.QueryID),
		serializedTransactions,
	}

	body, err := serializeJson(MessageClient{ClientID: clientId, Data: data})
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}
	return &message, nil
}

func SerializeMessage(clientId int64, transactionBatch []transaction.Transaction) (*middleware.Message, error) {
	data := []interface{}{}
	for _, tx := range transactionBatch {
		formattedTimestamp := tx.Timestamp.Format("2006/01/02 15:04")
		datum := []interface{}{
			formattedTimestamp,
			tx.FromBank,
			tx.ToBank,
			tx.FromAccount,
			tx.ToAccount,
			tx.Amount,
			tx.Currency,
			tx.PaymentFormat,
		}
		data = append(data, datum)
	}

	body, err := serializeJson(MessageClient{ClientID: clientId, Data: data})
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeRawTransactionsMessage(message *middleware.Message) (int64, []transaction.Transaction, bool, error) {
	var messageClient MessageClient
	if err := json.Unmarshal([]byte(message.Body), &messageClient); err != nil {
		return 0, nil, false, fmt.Errorf("deserializing message body: %w", err)
	}

	transactions := make([]transaction.Transaction, 0, len(messageClient.Data))

	for i, datum := range messageClient.Data {
		tx, err := sliceToTransaction(datum, i)
		if err != nil {
			return 0, nil, false, err
		}
		transactions = append(transactions, tx)
	}

	return messageClient.ClientID, transactions, len(transactions) == 0, nil
}

func DeserializeQueryResultMessage(message *middleware.Message) (int64, *transaction.QueryResult, bool, error) {
	var messageClient MessageClient
	if err := json.Unmarshal([]byte(message.Body), &messageClient); err != nil {
		return 0, nil, false, fmt.Errorf("deserializing results query message body: %w", err)
	}

	queryIDFloat, ok := messageClient.Data[0].(float64)
	if !ok {
		return 0, nil, false, fmt.Errorf("deserializing invalid query id")
	}
	queryID := transaction.QueryID(queryIDFloat)

	transactionRecords, ok := messageClient.Data[1].([]interface{})
	if !ok {
		return 0, nil, false, fmt.Errorf("deserializing invalid transactions payload")
	}

	deserializer, ok := queryDeserializers[queryID]
	if !ok {
		return 0, nil, false, fmt.Errorf("unsupported query id for deserialization: %d", queryID)
	}

	finalTransactions, err := deserializer(transactionRecords)
	if err != nil {
		return 0, nil, false, err
	}
	return messageClient.ClientID, &transaction.QueryResult{
		QueryID:      queryID,
		Transactions: finalTransactions,
	}, len(transactionRecords) == 0, nil
}

func SerializePaymentFormatAverageMessage(clientId int64, paymentFormatAverages []transaction.PaymentFormatAverage) (*middleware.Message, error) {
	serializedRecords := []interface{}{}

	for _, rec := range paymentFormatAverages {
		datum := []interface{}{
			rec.PaymentFormat,
			rec.Average,
			rec.Count,
		}
		serializedRecords = append(serializedRecords, datum)
	}

	body, err := serializeJson(MessageClient{ClientID: clientId, Data: serializedRecords})
	if err != nil {
		return nil, fmt.Errorf("serializing payment format averages: %w", err)
	}

	return &middleware.Message{Body: string(body)}, nil
}

func DeserializePaymentFormatAverageMessage(message *middleware.Message) (int64, []transaction.PaymentFormatAverage, bool, error) {
	var messageClient MessageClient
	if err := json.Unmarshal([]byte(message.Body), &messageClient); err != nil {
		return 0, nil, false, fmt.Errorf("deserializing payment format average body: %w", err)
	}

	records := make([]transaction.PaymentFormatAverage, 0, len(messageClient.Data))
	for _, datum := range messageClient.Data {
		fields, ok := datum.([]interface{})
		if !ok || len(fields) != 3 {
			return 0, nil, false, fmt.Errorf("invalid structure inside payment format average record")
		}

		rec := transaction.PaymentFormatAverage{
			PaymentFormat: fields[0].(string),
			Average:       fields[1].(float64),
			Count:         int(fields[2].(float64)),
		}
		records = append(records, rec)
	}

	return messageClient.ClientID, records, len(records) == 0, nil
}

func SerializeThresholdFilteredTransferMessage(clientId int64, bankPeakTransfers []transaction.ThresholdFilteredTransfer) (*middleware.Message, error) {
	serializedRecords := []interface{}{}

	for _, rec := range bankPeakTransfers {
		datum := []interface{}{
			rec.FromBank,
			rec.FromAccount,
			rec.PaymentFormat,
			rec.Amount,
		}
		serializedRecords = append(serializedRecords, datum)
	}

	body, err := serializeJson(MessageClient{ClientID: clientId, Data: serializedRecords})
	if err != nil {
		return nil, fmt.Errorf("serializing bank peak transfer: %w", err)
	}

	return &middleware.Message{Body: string(body)}, nil
}

func DeserializeThresholdFilteredTransferMessage(message *middleware.Message) (int64, []transaction.ThresholdFilteredTransfer, bool, error) {
	var messageClient MessageClient
	if err := json.Unmarshal([]byte(message.Body), &messageClient); err != nil {
		return 0, nil, false, fmt.Errorf("deserializing threshold filtered transfer body: %w", err)
	}

	records := make([]transaction.ThresholdFilteredTransfer, 0, len(messageClient.Data))
	for _, datum := range messageClient.Data {
		fields, ok := datum.([]interface{})
		if !ok || len(fields) != 4 {
			return 0, nil, false, fmt.Errorf("invalid structure inside payment format average record")
		}

		rec := transaction.ThresholdFilteredTransfer{
			FromBank:      int(fields[0].(float64)),
			FromAccount:   fields[1].(string),
			PaymentFormat: fields[2].(string),
			Amount:        fields[3].(float64),
		}
		records = append(records, rec)
	}

	return messageClient.ClientID, records, len(records) == 0, nil
}

// sliceToTransaction decodes a single raw JSON datum into a Transaction.
func sliceToTransaction(datum interface{}, index int) (transaction.Transaction, error) {
	fields, ok := datum.([]interface{})
	if !ok {
		return transaction.Transaction{}, fmt.Errorf("record %d: expected array, got %T", index, datum)
	}
	if len(fields) != 8 {
		return transaction.Transaction{}, fmt.Errorf("record %d: expected 8 fields, got %d", index, len(fields))
	}

	timestamp, ok1 := fields[0].(string)
	fromBank, ok2 := fields[1].(float64) // JSON numbers decode as float64
	toBank, ok3 := fields[2].(float64)
	fromAccount, ok4 := fields[3].(string)
	toAccount, ok5 := fields[4].(string)
	amount, ok6 := fields[5].(float64)
	currency, ok7 := fields[6].(string)
	paymentFormat, ok8 := fields[7].(string)

	if !ok1 || !ok2 || !ok3 || !ok4 || !ok5 || !ok6 || !ok7 || !ok8 {
		return transaction.Transaction{}, fmt.Errorf("record %d: type mismatch in one or more fields", index)
	}

	parsedTime, err := time.Parse("2006/01/02 15:04", timestamp)
	if err != nil {
		return transaction.Transaction{}, fmt.Errorf("record %d: invalid timestamp %q: %w", index, timestamp, err)
	}

	return transaction.Transaction{
		Timestamp:     parsedTime,
		FromBank:      int(fromBank),
		ToBank:        int(toBank),
		FromAccount:   fromAccount,
		ToAccount:     toAccount,
		Amount:        amount,
		Currency:      currency,
		PaymentFormat: paymentFormat,
	}, nil
}

// --- Serializer/Deserializer  ---

func serializeQuery1(qr transaction.QueryResult) ([]interface{}, error) {
	serialized := []interface{}{}
	records, ok := qr.Transactions.([]transaction.LowAmountTransfer)
	if !ok {
		return nil, fmt.Errorf("serializeQuery1: unexpected transactions type: %T", qr.Transactions)
	}
	for _, r := range records {
		datum := []interface{}{r.FromBank, r.FromAccount, r.ToBank, r.ToAccount, r.Amount}
		serialized = append(serialized, datum)
	}
	return serialized, nil
}

func deserializeQuery1(records []interface{}) (interface{}, error) {
	transactions := make([]transaction.LowAmountTransfer, 0, len(records))
	for _, datum := range records {
		fields, ok := datum.([]interface{})
		if !ok || len(fields) != 5 {
			return nil, fmt.Errorf("deserializing invalid structure for results query 1")
		}
		tx := transaction.LowAmountTransfer{
			FromBank:    int(fields[0].(float64)),
			FromAccount: fields[1].(string),
			ToBank:      int(fields[2].(float64)),
			ToAccount:   fields[3].(string),
			Amount:      fields[4].(float64),
		}
		transactions = append(transactions, tx)
	}
	return transactions, nil
}

func serializeQuery3(qr transaction.QueryResult) ([]interface{}, error) {
	serialized := []interface{}{}
	records, ok := qr.Transactions.([]transaction.ThresholdFilteredTransfer)
	if !ok {
		return nil, fmt.Errorf("serializeQuery3: unexpected transactions type: %T", qr.Transactions)
	}
	for _, r := range records {
		datum := []interface{}{r.FromBank, r.FromAccount, r.PaymentFormat, r.Amount}
		serialized = append(serialized, datum)
	}
	return serialized, nil
}

func deserializeQuery3(records []interface{}) (interface{}, error) {
	transactions := make([]transaction.ThresholdFilteredTransfer, 0, len(records))
	for _, datum := range records {
		fields, ok := datum.([]interface{})
		if !ok || len(fields) != 4 {
			return nil, fmt.Errorf("deserializing invalid structure for results query 3")
		}
		tx := transaction.ThresholdFilteredTransfer{
			FromBank:      int(fields[0].(float64)),
			FromAccount:   fields[1].(string),
			PaymentFormat: fields[2].(string),
			Amount:        fields[3].(float64),
		}
		transactions = append(transactions, tx)
	}
	return transactions, nil
}
