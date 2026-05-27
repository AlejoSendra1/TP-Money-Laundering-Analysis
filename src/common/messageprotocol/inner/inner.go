package inner

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

type MsgType uint32

const (
	TransactionBatch MsgType = iota + 1
	Ack
	EndOfRecords
	Query1Response
	Query2Response
	Query3Response
	Query4Response
	Query5Response
	QueryEOR
	SuspiciousAccount
	PossibleFraudDestinations
	ReadyForEOR
)

type MessageClient struct {
	ClientID int64         `json:"client_id"`
	MsgType  MsgType       `json:"msg_type"`
	Data     []interface{} `json:"data"`
}

type querySerializer func(transaction.QueryResult) ([]interface{}, error)
type queryDeserializer func([]interface{}) (interface{}, error)

var querySerializers = map[transaction.QueryID]querySerializer{
	transaction.Query1: serializeQuery1,
	transaction.Query3: serializeQuery3,
	transaction.Query2: serializeQuery2,
	transaction.Query5: serializeQuery5,
}
var queryDeserializers = map[transaction.QueryID]queryDeserializer{
	transaction.Query1: deserializeQuery1,
	transaction.Query3: deserializeQuery3,
	transaction.Query2: deserializeQuery2,
	transaction.Query5: deserializeQuery5,
}

func SerializeJson(messageClient MessageClient) ([]byte, error) {
	return json.Marshal(messageClient)
}

func DeserializeMessage(message *middleware.Message) (*MessageClient, error) {
	var messageClient MessageClient
	if err := json.Unmarshal([]byte(message.Body), &messageClient); err != nil {
		return nil, fmt.Errorf("deserializing message body: %w", err)
	}
	return &messageClient, nil
}

func SerializeEOF(clientId int64, mustPropagate bool, sender string) (*middleware.Message, error) {
	slog.Info("serializando eof")
	data := []interface{}{}

	// agregamos el tipo de msg
	data = append(data, []interface{}{
		mustPropagate,
		sender,
	})

	body, err := SerializeJson(MessageClient{ClientID: clientId, MsgType: EndOfRecords, Data: data})
	if err != nil {
		return nil, err
	}

	message := middleware.Message{Body: string(body)}

	return &message, nil
}
func DeserializeEOR(data []interface{}) (bool, string, error) {
	info, ok := (data[0]).([]interface{})
	if !ok {
		return false, "", fmt.Errorf("Unexpected data in eor msg, received data %v", data)
	}
	// deserializacion de los datos
	mustPropagate, ok := (info[0]).(bool)
	if !ok {
		return false, "", fmt.Errorf("Unexpected data in eor msg, expected bool, received data %v", data)
	}

	sender, ok := (info[1]).(string)
	if !ok {
		return false, "", fmt.Errorf("Unexpected data in eor msg, expected sender string, received data %v", data)
	}
	return mustPropagate, sender, nil
}

func SerializeMessage(clientId int64, transactionBatch []transaction.Transaction) (*middleware.Message, error) {
	data := []interface{}{}

	for _, transaction := range transactionBatch {
		formattedTimestamp := transaction.Timestamp.Format("2006/01/02 15:04")
		datum := []interface{}{
			formattedTimestamp,
			transaction.FromBank,
			transaction.ToBank,
			transaction.FromAccount,
			transaction.ToAccount,
			transaction.Amount,
			transaction.Currency,
			transaction.PaymentFormat,
		}
		data = append(data, datum)
	}

	body, err := SerializeJson(MessageClient{ClientID: clientId, MsgType: TransactionBatch, Data: data})
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

// ELIMINAR ESTA PORQUERIA
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

func DeserializeTransactionBatch(data []interface{}) ([]transaction.Transaction, error) {
	transactions := make([]transaction.Transaction, 0, len(data))

	for i, datum := range data {
		transaction, err := sliceToTransaction(datum, i)
		if err != nil {
			return transactions, err
		}
		transactions = append(transactions, transaction)
	}

	return transactions, nil
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

	body, err := SerializeJson(MessageClient{ClientID: clientId, Data: serializedRecords})
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

	body, err := SerializeJson(MessageClient{ClientID: clientId, Data: serializedRecords})
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
		return transaction.Transaction{}, fmt.Errorf("record %d: expected 8 fields, got %d — contents: %v", index, len(fields), fields)
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

// para hacer 2 fase commit
func SerializeReadyForEOR(clientID int64, workerID int) (*middleware.Message, error) {
	slog.Info("serializando ReadyForEOR")
	data := []interface{}{}

	data = append(data, []interface{}{
		workerID,
	})

	body, err := SerializeJson(MessageClient{ClientID: clientID, MsgType: ReadyForEOR, Data: data})
	if err != nil {
		return nil, err
	}

	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeReadyForEOR(data []interface{}) (int, error) {
	info, ok := (data[0]).([]interface{})
	if !ok {
		return 0, fmt.Errorf("Unexpected data in ReadyForEOR msg, received data %v", data)
	}
	// deserializacion de los datos
	senderID, ok := (info[0]).(float64)
	if !ok {
		return 0, fmt.Errorf("Unexpected data in ReadyForEOR msg, expected sender int, received data %v", data)
	}
	return int(senderID), nil
}

// ---------------------------------------------------------
// especifico de la Query 1 ------
// ----------------------------------------------------------

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

// ---------------------------------------------------------
// especifico de la Query 1 ------
// ----------------------------------------------------------
func SerializeQuery1ResultMessage(clientId int64, queryResult []transaction.LowAmountTransfer) (*middleware.Message, error) {
	data := []interface{}{}

	for _, transaction := range queryResult {
		datum := []interface{}{
			transaction.FromBank,
			transaction.FromAccount,
			transaction.ToBank,
			transaction.ToAccount,
			transaction.Amount,
		}
		data = append(data, datum)
	}

	body, err := SerializeJson(MessageClient{ClientID: clientId, MsgType: Query1Response, Data: data})
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

// ---------------------------------------------------------
// especifico de la Query 4 dsp ver como mover de aca ------
// ----------------------------------------------------------

func SerializeSuspiciousAccountInfo(clientID int64, susAccount string, possibleBridges map[string]struct{}) (*middleware.Message, error) {
	bridges := []interface{}{}
	for key, _ := range possibleBridges {
		bridges = append(bridges, key)
	}
	data := []interface{}{
		susAccount, // plain string at data[0]
		bridges,    // slice of bridges at data[1]
	}

	body, err := SerializeJson(MessageClient{ClientID: clientID, MsgType: SuspiciousAccount, Data: data})
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeSuspiciousMsgData(data []interface{}) (string, []string, error) { // fiaca implementar un type solo para la q4
	// la data deberia llegar como { fromAccount_Frombank , [ toAccount_toBank , toAccount_toBank , .... ] }
	transactions := []string{}

	susAccount, ok := data[0].(string)
	if !ok {
		return "", transactions, fmt.Errorf("type mismatch on sus account origin")
	}

	// Type-assert data[1] to []interface{} before ranging
	bridges, ok := data[1].([]interface{})
	if !ok {
		return "", transactions, fmt.Errorf("type mismatch on possible bridges list")
	}

	for _, possibleBridge := range bridges {
		possibleBridgeInfo, ok := possibleBridge.(string)
		if !ok {
			return "", transactions, fmt.Errorf("type mismatch on sus account possible bridge")
		}
		transactions = append(transactions, possibleBridgeInfo)
	}

	return susAccount, transactions, nil
}

func SerializesPossibleFraudDestinations(clientID int64, origin string, possibleBridge string, possibleFraudDestinations map[string]struct{}) (*middleware.Message, error) {
	dest := []interface{}{}
	for key, _ := range possibleFraudDestinations {
		dest = append(dest, key)
	}
	data := []interface{}{
		origin,         // plain string at data[0]
		possibleBridge, // plain string at data[1]
		dest,           // slice of bridges at data[2]
	}

	body, err := SerializeJson(MessageClient{ClientID: clientID, MsgType: PossibleFraudDestinations, Data: data})
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializePossibleFraudDestinations(data []interface{}) (string, string, []string, error) { // fiaca implementar un type solo para la q4
	// la data deberia llegar como { fromAccount_Frombank , toAccount_toBank , [ toAccount_toBank2 , .... ] }
	possibleFraudDestinations := []string{}

	susAccount, ok := data[0].(string)
	if !ok {
		return "", "", possibleFraudDestinations, fmt.Errorf("type mismatch on sus account origin")
	}

	bridge, ok := data[1].(string)
	if !ok {
		return "", "", possibleFraudDestinations, fmt.Errorf("type mismatch on bridge")
	}

	// Type-assert data[2] to []interface{} before ranging
	destinations, ok := data[2].([]interface{})
	if !ok {
		return "", "", possibleFraudDestinations, fmt.Errorf("type mismatch on possible fraud destinations list")
	}

	for _, possibleFraudDestination := range destinations {
		possibleFraudDestinationInfo, ok := possibleFraudDestination.(string)
		if !ok {
			return "", "", possibleFraudDestinations, fmt.Errorf("type mismatch on sus account possible bridge")
		}
		possibleFraudDestinations = append(possibleFraudDestinations, possibleFraudDestinationInfo)
	}

	return susAccount, bridge, possibleFraudDestinations, nil
}

func SerializeQ4SourceAccount(clientID int64, sourceAccount string) (*middleware.Message, error) {
	sourceAccountData := strings.Split(sourceAccount, "_")

	data := []interface{}{
		sourceAccountData[0],
		sourceAccountData[1],
	}

	body, err := SerializeJson(MessageClient{ClientID: clientID, MsgType: Query4Response, Data: data})
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeQ4SourceAccount(data []interface{}) (string, string, error) {
	susAccount, ok := data[0].(string)
	if !ok {
		return "", "", fmt.Errorf("type mismatch on sus account origin")
	}

	susAccountBank, ok := data[1].(string)
	if !ok {
		return "", "", fmt.Errorf("type mismatch on bridge")
	}

	return susAccount, susAccountBank, nil
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

func serializeQuery2(qr transaction.QueryResult) ([]interface{}, error) {
	serialized := []interface{}{}
	records, ok := qr.Transactions.([]transaction.MaxBankTransaction)
	if !ok {
		return nil, fmt.Errorf("serializeQuery2: unexpected transactions type: %T", qr.Transactions)
	}
	for _, r := range records {
		serialized = append(serialized, []interface{}{r.BankCode, r.Account, r.Amount})
	}
	return serialized, nil
}

func deserializeQuery2(records []interface{}) (interface{}, error) {
	transactions := make([]transaction.MaxBankTransaction, 0, len(records))
	for _, datum := range records {
		fields, ok := datum.([]interface{})
		if !ok || len(fields) != 3 {
			return nil, fmt.Errorf("deserializing invalid structure for results query 2")
		}
		transactions = append(transactions, transaction.MaxBankTransaction{
			BankCode: int(fields[0].(float64)),
			Account:  fields[1].(string),
			Amount:   fields[2].(float64),
		})
	}
	return transactions, nil
}

// SerializePaymentRecordMessage serializes a batch of PaymentRecord.
// Row format: [timestamp, amount, currency, payment_format]
func SerializePaymentRecordMessage(clientId int64, records []transaction.PaymentRecord) (*middleware.Message, error) {
	data := make([]interface{}, 0, len(records))
	for _, r := range records {
		data = append(data, []interface{}{
			r.Timestamp.Format("2006/01/02 15:04"),
			r.Amount,
			r.Currency,
			r.PaymentFormat,
		})
	}
	body, err := SerializeJson(MessageClient{ClientID: clientId, Data: data})
	if err != nil {
		return nil, fmt.Errorf("serializing payment records: %w", err)
	}
	return &middleware.Message{Body: string(body)}, nil
}

// DeserializePaymentRecordMessage deserializes a batch of PaymentRecord records.
// Row format: [timestamp(0), amount(1), currency(2), payment_format(3)]
func DeserializePaymentRecordMessage(message *middleware.Message) (int64, []transaction.PaymentRecord, bool, error) {
	var messageClient MessageClient
	if err := json.Unmarshal([]byte(message.Body), &messageClient); err != nil {
		return 0, nil, false, fmt.Errorf("deserializing payment record body: %w", err)
	}

	records := make([]transaction.PaymentRecord, 0, len(messageClient.Data))
	for _, datum := range messageClient.Data {
		fields, ok := datum.([]interface{})
		if !ok || len(fields) != 4 {
			return 0, nil, false, fmt.Errorf("invalid structure inside payment record (expected 4 fields, got %d)", len(fields))
		}
		timestamp, ok1 := fields[0].(string)
		amount, ok2 := fields[1].(float64)
		currency, ok3 := fields[2].(string)
		paymentFormat, ok4 := fields[3].(string)
		if !ok1 || !ok2 || !ok3 || !ok4 {
			return 0, nil, false, fmt.Errorf("type mismatch in payment record fields")
		}
		parsedTime, err := time.Parse("2006/01/02 15:04", timestamp)
		if err != nil {
			return 0, nil, false, fmt.Errorf("invalid timestamp %q in payment record: %w", timestamp, err)
		}
		records = append(records, transaction.PaymentRecord{
			Timestamp:     parsedTime,
			Amount:        amount,
			Currency:      currency,
			PaymentFormat: paymentFormat,
		})
	}

	return messageClient.ClientID, records, len(records) == 0, nil
}

// SerializePaymentFormatCountMessage serializes a batch of PaymentFormatCount records.
// Row format: [payment_format, count]
func SerializePaymentFormatCountMessage(clientId int64, records []transaction.PaymentFormatCount) (*middleware.Message, error) {
	data := make([]interface{}, 0, len(records))
	for _, r := range records {
		data = append(data, []interface{}{r.PaymentFormat, r.Count})
	}
	body, err := SerializeJson(MessageClient{ClientID: clientId, Data: data})
	if err != nil {
		return nil, fmt.Errorf("serializing payment format counts: %w", err)
	}
	return &middleware.Message{Body: string(body)}, nil
}

// DeserializePaymentFormatCountMessage deserializes a batch of PaymentFormatCount records.
// Row format: [payment_format(0), count(1)]
func DeserializePaymentFormatCountMessage(message *middleware.Message) (int64, []transaction.PaymentFormatCount, bool, error) {
	var messageClient MessageClient
	if err := json.Unmarshal([]byte(message.Body), &messageClient); err != nil {
		return 0, nil, false, fmt.Errorf("deserializing payment format count body: %w", err)
	}

	records := make([]transaction.PaymentFormatCount, 0, len(messageClient.Data))
	for _, datum := range messageClient.Data {
		fields, ok := datum.([]interface{})
		if !ok || len(fields) != 2 {
			return 0, nil, false, fmt.Errorf("invalid structure inside payment format count record")
		}
		paymentFormat, ok1 := fields[0].(string)
		count, ok2 := fields[1].(float64)
		if !ok1 || !ok2 {
			return 0, nil, false, fmt.Errorf("type mismatch in payment format count fields")
		}
		records = append(records, transaction.PaymentFormatCount{
			PaymentFormat: paymentFormat,
			Count:         int(count),
		})
	}
	return messageClient.ClientID, records, len(records) == 0, nil
}

// SerializeMaxBankTransactionMessage serializes a batch of MaxBankTransaction records.
// Row format: [bankCode, account, amount]
func SerializeMaxBankTransactionMessage(clientId int64, records []transaction.MaxBankTransaction) (*middleware.Message, error) {
	data := make([]interface{}, 0, len(records))
	for _, r := range records {
		data = append(data, []interface{}{r.BankCode, r.Account, r.Amount})
	}
	body, err := SerializeJson(MessageClient{ClientID: clientId, Data: data})
	if err != nil {
		return nil, fmt.Errorf("serializing max bank transactions: %w", err)
	}
	return &middleware.Message{Body: string(body)}, nil
}

// DeserializeMaxBankTransactionMessage deserializes a batch of MaxBankTransaction records.
// Row format: [bankCode(0), account(1), amount(2)]
func DeserializeMaxBankTransactionMessage(message *middleware.Message) (int64, []transaction.MaxBankTransaction, bool, error) {
	var messageClient MessageClient
	if err := json.Unmarshal([]byte(message.Body), &messageClient); err != nil {
		return 0, nil, false, fmt.Errorf("deserializing max bank transaction body: %w", err)
	}

	records := make([]transaction.MaxBankTransaction, 0, len(messageClient.Data))
	for _, datum := range messageClient.Data {
		fields, ok := datum.([]interface{})
		if !ok || len(fields) != 3 {
			return 0, nil, false, fmt.Errorf("invalid structure inside max bank transaction record")
		}
		records = append(records, transaction.MaxBankTransaction{
			BankCode: int(fields[0].(float64)),
			Account:  fields[1].(string),
			Amount:   fields[2].(float64),
		})
	}

	return messageClient.ClientID, records, len(records) == 0, nil
}

func serializeQuery5(qr transaction.QueryResult) ([]interface{}, error) {
	serialized := []interface{}{}
	records, ok := qr.Transactions.([]transaction.PaymentFormatCount)
	if !ok {
		return nil, fmt.Errorf("serializeQuery5: unexpected transactions type: %T", qr.Transactions)
	}
	for _, r := range records {
		serialized = append(serialized, []interface{}{r.PaymentFormat, r.Count})
	}
	return serialized, nil
}

func deserializeQuery5(records []interface{}) (interface{}, error) {
	transactions := make([]transaction.PaymentFormatCount, 0, len(records))
	for _, datum := range records {
		fields, ok := datum.([]interface{})
		if !ok || len(fields) != 2 {
			return nil, fmt.Errorf("deserializing invalid structure for results query 5")
		}
		transactions = append(transactions, transaction.PaymentFormatCount{
			PaymentFormat: fields[0].(string),
			Count:         int(fields[1].(float64)),
		})
	}
	return transactions, nil
}
