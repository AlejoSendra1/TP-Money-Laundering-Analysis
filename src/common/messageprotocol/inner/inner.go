package inner

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	//"errors"

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
	SuspiciousAccount
	PossibleFraudDestinations
	FraudSource
)

type MessageClient struct {
	ClientID int64         `json:"client_id"`
	MsgType  MsgType       `json:"msg_type"`
	Data     []interface{} `json:"data"`
}

func serializeJson(messageClient MessageClient) ([]byte, error) {
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
	data := []interface{}{}

	// agregamos el tipo de msg
	data = append(data, []interface{}{
		mustPropagate,
		sender,
	})

	body, err := serializeJson(MessageClient{ClientID: clientId, MsgType: EndOfRecords, Data: data})
	if err != nil {
		return nil, err
	}

	message := middleware.Message{Body: string(body)}

	return &message, nil
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

	body, err := serializeJson(MessageClient{ClientID: clientId, MsgType: TransactionBatch, Data: data})
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

	body, err := serializeJson(MessageClient{ClientID: clientID, MsgType: SuspiciousAccount, Data: data})
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

	body, err := serializeJson(MessageClient{ClientID: clientID, MsgType: PossibleFraudDestinations, Data: data})
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

	body, err := serializeJson(MessageClient{ClientID: clientID, MsgType: FraudSource, Data: data})
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
