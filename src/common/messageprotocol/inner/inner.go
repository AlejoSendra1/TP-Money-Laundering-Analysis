package inner

import (
	"encoding/json"
	"fmt"
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

	// agregamos el tipo de msg
	data = append(data, []interface{}{
		TransactionBatch,
	})
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
