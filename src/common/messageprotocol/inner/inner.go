package inner

import (
	"encoding/json"
	//	"errors"
	"tp_distribuidos/src/common/middleware"
	"tp_distribuidos/src/common/transaction"
)

func serializeJson(message []interface{}) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJson(message []byte) ([]interface{}, error) {
	var data []interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func SerializeMessage(transactionBatch []transaction.Transaction) (*middleware.Message, error) {
	data := []interface{}{}
	for _, transaction := range transactionBatch {
		datum := []interface{}{
			transaction.Id,
			transaction.Timestamp,
			transaction.From_Bank,
			transaction.Account,
			transaction.To_Bank,
			transaction.Account_1,
			transaction.Amount_Received,
			transaction.Receiving_Currency,
			transaction.Amount_Paid,
			transaction.Payment_Currency,
			transaction.Payment_Format,
		}
		data = append(data, datum)
	}

	body, err := serializeJson(data)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

/*

func DeserializeMessage(message *middleware.Message) ([]transaction.Transaction, error) {
	data, err := deserializeJson([]byte((*message).Body))
	if err != nil {
		return nil, err
	}

	transactions := []transaction.Transaction{}
	for _, datum := range data {
		fruitPair, ok := datum.([]interface{})
		if !ok {
			return nil, errors.New("Datum is not an array")
		}

		fruit, ok := fruitPair[0].(string)
		if !ok {
			return nil, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := fruitPair[1].(float64)
		if !ok {
			return nil, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitRecord := transaction.Transaction{Fruit: fruit, Amount: uint32(fruitAmount)}
		fruitRecords = append(fruitRecords, fruitRecord)
	}

	return transactions, nil
}
*/
