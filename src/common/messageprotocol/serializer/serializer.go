package serializer

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"
	"tp_distribuidos/common/messageprotocol/external"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/transaction"
)

const UINT64_SIZE uint32 = 8
const UINT32_SIZE uint32 = 4
const BOOL_SIZE uint32 = 1

func SerializeTransactions(transactions []transaction.Transaction) ([]byte, error) {
	data, err := json.Marshal(transactions)
	if err != nil {
		return make([]byte, 0), err
	}
	return appendLenght(data), nil
}

func DeserializeTransactions(data []byte) ([]transaction.Transaction, error) {
	var transactions []transaction.Transaction
	err := json.Unmarshal(data, &transactions)
	return transactions, err
}

func appendLenght(data []byte) []byte {
	length := make([]byte, UINT32_SIZE)
	binary.BigEndian.PutUint32(length, uint32(len(data)))
	return append(length, data...)
}

func SerializeString(value string) []byte {
	data := []byte(value)
	return appendLenght(data)
}

func DeserializeString(bytes []byte) string {
	return string(bytes[:])
}

func SerializeUint64(value uint64) []byte {
	data := make([]byte, UINT64_SIZE)
	binary.BigEndian.PutUint64(data, value)
	return data
}

func DeserializeUint64(bytes []byte) uint64 {
	return binary.BigEndian.Uint64(bytes)
}

func SerializeUint32(value uint32) []byte {
	data := make([]byte, UINT32_SIZE)
	binary.BigEndian.PutUint32(data, value)
	return data
}

func DeserializeUint32(bytes []byte) uint32 {
	return binary.BigEndian.Uint32(bytes)
}

func SerializeTime(value time.Time) ([]byte, error) {
	return value.MarshalBinary()
}

func DeserializeTime(bytes []byte) (time.Time, error) {
	var decodedTime time.Time
	err := decodedTime.UnmarshalBinary(bytes)
	if err != nil {
		return time.Now(), err
	}
	return decodedTime, nil
}

func SerializeFloat64(value float64) []byte {
	bits := math.Float64bits(value)
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, bits)
	return bytes
}

func DeserializeFloat64(bytes []byte) float64 {
	bits := binary.BigEndian.Uint64(bytes)
	return math.Float64frombits(bits)
}

func SerializeQuery4Response(response inner.MessageClient) ([]byte, error) {
	msgType := make([]byte, 4)
	binary.BigEndian.PutUint32(msgType, uint32(external.Query4Response))

	serialization, err := json.Marshal(response.Data)
	if err != nil {
		return msgType, nil
	}

	serialization = appendLenght(serialization)

	bytes := append(msgType, serialization...)
	return bytes, nil
}

func DeserializeQuery4Response(bytes []byte) ([]interface{}, error) {
	var data []interface{}

	if err := json.Unmarshal(bytes, data); err != nil {
		return data, fmt.Errorf("Deserializing message body: %w", err)
	}

	return data, nil
}
