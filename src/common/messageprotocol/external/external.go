package external

import (
	"io"

	"tp_distribuidos/src/common/messageprotocol/external/safeio"
	"tp_distribuidos/src/common/messageprotocol/external/serializer"
	"tp_distribuidos/src/common/transaction"
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

/*
func serializeTransactionRecord(transaction *transaction.Transaction) ([]byte, error) { //no modif
	msg := serializer.SerializeUint64(transaction.Id)
	serialization, err := serializer.SerializeTime(transaction.Timestamp)
	if err != nil {
		return msg, err
	}
	msg = append(msg, serialization...)
	msg = append(msg, serializer.SerializeUint64(transaction.From_Bank)...)
	msg = append(msg, serializer.SerializeString(transaction.Account)...)
	msg = append(msg, serializer.SerializeUint64(transaction.To_Bank)...)
	msg = append(msg, serializer.SerializeString(transaction.Account_1)...)
	msg = append(msg, serializer.SerializeFloat64(transaction.Amount_Received)...)
	msg = append(msg, serializer.SerializeString(transaction.Receiving_Currency)...)
	msg = append(msg, serializer.SerializeFloat64(transaction.Amount_Paid)...)
	msg = append(msg, serializer.SerializeString(transaction.Payment_Currency)...)
	msg = append(msg, serializer.SerializeString(transaction.Payment_Format)...)
	return msg, nil
}*/

func writeMsgType(writer io.Writer, msgType MsgType) error { // no modif
	msg := serializer.SerializeUint32(uint32(msgType))
	return safeio.WriteAll(writer, msg)
}

func ReadMsgType(reader io.Reader) (MsgType, error) { // no modif
	msgTypeSerialized, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return 0, err
	}
	msgType := MsgType(serializer.DeserializeUint32(msgTypeSerialized))

	return msgType, nil
}

func WriteTransactionBatch(writer io.Writer, transactionBatch *[]transaction.Transaction) error { // no modif
	msg := serializer.SerializeUint32(uint32(TransactionBatch))
	serial, err := serializer.SerializeTransactions(*transactionBatch)
	if err != nil {
		return err
	}
	msg = append(msg, serial...)
	return safeio.WriteAll(writer, msg)
}

func ReadTransactionBatch(reader io.Reader) (*[]transaction.Transaction, error) {
	serializedBytesToRead, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, err
	}
	bytesToRead := serializer.DeserializeUint32(serializedBytesToRead)

	serializedTransactions, err := safeio.ReadAll(reader, bytesToRead)
	if err != nil {
		return nil, err
	}

	transactions, err := serializer.DeserializeTransactions(serializedTransactions)
	if err != nil {
		return nil, err
	}

	return &transactions, nil
}

/*
func WriteFruitTop(writer io.Writer, fruitItemRecords []fruititem.FruitItem) error {
	msg := serializer.SerializeUint32(uint32(FruitTop))
	msg = append(msg, serializer.SerializeUint32(uint32(len(fruitItemRecords)))...)
	for _, fruitItemRecord := range fruitItemRecords {
		msg = append(msg, serializeFruitRecord(&fruitItemRecord)...)
	}

	return safeio.WriteAll(writer, msg)
}

func ReadFruitTop(reader io.Reader) ([]fruititem.FruitItem, error) {
	fruitRecordsAmountSerialized, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, err
	}
	fruitRecordsAmount := serializer.DeserializeUint32(fruitRecordsAmountSerialized)

	fruitRecords := make([]fruititem.FruitItem, fruitRecordsAmount)
	for i := 0; i < int(fruitRecordsAmount); i++ {
		fruitRecord, err := ReadFruitRecord(reader)
		if err != nil {
			return nil, err
		}
		fruitRecords[i] = *fruitRecord
	}

	return fruitRecords, nil
}
*/

func WriteAck(writer io.Writer) error { // no modif
	return writeMsgType(writer, Ack)
}

func WriteEndOfRecords(writer io.Writer) error { // no modif
	return writeMsgType(writer, EndOfRecords)
}
