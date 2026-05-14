package external

import (
	"io"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/external/safeio"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/external/serializer"
)

type MsgType uint32

const (
	FruitRecord MsgType = iota + 1
	FruitTop
	Ack
	EndOfRecords
)

func serializeFruitRecord(fruit *fruititem.FruitItem) []byte {
	msg := serializer.SerializeString(fruit.Fruit)
	msg = append(msg, serializer.SerializeUint32(fruit.Amount)...)
	return msg
}

func writeMsgType(writer io.Writer, msgType MsgType) error {
	msg := serializer.SerializeUint32(uint32(msgType))
	return safeio.WriteAll(writer, msg)
}

func ReadMsgType(reader io.Reader) (MsgType, error) {
	msgTypeSerialized, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return 0, err
	}
	msgType := MsgType(serializer.DeserializeUint32(msgTypeSerialized))

	return msgType, nil
}

func WriteFruitRecord(writer io.Writer, fruitItemRecord *fruititem.FruitItem) error {
	msg := serializer.SerializeUint32(uint32(FruitRecord))
	msg = append(msg, serializeFruitRecord(fruitItemRecord)...)

	return safeio.WriteAll(writer, msg)
}

func ReadFruitRecord(reader io.Reader) (*fruititem.FruitItem, error) {
	serializedFruitSize, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, err
	}
	fruitSize := serializer.DeserializeUint32(serializedFruitSize)

	serializedFruit, err := safeio.ReadAll(reader, fruitSize)
	if err != nil {
		return nil, err
	}
	fruit := serializer.DeserializeString(serializedFruit)

	serializedAmount, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, err
	}
	amount := serializer.DeserializeUint32(serializedAmount)

	fruitItem := fruititem.FruitItem{Fruit: fruit, Amount: amount}
	return &fruitItem, nil
}

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

func WriteAck(writer io.Writer) error {
	return writeMsgType(writer, Ack)
}

func WriteEndOfRecords(writer io.Writer) error {
	return writeMsgType(writer, EndOfRecords)
}
