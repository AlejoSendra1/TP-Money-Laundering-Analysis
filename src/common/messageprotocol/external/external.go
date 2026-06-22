package external

import (
	"io"

	"tp_distribuidos/common/messageprotocol/external/safeio"
	"tp_distribuidos/common/messageprotocol/serializer"
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
	ConnectionMsg
	ConnectionResponseMsg
	ReconnectionMsg
	ResponseAck
)

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

// --------------- TransactionBatch ---------------
func WriteTransactionBatch(writer io.Writer, batchSecuenceNumber int64, transactionBatch []transaction.Transaction) error { // no modif
	msg := serializer.SerializeUint32(uint32(TransactionBatch))
	secNum := serializer.SerializeUint64(uint64(batchSecuenceNumber))
	serial, err := serializer.SerializeTransactions(transactionBatch)
	if err != nil {
		return err
	}
	msg = append(msg, secNum...)
	msg = append(msg, serial...)
	return safeio.WriteAll(writer, msg)
}

func ReadTransactionBatch(reader io.Reader) (*[]transaction.Transaction, int64, error) {
	secNum, err := safeio.ReadAll(reader, serializer.UINT64_SIZE)
	if err != nil {
		return nil, 0, err
	}

	serializedBytesToRead, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, 0, err
	}
	bytesToRead := serializer.DeserializeUint32(serializedBytesToRead)

	serializedTransactions, err := safeio.ReadAll(reader, bytesToRead)
	if err != nil {
		return nil, 0, err
	}

	transactions, err := serializer.DeserializeTransactions(serializedTransactions)
	if err != nil {
		return nil, 0, err
	}

	return &transactions, int64(serializer.DeserializeUint64(secNum)), nil
}

// ----

func WriteAck(writer io.Writer) error {
	return writeMsgType(writer, Ack)
}

func WriteResponseAck(writer io.Writer) error {
	return writeMsgType(writer, ResponseAck)
}

func WriteEndOfRecords(writer io.Writer) error {
	return writeMsgType(writer, EndOfRecords)
}

// WriteQueriesResult escribe al cliente los resultados de las queries
// Por ahora solo soporta Query1
// Formato: [MsgType][numRecords][records...][EOF]
// Ejemplo:
// [Query1Response][numRecords][records...]
// [Query2Response][numRecords][records...]
// [Query3Response][numRecords][records...]
// [Query4Response][numRecords][records...]
// [Query5Response][numRecords][records...]
// [EOF]
func WriteQueriesResult(writer io.Writer, queriesResult *transaction.QueriesResult) error {
	if queriesResult == nil {
		return nil
	}

	for queryID, qr := range queriesResult.Results {
		switch queryID {
		case transaction.Query1:
			// MsgType
			msg := serializer.SerializeUint32(uint32(Query1Response))

			records, ok := qr.Transactions.([]transaction.LowAmountTransfer)
			if !ok {
				continue
			}
			// numRecords
			msg = append(msg, serializer.SerializeUint32(uint32(len(records)))...)
			// records...
			for _, r := range records {
				msg = append(msg, serializer.SerializeUint64(uint64(r.FromBank))...)
				msg = append(msg, serializer.SerializeString(r.FromAccount)...)
				msg = append(msg, serializer.SerializeUint64(uint64(r.ToBank))...)
				msg = append(msg, serializer.SerializeString(r.ToAccount)...)
				msg = append(msg, serializer.SerializeFloat64(r.Amount)...)
			}

			if err := safeio.WriteAll(writer, msg); err != nil {
				return err
			}
		default:
			continue
		}
	}
	return WriteEndOfRecords(writer)
}

// --------------- ConnectionMsg ---------------
func WriteConnectionMsg(writer io.Writer) error { // no modif
	msg := serializer.SerializeUint32(uint32(ConnectionMsg))
	return safeio.WriteAll(writer, msg)
}

// --------------- ConnectionResponseMsg ---------------
func WriteConnectionMsgResponse(writer io.Writer, id int64) error {
	msg := serializer.SerializeUint32(uint32(ConnectionResponseMsg))
	idBytes := serializer.SerializeUint64(uint64(id))

	msg = append(msg, idBytes...)
	return safeio.WriteAll(writer, msg)
}

func ReadConnectionMsgResponse(reader io.Reader) (int64, error) {
	idBytes, err := safeio.ReadAll(reader, serializer.UINT64_SIZE)
	if err != nil {
		return 0, err
	}
	return int64(serializer.DeserializeUint64(idBytes)), nil
}

// --------------- ReconnectionMsg ---------------
func WriteReconnectionMsg(writer io.Writer, id int64) error { // no modif
	msg := serializer.SerializeUint32(uint32(ReconnectionMsg))
	idBytes := serializer.SerializeUint64(uint64(id))

	msg = append(msg, idBytes...)
	return safeio.WriteAll(writer, msg)
}

func ReadReconnectionId(reader io.Reader) (int64, error) {
	idBytes, err := safeio.ReadAll(reader, serializer.UINT64_SIZE)
	if err != nil {
		return 0, err
	}
	return int64(serializer.DeserializeUint64(idBytes)), nil
}

// Helpers

func ReadUint32(reader io.Reader) (uint32, error) {
	bytes, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return 0, err
	}
	return serializer.DeserializeUint32(bytes), nil
}
