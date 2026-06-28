package client

import (
	"fmt"
	"log/slog"
	"tp_distribuidos/common/datasaver"
	"tp_distribuidos/common/messageprotocol/external"
	"tp_distribuidos/common/messageprotocol/external/safeio"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/messageprotocol/serializer"
	"tp_distribuidos/csvwriter"
)

func (client *Client) handleQueryResponse(queryCode uint32) error {
	secNumBytes, err := safeio.ReadAll(client.conn, serializer.UINT64_SIZE) // numero de secuencia del msg
	if err != nil {
		return err
	}
	newSecNum := int64(serializer.DeserializeUint64(secNumBytes))

	toRead, err := external.ReadUint32(client.conn)
	if err != nil {
		return err
	}

	read, err := safeio.ReadAll(client.conn, toRead)
	if err != nil {
		return err
	}

	toWrite, err := serializer.DeserializeQueryResponse(read)
	if err != nil {
		return err
	}

	// CHECKEO Q NO LO HAYA ESCRITO
	if state, exists := client.processedQueries[queryCode]; exists && newSecNum <= state.LastSecNum {
		slog.Warn("Ignoring duplicate message from gateway redelivery", "query", queryCode, "secNum", newSecNum)
		return nil
	}

	// ESCRIBO EN EL ARCHIVO Q CORRESPONDA
	if err := client.writer.WriteResult(queryCode, toWrite); err != nil {
		return err
	}

	qName := csvwriter.QueryCodeToName(queryCode)
	newOffset, err := client.writer.GetCurrentOffset(qName)
	if err != nil {
		return fmt.Errorf("failed to fetch current file size: %w", err)
	}

	// efectivisamos el cambio
	client.processedQueries[queryCode] = QueryState{
		LastSecNum:      newSecNum,
		LastWriteOffset: newOffset,
	}

	client.resultsLogsSaver.Save(
		LogData{
			QueryCode:  queryCode,
			QueryState: QueryState{LastSecNum: newSecNum, LastWriteOffset: newOffset},
		},
		client,
	)

	return nil
}

func (client *Client) recvManager() error {
	slog.Info("Manager de lectura iniciado...")
	defer close(client.ackChan)

	for {
		datasaver.Crash(datasaver.CrashAfterLog)

		msgType, err := external.ReadMsgType(client.conn)
		if err != nil {
			// Si cerramos la conexión por diseño, salimos limpio
			if !client.running.Load() {
				return nil
			}
			return fmt.Errorf("leyendo tipo de mensaje: %w", err)
		}

		switch inner.MsgType(msgType) {

		case inner.MsgType(external.Ack):
			slog.Debug("Notificando a la otra go rutine de la llegada del ack...")
			client.ackChan <- struct{}{}
			// Avisamos al otro hilo que puede considerar el msg como recibido.
			slog.Debug("Go rutine de envio notificada")
			continue

		case inner.Query1Response, inner.Query2Response, inner.Query3Response, inner.Query4Response, inner.Query5Response:

			if err := client.handleQueryResponse(uint32(msgType)); err != nil {
				return err
			}
			if err := client.sendResponseAck(); err != nil {
				return err
			}

		// 4. Fin de todo el procesamiento (El Gateway nos avisa que no hay más respuestas)
		case inner.EndOfRecords:
			datasaver.Crash(datasaver.CrashBeforeEOF)
			slog.Info("End of records total recibido del Gateway. Finalizando cliente.")

			if err := client.sendResponseAck(); err != nil {
				return err
			}
			return nil

		default:
			return fmt.Errorf("tipo de mensaje inesperado recibido en el cliente: %d", msgType)
		}

	}
}
