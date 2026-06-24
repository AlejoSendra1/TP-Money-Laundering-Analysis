package client

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"tp_distribuidos/common/datasaver"
	"tp_distribuidos/common/messageprotocol/external"
	"tp_distribuidos/common/transaction"
	"tp_distribuidos/transactionsfilereader"
)

func (client *Client) sendBatch(batch []transaction.Transaction) error {
	if len(batch) == 0 {
		return nil
	}

	if err := external.WriteTransactionBatch(client.conn, client.BatchesSecNum, batch); err != nil { // implementar esta func
		return err
	}

	return nil
}

func (client *Client) sendTransactionRecords() error {
	batchSizeAsString := os.Getenv("BATCH_SIZE")
	batchSize, err := strconv.Atoi(batchSizeAsString)
	if err != nil {
		slog.Info("Error reading batchSize from environment", "err", err)
		return err
	}

	transactionsReader, err := transactionsfilereader.NewTransactionsFileReader(client.config.InputFile, batchSize, client.BatchesSecNum)
	defer transactionsReader.Close()
	if err != nil {
		slog.Info("Error opening transactions file for reading", "err", err)
		return err
	}

	slog.Info("Comenzando el envio de transacciones...")
	for {
		records, err := transactionsReader.GetTransactionRecords()
		if err != nil {
			return err
		}

		if len(records) == 0 {
			slog.Info("No hay mas para mandar...")
			break
		}
		if err := client.sendBatch(records); err != nil {
			slog.Info("Error while sending transaction batch", "err", err)
			return err
		}

		// wait for recvManager to signal the ACK arrived
		slog.Debug("Esperando ack...")
		if _, ok := <-client.ackChan; !ok {
			return fmt.Errorf("ack channel closed unexpectedly")
		}
		slog.Debug("Ack recibido")

		// por cada linea se va a representar un batch enviado,
		//  osea, batch_size transacciones enviadas de arriba para para abajo
		var b byte
		b = 1
		client.dataSaver.Save(b, client)
		client.BatchesSecNum++
		datasaver.Crash(datasaver.CrashAfterSendData)
	}

	if err := external.WriteEndOfRecords(client.conn); err != nil {
		return err
	}

	if _, ok := <-client.ackChan; !ok {
		return fmt.Errorf("ack channel closed unexpectedly")
	}

	slog.Info("Batches enviados", "val", client.BatchesSecNum)

	return nil
}
