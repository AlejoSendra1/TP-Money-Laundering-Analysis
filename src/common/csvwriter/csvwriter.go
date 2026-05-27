package csvwriter

import (
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"tp_distribuidos/common/messageprotocol/external"
	"tp_distribuidos/common/transaction"
)

type CSVWriter struct {
	counter int64
	writer  *csv.Writer
	file    *os.File
}

var csvHeaders = []string{
	"timestamp",
	"from_bank",
	"to_bank",
	"from_account",
	"to_account",
	"amount",
	"currency",
	"payment_format",
}

func (c *CSVWriter) getCount() int64 {
	return c.counter
}

func NewCSVWriter(filepath string) (*CSVWriter, error) {
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("opening csv file: %w", err)
	}

	w := csv.NewWriter(file)

	// Only write headers if the file is empty
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("checking file size: %w", err)
	}
	if info.Size() == 0 {
		if err := w.Write(csvHeaders); err != nil {
			return nil, fmt.Errorf("writing csv headers: %w", err)
		}
		w.Flush()
	}

	return &CSVWriter{counter: 0, writer: w, file: file}, nil
}

func (c *CSVWriter) WriteBatch(transactions []transaction.Transaction) error {
	for i, tx := range transactions {
		if err := c.writer.Write(transactionToRow(tx)); err != nil {
			return fmt.Errorf("writing transaction %d: %w", i, err)
		}
		c.counter += 1
	}
	c.writer.Flush()
	return c.writer.Error()
}

func (c *CSVWriter) Close() error {
	c.writer.Flush()
	if err := c.writer.Error(); err != nil {
		return fmt.Errorf("flushing csv writer: %w", err)
	}
	return c.file.Close()
}

func transactionToRow(tx transaction.Transaction) []string {
	return []string{
		tx.Timestamp.Format("2006/01/02 15:04"),
		fmt.Sprint(tx.FromBank),
		fmt.Sprint(tx.ToBank),
		tx.FromAccount,
		tx.ToAccount,
		strconv.FormatFloat(tx.Amount, 'f', 2, 64),
		tx.Currency,
		tx.PaymentFormat,
	}
}

// WriteQ4Result receives the []interface{} from SerializeQ4SourceAccount's data field,
// which contains [part0, part1] from strings.Split(sourceAccount, "_")
func (c *CSVWriter) WriteResult(queryCode uint32, data []interface{}) error {
	slog.Info("Escribiendo")

	switch queryCode {
	case uint32(external.Query1Response):
		if err := c.WriteQ1Result(data); err != nil {
			slog.Error("writing transaction", "Error", err)
			return fmt.Errorf("writing transaction %w", err)
		}
	case uint32(external.Query4Response):
		if err := c.WriteQ4Result(data); err != nil {
			slog.Error("writing transaction", "Error", err)
			return fmt.Errorf("writing transaction %w", err)
		}
	}

	return c.writer.Error()
}

func (c *CSVWriter) WriteQ1Result(data []interface{}) error {
	slog.Info("Writing q1 result", "data", data)
	for _, transaction := range data {

		fields, ok := transaction.([]interface{})
		if !ok {
			slog.Error("While writing q1 result", "data", data)
			return fmt.Errorf("record: expected array, got %T", transaction)
		}

		fromBank, ok2 := fields[0].(float64)
		fromAccount, ok3 := fields[1].(string)
		toBank, ok4 := fields[2].(float64)
		toAccount, ok5 := fields[3].(string)
		amount, ok6 := fields[4].(float64)

		if !ok2 || !ok3 || !ok4 || !ok5 || !ok6 {
			return fmt.Errorf("record type mismatch in one or more fields")
		}
		myInt := int(fromBank)
		fromBankS := strconv.Itoa(myInt)
		myInt = int(toBank)
		toBankS := strconv.Itoa(myInt)
		amountS := strconv.FormatFloat(amount, 'f', 2, 64)

		if err := c.writer.Write([]string{"Q1", fromBankS, fromAccount, toBankS, toAccount, amountS}); err != nil {
			return fmt.Errorf("writing q4 result: %w", err)
		}
	}
	c.writer.Flush()
	c.counter++

	return c.writer.Error()
}

func (c *CSVWriter) WriteQ4Result(data []interface{}) error {
	part0, ok := data[0].(string)
	if !ok {
		slog.Error("Error data 0 no se pudo parsear a string", "data", data)
		return fmt.Errorf("expected string at index 0, got %T", data[0])
	}

	part1, ok := data[1].(string)
	if !ok {
		slog.Error("Error data 1 no se pudo parsear a string", "data", data)
		return fmt.Errorf("expected string at index 1, got %T", data[1])
	}

	// quitar string
	if err := c.writer.Write([]string{"Q4", part0, part1}); err != nil {
		return fmt.Errorf("writing q4 result: %w", err)
	}
	c.writer.Flush()
	c.counter++

	return c.writer.Error()
}
