package transactionsfilereader

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
	"tp_distribuidos/common/transaction"
)

const (
	TIMESTAMP_COLUMN      = 0
	FROM_BANK_COLUMN      = 1
	FROM_ACCOUNT_COLUMN   = 2
	TO_BANK_COLUMN        = 3
	TO_ACCOUNT_COLUMN     = 4
	AMOUNT_COLUMN         = 7
	CURRENCY_COLUMN       = 8
	PAYMENT_FORMAT_COLUMN = 9
)

type TransactionsFileReader struct {
	file      *os.File
	scanner   *bufio.Scanner
	batchSize int
}

// NewCSVWriter ahora recibe un basePath o archivo base para las transacciones comunes.
// Los archivos de las queries se crearán en el mismo directorio con nombres específicos.
func NewTransactionsFileReader(filepath string, batchSize int, batchesAlreadySent int64) (*TransactionsFileReader, error) {
	file, err := os.Open(filepath)
	if err != nil {
		slog.Info("Error while runninging input file", "err", err)
		return nil, err
	}

	//scanner.Scan() // para saltear la primera linea del archivo (saltea el header)

	scanner := bufio.NewScanner(file)

	slog.Info("salteando batches ya enviados....")
	for range batchesAlreadySent * int64(batchSize) {
		scanner.Scan()
	}
	slog.Info("Transacciones salteadas", "cantidad", batchesAlreadySent*int64(batchSize))

	return &TransactionsFileReader{
		file:      file,
		scanner:   scanner,
		batchSize: batchSize,
	}, nil
}

func (tfr *TransactionsFileReader) Close() {
	tfr.file.Close()
}

func (trf *TransactionsFileReader) GetTransactionRecords() ([]transaction.Transaction, error) {
	batch := make([]transaction.Transaction, 0, trf.batchSize)

	for trf.scanner.Scan() {
		columns := strings.Split(trf.scanner.Text(), ",")

		tx, err := parseTransaction(columns)
		if err != nil {
			slog.Info("Error while parsing transaction record", "err", err)
			return batch, err
		}

		batch = append(batch, tx)
		if len(batch) == trf.batchSize {
			return batch, nil
		}
	}

	slog.Info("Mandando batch a client - file reader")
	return batch, nil
}

func parseTransaction(columns []string) (transaction.Transaction, error) {
	timestamp, err := time.Parse("2006/01/02 15:04", columns[TIMESTAMP_COLUMN])
	if err != nil {
		return transaction.Transaction{}, fmt.Errorf("invalid timestamp %q: %w", columns[TIMESTAMP_COLUMN], err)
	}

	fromBank, err := strconv.Atoi(columns[FROM_BANK_COLUMN])
	if err != nil {
		return transaction.Transaction{}, fmt.Errorf("invalid from_bank %q: %w", columns[FROM_BANK_COLUMN], err)
	}

	toBank, err := strconv.Atoi(columns[TO_BANK_COLUMN])
	if err != nil {
		return transaction.Transaction{}, fmt.Errorf("invalid to_bank %q: %w", columns[TO_BANK_COLUMN], err)
	}

	amountReceived, err := strconv.ParseFloat(columns[AMOUNT_COLUMN], 64)
	if err != nil {
		return transaction.Transaction{}, fmt.Errorf("invalid amount_received %q: %w", columns[AMOUNT_COLUMN], err)
	}

	return transaction.Transaction{
		Timestamp:     timestamp,
		FromBank:      fromBank,
		ToBank:        toBank,
		FromAccount:   columns[FROM_ACCOUNT_COLUMN],
		ToAccount:     columns[TO_ACCOUNT_COLUMN],
		Amount:        amountReceived,
		Currency:      columns[CURRENCY_COLUMN],
		PaymentFormat: columns[PAYMENT_FORMAT_COLUMN],
	}, nil
}
