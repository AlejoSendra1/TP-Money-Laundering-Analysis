package csvwriter

import (
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"tp_distribuidos/common/messageprotocol/external"
	"tp_distribuidos/common/messageprotocol/inner"
)

var queryHeaders = map[string][]string{
	"q1": {
		"from_bank",
		"from_account",
		"to_bank",
		"to_account",
		"amount_paid",
	},
	"q2": {
		"from_bank",
		"account",
		"bank_name",
		"amount_paid",
	},
	"q3": {
		"from_bank",
		"account",
		"payment_format",
		"amount_paid",
	},
	"q4": {
		"bank",
		"account",
	},
	"q5": {
		"type",
		"count",
	},
}

type CSVWriter struct {
	counter      int64
	basePath     string
	queryFiles   map[string]*os.File
	queryWriters map[string]*csv.Writer
	q4mutex      sync.Mutex
	q4Written    map[[2]string]struct{}
}

func (c *CSVWriter) getCount() int64 {
	return c.counter
}

// NewCSVWriter ahora recibe un basePath o archivo base para las transacciones comunes.
// Los archivos de las queries se crearán en el mismo directorio con nombres específicos.
func NewCSVWriter(baseFilepath string) (*CSVWriter, error) {
	return &CSVWriter{
		counter:      0,
		basePath:     filepath.Dir(baseFilepath),
		queryFiles:   make(map[string]*os.File),
		queryWriters: make(map[string]*csv.Writer),
		q4Written:    make(map[[2]string]struct{}),
	}, nil
}

// getQueryWriter obtiene o inicializa de forma diferida (lazy) el escritor para una query específica
func (c *CSVWriter) getQueryWriter(queryName string) (*csv.Writer, error) {
	if w, exists := c.queryWriters[queryName]; exists {
		return w, nil
	}

	// Construye el path: ej. "resultado_q1.csv" en el mismo directorio
	fileName := fmt.Sprintf("%s_results_%s.csv", c.basePath, queryName)
	fullPath := filepath.Join(c.basePath, fileName)

	file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("opening file for %s: %w", queryName, err)
	}
	w := csv.NewWriter(file)

	if headers, exists := queryHeaders[queryName]; exists {
		if err := w.Write(headers); err != nil {
			file.Close()
			return nil, fmt.Errorf("writing headers for %s: %w", queryName, err)
		}
		w.Flush()
	}
	c.queryFiles[queryName] = file
	c.queryWriters[queryName] = w

	return w, nil
}

func (c *CSVWriter) Close() error {
	var closeErr error

	// Cerrar todos los archivos de queries abiertos
	for name, w := range c.queryWriters {
		w.Flush()
		if err := w.Error(); err != nil && closeErr == nil {
			closeErr = fmt.Errorf("flushing writer for %s: %w", name, err)
		}
	}
	for name, f := range c.queryFiles {
		if err := f.Close(); err != nil && closeErr == nil {
			closeErr = fmt.Errorf("closing file for %s: %w", name, err)
		}
	}

	return closeErr
}

func (c *CSVWriter) WriteResult(queryCode uint32, data []interface{}) error {
	//slog.Info("Escribiendo resultado por query")

	switch queryCode {
	case uint32(external.Query1Response):
		if err := c.WriteQ1Result(data); err != nil {
			slog.Error("writing transaction q1", "Error", err)
			return fmt.Errorf("writing transaction q1: %w", err)
		}
	case uint32(inner.Query2Response):
		return c.writeQ2Result(data)
	case uint32(inner.Query3Response):
		return c.writeQ3Result(data)
	case uint32(external.Query4Response):
		if err := c.WriteQ4Result(data); err != nil {
			slog.Error("writing transaction q4", "Error", err)
			return fmt.Errorf("writing transaction q4: %w", err)
		}
	case uint32(inner.Query5Response):
		return c.writeQ5Result(data)
	default:
		return fmt.Errorf("unsupported query code: %d", queryCode)
	}

	return nil
}

func (c *CSVWriter) WriteQ1Result(data []interface{}) error {
	w, err := c.getQueryWriter("q1")
	if err != nil {
		return err
	}

	//slog.Info("Writing q1 result", "data", data)
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
		fromBankS := strconv.Itoa(int(fromBank))
		toBankS := strconv.Itoa(int(toBank))
		amountS := strconv.FormatFloat(amount, 'f', 2, 64)

		// Quitamos el prefijo "Q1" si ya va a estar en su propio archivo exclusivo (opcional, lo dejé por compatibilidad)
		if err := w.Write([]string{fromBankS, fromAccount, toBankS, toAccount, amountS}); err != nil {
			return fmt.Errorf("writing q1 row: %w", err)
		}
	}
	w.Flush()
	c.counter++
	return w.Error()
}

func (c *CSVWriter) writeQ2Result(data []interface{}) error {
	w, err := c.getQueryWriter("q2")
	if err != nil {
		return err
	}

	records, ok := data[0].([]interface{})
	if !ok {
		return fmt.Errorf("q2: expected nested []interface{}, got %T", data[0])
	}
	for _, rec := range records {
		fields, ok := rec.([]interface{})
		if !ok || len(fields) != 3 {
			return fmt.Errorf("q2: invalid record structure: %v", rec)
		}
		bankCode, ok1 := fields[0].(float64)
		account, ok2 := fields[1].(string)
		amount, ok3 := fields[2].(float64)
		if !ok1 || !ok2 || !ok3 {
			return fmt.Errorf("q2: type mismatch in fields: %v", fields)
		}
		row := []string{
			strconv.Itoa(int(bankCode)),
			account,
			strconv.FormatFloat(amount, 'f', 2, 64),
		}
		if err := w.Write(row); err != nil {
			return fmt.Errorf("q2: writing row: %w", err)
		}
	}
	w.Flush()
	c.counter++
	return w.Error()
}

func (c *CSVWriter) writeQ3Result(data []interface{}) error {
	w, err := c.getQueryWriter("q3")
	if err != nil {
		return err
	}

	records := data[0].([]interface{})
	for _, rec := range records {
		fields, ok := rec.([]interface{})
		if !ok || len(fields) != 4 {
			return fmt.Errorf("q3: invalid record structure: %v", rec)
		}
		fromBank, ok1 := fields[0].(float64)
		fromAccount, ok2 := fields[1].(string)
		paymentFormat, ok3 := fields[2].(string)
		amount, ok4 := fields[3].(float64)
		if !ok1 || !ok2 || !ok3 || !ok4 {
			return fmt.Errorf("q3: type mismatch in fields: %v", fields)
		}
		row := []string{
			strconv.Itoa(int(fromBank)),
			fromAccount,
			paymentFormat,
			strconv.FormatFloat(amount, 'f', 2, 64),
		}
		if err := w.Write(row); err != nil {
			return fmt.Errorf("q3: writing row: %w", err)
		}
	}
	w.Flush()
	c.counter++
	return w.Error()
}

func (c *CSVWriter) WriteQ4Result(data []interface{}) error {
	w, err := c.getQueryWriter("q4")
	if err != nil {
		return err
	}

	part0, ok := data[0].(string)
	part1, ok1 := data[1].(string)
	part2, ok2 := data[2].(string)
	part3, ok3 := data[3].(string)

	if !ok || !ok1 || !ok2 || !ok3 {
		slog.Error("Error data no se pudo parsear a string", "data", data)
		return fmt.Errorf("expected strings in data fields for q4")
	}

	c.q4mutex.Lock()
	pair1 := [2]string{part0, part1}
	if _, seen := c.q4Written[pair1]; !seen {
		if err := w.Write([]string{part0, part1}); err != nil {
			return fmt.Errorf("writing q4 row 1: %w", err)
		}
		c.q4Written[pair1] = struct{}{}
	}

	pair2 := [2]string{part2, part3}
	if _, seen := c.q4Written[pair2]; !seen {
		if err := w.Write([]string{part2, part3}); err != nil {
			return fmt.Errorf("writing q4 row 2: %w", err)
		}
		c.q4Written[pair2] = struct{}{}
	}
	c.q4mutex.Unlock()

	w.Flush()
	c.counter++
	return w.Error()
}

func (c *CSVWriter) writeQ5Result(data []interface{}) error {
	w, err := c.getQueryWriter("q5")
	if err != nil {
		return err
	}

	records := data[0].([]interface{})
	for _, rec := range records {
		fields, ok := rec.([]interface{})
		if !ok || len(fields) != 2 {
			return fmt.Errorf("q5: invalid record structure: %v", rec)
		}
		paymentFormat, ok1 := fields[0].(string)
		count, ok2 := fields[1].(float64)
		if !ok1 || !ok2 {
			return fmt.Errorf("q5: type mismatch in fields: %v", fields)
		}
		row := []string{
			paymentFormat,
			strconv.Itoa(int(count)),
		}
		if err := w.Write(row); err != nil {
			return fmt.Errorf("q5: writing row: %w", err)
		}
	}
	w.Flush()
	c.counter++
	return w.Error()
}
