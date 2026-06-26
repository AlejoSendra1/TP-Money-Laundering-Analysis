package transactions_saver

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
	"tp_distribuidos/common/batch_utils"
	"tp_distribuidos/common/transaction"
)

type SendTransactions func(int64, []transaction.ThresholdFilteredTransfer) error

type Storage struct {
	mu       sync.Mutex // Protege archivo
	filePath string
}

func NewStorage(storage, name string) *Storage {
	filePath := filepath.Join(storage, name)
	return &Storage{filePath: filePath}
}

func (storage *Storage) StoreTransactions(transactions []transaction.ThresholdFilteredTransfer) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	file, err := os.OpenFile(storage.filePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0664)
	if err != nil {
		return fmt.Errorf("opening transaction file on disk: %w", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	jsonData, err := json.Marshal(transactions)
	if err != nil {
		return fmt.Errorf("marshaling transaction batch to json: %w", err)
	}
	if _, err = writer.Write(append(jsonData, '\n')); err != nil {
		return fmt.Errorf("writing transaction batch to disk: %w", err)
	}
	if err = writer.Flush(); err != nil {
		return fmt.Errorf("flushing transaction batch buffer: %w", err)
	}

	if err = file.Sync(); err != nil {
		return fmt.Errorf("syncing file to disk: %w", err)
	}

	return nil
}

func (storage *Storage) FlushTransactions(clientID int64, send SendTransactions) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	if storage.filePath == "" {
		slog.Info("No transactions to flush from disk", "clientID", clientID)
		return nil
	}

	slog.Info("Flushing transactions from disk to output", "clientID", clientID, "filePath", storage.filePath)
	if err := storage.readAndSendFromFile(clientID, send); err != nil {
		return err
	}
	time.Sleep(15 * time.Second) // Pequeña pausa para asegurar que los mensajes se envíen antes de eliminar el archivo
	slog.Info("Flushed transactions from disk to output", "clientID", clientID, "filePath", storage.filePath)

	if err := os.Remove(storage.filePath); err != nil && !os.IsNotExist(err) {
		slog.Error("Failed to delete temporary file from disk", "path", storage.filePath, "err", err)
	} else if !os.IsNotExist(err) {
		slog.Debug("Temporary client file deleted from disk successfully", "clientID", clientID)
	}
	return nil
}

func (storage *Storage) readAndSendFromFile(clientID int64, send SendTransactions) error {
	file, err := os.Open(storage.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("opening file for flushing: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	// Ojo si el batch es muy grande, scanner puede fallar en ese caso.

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var txs []transaction.ThresholdFilteredTransfer
		if err := json.Unmarshal(line, &txs); err != nil {
			slog.Warn("Line corrupted", "err", err, "filePath", storage.filePath)
			continue
		}
		if len(txs) == 0 {
			continue
		}
		batch_utils.SortBatch(txs, func(a, b transaction.ThresholdFilteredTransfer) bool {
			if a.Timestamp != b.Timestamp {
				return a.Timestamp < b.Timestamp
			}
			return a.Amount > b.Amount
		})
		if err := send(clientID, txs); err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading transactions file stream: %w", err)
	}
	return nil
}

func (storage *Storage) RemoveFile() error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if storage.filePath == "" {
		return nil
	}

	err := os.Remove(storage.filePath)
	if err == nil {
		slog.Debug("Temporary client file deleted from disk successfully", "path", storage.filePath)
		return nil
	}

	if !errors.Is(err, os.ErrNotExist) {
		slog.Error("Failed to delete temporary file from disk", "path", storage.filePath, "err", err)
		return err
	}

	return nil
}
