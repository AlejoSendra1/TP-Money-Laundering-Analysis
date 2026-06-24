package datasaver

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"tp_distribuidos/common/worker"
)

// PARA RESTAURAR A UN WORKER CAIDO USAR
// RESTAURATE=TRUE (creando la lectura de la env var primero)

const RESTORATION_FILE_SUFIX = "_restoration_point" + ".txt"

type DataSaver struct {
	restorationFileName string
	file                *os.File
	writer              *bufio.Writer
	reader              *json.Decoder
	logsUntilCheckpoint int
	logCounter          int
	pendingRecord       *FileRecord
	validOffset         int64 // byte offset right after the last successfully parsed record
}

type RecordType string

const (
	CheckpointType RecordType = "C"
	LogType        RecordType = "L"
)

type FileRecord struct {
	Type    RecordType      `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func NewDataSaver(filename string, logsUntilCheckpoint int) (*DataSaver, error) {

	slog.Info("Creando datasasver")
	filename = filename + RESTORATION_FILE_SUFIX
	restorationFile, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return &DataSaver{}, fmt.Errorf("failed to open file: %w", err)
	}

	writer := bufio.NewWriter(restorationFile)
	slog.Info("datasasver creado")

	return &DataSaver{
		restorationFileName: filename,
		file:                restorationFile,
		writer:              writer,
		reader:              nil,
		logsUntilCheckpoint: logsUntilCheckpoint,
		logCounter:          0,
	}, nil
}

func (ds *DataSaver) Close() {
	ds.writer.Flush()
	ds.file.Close()
}

// / ----------------------------------------- FUNCIONES DE GUARDADO ------------------------------------------

// Para que el worker no se preocupe de realizar el checkpoint
// este se hara cuando el saver detecte cumplimiento de su condicion de creacion
func (ds *DataSaver) Save(content any, w worker.Worker) {
	ds.Log(content) // persistencia de datos
	//Crash(CrashAfterLog) // para testing
	// gestion del checkpoint
	ds.logCounter++
	if ds.logCounter >= ds.logsUntilCheckpoint {
		slog.Info("Guardando checkpoint")
		ds.logCounter = 0
		ds.SaveCheckpoint(w.GetCheckpointData())
		//	Crash(CrashAfterCheckpoint) // para testing
	}
}

// logueo del resultado procesado
func (ds *DataSaver) Log(v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}
	record := FileRecord{Type: LogType, Payload: payload}
	jsonData, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	// write and flush
	if _, err := ds.writer.Write(append(jsonData, '\n')); err != nil {
		return fmt.Errorf("writing transaction batch to disk: %w", err)
	}
	Crash(CrashBeforeFlush)
	if err := ds.writer.Flush(); err != nil {
		return fmt.Errorf("flushing transaction batch buffer: %w", err)
	}

	return nil
}

// SaveCheckpoint debe ser utilizado como opcion de guardado una vez procesadas multiples transacciones
// para evitar cuellos de botella.
// Este metodo escribe un checkpoint al comienzo del archivo eliminando logs viejos de forma atomica.
func (ds *DataSaver) SaveCheckpoint(checkpoint any) error {
	payload, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	record := FileRecord{Type: CheckpointType, Payload: payload}
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("checkpoint serialization failed: %w", err)
	}

	return ds.writeFile(data)
}

// escritura atomica
// WriteFile escribe los datos en un archivo temporal y pisa el archivo persistente una vez exitosa la escritura.
func (ds *DataSaver) writeFile(data []byte) (err error) {
	ds.file.Close()

	fi, err := os.Stat(ds.restorationFileName)
	if err == nil && !fi.Mode().IsRegular() {
		return fmt.Errorf("%s already exists and is not a regular file", ds.restorationFileName)
	}
	f, err := os.CreateTemp(filepath.Dir(ds.restorationFileName), filepath.Base(ds.restorationFileName)+".tmp")
	if err != nil {
		return err
	}

	tmpName := f.Name()
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(tmpName)
		}
	}()
	if _, err := f.Write(append(data, '\n')); err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	// Rename
	if err := os.Rename(tmpName, ds.restorationFileName); err != nil {
		return err
	}

	// reseteamos el puntero al archivo y el writer
	file, err := os.OpenFile(ds.restorationFileName, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}

	// para evitar q los logs pisen el checkpoint en la primer escritura
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		file.Close()
		return fmt.Errorf("failed to seek to end of file: %w", err)
	}

	ds.file = file
	ds.writer = bufio.NewWriter(file)

	return nil
}

// / ----------------------------------------- FUNCIONES DE CARGA ------------------------------------------

// Lee el primer elemento guardado en el archivo.
// En caso de ser un checkpoint y coincidir con el tipo de la variable target la actualizara in place
// y dejara el decoder en un estado consistente para realizar a continuacion la recuperacion de los logs.
func (ds *DataSaver) GetRestaurationCheckpoint(target any) (bool, error) {
	slog.Info("obteniendo checkpoint")
	if ds.file == nil || !ds.fileExists() {
		return false, fmt.Errorf("pointer to restoration file is null")
	}

	decoder := json.NewDecoder(ds.file)
	ds.reader = decoder

	var record FileRecord
	if err := decoder.Decode(&record); err != nil {
		if errors.Is(err, io.EOF) {
			slog.Info("No hay checkpoint que restaurar")
			return false, nil
		}
		return false, fmt.Errorf("reading data file: %w", err)
	}

	if record.Type == CheckpointType {
		if err := json.Unmarshal(record.Payload, target); err != nil {
			return false, fmt.Errorf("failed to unmarshal checkpoint payload: %w", err)
		}
		// InputOffset() returns the byte offset consumed so far by the decoder
		ds.validOffset = decoder.InputOffset()
		return true, nil
	}

	slog.Info("No se leyo un checkpoint type")
	ds.pendingRecord = &record
	return false, nil
}

// lee cada batch y lo parsea in place en la variable pasada por parametro
func (ds *DataSaver) GetDataFromLogs(target any) (bool, error) {

	if !ds.fileExists() {
		return false, nil
	}

	var record FileRecord

	if ds.pendingRecord != nil {
		record = *ds.pendingRecord
		ds.pendingRecord = nil
	} else {
		if ds.reader == nil {
			ds.reader = json.NewDecoder(ds.file)
		}

		if err := ds.reader.Decode(&record); err != nil {
			if errors.Is(err, io.EOF) {
				ds.reader = nil
				return false, nil
			}

			// Because this is an append-only log, any syntax error mid-file is assumed
			// to be a torn write at the tail from a crash. We cleanly truncate and stop recovery.
			slog.Warn("Detected a corrupted log entry (likely torn write). Truncating recovery here.",
				"error", err.Error(), "truncateOffset", ds.validOffset)
			if truncErr := ds.truncateToValidOffset(); truncErr != nil {
				return false, fmt.Errorf("truncating corrupted tail: %w", truncErr)
			}
			ds.reader = nil
			return false, nil
		}
	}

	if record.Type == LogType {
		if err := json.Unmarshal(record.Payload, target); err != nil {
			return false, fmt.Errorf("failed to unmarshal log payload: %w", err)
		}
	}

	// Track exact valid offset
	ds.validOffset = ds.reader.InputOffset()
	ds.logCounter++

	// Return true to indicate we successfully parsed a log and the loop should continue
	return true, nil
}

func (ds *DataSaver) truncateToValidOffset() error {
	if err := ds.file.Truncate(ds.validOffset); err != nil {
		return fmt.Errorf("truncating to offset %d: %w", ds.validOffset, err)
	}
	if _, err := ds.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seeking to new end: %w", err)
	}
	ds.writer = bufio.NewWriter(ds.file)
	return nil
}

func (ds *DataSaver) fileExists() bool {
	_, err := os.Stat(ds.file.Name())
	if err == nil {
		return true // File exists
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false // File explicitly does not exist
	}
	// File may exist but is inaccessible (e.g., permission denied)
	return false
}
