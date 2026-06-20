package datasaver

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"tp_distribuidos/common/worker"
)

const RESTORATION_FILE_SUFIX = "_restoration_point" + ".txt"

type DataSaver struct {
	restorationFileName string
	file                *os.File
	writer              *bufio.Writer
	reader              *bufio.Scanner
	logsUntilCheckpoint int
	logCounter          int
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
	// Open the file in Append mode (create it if it doesn't exist)
	// 0644  gives read/write permissions to the owner
	filename = filename + RESTORATION_FILE_SUFIX
	restorationFile, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return &DataSaver{}, fmt.Errorf("failed to open file: %w", err)
	}

	writer := bufio.NewWriter(restorationFile)

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

func (ds *DataSaver) Clean() {
	ds.file.Truncate(0)
}

// / ----------------------------------------- FUNCIONES DE GUARDADO ------------------------------------------
// funciones utilizadas para guardar el estado del nodo previo a una posible caida

// logueo del resultado procesado
func (ds *DataSaver) Log(v any) error {
	// wrapp content
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

	if err := ds.writer.Flush(); err != nil {
		return fmt.Errorf("flushing transaction batch buffer: %w", err)
	}

	return nil
}

// SaveCheckpoint debe ser utilizado como opcion de guardado una vez procesadas multiples transacciones
// para evitar cuellos de botella. cualquier tipo de dato es valido y guardado de forma atomica eliminando
// logs viejos.
func (ds *DataSaver) SaveCheckpoint(checkpoint any) error {
	// wrapps the content
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

// Para que el worker no se preocupe de realizar el checkpoint
// este se hara cuando el saver detecte cumplimiento de su condicion de creacion
func (ds *DataSaver) Save(content any, w worker.Worker) {
	ds.Log(content)      // persistencia de datos
	Crash(CrashAfterLog) // para testing
	// gestion del checkpoint
	ds.logCounter++
	if ds.logCounter >= ds.logsUntilCheckpoint {
		slog.Info("Guardando checkpoint")
		ds.logCounter = 0
		ds.SaveCheckpoint(w.GetCheckpointData())
	}
}

// escritura atomica
// WriteFile writes data to filename+some suffix, then renames it into filename.
// The perm argument but if the target filename already
// exists then the target file's attributes and ACLs are preserved. If the target
// filename already exists but is not a regular file, WriteFile returns an error.
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
	file, err := os.OpenFile(ds.restorationFileName, os.O_RDWR, 0644)
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
// funciones utilizadas para recuperar el estado del nodo postiormente a su recuperacion
func (ds *DataSaver) GetRestaurationCheckpoint(target any) (bool, error) {
	if ds.file == nil {
		return false, fmt.Errorf("pointer to restoration file is null")
	}

	scanner := bufio.NewScanner(ds.file)
	const maxCapacity = 10 * 1024 * 1024 // 10 MB
	buf := make([]byte, bufio.MaxScanTokenSize)
	scanner.Buffer(buf, maxCapacity)
	ds.reader = scanner

	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return false, fmt.Errorf("reading data file: %w", err) //
		}
		slog.Info("No hay nada que restaurar")
		return false, nil
	}

	line := scanner.Bytes()
	if len(line) == 0 {
		ds.reader = nil
		return false, nil
	}

	var record FileRecord
	if err := json.Unmarshal(line, &record); err != nil {
		return false, fmt.Errorf("parsing checkpoint: %w", err) //
	}

	if record.Type == CheckpointType {
		if err := json.Unmarshal(record.Payload, target); err != nil {
			return false, fmt.Errorf("failed to unmarshal checkpoint payload: %w", err) //
		}
		return true, nil
	}
	slog.Info("No se leyo un checkpoint type")
	ds.reader = nil
	return false, nil
}

// lee cada batch y lo parsea in place en la variable indicada por parametro
func (ds *DataSaver) GetDataFromLogs(target any) (bool, error) {
	if ds.reader == nil {
		scanner := bufio.NewScanner(ds.file)
		const maxCapacity = 10 * 1024 * 1024 // 10 MB
		buf := make([]byte, bufio.MaxScanTokenSize)
		scanner.Buffer(buf, maxCapacity)
		ds.reader = scanner
	}

	// Scan through the file line by line
	thereIsMore := ds.reader.Scan()

	if err := ds.reader.Err(); err != nil {
		return false, fmt.Errorf("error reading file: %w", err)
	}

	if !thereIsMore {
		ds.reader = nil
		return false, nil
	}

	line := ds.reader.Bytes()
	if len(line) == 0 {
		ds.reader = nil
		return false, nil
	}

	var record FileRecord
	if err := json.Unmarshal(line, &record); err != nil {
		return false, fmt.Errorf("parsing Log: %w", err)
	}

	if record.Type == LogType {
		if err := json.Unmarshal(record.Payload, target); err != nil {
			return false, fmt.Errorf("failed to unmarshal checkpoint payload: %w", err)
		}

	}

	return thereIsMore, nil
}
