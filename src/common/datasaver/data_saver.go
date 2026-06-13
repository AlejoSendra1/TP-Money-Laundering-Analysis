package datasaver

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type DataSaver struct {
	checkpointFileName string
	logsFile           *os.File
	logsWriter         *bufio.Writer
	logsReader         *bufio.Scanner
}

func NewDataSaver(filename string) (*DataSaver, error) {
	// Open the file in Append mode (create it if it doesn't exist)
	// 0644 gives read/write permissions to the owner
	logsFile, err := os.OpenFile(filename+"_"+"logs", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return &DataSaver{}, fmt.Errorf("failed to open file: %w", err)
	}

	writer := bufio.NewWriter(logsFile)

	return &DataSaver{
		checkpointFileName: filename + "_" + "checkpoint",
		logsFile:           logsFile,
		logsWriter:         writer,
		logsReader:         nil,
	}, nil
}

func (ds *DataSaver) Close() {
	ds.logsFile.Close()
}

// / ----------------------------------------- FUNCIONES DE GUARDADO ------------------------------------------
// funciones utilizadas para guardar el estado del nodo previo a una posible caida

// logueo del resultado procesado
func (ds *DataSaver) Log(v any) error {
	// Convert struct to JSON text bytes
	jsonData, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	if _, err := ds.logsWriter.Write(append(jsonData, '\n')); err != nil {
		return fmt.Errorf("writing transaction batch to disk: %w", err)
	}

	if err := ds.logsWriter.Flush(); err != nil {
		return fmt.Errorf("flushing transaction batch buffer: %w", err)
	}

	return nil
}

// SaveCheckpoint debe ser utilizado como opcion de guardado una vez procesadas multiples transacciones
// para evitar cuellos de botella. cualquier tipo de dato es valido y guardado de forma atomica eliminando
// logs viejos.
func (ds *DataSaver) SaveCheckpoint(checkpoint any) error {
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return fmt.Errorf("checkpoint serialization failed: %w", err)
	}
	return ds.writeFile(data)
}

// escritura atomica
// chequear

// WriteFile writes data to filename+some suffix, then renames it into filename.
// The perm argument but if the target filename already
// exists then the target file's attributes and ACLs are preserved. If the target
// filename already exists but is not a regular file, WriteFile returns an error.
func (ds *DataSaver) writeFile(data []byte) (err error) {
	fi, err := os.Stat(ds.checkpointFileName)
	if err == nil && !fi.Mode().IsRegular() {
		return fmt.Errorf("%s already exists and is not a regular file", ds.checkpointFileName)
	}
	f, err := os.CreateTemp(filepath.Dir(ds.checkpointFileName), filepath.Base(ds.checkpointFileName)+".tmp")
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
	if _, err := f.Write(data); err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	// Rename srcFile to dstFile
	if err := os.Rename(tmpName, ds.checkpointFileName); err != nil {
		return err
	}

	// reseteamos el puntero al archivo y el writer
	file, err := os.OpenFile(ds.logsFile.Name(), os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	ds.logsFile = file

	writer := bufio.NewWriter(file)
	ds.logsWriter = writer

	return nil
}

// / ----------------------------------------- FUNCIONES DE CARGA ------------------------------------------
// funciones utilizadas para recuperar el estado del nodo postiormente a su recuperacion
func (ds *DataSaver) GetRestaurationCheckpoint(target any) error {
	checkpointFile, err := os.OpenFile(ds.checkpointFileName, os.O_RDONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer checkpointFile.Close()

	scanner := bufio.NewScanner(checkpointFile)

	scanner.Scan()

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file: %w", err)
	}

	// Unmarshal the bytes back into the struct pointer
	return json.Unmarshal(scanner.Bytes(), target)
}

// lee cada batch y lo parsea in place en la variable indicada por parametro
func (ds *DataSaver) GetDataFromLogs(target any) (bool, error) {
	if ds.logsReader == nil {
		ds.logsReader = bufio.NewScanner(ds.logsFile)
	}

	// Scan through the file line by line
	thereIsMore := ds.logsReader.Scan()

	if err := ds.logsReader.Err(); err != nil {
		return false, fmt.Errorf("error reading file: %w", err)
	}

	// Unmarshal the bytes back into the struct pointer
	err := json.Unmarshal(ds.logsReader.Bytes(), target)
	if err != nil {
		return false, fmt.Errorf("failed to unmarshal line data: %w", err)
	}

	if !thereIsMore {
		ds.logsReader = nil
	}

	return thereIsMore, nil
}
