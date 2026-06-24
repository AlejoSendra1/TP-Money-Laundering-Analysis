package client

import (
	"log/slog"
)

// agregar a vars de entorno
const LOGS_UNTIL_CHECKPOINT = 200

type LogData struct {
	QueryCode  uint32     `json:"query_code"`
	QueryState QueryState `json:"query_state"`
}

type QueryState struct {
	LastSecNum      int64 `json:"last_sec_num"`
	LastWriteOffset int64 `json:"file_offset"`
}

type CheckpointData struct {
	AssignedID    int64 `json:"assigned_id"`
	BatchesSecNum int64 `json:"batches_sec_num"`
	// para el caso de restaurateResultsState
	ReceivingSecNum  int64                 `json:"receiving_sec_num"`
	ProcessedQueries map[uint32]QueryState `json:"processed_queries"`
}

func (client *Client) GetCheckpointData() any {
	client.mutex.Lock()
	defer client.mutex.Unlock()
	return CheckpointData{
		AssignedID:       client.assignedID,
		BatchesSecNum:    client.BatchesSecNum,
		ProcessedQueries: client.processedQueries,
	}
}
func (client *Client) restaurateState() error {
	var checkpoint CheckpointData
	thereIsCheckpoint, err := client.dataSaver.GetRestaurationCheckpoint(&checkpoint)
	if err != nil {
		return err
	}
	if !thereIsCheckpoint {
		id, err := sendConnectMsg(client.conn) // para obtener el id en caso de requerir reconexion
		if err != nil {
			return err
		}
		client.assignedID = id
		slog.Info("Connection was succefull", "Id assigned", client.assignedID)
		return nil
	}

	slog.Info("Checkpoint levantado", "values", checkpoint)
	client.assignedID = checkpoint.AssignedID
	client.BatchesSecNum = checkpoint.BatchesSecNum

	var b byte
	for {
		thereIsLogs, err := client.dataSaver.GetDataFromLogs(&b)
		if err != nil {
			return err
		}
		if !thereIsLogs {
			break
		}
		client.BatchesSecNum++
	}

	if err := client.restaurateResultsState(); err != nil { // restauramos la fase de envio
		return err
	}
	slog.Info("cargando en base a checkpoint")
	err = sendReconnectMsg(client.conn, client.assignedID)
	if err != nil {
		return err
	}
	slog.Info("Connection was succefull", "Id recuperated", client.assignedID)

	return nil
}

// restaurateResultsState rolls back any uncommitted, corrupted, or duplicated CSV lines written right before a crash.
func (client *Client) restaurateResultsState() error {
	var checkpoint CheckpointData

	thereIsCheckpoint, err := client.resultsLogsSaver.GetRestaurationCheckpoint(&checkpoint)
	if err != nil {
		return err
	}

	if thereIsCheckpoint && checkpoint.ProcessedQueries != nil {
		client.processedQueries = checkpoint.ProcessedQueries
	} else {
		client.processedQueries = make(map[uint32]QueryState)
	}

	var savedDataVar LogData
	var thereIsLogs bool

	for {
		thereIsLogs, err = client.resultsLogsSaver.GetDataFromLogs(&savedDataVar)
		if err != nil { // habria q modificar para retrys
			return err
		}
		if !thereIsLogs {
			break
		}
		client.processedQueries[savedDataVar.QueryCode] = savedDataVar.QueryState
	}

	for qCode, state := range client.processedQueries {
		slog.Info("Rolling back partial CSV writes", "query", qCode, "targetOffset", state.LastWriteOffset)
		if err := client.writer.TruncateFile(qCode, state.LastWriteOffset); err != nil {
			slog.Error("Failed to roll back CSV state", "query", qCode, "err", err)
			return err
		}
	}
	return nil
}
