package join_q2

import (
	"log/slog"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/worker"
)

const LOGS_UNTIL_CHECKPOINT = 250

// struct usado para el guardado de checkpoints y recuperacion de datos
type CheckpointData struct {
	TopByClient      map[int64]map[int]bankEntry `json:"top_by_client"`
	EofCountByClient map[int64][]string          `json:"eof_count_by_client"`
}

func (j *JoinQ2) GetCheckpointData() any {
	return CheckpointData{
		TopByClient:      j.topByClient,
		EofCountByClient: j.eofCountByClient,
	}
}

func (j *JoinQ2) Restaurate() error {
	//j.restoring = true
	// primero restauramos el checkpoint
	var checkpoint CheckpointData

	thereIsCheckpoint, err := j.datasaver.GetRestaurationCheckpoint(&checkpoint)
	if err != nil {
		return err
	}
	if thereIsCheckpoint == true {
		slog.Info("cargando en base a checkpoint")
		if checkpoint.TopByClient != nil {
			j.topByClient = checkpoint.TopByClient
		}
		if checkpoint.EofCountByClient != nil {
			j.eofCountByClient = checkpoint.EofCountByClient
		}
	}

	var savedDataVar middleware.Message // este tipo de dato es lo unico guardado para este worker
	var thereIsLogs bool

	for {
		thereIsLogs, err = j.datasaver.GetDataFromLogs(&savedDataVar)
		if err != nil { // habria q modificar para retrys
			return err
		}
		if !thereIsLogs {
			break
		}
		if err := worker.HandleMessageV2(&savedDataVar, j.mssgHandlers); err != nil {
			return err
		}
	}

	//j.restoring = false
	return nil
}
