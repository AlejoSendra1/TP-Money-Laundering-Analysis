package q4_join

import (
	"log/slog"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/worker"
)

const LOGS_UNTIL_CHECKPOINT = 250

// struct usado para el guardado de checkpoints y recuperacion de datos
type CheckpointData struct {
	SourceSinkRegisters   map[int64]map[string]map[string][]string `json:"source_sink_registers"`
	BridgeWorkersNotified map[int64][]string                       `json:"bridge_workers_notified"`
}

func (j *Join) GetCheckpointData() any {
	return CheckpointData{
		SourceSinkRegisters:   j.sourceSinkRegisters,
		BridgeWorkersNotified: j.bridgeWorkersNotified,
	}
}

func (j *Join) Restaurate() error {
	j.restoring = true
	// primero restauramos el checkpoint
	var checkpoint CheckpointData

	thereIsCheckpoint, err := j.dataSaver.GetRestaurationCheckpoint(&checkpoint)
	if err != nil {
		return err
	}
	if thereIsCheckpoint == true {
		slog.Info("cargando en base a checkpoint")
		j.sourceSinkRegisters = checkpoint.SourceSinkRegisters
		j.bridgeWorkersNotified = checkpoint.BridgeWorkersNotified
	}

	var savedDataVar middleware.Message // este tipo de dato es lo unico guardado para este worker
	var thereIsLogs bool

	for {
		thereIsLogs, err = j.dataSaver.GetDataFromLogs(&savedDataVar)
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

	j.restoring = false
	return nil
}
