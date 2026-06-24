package bridge_matcher

import (
	"log/slog"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/worker"
)

const LOGS_UNTIL_CHECKPOINT = 250

// struct usado para el guardado de checkpoints y recuperacion de datos
type CheckpointData struct {
	Registers            map[int64]map[string]map[string]int `json:"registers"`
	GroupWorkersNotified map[int64][]string                  `json:"group_workers_notified"`
	BridgesReadyForEOR   map[int64][]int                     `json:"bridges_ready_for_eor"`
}

func (bm *BridgeMatcher) GetCheckpointData() any {
	return CheckpointData{
		Registers:            bm.Registers,
		GroupWorkersNotified: bm.groupWorkersNotified,
		BridgesReadyForEOR:   bm.bridgesReadyForEOR,
	}
}

func (bm *BridgeMatcher) Restaurate() error {
	bm.restoring = true
	// primero restauramos el checkpoint
	var checkpoint CheckpointData

	thereIsCheckpoint, err := bm.dataSaver.GetRestaurationCheckpoint(&checkpoint)
	if err != nil {
		return err
	}
	if thereIsCheckpoint == true {
		slog.Info("cargando en base a checkpoint")
		bm.Registers = checkpoint.Registers
		bm.groupWorkersNotified = checkpoint.GroupWorkersNotified
		bm.bridgesReadyForEOR = checkpoint.BridgesReadyForEOR
	}

	var savedDataVar middleware.Message // este tipo de dato es lo unico guardado para este worker
	var thereIsLogs bool

	for {
		thereIsLogs, err = bm.dataSaver.GetDataFromLogs(&savedDataVar)
		if err != nil { // habria q modificar para retrys
			return err
		}
		if !thereIsLogs {
			break
		}

		if err := worker.HandleMessageV2(&savedDataVar, bm.mssgHandlers); err != nil {
			return err
		}
	}

	bm.restoring = false
	return nil
}
