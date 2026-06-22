package group

import (
	"log/slog"
)

const LOGS_UNTIL_CHECKPOINT = 20

// struct usado para el guardado de checkpoints y recuperacion de datos
type CheckpointData struct {
	EofCounter map[int64]map[string]int `json:"eofCounter"`
}
type EORdata struct {
	CliID  int64  `json:"cli_id"`
	Sender string `json:"sender"`
}

func (g *Group) GetCheckpointData() any {
	return CheckpointData{
		EofCounter: g.eofCounter,
	}
}

func (g *Group) Restaurate() error {
	// primero restauramos el checkpoint
	var checkpoint CheckpointData

	thereIsCheckpoint, err := g.datasaver.GetRestaurationCheckpoint(&checkpoint)
	if err != nil {
		return err
	}
	if thereIsCheckpoint == true {
		slog.Info("cargando en base a checkpoint")
		g.eofCounter = checkpoint.EofCounter
	}

	var savedDataVar EORdata // este tipo de dato es lo unico guardado para este worker
	var thereIsLogs bool

	for {
		thereIsLogs, err = g.datasaver.GetDataFromLogs(&savedDataVar)
		if err != nil { // habria q modificar para retrys
			return err
		}
		if !thereIsLogs {
			break
		}

		if err := g.handleClientFinalization(savedDataVar.CliID, savedDataVar.Sender); err != nil {
			return err
		}
	}

	return nil
}
