package join_q2

import (
	"fmt"
	"log/slog"
	"tp_distribuidos/common/datasaver"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
	"tp_distribuidos/common/transaction"
)

type FlushHandler struct {
	datasaver    *datasaver.DataSaver
	owner        *JoinQ2
	pendingFlush map[int64]flushState // clients that are mid-flush (resumed after crash)
}

// flushState holds the sorted rows for a client that is mid-flush, together
// with the index of the next chunk that still needs to be sent.  It is kept
// in memory (and mirrored to the DataSaver) so that a crashed worker can
// resume exactly where it left off instead of re-sending already-delivered
// chunks or starting over from the beginning.
type flushState struct {
	rows      []transaction.MaxBankTransaction // deterministically sorted full result set
	nextChunk int                              // index (in chunks, not rows) of the next unsent batch
}

func NewFlushHandler(workerName string) (*FlushHandler, error) {
	dataSaver, err := datasaver.NewDataSaver(fmt.Sprintf("/persistence/%s_flushing", workerName), LOGS_UNTIL_CHECKPOINT)
	if err != nil {
		return nil, err
	}
	return &FlushHandler{
		datasaver:    dataSaver,
		pendingFlush: make(map[int64]flushState), // add this
	}, nil
}

func (fh *FlushHandler) GetCheckpointData() any {
	return fh.pendingFlush
}

func (fh *FlushHandler) restaurate() error {
	// primero restauramos el checkpoint
	var checkpoint map[int64]flushState

	thereIsCheckpoint, err := fh.datasaver.GetRestaurationCheckpoint(&checkpoint)
	if err != nil {
		return err
	}
	if thereIsCheckpoint == true {
		slog.Info("cargando en base a checkpoint")
		if len(checkpoint) != 0 {
			fh.pendingFlush = checkpoint
		}
	}
	if fh.pendingFlush == nil {
		fh.pendingFlush = make(map[int64]flushState)
	}

	var newStartingPointByClient map[int64]int64 = make(map[int64]int64)
	var savedDataVar int64 // este tipo de dato es lo unico guardado para este worker
	var thereIsLogs bool
	for {
		thereIsLogs, err = fh.datasaver.GetDataFromLogs(&savedDataVar)
		if err != nil { // habria q modificar para retrys
			return err
		}
		if !thereIsLogs {
			break
		}
		newStartingPointByClient[savedDataVar] += 1
	}

	for client, newStartingPoint := range newStartingPointByClient {
		flushState := fh.pendingFlush[client]
		// Add log count to the checkpoint's nextChunk (don't overwrite it),
		// because the checkpoint may already reflect partial progress.
		flushState.nextChunk += int(newStartingPoint)
		fh.pendingFlush[client] = flushState
	}
	return nil
}

// resumePendingFlushes re-drives any flushState that was persisted but not
// completed before the last crash.  It is called once, at startup, before
// the worker begins consuming new messages.
func (fh *FlushHandler) resumePendingFlushes(batchSize int, outputQueue *middleware.Middleware, eofFunc func(int64) error) error {
	if err := fh.restaurate(); err != nil {
		return err
	}
	for clientID, fs := range fh.pendingFlush {
		slog.Info("Resuming interrupted flush", "client_id", clientID, "next_chunk", fs.nextChunk)
		if err := fh.SendChunksFrom(clientID, &fs, batchSize, outputQueue); err != nil {
			slog.Error("Resuming flush failed", "client_id", clientID, "err", err)
			// Leave the state intact – the next restart will try again.
			continue
		}
		if err := eofFunc(clientID); err != nil {
			slog.Error("Resuming flush: sendEOF failed", "client_id", clientID, "err", err)
			continue
		}
		fh.Delete(clientID)
	}
	return nil
}

// sendChunksFrom sends all chunks for clientID starting from fs.nextChunk,
// advancing the persisted offset after each successful send so a crash can
// resume from the correct position.
func (fh *FlushHandler) SendChunksFrom(clientID int64, fs *flushState, batchSize int, outputQueue *middleware.Middleware) error {
	totalRows := len(fs.rows)

	// Convert chunk index to row index and iterate forward.
	startRow := fs.nextChunk * batchSize
	for i := startRow; i < totalRows; i += batchSize {
		end := i + batchSize
		if end > totalRows {
			end = totalRows
		}
		chunk := fs.rows[i:end]

		msg, err := inner.SerializeQueryResultMessage(clientID, transaction.QueryResult{
			QueryID:      transaction.Query2,
			Transactions: chunk,
		})
		if err != nil {
			return fmt.Errorf("serializing data chunk: %w", err)
		}
		if err := (*outputQueue).Send(*msg); err != nil {
			return fmt.Errorf("sending data chunk: %w", err)
		}

		// Advance the cursor and persist it so a crash after this point
		// skips this chunk on the next startup.
		fs.nextChunk++
		// Also update the map entry so that any automatic DataSaver checkpoint
		// reflects the real progress (the map stores values, not pointers).
		fh.pendingFlush[clientID] = *fs
		fh.datasaver.Save(clientID, fh)
	}
	return nil
}

func (fh *FlushHandler) SaveFlushState(clientID int64, fs *flushState) {
	fh.pendingFlush[clientID] = *fs
	fh.datasaver.SaveCheckpoint(fh.pendingFlush)
}

func (fh *FlushHandler) Delete(clientID int64) {
	delete(fh.pendingFlush, clientID)
	fh.datasaver.SaveCheckpoint(fh.pendingFlush)
}
