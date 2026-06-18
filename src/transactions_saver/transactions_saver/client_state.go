package transactions_saver

import (
	"log/slog"
	"sync"
	"tp_distribuidos/common/batch_utils"
)

type Stage int

const (
	StageBufferingData  Stage = iota // Recibe datos y los baja a disco (espera EOFs)
	StageFlushingDisk                // Llegaron los promedios, está leyendo el disco y mandando a la red
	StageSendingNetwork              // El disco ya esta vacio, los datos nuevos van directo a output queue
)

type ClientState struct {
	mu               sync.Mutex // Protege el estado del cliente
	stage            Stage
	notificationEOFs batch_utils.Set[string]
	receivedFlowEOF  bool
	qtyTx            int      // Para debug
	Storage          *Storage // Abstraccion del archivo
}

func NewClientState(filePath string, name string) *ClientState {
	return &ClientState{
		notificationEOFs: batch_utils.NewSet[string](),
		stage:            StageBufferingData,
		Storage:          NewStorage(filePath, name),
	}
}

// Verifica si se debe almacenar los datos o enviarlos directamente al output
func (clientState *ClientState) ShouldBuffData(count int) bool {
	clientState.mu.Lock()
	defer clientState.mu.Unlock()
	clientState.qtyTx += count

	// Si ya flusheamos o estamos en eso, le avisamos al caller que mande directo a la output queue
	if clientState.stage == StageFlushingDisk || clientState.stage == StageSendingNetwork {
		return false
	}
	// Por defecto, seguimos acumulando en disco
	return true
}

// Verifica si llegaron todas las notificaciones
func (clientState *ClientState) ShouldStartFlush(maxAmount int, sender string) (shouldFlush bool) {
	clientState.mu.Lock()
	defer clientState.mu.Unlock()

	clientState.notificationEOFs.Add(sender)
	// Si todavia faltan notificaciones de los promedios, no hacemos nada
	if clientState.notificationEOFs.Size() != maxAmount {
		return false
	}

	// Ya estan los promedios, cambiamos de estado para vaciar el disco
	clientState.stage = StageFlushingDisk
	return true
}

// Marca que ya flusheo y checkea si puede terminar
func (clientState *ClientState) MarkFlushAndCheckFinish() bool {
	clientState.mu.Lock()
	defer clientState.mu.Unlock()

	clientState.stage = StageSendingNetwork
	if clientState.shouldFinish() {
		return true
	}
	return false
}

// Marca que recibio el EOF y checkea si puede terminar
func (clientState *ClientState) MarkEOFAndCheckFinish() bool {
	clientState.mu.Lock()
	defer clientState.mu.Unlock()

	clientState.receivedFlowEOF = true
	if clientState.shouldFinish() {
		return true
	}
	return false
}

// Solo podemos cerrar si el flujo de datos termino (recibi eof) y el disco ya fue vaciado
func (clientState *ClientState) shouldFinish() bool {
	slog.Info("Size transactions send:", "size", clientState.qtyTx)
	return clientState.receivedFlowEOF && (clientState.stage == StageSendingNetwork)
}
