package transactions_saver

import (
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
	mu               sync.Mutex              `json:"-"` // Protege el estado del cliente
	Stage            Stage                   `json:"stage"`
	NotificationEOFs batch_utils.Set[string] `json:"notificationEOFs"`
	ReceivedFlowEOF  bool                    `json:"receivedFlowEOF"`
	StorageFilePath  string                  `json:"storageFilePath"`
	StorageFileName  string                  `json:"storageFileName"`
	Storage          *Storage                // Abstraccion del archivo
}

func NewClientState(filePath string, name string) *ClientState {
	return &ClientState{
		NotificationEOFs: batch_utils.NewSet[string](),
		Stage:            StageBufferingData,
		StorageFilePath:  filePath,
		StorageFileName:  name,
		Storage:          NewStorage(filePath, name),
	}
}

// Verifica si se debe almacenar los datos o enviarlos directamente al output
func (clientState *ClientState) ShouldBuffData() bool {
	clientState.mu.Lock()
	defer clientState.mu.Unlock()

	// Si ya flusheamos o estamos en eso, le avisamos al caller que mande directo a la output queue
	if clientState.Stage == StageFlushingDisk || clientState.Stage == StageSendingNetwork {
		return false
	}
	// Por defecto, seguimos acumulando en disco
	return true
}

// Verifica si llegaron todas las notificaciones
func (clientState *ClientState) ShouldStartFlush(maxAmount int, sender string) (shouldFlush bool) {
	clientState.mu.Lock()
	defer clientState.mu.Unlock()

	clientState.NotificationEOFs.Add(sender)
	// Si todavia faltan notificaciones de los promedios, no hacemos nada
	if clientState.NotificationEOFs.Size() != maxAmount {
		return false
	}

	// Ya estan los promedios, cambiamos de estado para vaciar el disco
	clientState.Stage = StageFlushingDisk
	return true
}

// Marca que ya flusheo y checkea si puede terminar
func (clientState *ClientState) MarkFlushAndCheckFinish() bool {
	clientState.mu.Lock()
	defer clientState.mu.Unlock()

	clientState.Stage = StageSendingNetwork
	if clientState.shouldFinish() {
		return true
	}
	return false
}

// Marca que recibio el EOF y checkea si puede terminar
func (clientState *ClientState) MarkEOFAndCheckFinish() bool {
	clientState.mu.Lock()
	defer clientState.mu.Unlock()

	clientState.ReceivedFlowEOF = true
	if clientState.shouldFinish() {
		return true
	}
	return false
}

// Solo podemos cerrar si el flujo de datos termino (recibi eof) y el disco ya fue vaciado
func (clientState *ClientState) shouldFinish() bool {
	return clientState.ReceivedFlowEOF && (clientState.Stage == StageSendingNetwork)
}
