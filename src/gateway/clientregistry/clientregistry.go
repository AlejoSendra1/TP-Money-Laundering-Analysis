package clientregistry

import (
	"net"
	"sync"

	"tp_distribuidos/messagehandler"
)

type ClientState struct {
	Conn    net.Conn
	Handler *messagehandler.MessageHandler
	AckCh   chan struct{}
}

type ClientRegistry struct {
	mutex   sync.Mutex
	clients map[int64]*ClientState

	sequenceNumbersByClientMutex sync.Mutex
	sequenceNumbersByClient      map[int64]int64

	sentEORByClientMutex sync.Mutex
	sentEORByClient      map[int64]bool

	sentSecuenceNumberByClientMutex sync.Mutex
	sentSecuenceNumberByClient      map[int64]int64
}

func NewClientRegistry() ClientRegistry {
	return ClientRegistry{
		clients:                    make(map[int64]*ClientState),
		sequenceNumbersByClient:    make(map[int64]int64),
		sentEORByClient:            make(map[int64]bool),
		sentSecuenceNumberByClient: make(map[int64]int64),
	}
}

func (registry *ClientRegistry) Add(clientID int64, client ClientState) {
	registry.mutex.Lock()
	registry.clients[clientID] = &client
	registry.mutex.Unlock()

	registry.sentSecuenceNumberByClientMutex.Lock()
	registry.sentSecuenceNumberByClient[clientID] = 0
	registry.sentSecuenceNumberByClientMutex.Unlock()
}

func (registry *ClientRegistry) Remove(id int64) {
	registry.mutex.Lock()
	delete(registry.clients, id)
	registry.mutex.Unlock()

	registry.sequenceNumbersByClientMutex.Lock()
	delete(registry.sequenceNumbersByClient, id)
	registry.sequenceNumbersByClientMutex.Unlock()

	registry.sentEORByClientMutex.Lock()
	delete(registry.sentEORByClient, id)
	registry.sentEORByClientMutex.Unlock()

	registry.sentSecuenceNumberByClientMutex.Lock()
	delete(registry.sentSecuenceNumberByClient, id)
	registry.sentSecuenceNumberByClientMutex.Unlock()
}

func (registry *ClientRegistry) WithLock(action func(map[int64]*ClientState)) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	action(registry.clients)
}

// para manejo de los repetidos del lado externo / cliente - provenientes del cliente
func (registry *ClientRegistry) GetSecuenceNumber(clientID int64) int64 {
	registry.sequenceNumbersByClientMutex.Lock()
	defer registry.sequenceNumbersByClientMutex.Unlock()
	return registry.sequenceNumbersByClient[clientID]
}

func (registry *ClientRegistry) IncrementSequenceNumber(clientID int64) {
	registry.sequenceNumbersByClientMutex.Lock()
	defer registry.sequenceNumbersByClientMutex.Unlock()
	registry.sequenceNumbersByClient[clientID]++
}

func (registry *ClientRegistry) UserSentEOF(clientID int64) bool {
	registry.sentEORByClientMutex.Lock()
	defer registry.sentEORByClientMutex.Unlock()
	exist, _ := registry.sentEORByClient[clientID]
	registry.sentEORByClient[clientID] = true
	return exist
}

/// -------------------------------------------------------------------------------------

// para manejo de los repetidos del lado externo / cliente - enviados al cliente
func (registry *ClientRegistry) GetSecuenceNumberToSent(clientID int64) int64 {
	registry.sentSecuenceNumberByClientMutex.Lock()
	defer registry.sentSecuenceNumberByClientMutex.Unlock()
	return registry.sentSecuenceNumberByClient[clientID]
}

func (registry *ClientRegistry) IncrementSequenceNumberToSent(clientID int64) {
	registry.sentSecuenceNumberByClientMutex.Lock()
	defer registry.sentSecuenceNumberByClientMutex.Unlock()
	registry.sentSecuenceNumberByClient[clientID]++
}
