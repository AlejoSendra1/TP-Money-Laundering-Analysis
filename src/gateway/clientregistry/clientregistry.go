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
	mutex                   sync.Mutex
	clients                 map[int64]*ClientState
	sequenceNumbersByClient map[int64]int64
	sentEORByClient         map[int64]bool
}

func NewClientRegistry() ClientRegistry {
	return ClientRegistry{
		clients:                 make(map[int64]*ClientState),
		sequenceNumbersByClient: make(map[int64]int64),
		sentEORByClient:         make(map[int64]bool),
	}
}

func (registry *ClientRegistry) Add(clientID int64, client ClientState) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	registry.clients[clientID] = &client
}

func (registry *ClientRegistry) Remove(id int64) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	delete(registry.clients, id)
	delete(registry.sequenceNumbersByClient, id)
	delete(registry.sentEORByClient, id)
}

func (registry *ClientRegistry) WithLock(action func(map[int64]*ClientState)) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	action(registry.clients)
}

// para manejo de los repetidos del lado externo / cliente
func (registry *ClientRegistry) GetAndIncrementSequence(clientID int64) int64 {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	seq := registry.sequenceNumbersByClient[clientID]
	registry.sequenceNumbersByClient[clientID]++
	return seq
}

func (registry *ClientRegistry) UserSentEOF(clientID int64) bool {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	exist, _ := registry.sentEORByClient[clientID]
	registry.sentEORByClient[clientID] = true
	return exist
}
