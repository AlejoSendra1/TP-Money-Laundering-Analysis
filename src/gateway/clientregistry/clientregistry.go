package clientregistry

import (
	"net"
	"sync"

	"tp_distribuidos/messagehandler"
)

type ClientState struct {
	Conn    net.Conn
	Handler *messagehandler.MessageHandler
}

type ClientRegistry struct {
	mutex   sync.Mutex
	clients map[int64]*ClientState
}

func NewClientRegistry() ClientRegistry {
	return ClientRegistry{clients: make(map[int64]*ClientState)}
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
}

func (registry *ClientRegistry) WithLock(action func(map[int64]*ClientState)) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	action(registry.clients)
}

func (registry *ClientRegistry) GetClients() *(map[int64]*ClientState) {
	return &registry.clients
}
