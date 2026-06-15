package batch_utils

import "sync"

type MultiClientDeduplicator struct {
	deduplicatorByClient map[int]*BatchDeduplicator
	maxBatchSize         int
	mutex                sync.Mutex
}

func NewMultiClientDeduplicator(maxBatchSize int) *MultiClientDeduplicator {
	return &MultiClientDeduplicator{
		deduplicatorByClient: make(map[int]*BatchDeduplicator),
		maxBatchSize:         maxBatchSize,
	}
}

func (m *MultiClientDeduplicator) IsDuplicate(clientID int, id BatchID) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.getOrCreate(clientID).IsDuplicate(id)
}

func (m *MultiClientDeduplicator) Load(clientID int, id BatchID) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.getOrCreate(clientID).Load(id)
}

func (m *MultiClientDeduplicator) RemoveClient(clientID int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.deduplicatorByClient, clientID)
}

func (m *MultiClientDeduplicator) getOrCreate(clientID int) *BatchDeduplicator {
	if _, exists := m.deduplicatorByClient[clientID]; !exists {
		m.deduplicatorByClient[clientID] = NewBatchDeduplicator(m.maxBatchSize)
	}
	return m.deduplicatorByClient[clientID]
}
