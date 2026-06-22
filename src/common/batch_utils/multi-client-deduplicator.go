package batch_utils

import "sync"

type MultiClientDeduplicator struct {
	deduplicatorByClient map[int64]*BatchDeduplicator
	maxBatchSize         int
	mutex                sync.Mutex
}

func NewMultiClientDeduplicator(maxBatchSize int) *MultiClientDeduplicator {
	return &MultiClientDeduplicator{
		deduplicatorByClient: make(map[int64]*BatchDeduplicator),
		maxBatchSize:         maxBatchSize,
	}
}

func (m *MultiClientDeduplicator) IsDuplicate(clientID int64, id BatchID) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.getOrCreate(clientID).IsDuplicate(id)
}

func (m *MultiClientDeduplicator) Load(clientID int64, id BatchID) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.getOrCreate(clientID).Load(id)
}

func (m *MultiClientDeduplicator) RemoveClient(clientID int64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.deduplicatorByClient, clientID)
}

func (m *MultiClientDeduplicator) getOrCreate(clientID int64) *BatchDeduplicator {
	if _, exists := m.deduplicatorByClient[clientID]; !exists {
		m.deduplicatorByClient[clientID] = NewBatchDeduplicator(m.maxBatchSize)
	}
	return m.deduplicatorByClient[clientID]
}

// para desencapsular
func (m *MultiClientDeduplicator) IsDuplicateNoUpdate(clientID int64, id BatchID) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.getOrCreate(clientID).IsDuplicateNoUpdate(id)
}
