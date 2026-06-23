package batch_utils

import "sync"

type MultiClientDeduplicator struct {
	DeduplicatorByClient map[int64]*BatchDeduplicator `json:"batchDeduplicator"`
	MaxBatchSize         int                          `json:"maxBatchSize"`
	mutex                sync.Mutex                   `json:"-"`
}

func NewMultiClientDeduplicator(maxBatchSize int) *MultiClientDeduplicator {
	return &MultiClientDeduplicator{
		DeduplicatorByClient: make(map[int64]*BatchDeduplicator),
		MaxBatchSize:         maxBatchSize,
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
	delete(m.DeduplicatorByClient, clientID)
}

func (m *MultiClientDeduplicator) getOrCreate(clientID int64) *BatchDeduplicator {
	if _, exists := m.DeduplicatorByClient[clientID]; !exists {
		m.DeduplicatorByClient[clientID] = NewBatchDeduplicator(m.MaxBatchSize)
	}
	return m.DeduplicatorByClient[clientID]
}

// para desencapsular
func (m *MultiClientDeduplicator) IsDuplicateNoUpdate(clientID int64, id BatchID) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.getOrCreate(clientID).IsDuplicateNoUpdate(id)
}
