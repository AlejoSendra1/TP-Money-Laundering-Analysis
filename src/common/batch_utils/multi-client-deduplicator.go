package batch_utils

import "sync"

type MultiClientDeduplicator struct {
	DeduplicatorByClient map[int]*BatchDeduplicator `json:"batchDeduplicator"`
	MaxBatchSize         int                        `json:"maxBatchSize"`
	mutex                sync.Mutex                 `json:"-"`
}

func NewMultiClientDeduplicator(maxBatchSize int) *MultiClientDeduplicator {
	return &MultiClientDeduplicator{
		DeduplicatorByClient: make(map[int]*BatchDeduplicator),
		MaxBatchSize:         maxBatchSize,
	}
}

func (m *MultiClientDeduplicator) IsDuplicate(clientID int, id BatchID) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.getOrCreate(clientID).IsDuplicate(id)
}

func (m *MultiClientDeduplicator) RemoveClient(clientID int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.DeduplicatorByClient, clientID)
}

func (m *MultiClientDeduplicator) getOrCreate(clientID int) *BatchDeduplicator {
	if _, exists := m.DeduplicatorByClient[clientID]; !exists {
		m.DeduplicatorByClient[clientID] = NewBatchDeduplicator(m.MaxBatchSize)
	}
	return m.DeduplicatorByClient[clientID]
}
