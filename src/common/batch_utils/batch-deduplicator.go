package batch_utils

type BatchDeduplicator struct {
	Seen  Set[uint64] `json:"seen"`
	Queue *queue      `json:"queue"`
}

func NewBatchDeduplicator(maxSize int) *BatchDeduplicator {
	return &BatchDeduplicator{
		Seen:  NewSet[uint64](),
		Queue: newQueue(maxSize),
	}
}

func (d *BatchDeduplicator) IsDuplicate(id BatchID) bool {
	if d.Seen.Contains(id.Hash) {
		return true
	}
	d.discardOldestIfFull()
	d.add(id)
	return false
}

func (d *BatchDeduplicator) discardOldestIfFull() {
	if d.Queue.isFull() {
		oldest := d.Queue.pop()
		d.Seen.Remove(oldest.Hash)
	}
}

func (d *BatchDeduplicator) add(id BatchID) {
	d.Queue.push(id)
	d.Seen.Add(id.Hash)
}

// para desencapsular
func (d *BatchDeduplicator) IsDuplicateNoUpdate(id BatchID) bool {
	return d.seen.Contains(id)
}
