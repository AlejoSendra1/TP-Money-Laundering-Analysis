package batch_utils

type BatchDeduplicator struct {
	seen  Set[BatchID]
	queue *queue
}

func NewBatchDeduplicator(maxSize int) *BatchDeduplicator {
	return &BatchDeduplicator{
		seen:  NewSet[BatchID](),
		queue: newQueue(maxSize),
	}
}

func (d *BatchDeduplicator) IsDuplicate(id BatchID) bool {
	if d.seen.Contains(id) {
		return true
	}
	d.discardOldestIfFull()
	d.add(id)
	return false
}

func (d *BatchDeduplicator) Load(id BatchID) {
	d.discardOldestIfFull()
	d.add(id)
}

func (d *BatchDeduplicator) discardOldestIfFull() {
	if d.queue.isFull() {
		oldest := d.queue.pop()
		d.seen.Remove(oldest)
	}
}

func (d *BatchDeduplicator) add(id BatchID) {
	d.queue.push(id)
	d.seen.Add(id)
}

// para desencapsular
func (d *BatchDeduplicator) IsDuplicateNoUpdate(id BatchID) bool {
	return d.seen.Contains(id)
}
