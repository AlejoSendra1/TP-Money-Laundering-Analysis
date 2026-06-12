package batch_utils

type BatchDeduplicator struct {
	seen  map[BatchID]struct{}
	queue *queue
}

func NewBatchDeduplicator(maxSize int) *BatchDeduplicator {
	return &BatchDeduplicator{
		seen:  make(map[BatchID]struct{}),
		queue: newQueue(maxSize),
	}
}

func (d *BatchDeduplicator) IsDuplicate(id BatchID) bool {
	if _, exists := d.seen[id]; exists {
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
		delete(d.seen, oldest)
	}
}

func (d *BatchDeduplicator) add(id BatchID) {
	d.queue.push(id)
	d.seen[id] = struct{}{}
}
