package batch_utils

type queue struct {
	buffer  []BatchID
	head    int
	size    int
	maxSize int
}

func newQueue(maxSize int) *queue {
	return &queue{
		buffer:  make([]BatchID, maxSize),
		maxSize: maxSize,
	}
}

func (q *queue) isFull() bool {
	return q.size == q.maxSize
}

func (q *queue) push(val BatchID) {
	tail := (q.head + q.size) % q.maxSize
	q.buffer[tail] = val
	q.size++
}

func (q *queue) pop() BatchID {
	val := q.buffer[q.head]
	q.head = (q.head + 1) % q.maxSize
	q.size--
	return val
}
