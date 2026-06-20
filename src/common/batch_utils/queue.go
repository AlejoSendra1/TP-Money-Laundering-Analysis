package batch_utils

type queue struct {
	Buffer  []BatchID `json:"buffer"`
	Head    int       `json:"head"`
	Size    int       `json:"size"`
	MaxSize int       `json:"maxSize"`
}

func newQueue(maxSize int) *queue {
	return &queue{
		Buffer:  make([]BatchID, maxSize),
		MaxSize: maxSize,
	}
}

func (q *queue) isFull() bool {
	return q.Size == q.MaxSize
}

func (q *queue) push(val BatchID) {
	tail := (q.Head + q.Size) % q.MaxSize
	q.Buffer[tail] = val
	q.Size++
}

func (q *queue) pop() BatchID {
	val := q.Buffer[q.Head]
	q.Head = (q.Head + 1) % q.MaxSize
	q.Size--
	return val
}
