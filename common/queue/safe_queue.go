package queue

import "sync"

type SafeQueue struct {
	Queue *Queue
	mutex sync.RWMutex
}

func NewSafeQueue(capacity int) *SafeQueue {
	return &SafeQueue{
		Queue: NewQueue(capacity),
	}
}

func (q *SafeQueue) Dequeue() interface{} {
	q.mutex.Lock()
	v := q.Queue.Dequeue()
	q.mutex.Unlock()
	return v
}

func (q *SafeQueue) Enqueue(value interface{}) bool {
	q.mutex.Lock()
	b := q.Queue.Enqueue(value)
	q.mutex.Unlock()
	return b
}

func (q *SafeQueue) Front() *Node {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.Queue.length == 0 { return nil }
	return q.Queue.front.next
}

func (q *SafeQueue) Rear() *Node {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.Queue.length == 0 { return nil }
	return q.Queue.rear.previous
}

func (q *SafeQueue) IsEmpty() bool {
	q.mutex.RLock()
	defer q.mutex.RUnlock()
	return q.Queue.length == 0
}

func (q *SafeQueue) Length() int {
	return q.Queue.length
}

func (q *SafeQueue) ReplaceFront(value interface{}) bool {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	return q.Queue.ReplaceFront(value)
}