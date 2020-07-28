package queue

type Node struct {
	value    interface{}
	previous *Node
	next     *Node
}

func (n *Node) Value() interface{} {
	return n.value
}

func (n *Node) Set(value interface{}) {
	n.value = value
}

func (n *Node) Previous() *Node {
	return n.previous
}

func (n *Node) Next() *Node {
	return n.next
}

type Queue struct {
	front    *Node
	rear     *Node
	length   int
	capacity int
}

func NewQueue(capacity int) *Queue {
	if capacity <= 0 {
		capacity = 100
	}

	front := &Node{ value: nil, previous: nil,}
	rear := &Node{ value: nil, previous: front,}

	front.next = rear
	return &Queue{
		front:    front,
		rear:     rear,
		capacity: capacity,
	}
}

func (q *Queue) Length() int {
	return q.length
}

func (q *Queue) Capacity() int {
	return q.capacity
}

func (q *Queue) Front() *Node {
	if q.length == 0 { return nil }
	return q.front.next
}

func (q *Queue) Rear() *Node {
	if q.length == 0 { return nil }
	return q.rear.previous
}

func (q *Queue) Enqueue(value interface{}) bool {
	if q.length == q.capacity || value == nil { return false }

	node := &Node{ value: value,}
	if q.length == 0 { q.front.next = node }

	node.previous = q.rear.previous
	node.next = q.rear
	q.rear.previous.next = node
	q.rear.previous = node
	q.length++

	return true
}

func (q *Queue) Dequeue() interface{} {
	if q.length == 0 { return nil }

	result := q.front.next
	q.front.next = result.next
	result.next = nil
	result.previous = nil
	q.length--

	return result.value
}

func (q *Queue) ReplaceFront(value interface{}) bool {
	if q.length == 0 || value == nil || q.front.next == nil { return false }

	node := &Node{ value: value,}
	preNode := q.front.next
	q.front.next = node
	q.front.next.next = preNode.next
	q.front.next.next.previous = node
	preNode = nil
	return true
}
