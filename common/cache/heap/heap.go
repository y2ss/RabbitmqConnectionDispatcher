package heap

import (
	"container/heap"
)

// An Item is something we manage in a priority queue.
type Item struct {
	Value    interface{} // The value of the item; arbitrary.
	Priority int64    // The priority of the item in the queue.
	Index    int // The index of the item in the heap. The index is needed by update and is maintained by the heap.Interface methods.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue struct {
	Items []*Item
	Map   map[interface{}]int
}

func (pq PriorityQueue) Len() int { return len(pq.Items) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq.Items[i].Priority > pq.Items[j].Priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq.Items[i], pq.Items[j] = pq.Items[j], pq.Items[i]
	pq.Items[i].Index = i
	pq.Items[j].Index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(pq.Items)
	item := x.(*Item)
	if n, ok := pq.Map[item.Value]; ok == true {
		pq.Items[n].Priority = item.Priority
		return
	}
	item.Index = n
	pq.Items = append(pq.Items, item)
	pq.Map[item.Value] = n
}

func (pq *PriorityQueue) Pop() interface{} {
	old := pq.Items
	n := len(old)
	item := old[n-1]
	item.Index = -1 // for safety
	pq.Items = old[0 : n-1]
	delete(pq.Map, item.Value)
	return item
}

func (pq *PriorityQueue) Peak() interface{} {
	old := *pq
	return old.Items[len(old.Items) - 1]
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) Update(item *Item, value interface{}, priority int64) {
	item.Value = value
	item.Priority = priority
	heap.Fix(pq, item.Index)
}
