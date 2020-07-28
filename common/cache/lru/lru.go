package lru

import (
	"container/list"
	"errors"
	"time"
)

// EvictCallback is used to get a callback when a cache entry is evicted
type EvictCallback func(key interface{}, value interface{})

// LRU implements a non-thread safe fixed size LRU cache
type LRUCache struct {
	size      int
	evictList *list.List
	items     map[interface{}]*list.Element
	onEvict   EvictCallback
}

// entry is used to hold a value in the evictList
type entry struct {
	key     interface{}
	value   interface{}
	expired int64
	ts      int64
}

// NewLRU constructs an LRU of the given size
func NewLRU(size int, onEvict EvictCallback) (*LRUCache, error) {
	if size <= 0 {
		return nil, errors.New("Must provide a positive size")
	}
	c := &LRUCache{
		size:      size,
		evictList: list.New(),
		items:     make(map[interface{}]*list.Element),
		onEvict:   onEvict,
	}
	return c, nil
}

// Purge is used to completely clear the cache.
func (c *LRUCache) Purge() {
	for k, v := range c.items {
		if c.onEvict != nil {
			c.onEvict(k, v.Value.(*entry).value)
		}
		delete(c.items, k)
	}
	c.evictList.Init()
}

// Add adds a value to the cache.  Returns true if an eviction occurred.
func (c *LRUCache) Add(key, value interface{}) (evicted bool) {
	// Check for existing item
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		ent.Value.(*entry).value = value
		return false
	}

	// Add new item
	ent := &entry{key, value, 0, 0,}
	entry := c.evictList.PushFront(ent)
	c.items[key] = entry

	evict := c.evictList.Len() > c.size
	// Verify size not exceeded
	if evict {
		c.removeOldest()
	}
	return evict
}

func (c *LRUCache) AddWithExpired(key, value interface{}, expired int64) (evicted bool) {
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		ent.Value.(*entry).value = value
		//updates expired
		ent.Value.(*entry).ts = time.Now().Unix()
		ent.Value.(*entry).expired = expired
		return false
	}

	// Add new item
	ent := &entry{key, value, expired, time.Now().Unix(),}
	entry := c.evictList.PushFront(ent)
	c.items[key] = entry

	evict := c.evictList.Len() > c.size
	// Verify size not exceeded
	if evict {
		c.removeOldest()
	}
	return evict
}

func (c *LRUCache) CheckExpired(key interface{}) bool {
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		if ent.Value.(*entry).expired != 0 &&
			time.Now().Unix() - ent.Value.(*entry).ts > ent.Value.(*entry).expired {
			return true
		}
	}
	return false
}

func (c *LRUCache) Get(key interface{}) (value interface{}, ok bool) {
	if ent, ok := c.items[key]; ok {
		//expired check
		if ent.Value.(*entry).expired != 0 &&
			time.Now().Unix() - ent.Value.(*entry).ts > ent.Value.(*entry).expired {
			//fast remove
			c.removeElement(ent, true)
			return nil, false
		}

		c.evictList.MoveToFront(ent)
		if ent.Value.(*entry) == nil {
			return nil, false
		}
		return ent.Value.(*entry).value, true
	}
	return
}

func (c *LRUCache) Contains(key interface{}) (ok bool) {
	_, ok = c.items[key]
	return ok
}

// Peek returns the key value (or undefined if not found) without updating
// the "recently used"-ness of the key.
func (c *LRUCache) Peek(key interface{}) (value interface{}, ok bool) {
	var ent *list.Element
	if ent, ok = c.items[key]; ok {
		return ent.Value.(*entry).value, true
	}
	return nil, ok
}

func (c *LRUCache) Remove(key interface{}) (present bool) {
	if ent, ok := c.items[key]; ok {
		c.removeElement(ent, false)
		return true
	}
	return false
}

// RemoveOldest removes the oldest item from the cache.
func (c *LRUCache) RemoveOldest() (key interface{}, value interface{}, ok bool) {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent, true)
		kv := ent.Value.(*entry)
		return kv.key, kv.value, true
	}
	return nil, nil, false
}

// GetOldest returns the oldest entry
func (c *LRUCache) GetOldest() (key interface{}, value interface{}, ok bool) {
	ent := c.evictList.Back()
	if ent != nil {
		kv := ent.Value.(*entry)
		return kv.key, kv.value, true
	}
	return nil, nil, false
}

// Keys returns a slice of the keys in the cache, from oldest to newest.
func (c *LRUCache) Keys() []interface{} {
	keys := make([]interface{}, len(c.items))
	i := 0
	for ent := c.evictList.Back(); ent != nil; ent = ent.Prev() {
		keys[i] = ent.Value.(*entry).key
		i++
	}
	return keys
}

// Len returns the number of items in the cache.
func (c *LRUCache) Len() int {
	return c.evictList.Len()
}

// Resize changes the cache size.
func (c *LRUCache) Resize(size int) (evicted int) {
	diff := c.Len() - size
	if diff < 0 {
		diff = 0
	}
	for i := 0; i < diff; i++ {
		c.removeOldest()
	}
	c.size = size
	return diff
}

// removeOldest removes the oldest item from the cache.
func (c *LRUCache) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent, true)
	}
}

// removeElement is used to remove a given list element from the cache
func (c *LRUCache) removeElement(e *list.Element, evict bool) {
	c.evictList.Remove(e)
	kv := e.Value.(*entry)
	delete(c.items, kv.key)
	if evict && c.onEvict != nil {
		c.onEvict(kv.key, kv.value)
	}
}
