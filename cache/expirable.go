package cache

import (
	"sync"
	"time"
)

type EvictCallback[K comparable, V any] func(key K, value V)

type LRU[K comparable, V any] struct {
	size      int
	evictList *LruList[K, V]
	items     map[K]*Entry[K, V]
	onEvict   EvictCallback[K, V]

	mu   sync.Mutex
	ttl  time.Duration
	done chan struct{}

	buckets           []bucket[K, V]
	nextCleanupBucket uint8
}

// bucket is a container for holding entries to be expired
type bucket[K comparable, V any] struct {
	entries     map[K]*Entry[K, V]
	newestEntry time.Time
}

// noEvictionTTL - very long ttl to prevent eviction
const noEvictionTTL = time.Hour * 24 * 365 * 10
const numBuckets = 100

func NewLRU[K comparable, V any](size int, onEvict EvictCallback[K, V], ttl time.Duration) *LRU[K, V] {
	if size < 0 {
		size = 0
	}
	if ttl <= 0 {
		ttl = noEvictionTTL
	}

	res := LRU[K, V]{
		ttl:       ttl,
		size:      size,
		evictList: NewList[K, V](),
		items:     make(map[K]*Entry[K, V]),
		onEvict:   onEvict,
		done:      make(chan struct{}),
	}

	res.buckets = make([]bucket[K, V], numBuckets)
	for i := 0; i < numBuckets; i++ {
		res.buckets[i] = bucket[K, V]{entries: make(map[K]*Entry[K, V])}
	}

	// enable deleteExpired() running in separate goroutine for cache with non-zero TTL
	// Important: done channel is never closed, so deleteExpired() goroutine will never exit,
	// it's decided to add functionality to close it in the version later than v2.
	if res.ttl != noEvictionTTL {
		go func(done <-chan struct{}) {
			ticker := time.NewTicker(res.ttl / numBuckets)
			defer ticker.Stop()
			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					res.deleteExpired()
				}
			}
		}(res.done)
	}
	return &res
}

func (c *LRU[K, V]) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range c.items {
		if c.onEvict != nil {
			c.onEvict(k, v.Value)
		}
		delete(c.items, k)
	}
	for _, b := range c.buckets {
		for _, ent := range b.entries {
			delete(b.entries, ent.Key)
		}
	}
	c.evictList.Init()
}

func (c *LRU[K, V]) Extend(key K, value V) (evicted bool) {
	now := time.Now()

	// Check for existing item
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		c.removeFromBucket(ent)
		ent.Value = value
		ent.ExpiresAt = now.Add(c.ttl)
		c.addToBucket(ent)
		return false
	}

	// Add new item
	ent := c.evictList.PushFrontExpirable(key, value, now.Add(c.ttl))
	c.items[key] = ent
	c.addToBucket(ent)

	evict := c.size > 0 && c.evictList.Length() > c.size
	if evict {
		c.removeOldest()
	}
	return evict
}

func (c *LRU[K, V]) Add(key K, value V) (evicted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()

	// Check for existing item
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		c.removeFromBucket(ent)
		ent.Value = value
		ent.ExpiresAt = now.Add(c.ttl)
		c.addToBucket(ent)
		return false
	}

	// Add new item
	ent := c.evictList.PushFrontExpirable(key, value, now.Add(c.ttl))
	c.items[key] = ent
	c.addToBucket(ent)

	evict := c.size > 0 && c.evictList.Length() > c.size
	if evict {
		c.removeOldest()
	}
	return evict
}

func (c *LRU[K, V]) Get(key K) (value V, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var ent *Entry[K, V]
	if ent, ok = c.items[key]; ok {
		if time.Now().After(ent.ExpiresAt) {
			return value, false
		}
		c.evictList.MoveToFront(ent)
		return ent.Value, true
	}
	return
}

func (c *LRU[K, V]) Contains(key K) (ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok = c.items[key]
	return ok
}

func (c *LRU[K, V]) Peek(key K) (value V, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var ent *Entry[K, V]
	if ent, ok = c.items[key]; ok {
		return ent.Value, true
	}
	return
}

func (c *LRU[K, V]) Remove(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ent, ok := c.items[key]; ok {
		c.removeElement(ent)
		return true
	}
	return false
}

func (c *LRU[K, V]) RemoveOldest() (key K, value V, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ent := c.evictList.Back(); ent != nil {
		c.removeElement(ent)
		return ent.Key, ent.Value, true
	}
	return
}

func (c *LRU[K, V]) GetOldest() (key K, value V, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ent := c.evictList.Back(); ent != nil {
		return ent.Key, ent.Value, true
	}
	return
}

func (c *LRU[K, V]) Keys() []K {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := make([]K, 0, len(c.items))
	for ent := c.evictList.Back(); ent != nil; ent = ent.PrevEntry() {
		keys = append(keys, ent.Key)
	}
	return keys
}

func (c *LRU[K, V]) Values() []V {
	c.mu.Lock()
	defer c.mu.Unlock()
	values := make([]V, len(c.items))
	i := 0
	now := time.Now()
	for ent := c.evictList.Back(); ent != nil; ent = ent.PrevEntry() {
		if now.After(ent.ExpiresAt) {
			continue
		}
		values[i] = ent.Value
		i++
	}
	return values
}

func (c *LRU[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.evictList.Length()
}

func (c *LRU[K, V]) Resize(size int) (evicted int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if size <= 0 {
		c.size = 0
		return 0
	}
	diff := c.evictList.Length() - size
	if diff < 0 {
		diff = 0
	}
	for i := 0; i < diff; i++ {
		c.removeOldest()
	}
	c.size = size
	return diff
}

// Close destroys cleanup goroutine. To clean up the cache, run Purge() before Close().
// func (c *LRU[K, V]) Close() {
//	c.mu.Lock()
//	defer c.mu.Unlock()
//	select {
//	case <-c.done:
//		return
//	default:
//	}
//	close(c.done)
// }

func (c *LRU[K, V]) removeOldest() {
	if ent := c.evictList.Back(); ent != nil {
		c.removeElement(ent)
	}
}

func (c *LRU[K, V]) removeElement(e *Entry[K, V]) {
	c.evictList.Remove(e)
	delete(c.items, e.Key)
	c.removeFromBucket(e)
	if c.onEvict != nil {
		c.onEvict(e.Key, e.Value)
	}
}

func (c *LRU[K, V]) deleteExpired() {
	c.mu.Lock()
	bucketIdx := c.nextCleanupBucket
	timeToExpire := time.Until(c.buckets[bucketIdx].newestEntry)
	if timeToExpire > 0 {
		c.mu.Unlock()
		time.Sleep(timeToExpire)
		c.mu.Lock()
	}
	for _, ent := range c.buckets[bucketIdx].entries {
		c.removeElement(ent)
	}
	c.nextCleanupBucket = (c.nextCleanupBucket + 1) % numBuckets
	c.mu.Unlock()
}

func (c *LRU[K, V]) addToBucket(e *Entry[K, V]) {
	bucketID := (numBuckets + c.nextCleanupBucket - 1) % numBuckets
	e.ExpireBucket = bucketID
	c.buckets[bucketID].entries[e.Key] = e
	if c.buckets[bucketID].newestEntry.Before(e.ExpiresAt) {
		c.buckets[bucketID].newestEntry = e.ExpiresAt
	}
}

func (c *LRU[K, V]) removeFromBucket(e *Entry[K, V]) {
	delete(c.buckets[e.ExpireBucket].entries, e.Key)
}
