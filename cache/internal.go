package cache

import "time"

type Entry[K comparable, V any] struct {
	next, prev   *Entry[K, V]
	list         *LruList[K, V]
	Key          K
	Value        V
	ExpiresAt    time.Time
	ExpireBucket uint8
}

func (e *Entry[K, V]) PrevEntry() *Entry[K, V] {
	if p := e.prev; e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

type LruList[K comparable, V any] struct {
	root Entry[K, V]
	len  int
}

// Init initializes or clears list l.
func (l *LruList[K, V]) Init() *LruList[K, V] {
	l.root.next = &l.root
	l.root.prev = &l.root
	l.len = 0
	return l
}

func NewList[K comparable, V any]() *LruList[K, V] { return new(LruList[K, V]).Init() }
func (l *LruList[K, V]) Length() int               { return l.len }

// Back returns the last element of list l or nil if the list is empty.
func (l *LruList[K, V]) Back() *Entry[K, V] {
	if l.len == 0 {
		return nil
	}
	return l.root.prev
}

// lazyInit lazily initializes a zero List Value.
func (l *LruList[K, V]) lazyInit() {
	if l.root.next == nil {
		l.Init()
	}
}

func (l *LruList[K, V]) insert(e, at *Entry[K, V]) *Entry[K, V] {
	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e
	e.list = l
	l.len++
	return e
}

func (l *LruList[K, V]) insertValue(k K, v V, expiresAt time.Time, at *Entry[K, V]) *Entry[K, V] {
	return l.insert(&Entry[K, V]{Value: v, Key: k, ExpiresAt: expiresAt}, at)
}

// Remove removes e from its list, decrements l.len
func (l *LruList[K, V]) Remove(e *Entry[K, V]) V {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.next = nil
	e.prev = nil
	e.list = nil
	l.len--

	return e.Value
}

// move moves e to next to at.
func (l *LruList[K, V]) move(e, at *Entry[K, V]) {
	if e == at {
		return
	}
	e.prev.next = e.next
	e.next.prev = e.prev

	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e
}

func (l *LruList[K, V]) PushFront(k K, v V) *Entry[K, V] {
	l.lazyInit()
	return l.insertValue(k, v, time.Time{}, &l.root)
}

func (l *LruList[K, V]) PushFrontExpirable(k K, v V, expiresAt time.Time) *Entry[K, V] {
	l.lazyInit()
	return l.insertValue(k, v, expiresAt, &l.root)
}

func (l *LruList[K, V]) MoveToFront(e *Entry[K, V]) {
	if e.list != l || l.root.next == e {
		return
	}
	l.move(e, &l.root)
}
