package statement

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	expirable "github.com/louvri/gosl/cache"
)

type statement struct {
	cache *expirable.LRU[string, Cache]
	mu    sync.RWMutex
}

type Statement interface {
	Build(statementKey, query string, db *sqlx.DB, allowEvict, inUse bool) (Cache, error)
	Set(key string, inUse bool)
	Mount(key string) (*sqlx.Stmt, error)
	Get(key string) (Cache, error)
	Unmount(key string)
}

func New(cap int, cacheTTL time.Duration) Statement {
	stmt := &statement{}

	stmt.cache = expirable.NewLRU[string, Cache](
		cap,
		func(key string, cached Cache) {
			if cached.inUse || !cached.allowEvict {
				stmt.cache.Extend(key, cached)
				return
			}
			if cached.stmt != nil {
				if err := cached.stmt.Close(); err != nil {
					log.Printf("failed to close evicted stmt for key %s: %v", key, err)
				}
			}
		},
		cacheTTL,
	)

	return stmt
}

func (s *statement) Build(statementKey, query string, db *sqlx.DB, allowEvict, inUse bool) (Cache, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Return existing if exists
	if cache, ok := s.cache.Get(statementKey); ok {
		return cache, nil
	}

	// Prepare new statement
	stmt, err := db.Preparex(query)
	if err != nil {
		return Cache{}, err
	}

	c := Cache{}
	c.Set(stmt, allowEvict, inUse)
	s.cache.Add(statementKey, c)
	return c, nil
}

func (s *statement) Set(key string, inUse bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if cache, ok := s.cache.Get(key); ok {
		cache.inUse = inUse
		s.cache.Add(key, cache)
	}
}

func (s *statement) Mount(key string) (*sqlx.Stmt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var existing Cache
	if cached, ok := s.cache.Peek(key); ok && (!cached.allowEvict || cached.inUse) {
		cached.inUse = true
		existing = cached
	}

	if cache, ok := s.cache.Get(key); ok {
		cache.inUse = true
		existing = cache
	}
	if existing.stmt == nil {
		return nil, errors.New("not_exists")
	}
	s.cache.Add(key, existing)
	return existing.stmt, nil
}

func (s *statement) Unmount(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var existing Cache
	if cached, ok := s.cache.Peek(key); ok && (!cached.allowEvict || cached.inUse) {
		cached.inUse = false
		existing = cached
	}

	if cache, ok := s.cache.Get(key); ok {
		cache.inUse = false
		existing = cache
	}
	s.cache.Add(key, existing)
}

func (s *statement) Get(key string) (Cache, error) {
	if cache, ok := s.cache.Get(key); ok {
		return cache, nil
	}
	return Cache{}, errors.New("not_exists")
}
