package engine

import (
	"sync"
	"time"
	//"github.com/plar/go-adaptive-radix-tree"
)

var (
	// various data types in values
	_ store = &strStore{}
	_ store = &setStore{}
	_ store = &hashStore{}
)

type store interface {
	evict(cache *hash.Hash)
}

type strStore struct {
	sync.RWMutex
	//*art.New()
}

func newStrStore() *strStore {
	n := &strStore{}
	//n.Tree = art.New()
	return n
}

func (s *strStore) get(key string) (val interface{}, err error) {
	val = s.Search([]byte(key))
	if val == nil {
		return nil, ErrInvalidKey
	}
	return
}

func (s *strStore) Keys() (keys []string) {
	s.Each(func(node *art.Node) {
		if node.IsLeaf() {
			key := string(node.Key())
			keys = append(keys, key)
		}
	})
	return
}

func (s *strStore) evict(cache *hash.Hash) {
	s.Lock()
	defer s.Unlock()

	keys := s.Keys()
	expiredKeys := make([]string, 0, 1)

	for _, k := range keys {
		ttl := cache.HGet(String, k)
		if ttl == nil {
			continue
		}
		if time.Now().Unix() > ttl.(int64) {
			expiredKeys = append(expiredKeys, k)
		}
	}

	for _, k := range expiredKeys {
		s.Delete([]byte(k))
		cache.HDel(String, k)
	}
}

type hashStore struct {
	sync.RWMutex
	*hash.Hash
}

func newHashStore() *hashStore {
	n := &hashStore{}
	n.Hash = hash.New()
	return n
}

func (h *hashStore) evict(cache *hash.Hash) {
	h.Lock()
	defer h.Unlock()

	keys := h.Keys()
	expiredKeys := make([]string, 0, 1)

	for _, k := range keys {
		ttl := cache.HGet(Hash, k)
		if ttl == nil {
			continue
		}
		if time.Now().Unix() > ttl.(int64) {
			expiredKeys = append(expiredKeys, k)
		}
	}

	for _, k := range expiredKeys {
		h.HClear(k)
		cache.HDel(Hash, k)
	}
}

type setStore struct {
	sync.RWMutex
	*set.Set
}

func newSetStore() *setStore {
	n := &setStore{}
	n.Set = set.New()
	return n
}

func (s *setStore) evict(cache *hash.Hash) {
	s.Lock()
	defer s.Unlock()

	keys := s.Keys()
	expiredKeys := make([]string, 0, 1)

	for _, k := range keys {
		ttl := cache.HGet(Set, k)
		if ttl == nil {
			continue
		}
		if time.Now().Unix() > ttl.(int64) {
			expiredKeys = append(expiredKeys, k)
		}
	}

	for _, k := range expiredKeys {
		s.SClear(k)
		cache.HDel(Set, k)
	}
}
