package embedded

import (
	"strings"
	"sync"
)

type kv struct {
	kv   map[string][]byte
	kvMu sync.RWMutex
}

func newKv() *kv {
	return &kv{
		kv: make(map[string][]byte),
	}
}

func (s *kv) Get(key string) ([]byte, bool) {
	s.kvMu.RLock()
	defer s.kvMu.RUnlock()
	v, ok := s.kv[key]
	return v, ok
}

type entry struct {
	Key   string
	Value []byte
}

func (s *kv) List(prefix string) []entry {
	s.kvMu.RLock()
	defer s.kvMu.RUnlock()
	entries := make([]entry, 0)
	//TODO use trie to accelerate query
	for k, v := range s.kv {
		if strings.HasPrefix(k, prefix) {
			entries = append(entries, entry{k, v})
		}
	}
	return entries
}

func (s *kv) Set(k string, v []byte) error {
	s.kvMu.Lock()
	defer s.kvMu.Unlock()
	s.kv[k] = v
	return nil
}

func (s *kv) Delete(k string) error {
	s.kvMu.Lock()
	defer s.kvMu.Unlock()
	delete(s.kv, k)
	return nil
}
