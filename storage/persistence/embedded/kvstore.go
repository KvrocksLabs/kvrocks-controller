package embedded

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"strings"
	"sync"

	"github.com/apache/kvrocks-controller/logger"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.uber.org/zap"
)

type kv struct {
	kv          map[string][]byte
	kvMu        sync.RWMutex
	proposeC    chan<- string // channel for proposing updates
	snapshotter *snap.Snapshotter
}

func newKv(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *commit, errorC <-chan error) *kv {
	s := &kv{
		kv:          make(map[string][]byte),
		proposeC:    proposeC,
		snapshotter: snapshotter,
	}
	snapshot, err := s.loadSnapshot()
	if err != nil {
		logger.Get().With(zap.Error(err)).Panic("Failed to initialize snapshot")
	}
	if snapshot != nil {
		logger.Get().Sugar().Infof("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
		if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
			logger.Get().With(zap.Error(err)).Error("Failed to recover snapshot")
		}
	}
	// read commits from raft into kvStore map until error
	go s.readCommits(commitC, errorC)
	return s
}

func (s *kv) loadSnapshot() (*raftpb.Snapshot, error) {
	if snapshot, err := s.snapshotter.Load(); err != nil {
		if errors.Is(err, snap.ErrNoSnapshot) || errors.Is(err, snap.ErrEmptySnapshot) {
			return nil, nil
		}
		return nil, err
	} else {
		return snapshot, nil
	}
}
func (s *kv) recoverFromSnapshot(snapshot []byte) error {
	var store map[string][]byte
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.kvMu.Lock()
	defer s.kvMu.Unlock()
	s.kv = store
	return nil
}

func (s *kv) getSnapshot() ([]byte, error) {
	s.kvMu.RLock()
	defer s.kvMu.RUnlock()
	return json.Marshal(s.kv)
}

func (s *kv) readCommits(commitC <-chan *commit, errorC <-chan error) {
	for c := range commitC {
		if c == nil {
			// signaled to load snapshot
			snapshot, err := s.loadSnapshot()
			if err != nil {
				logger.Get().With(zap.Error(err)).Error("Failed to load snapshot")
			}
			if snapshot != nil {
				logger.Get().Sugar().Infof("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
				if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
					logger.Get().With(zap.Error(err)).Error("Failed to recover snapshot")
				}
			}
			continue
		}

		for _, data := range c.data {
			var e entry
			dec := gob.NewDecoder(bytes.NewBufferString(data))
			if err := dec.Decode(&e); err != nil {
				logger.Get().With(zap.Error(err)).Error("Failed to decode message")
			}
			s.kvMu.Lock()
			if e.Value == nil {
				delete(s.kv, e.Key)
			} else {
				s.kv[e.Key] = e.Value
			}
			s.kvMu.Unlock()
		}
		close(c.applyDoneC)
	}
	if err, ok := <-errorC; ok {
		logger.Get().With(zap.Error(err)).Error("Error occurred during reading commits")
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

func (s *kv) Propose(k string, v []byte) {
	var buf strings.Builder
	if err := gob.NewEncoder(&buf).Encode(entry{k, v}); err != nil {
		logger.Get().With(zap.Error(err)).Error("Failed to propose changes")
	}
	s.proposeC <- buf.String()
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
