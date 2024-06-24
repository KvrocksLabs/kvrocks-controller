package embedded

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/logger"
	persistence "github.com/apache/kvrocks-controller/store/engine"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.uber.org/zap"
)

type Peer struct {
	Api  string `yaml:"api"`
	Raft string `yaml:"raft"`
}

type Config struct {
	Peers []Peer `yaml:"peers"`
	Join  bool   `yaml:"join"`
}

func parseConfig(id string, cfg *Config) (int, []string, []string, error) {
	apiPeers := make([]string, len(cfg.Peers))
	raftPeers := make([]string, len(cfg.Peers))
	nodeId := -1
	for i, peer := range cfg.Peers {
		if peer.Api == id {
			nodeId = i + 1
		}
		apiPeers[i] = peer.Api
		if !strings.HasPrefix(peer.Raft, "http://") {
			raftPeers[i] = fmt.Sprintf("http://%s", peer.Raft)
		} else {
			raftPeers[i] = peer.Raft
		}
	}
	if nodeId == -1 {
		return 0, apiPeers, raftPeers, errors.New(fmt.Sprintf("Address %s is not in embedded store peers configuration", id))
	}
	return nodeId, apiPeers, raftPeers, nil
}

type Embedded struct {
	kv          map[string][]byte
	kvMu        sync.RWMutex
	snapshotter *snap.Snapshotter

	node *raftNode

	myID    string
	PeerIDs []string

	quitCh         chan struct{}
	leaderChangeCh <-chan bool
	proposeCh      chan string
	confChangeCh   chan raftpb.ConfChange
}

func New(id string, cfg *Config) (*Embedded, error) {
	nodeId, apiPeers, raftPeers, err := parseConfig(id, cfg)
	if err != nil {
		return nil, err
	}

	proposeCh := make(chan string)
	confChangeCh := make(chan raftpb.ConfChange)
	leaderChangeCh := make(chan bool)
	commitCh := make(chan *commit)
	errorCh := make(chan error)
	snapshotterReady := make(chan *snap.Snapshotter, 1)

	e := &Embedded{
		kv:             make(map[string][]byte),
		myID:           id,
		PeerIDs:        apiPeers,
		quitCh:         make(chan struct{}),
		leaderChangeCh: leaderChangeCh,
		proposeCh:      proposeCh,
		confChangeCh:   confChangeCh,
	}

	getSnapshot := func() ([]byte, error) {
		e.kvMu.RLock()
		defer e.kvMu.RUnlock()
		return json.Marshal(e.kv)
	}
	// start raft node synchronization loop
	e.node = newRaftNode(nodeId, raftPeers, cfg.Join, getSnapshot, proposeCh, confChangeCh, leaderChangeCh, commitCh, errorCh, snapshotterReady)

	// block until snapshotter initialized
	e.snapshotter = <-snapshotterReady
	snapshot, err := e.loadSnapshot()
	if err != nil {
		logger.Get().With(zap.Error(err)).Panic("Failed to initialize snapshot")
	}
	if snapshot != nil {
		logger.Get().Sugar().Infof("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
		if err := e.recoverFromSnapshot(snapshot.Data); err != nil {
			logger.Get().With(zap.Error(err)).Error("Failed to recover snapshot")
		}
	}

	go e.readCommits(commitCh, errorCh)
	return e, nil
}

func (e *Embedded) loadSnapshot() (*raftpb.Snapshot, error) {
	if snapshot, err := e.snapshotter.Load(); err != nil {
		if errors.Is(err, snap.ErrNoSnapshot) || errors.Is(err, snap.ErrEmptySnapshot) {
			return nil, nil
		}
		return nil, err
	} else {
		return snapshot, nil
	}
}
func (e *Embedded) recoverFromSnapshot(snapshot []byte) error {
	var store map[string][]byte
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	e.kvMu.Lock()
	defer e.kvMu.Unlock()
	e.kv = store
	return nil
}

func (e *Embedded) readCommits(commitCh <-chan *commit, errorCh <-chan error) {
	for c := range commitCh {
		if c == nil {
			// signaled to load snapshot
			snapshot, err := e.loadSnapshot()
			if err != nil {
				logger.Get().With(zap.Error(err)).Error("Failed to load snapshot")
			}
			if snapshot != nil {
				logger.Get().Sugar().Infof("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
				if err := e.recoverFromSnapshot(snapshot.Data); err != nil {
					logger.Get().With(zap.Error(err)).Error("Failed to recover snapshot")
				}
			}
			continue
		}

		for _, data := range c.data {
			var entry persistence.Entry
			dec := gob.NewDecoder(bytes.NewBufferString(data))
			if err := dec.Decode(&entry); err != nil {
				logger.Get().With(zap.Error(err)).Error("Failed to decode message")
			}
			e.kvMu.Lock()
			if entry.Value == nil {
				delete(e.kv, entry.Key)
			} else {
				e.kv[entry.Key] = entry.Value
			}
			e.kvMu.Unlock()
		}
		close(c.applyDoneC)
	}
	if err, ok := <-errorCh; ok {
		logger.Get().With(zap.Error(err)).Error("Error occurred during reading commits")
	}
}

func (e *Embedded) ID() string {
	return e.myID
}

func (e *Embedded) Leader() string {
	if e.node.leader.Load() == 0 {
		return e.myID
	}
	return e.PeerIDs[e.node.leader.Load()-1]
}

func (e *Embedded) LeaderChange() <-chan bool {
	return e.leaderChangeCh
}

func (e *Embedded) IsReady(ctx context.Context) bool {
	for {
		select {
		case <-e.quitCh:
			return false
		case <-time.After(100 * time.Millisecond):
			if e.node.leader.Load() != 0 {
				return true
			}
		case <-ctx.Done():
			return e.node.leader.Load() != 0
		}
	}
}

func (e *Embedded) Get(_ context.Context, key string) ([]byte, error) {
	e.kvMu.RLock()
	defer e.kvMu.RUnlock()
	value, ok := e.kv[key]
	if !ok {
		return nil, consts.ErrNotFound
	}
	return value, nil
}

func (e *Embedded) Exists(ctx context.Context, key string) (bool, error) {
	_, err := e.Get(ctx, key)
	if err != nil {
		if errors.Is(err, consts.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (e *Embedded) Propose(k string, v []byte) {
	var buf strings.Builder
	if err := gob.NewEncoder(&buf).Encode(persistence.Entry{Key: k, Value: v}); err != nil {
		logger.Get().With(zap.Error(err)).Error("Failed to propose changes")
	}
	e.proposeCh <- buf.String()
}

func (e *Embedded) Set(_ context.Context, key string, value []byte) error {
	e.Propose(key, value)
	return nil
}

func (e *Embedded) Delete(_ context.Context, key string) error {
	e.Propose(key, nil)
	return nil
}

func (e *Embedded) List(_ context.Context, prefix string) ([]persistence.Entry, error) {
	entries := make([]persistence.Entry, 0)
	prefixLen := len(prefix)
	e.kvMu.RLock()
	defer e.kvMu.RUnlock()
	//TODO use trie to accelerate query
	for k, v := range e.kv {
		if !strings.HasPrefix(k, prefix) || k == prefix {
			continue
		}
		key := strings.TrimLeft(k[prefixLen+1:], "/")
		if strings.ContainsRune(key, '/') {
			continue
		}
		entries = append(entries, persistence.Entry{Key: key, Value: v})
	}
	return entries, nil
}

func (e *Embedded) Close() error {
	close(e.quitCh)
	return nil
}
