package embedded

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/apache/kvrocks-controller/metadata"
	"github.com/apache/kvrocks-controller/storage/persistence"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type Config struct {
	Addrs []string `yaml:"addrs"`
	Id    int      `yaml:"id"`
}

type Embedded struct {
	kv *kv

	leaderMu sync.RWMutex
	leaderID string
	myID     string

	quitCh         chan struct{}
	leaderChangeCh chan bool
	proposeCh      chan string
	confChangeCh   chan raftpb.ConfChange
}

func New(id string, cfg *Config) (*Embedded, error) {
	proposeCh := make(chan string)
	confChangeCh := make(chan raftpb.ConfChange)

	var kvs *kv
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	commitC, errorC, snapshotterReady := newRaftNode(cfg.Id, cfg.Addrs, false, getSnapshot, proposeCh, confChangeCh)

	kvs = newKv(<-snapshotterReady, proposeCh, commitC, errorC)

	embedded := Embedded{
		kv:             kvs,
		myID:           id,
		quitCh:         make(chan struct{}),
		leaderChangeCh: make(chan bool, 1),
		proposeCh:      proposeCh,
		confChangeCh:   confChangeCh,
	}
	return &embedded, nil
}

func (e *Embedded) ID() string {
	return e.myID
}

func (e *Embedded) Leader() string {
	e.leaderMu.RLock()
	defer e.leaderMu.RUnlock()
	return e.leaderID
}

func (e *Embedded) LeaderChange() <-chan bool {
	return e.leaderChangeCh
}

func (e *Embedded) IsReady(_ context.Context) bool {
	select {
	case <-e.quitCh:
		return false
	default:
		return true
	}
}

func (e *Embedded) Get(_ context.Context, key string) ([]byte, error) {
	value, ok := e.kv.Get(key)
	if !ok {
		return nil, metadata.ErrEntryNoExists
	}
	return value, nil
}

func (e *Embedded) Exists(ctx context.Context, key string) (bool, error) {
	_, err := e.Get(ctx, key)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryNoExists) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (e *Embedded) Set(_ context.Context, key string, value []byte) error {
	e.kv.Propose(key, value)
	return nil
}

func (e *Embedded) Delete(_ context.Context, key string) error {
	e.kv.Propose(key, nil)
	return nil
}

func (e *Embedded) List(_ context.Context, prefix string) ([]persistence.Entry, error) {
	kvs := e.kv.List(prefix)
	prefixLen := len(prefix)
	entries := make([]persistence.Entry, 0)
	for _, entry := range kvs {
		if entry.Key == prefix {
			continue
		}
		key := strings.TrimLeft(entry.Key[prefixLen+1:], "/")
		if strings.ContainsRune(key, '/') {
			continue
		}
		entries = append(entries, persistence.Entry{
			Key:   key,
			Value: entry.Value,
		})
	}
	return entries, nil
}

func (e *Embedded) Close() error {
	close(e.quitCh)
	return nil
}
