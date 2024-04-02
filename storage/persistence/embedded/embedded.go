package embedded

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/kvrocks-controller/metadata"
	"github.com/apache/kvrocks-controller/storage/persistence"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
)

type Config struct {
	Addrs []string `yaml:"addrs"`
	Id    int      `yaml:"id"`
}

type Embedded struct {
	kv   *kv
	node *raftNode

	myID string

	quitCh         chan struct{}
	leaderChangeCh <-chan bool
	proposeCh      chan string
	confChangeCh   chan raftpb.ConfChange
}

func New(_ string, cfg *Config) (*Embedded, error) {
	proposeCh := make(chan string)
	confChangeCh := make(chan raftpb.ConfChange)
	leaderChangeCh := make(chan bool)
	commitCh := make(chan *commit)
	errorCh := make(chan error)
	snapshotterReady := make(chan *snap.Snapshotter, 1)

	var kvs *kv
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	for i, addr := range cfg.Addrs {
		if !strings.HasPrefix(addr, "http://") {
			cfg.Addrs[i] = fmt.Sprintf("http://%s", addr)
		}
	}
	node := newRaftNode(cfg.Id, cfg.Addrs, false, getSnapshot, proposeCh, confChangeCh, leaderChangeCh, commitCh, errorCh, snapshotterReady)

	kvs = newKv(<-snapshotterReady, proposeCh, commitCh, errorCh)

	embedded := Embedded{
		kv:             kvs,
		node:           node,
		myID:           strconv.Itoa(cfg.Id),
		quitCh:         make(chan struct{}),
		leaderChangeCh: leaderChangeCh,
		proposeCh:      proposeCh,
		confChangeCh:   confChangeCh,
	}
	return &embedded, nil
}

func (e *Embedded) ID() string {
	return e.myID
}

func (e *Embedded) Leader() string {
	return strconv.FormatUint(e.node.leader.Load(), 10)
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
