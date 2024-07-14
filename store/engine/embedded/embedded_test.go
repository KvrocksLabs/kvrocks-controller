package embedded

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/apache/kvrocks-controller/consts"

	"github.com/stretchr/testify/assert"
)

func createConfig(peers []string, path string) *Config {
	peerList := make([]Peer, len(peers))
	for i, peer := range peers {
		peerList[i] = Peer{
			Api:  peer,
			Raft: fmt.Sprintf("http://%s", peer),
		}
	}

	return &Config{
		Peers: peerList,
		Join:  false,
		Path:  path,
	}
}

func TestNew(t *testing.T) {
	dir, _ := os.MkdirTemp("", "TestNew")
	defer os.RemoveAll(dir)
	cfg := createConfig([]string{"localhost:20000", "localhost:20001"}, dir)
	_, err := New("localhost:20000", cfg)
	assert.NoError(t, err)
}

func TestEmbedded_Propose(t *testing.T) {
	dir, _ := os.MkdirTemp("", "TestEmbedded_Propose")
	defer os.RemoveAll(dir)
	cfg := createConfig([]string{"localhost:20002"}, dir)

	e1, err := New("localhost:20002", cfg)
	assert.NoError(t, err)

	// Wait to be the leader
	<-e1.LeaderChange()

	assert.True(t, e1.IsReady(context.Background()))
	e1.Propose("key", []byte("value"))
}

func TestEmbedded_SetAndGet(t *testing.T) {
	dir, _ := os.MkdirTemp("", "TestEmbedded_SetAndGet")
	defer os.RemoveAll(dir)
	cfg := createConfig([]string{"localhost:20004", "localhost:20005"}, dir)

	e1, err := New("localhost:20004", cfg)
	assert.NoError(t, err)
	e2, err := New("localhost:20005", cfg)
	assert.NoError(t, err)
	e := []*Embedded{e1, e2}

	leader := -1
	select {
	case <-e1.LeaderChange():
		leader = 0
	case <-e2.LeaderChange():
		leader = 1
	}

	err = e[leader].Set(context.Background(), "key", []byte("value"))
	assert.NoError(t, err)

	var wg sync.WaitGroup
	for _, e := range []*Embedded{e1, e2} {
		wg.Add(1)
		go func(e *Embedded) {
			defer wg.Done()
			time.Sleep(time.Second)
			value, err := e.Get(context.Background(), "key")
			assert.NoError(t, err)
			assert.Equal(t, []byte("value"), value)
		}(e)
	}
	wg.Wait()
}

func TestEmbedded_Delete(t *testing.T) {
	dir, _ := os.MkdirTemp("", "TestEmbedded_Delete")
	defer os.RemoveAll(dir)
	cfg := createConfig([]string{"localhost:20006", "localhost:20007"}, dir)

	e1, err := New("localhost:20006", cfg)
	assert.NoError(t, err)
	e2, err := New("localhost:20007", cfg)
	assert.NoError(t, err)
	e := []*Embedded{e1, e2}

	leader := -1
	select {
	case <-e1.LeaderChange():
		leader = 0
	case <-e2.LeaderChange():
		leader = 1
	}

	err = e[leader].Set(context.Background(), "key", []byte("value"))
	assert.NoError(t, err)

	err = e[leader].Delete(context.Background(), "key")
	assert.NoError(t, err)

	var wg sync.WaitGroup
	for _, e := range []*Embedded{e1, e2} {
		wg.Add(1)
		go func(e *Embedded) {
			defer wg.Done()
			time.Sleep(time.Second)
			_, err := e.Get(context.Background(), "key")
			assert.ErrorIs(t, err, consts.ErrNotFound)
		}(e)
	}
	wg.Wait()
}

func TestEmbedded_List(t *testing.T) {
	dir, _ := os.MkdirTemp("", "TestEmbedded_List")
	defer os.RemoveAll(dir)
	cfg := createConfig([]string{"localhost:20008"}, dir)

	e1, err := New("localhost:20008", cfg)
	assert.NoError(t, err)

	<-e1.LeaderChange()

	err = e1.Set(context.Background(), "key1", []byte("value1"))
	assert.NoError(t, err)

	err = e1.Set(context.Background(), "key2", []byte("value2"))
	assert.NoError(t, err)

	time.Sleep(time.Second)
	entries, err := e1.List(context.Background(), "key")
	assert.NoError(t, err)
	assert.Len(t, entries, 2)
}
