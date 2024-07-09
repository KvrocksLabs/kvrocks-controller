package embedded

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func createConfig(peers []string) *Config {
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
	}
}

func TestNew(t *testing.T) {
	cfg := createConfig([]string{"localhost:1234", "localhost:1235"})
	_, err := New("localhost:1234", cfg)
	assert.NoError(t, err)
}

func TestEmbedded_Propose(t *testing.T) {
	cfg := createConfig([]string{"localhost:1234", "localhost:1235"})

	e1, err := New("localhost:1234", cfg)
	assert.NoError(t, err)

	e1.Propose("key", []byte("value"))
}

func TestEmbedded_SetAndGet(t *testing.T) {
	cfg := createConfig([]string{"localhost:1234", "localhost:1235"})

	e1, err := New("localhost:1234", cfg)
	assert.NoError(t, err)
	e2, err := New("localhost:1235", cfg)
	assert.NoError(t, err)

	err = e1.Set(context.Background(), "key", []byte("value"))
	assert.NoError(t, err)

	var wg sync.WaitGroup
	for _, e := range []*Embedded{e1, e2} {
		wg.Add(1)
		go func(e *Embedded) {
			defer wg.Done()
			time.Sleep(20 * time.Second)
			value, err := e.Get(context.Background(), "key")
			assert.NoError(t, err)
			assert.Equal(t, []byte("value"), value)
		}(e)
	}
	wg.Wait()
}

func TestEmbedded_Delete(t *testing.T) {
	cfg := createConfig([]string{"localhost:1234", "localhost:1235"})

	e, err := New("localhost:1234", cfg)
	assert.NoError(t, err)

	err = e.Set(context.Background(), "key", []byte("value"))
	assert.NoError(t, err)

	err = e.Delete(context.Background(), "key")
	assert.NoError(t, err)

	_, err = e.Get(context.Background(), "key")
	assert.Error(t, err)
}

func TestEmbedded_List(t *testing.T) {
	cfg := createConfig([]string{"localhost:1234", "localhost:1235"})

	e, err := New("localhost:1234", cfg)
	assert.NoError(t, err)

	err = e.Set(context.Background(), "key1", []byte("value1"))
	assert.NoError(t, err)

	err = e.Set(context.Background(), "key2", []byte("value2"))
	assert.NoError(t, err)

	entries, err := e.List(context.Background(), "")
	assert.NoError(t, err)
	assert.Len(t, entries, 2)
}
