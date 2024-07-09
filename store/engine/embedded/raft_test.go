package embedded

import (
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
)

func mockRaftNode(count int) ([]*raftNode, []chan string, []chan *commit) {
	nodes := make([]*raftNode, count)
	snapshotterReadyList := make([]chan *snap.Snapshotter, count)
	proposeChList := make([]chan string, count)
	commitChList := make([]chan *commit, count)
	peers := make([]string, count)
	for i := 0; i < count; i++ {
		peers[i] = fmt.Sprintf("http://127.0.0.1:%d", 1212+i)
	}

	for i := 0; i < count; i++ {
		snapshotterReadyList[i] = make(chan *snap.Snapshotter, 1)
		proposeChList[i] = make(chan string)
		commitChList[i] = make(chan *commit)
		nodes[i] = newRaftNode(
			i+1,
			peers,
			false,
			func() ([]byte, error) {
				return nil, nil
			},
			proposeChList[i],
			make(chan raftpb.ConfChange),
			make(chan bool),
			commitChList[i],
			make(chan error),
			snapshotterReadyList[i],
		)
	}
	for i := 0; i < count; i++ {
		<-snapshotterReadyList[i]
	}
	return nodes, proposeChList, commitChList
}

func TestRaftNode_processMessages(t *testing.T) {
	nodes, _, _ := mockRaftNode(1)
	node := nodes[0]
	msgs := []raftpb.Message{
		{
			Type: raftpb.MsgSnap,
			Snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}},
				},
			},
		},
		{
			Type: raftpb.MsgProp,
		},
	}

	expected := raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	node.confState = expected

	result := node.processMessages(msgs)

	assert.Equal(t, result[0].Snapshot.Metadata.ConfState, expected)
}

func TestRaftNode_saveSnap(t *testing.T) {
	nodes, _, _ := mockRaftNode(1)
	node := nodes[0]

	snapshot := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     1,
			Term:      1,
			ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}},
		},
		Data: []byte("test data"),
	}

	// Save the snapshot
	err := node.saveSnap(snapshot)
	assert.NoError(t, err, "Failed to save snapshot")

	// Check if the snapshot path 'storage-%d-snap' exists in disk
	_, err = os.Stat(fmt.Sprintf("storage-%d", node.id))
	assert.NoError(t, err, "Cannot find saved wal")

	_, err = os.Stat(fmt.Sprintf("storage-%d-snap", node.id))
	assert.NoError(t, err, "Cannot find saved snapshot")

	savedSnap, err := node.snapshotter.Load()
	assert.NoError(t, err, "Failed to load snapshot")

	assert.Equal(t, snapshot, *savedSnap)

	// Cleanup after test
	t.Cleanup(func() {
		_ = os.RemoveAll(fmt.Sprintf("storage-%d", node.id))
		_ = os.RemoveAll(fmt.Sprintf("storage-%d-snap", node.id))
	})
}

func TestRaftNode_EventualConsistency(t *testing.T) {
	// Create two mock raftNodes
	nodes, proposeChList, commitChList := mockRaftNode(3)

	// Define the data to be proposed
	data := [][]string{
		{"data1-1", "data1-2", "data1-3", "data1-1", "data1-2", "data1-3", "data1-1", "data1-2", "data1-3"},
		{"data2-1", "data2-2", "data2-3", "data2-1", "data2-2", "data2-3", "data2-1", "data2-2", "data2-3"},
		{"data3-1", "data3-2", "data3-3", "data3-1", "data3-2", "data3-3", "data3-1", "data3-2", "data3-3"},
	}

	// Start two goroutines to propose data
	for i, row := range data {
		go func(i int, row []string) {
			for _, column := range row {
				proposeChList[i] <- column
			}
		}(i, row)
	}

	// Start two goroutines to read commits
	commits := [][]string{
		make([]string, 0, len(data[0])),
		make([]string, 0, len(data[0])),
		make([]string, 0, len(data[0])),
	}

	for i, ch := range commitChList {
		go func(i int, ch chan *commit) {
			for commit := range ch {
				commits[i] = append(commits[i], commit.data...)
			}
		}(i, ch)
	}

	// Wait for all data to be proposed and committed
	time.Sleep(100 * time.Second)

	// Check the consistency of all data
	sort.Strings(commits[0])
	for i := 1; i < len(commits); i++ {
		sort.Strings(commits[i])
		assert.Equal(t, commits[0], commits[i])
	}

	// Cleanup after test
	t.Cleanup(func() {
		for _, node := range nodes {
			_ = os.RemoveAll(fmt.Sprintf("storage-%d", node.id))
			_ = os.RemoveAll(fmt.Sprintf("storage-%d-snap", node.id))
		}
	})
}
