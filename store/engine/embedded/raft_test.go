/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package embedded

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.uber.org/atomic"
)

func mockRaftNode(count int, path string, basePort int) ([]*raftNode, []chan string, []chan bool, []chan *commit) {
	nodes := make([]*raftNode, count)
	snapshotterReadyList := make([]chan *snap.Snapshotter, count)
	proposeChList := make([]chan string, count)
	leaderChangeChList := make([]chan bool, count)
	commitChList := make([]chan *commit, count)
	peers := make([]string, count)
	for i := 0; i < count; i++ {
		peers[i] = fmt.Sprintf("http://127.0.0.1:%d", basePort+i)
	}

	for i := 0; i < count; i++ {
		snapshotterReadyList[i] = make(chan *snap.Snapshotter, 1)
		proposeChList[i] = make(chan string)
		leaderChangeChList[i] = make(chan bool)
		commitChList[i] = make(chan *commit)
		notifier := &raftNotifier{
			proposeChList[i],
			make(chan raftpb.ConfChange),
			leaderChangeChList[i],
			commitChList[i],
			make(chan error),
			snapshotterReadyList[i],
		}
		nodes[i] = newRaftNode(i+1, peers, false, path, func() ([]byte, error) {
			return nil, nil
		}, notifier)
	}
	for i := 0; i < count; i++ {
		<-snapshotterReadyList[i]
	}
	return nodes, proposeChList, leaderChangeChList, commitChList
}

func TestRaftNode_processMessages(t *testing.T) {
	dir, _ := os.MkdirTemp("", "TestRaftNode_processMessages")
	defer os.RemoveAll(dir)

	//nolint:dogsled
	nodes, _, _, _ := mockRaftNode(1, dir, 10000)
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
	dir, _ := os.MkdirTemp("", "TestRaftNode_saveSnap")
	defer os.RemoveAll(dir)
	//nolint:dogsled
	nodes, _, _, _ := mockRaftNode(1, dir, 10001)
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

	_, err = os.Stat(filepath.Join(dir, fmt.Sprintf("storage-%d-snap/%016x-%016x.snap", 1, 1, 1)))
	assert.NoError(t, err, "Cannot find saved snapshot")

	_, err = os.Stat(filepath.Join(dir, fmt.Sprintf("storage-%d/%016x-%016x.wal", 1, 0, 0)))
	assert.NoError(t, err, "Cannot find saved wal")

	savedSnap, err := node.snapshotter.Load()
	assert.NoError(t, err, "Failed to load snapshot")

	assert.Equal(t, snapshot, *savedSnap)
}

func TestRaftNode_EventualConsistency(t *testing.T) {
	tests := []struct {
		name  string
		count int
	}{
		{"single", 1},
		{"double", 2},
		{"triple", 3},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir, _ := os.MkdirTemp("", fmt.Sprintf("TestRaftNode_EventualConsistency_%s", test.name))
			defer os.RemoveAll(dir)

			// Create two mock raftNodes
			prefixSum := test.count * (test.count - 1) / 2
			_, proposeChList, leaderChangeChList, commitChList := mockRaftNode(test.count, dir, 10002+prefixSum)

			// Define the data to be proposed
			data := []string{"data1-1", "data1-2", "data1-3"}
			leader := atomic.NewInt64(-1)
			for i := 0; i < test.count; i++ {
				go func(i int64) {
					for range leaderChangeChList[i] {
						leader.Store(i)
					}
				}(int64(i))
			}

			for leader.Load() < 0 {
				time.Sleep(10 * time.Millisecond)
			}

			// Start two goroutines to propose data
			for _, column := range data {
				proposeChList[leader.Load()] <- column
			}

			// Start two goroutines to read commits
			var commits [][]string
			for i := 0; i < test.count; i++ {
				commits = append(commits, make([]string, 0, len(data)))
			}

			for i, ch := range commitChList {
				commits[i] = append(commits[i], (<-ch).data...)
			}

			// Check the consistency of all data
			sort.Strings(commits[0])
			assert.NotEmpty(t, commits[0])
			for i := 1; i < test.count; i++ {
				sort.Strings(commits[i])
				assert.Equal(t, commits[0], commits[i])
			}
		})
	}
}
