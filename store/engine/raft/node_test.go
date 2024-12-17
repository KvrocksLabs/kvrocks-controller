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

package raft

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
)

type TestCluster struct {
	nodes []*Node
}

func NewTestCluster(n int) *TestCluster {
	if n > 16 {
		n = 16
	}
	nodes := make([]*Node, n)
	randomStartPort := rand.Int31n(1024) + 10000
	peers := make([]string, n)
	for i := 0; i < n; i++ {
		peers[i] = fmt.Sprintf("http://127.0.0.1:%d", randomStartPort+int32(i))
	}
	for i := 0; i < n; i++ {
		nodes[i], _ = New(&Config{
			ID:               uint64(i + 1),
			DataDir:          fmt.Sprintf("/tmp/kvrocks/raft/%d", randomStartPort+int32(i)),
			Peers:            peers,
			HeartbeatSeconds: 1,
			ElectionSeconds:  2,
		})
		// drain leader change events
		go func() {
			for range nodes[i].LeaderChange() {
			}
		}()
	}
	return &TestCluster{nodes: nodes}
}

func (c *TestCluster) createNode(peers []string) (*Node, error) {
	randomPort := rand.Int31n(1024) + 20000
	addr := fmt.Sprintf("http://127.0.0.1:%d", randomPort)
	node, err := New(&Config{
		ID:               uint64(len(peers) + 1),
		DataDir:          fmt.Sprintf("/tmp/kvrocks/raft/%d", randomPort),
		Peers:            append(peers, addr),
		HeartbeatSeconds: 1,
		ElectionSeconds:  2,
	})
	if err != nil {
		return nil, err
	}
	c.nodes = append(c.nodes, node)
	return node, nil
}

func (c *TestCluster) AddNode(ctx context.Context, nodeID uint64, peer string) error {
	if len(c.nodes) == 0 {
		return nil
	}
	return c.nodes[0].AddPeer(ctx, nodeID, peer)
}

func (c *TestCluster) RemoveNode(ctx context.Context, nodeID uint64) error {
	var node *Node
	for i, n := range c.nodes {
		if n.config.ID == nodeID {
			node = n
			c.nodes = append(c.nodes[:i], c.nodes[i+1:]...)
			break
		}
	}
	if len(c.nodes) == 0 || node == nil {
		return nil
	}
	return node.RemovePeer(ctx, nodeID)
}

func (c *TestCluster) SetSnapshotThreshold(threshold uint64) {
	for _, n := range c.nodes {
		n.SetSnapshotThreshold(threshold)
	}
}

func (c *TestCluster) IsReady(ctx context.Context) bool {
	for _, n := range c.nodes {
		if !n.IsReady(ctx) {
			return false
		}
	}
	return true
}

func (c *TestCluster) GetNode(i int) *Node {
	if i < 0 || i >= len(c.nodes) {
		return nil
	}
	return c.nodes[i]
}

func (c *TestCluster) GetLeaderNode() *Node {
	leaderID := raft.None
	for _, n := range c.nodes {
		if n.GetRaftLead() == leaderID {
			continue
		}
		leaderID = n.GetRaftLead()
	}
	if leaderID == raft.None {
		return nil
	}
	return c.GetNode(int(leaderID - 1))
}

func (c *TestCluster) ListNodes() []*Node {
	return c.nodes
}

func (c *TestCluster) Restart() error {
	for _, n := range c.nodes {
		n.Close()
	}
	for _, n := range c.nodes {
		if err := n.run(); err != nil {
			return err
		}
	}
	return nil
}

func (c *TestCluster) Close() {
	for _, n := range c.nodes {
		n.Close()
		os.RemoveAll(n.config.DataDir)
	}
}

func TestCluster_SingleNode(t *testing.T) {
	cluster := NewTestCluster(1)
	defer cluster.Close()

	ctx := context.Background()
	require.Eventually(t, func() bool {
		return cluster.IsReady(ctx)
	}, 10*time.Second, 100*time.Millisecond)

	n := cluster.GetNode(0)
	require.NotNil(t, n)
	require.NoError(t, n.Set(ctx, "foo", []byte("bar")))

	require.Eventually(t, func() bool {
		gotBytes, _ := n.Get(ctx, "foo")
		return string(gotBytes) == "bar"
	}, 1*time.Second, 100*time.Millisecond)
}

func TestCluster_MultiNodes(t *testing.T) {
	cluster := NewTestCluster(3)
	defer cluster.Close()

	ctx := context.Background()
	require.Eventually(t, func() bool {
		return cluster.IsReady(ctx)
	}, 10*time.Second, 100*time.Millisecond)

	t.Run("works well with all nodes ready", func(t *testing.T) {
		n1 := cluster.GetNode(0)
		n2 := cluster.GetNode(1)
		require.NoError(t, n1.Set(ctx, "foo", []byte("bar")))
		require.Eventually(t, func() bool {
			got, _ := n2.Get(ctx, "foo")
			return string(got) == "bar"
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("works well if 1/3 nodes down", func(t *testing.T) {
		oldLeaderNode := cluster.GetLeaderNode()
		require.NotNil(t, oldLeaderNode)
		oldLeaderNode.Close()

		require.Eventually(t, func() bool {
			newLeaderNode := cluster.GetLeaderNode()
			return newLeaderNode != nil && newLeaderNode != oldLeaderNode
		}, 10*time.Second, 200*time.Millisecond)

		leaderNode := cluster.GetLeaderNode()
		require.NoError(t, leaderNode.Set(ctx, "foo", []byte("bar")))
	})
}

func TestCluster_AddRemovePeer(t *testing.T) {
	cluster := NewTestCluster(3)
	defer cluster.Close()

	ctx := context.Background()
	require.Eventually(t, func() bool {
		return cluster.IsReady(ctx)
	}, 10*time.Second, 100*time.Millisecond)

	n1 := cluster.GetNode(0)
	require.NoError(t, n1.Set(ctx, "foo", []byte("bar")))
	require.Eventually(t, func() bool {
		got, _ := n1.Get(ctx, "foo")
		return string(got) == "bar"
	}, 1*time.Second, 100*time.Millisecond)

	t.Run("add a new peer node", func(t *testing.T) {
		n4, err := cluster.createNode(n1.config.Peers)
		require.NoError(t, err)
		require.NotNil(t, n4)

		require.NoError(t, cluster.AddNode(ctx, n4.config.ID, n4.Addr()))
		require.Eventually(t, func() bool {
			return n4.IsReady(ctx)
		}, 10*time.Second, 100*time.Millisecond)

		require.NoError(t, n4.Set(ctx, "foo", []byte("bar-1")))
		require.Eventually(t, func() bool {
			got, _ := n1.Get(ctx, "foo")
			return string(got) == "bar-1"
		}, 1*time.Second, 100*time.Millisecond)
		require.Len(t, n1.ListPeers(), 4)
	})

	t.Run("remove a peer node", func(t *testing.T) {
		cluster.RemoveNode(ctx, 4)
		require.Eventually(t, func() bool {
			return len(n1.ListPeers()) == 3
		}, 10*time.Second, 100*time.Millisecond)
	})
}

func TestTriggerSnapshot(t *testing.T) {
	cluster := NewTestCluster(3)
	defer cluster.Close()

	ctx := context.Background()
	require.Eventually(t, func() bool {
		return cluster.IsReady(ctx)
	}, 10*time.Second, 100*time.Millisecond)

	cnt := 128
	cluster.SetSnapshotThreshold(uint64(cnt / 5))

	n := cluster.GetNode(0)
	require.NotNil(t, n)
	for i := 0; i < cnt; i++ {
		require.NoError(t, n.Set(ctx, fmt.Sprintf("foo%d", i), []byte("bar")))
	}

	// Use Eventually to wait for snapshot to be triggered
	require.Eventually(t, func() bool {
		allNodesHasSnapshot := true
		for _, n := range cluster.ListNodes() {
			snapshot, err := n.dataStore.loadSnapshotFromDisk()
			require.NoError(t, err)
			if snapshot.Metadata.Index <= 0 {
				allNodesHasSnapshot = false
				break
			}
		}
		return allNodesHasSnapshot
	}, 10*time.Second, 100*time.Millisecond)

	require.NoError(t, cluster.Restart())
	require.Eventually(t, func() bool {
		return cluster.IsReady(ctx)
	}, 10*time.Second, 100*time.Millisecond)

	// Can restore data from snapshot correctly after restart
	for i := 0; i < cnt; i++ {
		gotBytes, _ := n.Get(ctx, fmt.Sprintf("foo%d", i))
		require.Equal(t, "bar", string(gotBytes))
	}
}
