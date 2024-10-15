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
package store

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusterNode(t *testing.T) {
	ctx := context.Background()
	nodeAddr0 := "127.0.0.1:7770"
	nodeAddr1 := "127.0.0.1:7771"
	node0 := NewClusterNode(nodeAddr0, "")
	node1 := NewClusterNode(nodeAddr1, "")
	redisCli := node0.GetClient()

	defer func() {
		require.NoError(t, redisCli.FlushAll(ctx).Err())
		require.NoError(t, redisCli.Do(ctx, "COMPACT").Err())
		require.NoError(t, redisCli.Do(ctx, "CLUSTER", "RESET").Err())
	}()

	t.Run("Check the cluster mode", func(t *testing.T) {
		_, err := node0.CheckClusterMode(ctx)
		require.NoError(t, err)

		require.NoError(t, redisCli.Do(ctx, "CLUSTER", "RESET").Err())
		// set the cluster topology
		cluster := &Cluster{Shards: Shards{
			{Nodes: []Node{node0}, SlotRanges: []SlotRange{
				{Start: 0, Stop: 100},
				{Start: 102, Stop: 300},
				{Start: 302, Stop: 16383},
			}},
			{Nodes: []Node{node1}, SlotRanges: []SlotRange{}},
		}}

		cluster.Version.Store(1)
		require.NoError(t, node0.SyncClusterInfo(ctx, cluster))
		clusterInfo, err := node0.GetClusterInfo(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 1, clusterInfo.CurrentEpoch)
	})

	t.Run("Check the cluster node0 info", func(t *testing.T) {
		require.NoError(t, redisCli.Set(ctx, "foo", "bar", 0).Err())
		info, err := node0.GetClusterNodeInfo(ctx)
		require.NoError(t, err)
		require.True(t, info.Sequence > 0)
	})

	t.Run("Parse the cluster node info", func(t *testing.T) {
		clusterNodesStr, err := node0.GetClusterNodesString(ctx)
		require.NoError(t, err)
		clusterNodes, err := ParseCluster(clusterNodesStr)
		require.NoError(t, err)
		require.EqualValues(t, 1, clusterNodes.Version.Load())
		require.Len(t, clusterNodes.Shards, 2)
		require.Len(t, clusterNodes.Shards[0].Nodes, 1)
		require.EqualValues(t, []SlotRange{
			{Start: 0, Stop: 100},
			{Start: 102, Stop: 300},
			{Start: 302, Stop: 16383},
		}, clusterNodes.Shards[0].SlotRanges)
		require.EqualValues(t, nodeAddr0, clusterNodes.Shards[0].Nodes[0].Addr())
		require.EqualValues(t, node0.ID(), clusterNodes.Shards[0].Nodes[0].ID())

		// Ensure empty slot range is allowed in the cluster node info
		require.Len(t, clusterNodes.Shards[1].Nodes, 1)
		require.EqualValues(t, []SlotRange{}, clusterNodes.Shards[1].SlotRanges)
		require.EqualValues(t, nodeAddr1, clusterNodes.Shards[1].Nodes[0].Addr())
	})
}

func TestNodeInfo_Validate(t *testing.T) {
	node := &ClusterNode{}
	require.EqualError(t, node.Validate(), "node id shouldn't be empty")
	node.id = "1234"
	require.EqualError(t, node.Validate(), "the length of node id must be 40")
	node.id = strings.Repeat("1", NodeIDLen)
	require.EqualError(t, node.Validate(), "node role should be 'master' or 'slave'")
	node.role = RoleMaster
	node.addr = "1.2.3.4"
	require.NoError(t, node.Validate())
}
