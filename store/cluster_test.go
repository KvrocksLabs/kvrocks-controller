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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks-controller/consts"
)

func TestCluster_PromoteNewMaster(t *testing.T) {
	shard := NewShard()
	shard.SlotRanges = []SlotRange{{Start: 0, Stop: 1023}}

	node0 := NewClusterMockNode()
	node0.SetRole(RoleMaster)

	node1 := NewClusterMockNode()
	node1.SetRole(RoleSlave)
	node1.Sequence = 200

	node2 := NewClusterMockNode()
	node2.SetRole(RoleSlave)
	node2.Sequence = 100

	node3 := NewClusterMockNode()
	node3.SetRole(RoleSlave)
	node3.Sequence = 300

	shard.Nodes = []Node{node0}
	cluster := &Cluster{
		Shards: Shards{shard},
	}

	ctx := context.Background()
	_, err := cluster.PromoteNewMaster(ctx, -1, node0.ID(), "")
	require.ErrorIs(t, err, consts.ErrIndexOutOfRange)
	_, err = cluster.PromoteNewMaster(ctx, 1, node0.ID(), "")
	require.ErrorIs(t, err, consts.ErrIndexOutOfRange)
	_, err = cluster.PromoteNewMaster(ctx, 0, node0.ID(), "")
	require.ErrorIs(t, err, consts.ErrShardNoReplica)

	shard.Nodes = append(shard.Nodes, node1, node2, node3)
	_, err = cluster.PromoteNewMaster(ctx, 0, node1.ID(), "")
	require.ErrorIs(t, err, consts.ErrNodeIsNotMaster)

	newMasterID, err := cluster.PromoteNewMaster(ctx, 0, node0.ID(), "")
	require.NoError(t, err)
	require.Equal(t, node3.ID(), newMasterID)

	// test preferredNodeID
	newMasterID, err = cluster.PromoteNewMaster(ctx, 0, node3.ID(), node2.ID())
	require.NoError(t, err)
	require.Equal(t, node2.ID(), newMasterID)
}
