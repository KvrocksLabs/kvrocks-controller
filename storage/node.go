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
package storage

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/net/context"

	"github.com/apache/kvrocks-controller/metadata"
	"github.com/apache/kvrocks-controller/util"
)

// ListNodes return the list of nodes under the specified shard
func (s *Storage) ListNodes(ctx context.Context, ns, cluster string, shardIdx int) ([]metadata.NodeInfo, error) {
	shard, err := s.getShard(ctx, ns, cluster, shardIdx)
	if err != nil {
		return nil, fmt.Errorf("get shard: %w", err)
	}
	return shard.Nodes, nil
}

// GetMasterNode return the master of node under the specified shard
func (s *Storage) GetMasterNode(ctx context.Context, ns, cluster string, shardIdx int) (metadata.NodeInfo, error) {
	nodes, err := s.ListNodes(ctx, ns, cluster, shardIdx)
	if err != nil {
		return metadata.NodeInfo{}, err
	}

	for _, node := range nodes {
		if node.Role == metadata.RoleMaster {
			return node, nil
		}
	}
	return metadata.NodeInfo{}, metadata.ErrEntryNoExists
}

// CreateNode add a node under the specified shard
func (s *Storage) CreateNode(ctx context.Context, ns, cluster string, shardIdx int, node *metadata.NodeInfo) error {
	clusterInfo, err := s.GetClusterInfo(ctx, ns, cluster)
	if err != nil {
		return fmt.Errorf("get cluster: %w", err)
	}
	if shardIdx >= len(clusterInfo.Shards) || shardIdx < 0 {
		return metadata.ErrIndexOutOfRange
	}
	for _, shard := range clusterInfo.Shards {
		if len(shard.Nodes) == 0 {
			continue
		}
		for _, existedNode := range shard.Nodes {
			if existedNode.Addr == node.Addr || existedNode.ID == node.ID {
				return metadata.ErrEntryExisted
			}
		}
	}

	shard := clusterInfo.Shards[shardIdx]
	if shard.Nodes == nil {
		shard.Nodes = make([]metadata.NodeInfo, 0)
	}

	// NodeRole check
	if len(shard.Nodes) == 0 && !node.IsMaster() {
		return errors.New("you MUST add master node first")
	}
	if len(shard.Nodes) != 0 && node.IsMaster() {
		return errors.New("the master node has already added in this shard")
	}
	clusterInfo.Version++
	clusterInfo.Shards[shardIdx].Nodes = append(clusterInfo.Shards[shardIdx].Nodes, *node)
	if err := s.updateCluster(ctx, ns, clusterInfo); err != nil {
		return err
	}
	s.EmitEvent(Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shardIdx,
		NodeID:    node.ID,
		Type:      EventNode,
		Command:   CommandCreate,
	})
	return nil
}

// RemoveNode delete the node from the specified shard
func (s *Storage) RemoveNode(ctx context.Context, ns, cluster string, shardIdx int, nodeID string) error {
	if len(nodeID) != metadata.NodeIdLen {
		return errors.New("invalid node length")
	}
	clusterInfo, err := s.GetClusterInfo(ctx, ns, cluster)
	if err != nil {
		return fmt.Errorf("get cluster: %w", err)
	}
	if shardIdx >= len(clusterInfo.Shards) || shardIdx < 0 {
		return metadata.ErrIndexOutOfRange
	}
	shard := clusterInfo.Shards[shardIdx]
	if shard.Nodes == nil {
		return metadata.ErrEntryNoExists
	}
	nodeIdx := -1
	for idx, node := range shard.Nodes {
		if strings.HasPrefix(node.ID, nodeID) {
			nodeIdx = idx
			break
		}
	}
	if nodeIdx == -1 {
		return metadata.ErrEntryNoExists
	}
	node := shard.Nodes[nodeIdx]
	if len(shard.SlotRanges) != 0 {
		if len(shard.Nodes) == 1 || node.IsMaster() {
			return errors.New("still some slots in this shard, please migrate them first")
		}
	} else {
		if node.IsMaster() && len(shard.Nodes) > 1 {
			return errors.New("please remove slave shards first")
		}
	}
	clusterInfo.Version++
	clusterInfo.Shards[shardIdx].Nodes = append(clusterInfo.Shards[shardIdx].Nodes[:nodeIdx], clusterInfo.Shards[shardIdx].Nodes[nodeIdx+1:]...)
	if err := s.updateCluster(ctx, ns, clusterInfo); err != nil {
		return err
	}
	s.EmitEvent(Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shardIdx,
		NodeID:    node.ID,
		Type:      EventNode,
		Command:   CommandRemove,
	})
	return nil
}

// PromoteNewMaster delete the master node from the specified shard
func (s *Storage) PromoteNewMaster(ctx context.Context, ns, cluster string, shardIdx int, oldMasterNodeID string) error {
	clusterInfo, err := s.GetClusterInfo(ctx, ns, cluster)
	if err != nil {
		return fmt.Errorf("get cluster: %w", err)
	}
	if shardIdx >= len(clusterInfo.Shards) || shardIdx < 0 {
		return metadata.ErrIndexOutOfRange
	}
	shard := clusterInfo.Shards[shardIdx]
	if shard.Nodes == nil || len(shard.Nodes) == 1 {
		return metadata.ErrShardNoReplica
	}

	var oldMasterNode *metadata.NodeInfo
	for idx, node := range shard.Nodes {
		if node.ID == oldMasterNodeID {
			oldMasterNode = &shard.Nodes[idx]
			break
		}
	}
	if oldMasterNode == nil {
		return fmt.Errorf("master node: %s not found in shard: %d", oldMasterNodeID, shardIdx)
	}
	if oldMasterNode.Role != metadata.RoleMaster {
		return errors.New("node %s role is not master")
	}

	var (
		oldMasterNodeIndex = -1
		fastestSlaveIndex  = -1
		maxReplOffset      uint64
	)
	for idx, node := range shard.Nodes {
		if node.ID == oldMasterNodeID {
			oldMasterNodeIndex = idx
			continue
		}
		nodeInfo, err := util.NodeInfoCmd(ctx, &node)
		if err != nil {
			continue
		}
		offsetNodeStr := nodeInfo.SlaveReplication.SlaveReplOffset
		if len(offsetNodeStr) == 0 {
			continue
		}
		offsetNode, _ := strconv.ParseUint(offsetNodeStr, 10, 64)
		if offsetNode >= maxReplOffset {
			maxReplOffset = offsetNode
			fastestSlaveIndex = idx
		}
	}
	if oldMasterNodeIndex == -1 {
		return metadata.ErrEntryNoExists
	}
	if fastestSlaveIndex == -1 {
		return metadata.ErrShardNoMatchPromoteNode
	}

	shard.Nodes[fastestSlaveIndex].Role = metadata.RoleMaster
	shard.Nodes[oldMasterNodeIndex].Role = metadata.RoleSlave
	clusterInfo.Version++
	clusterInfo.Shards[shardIdx] = shard
	if err := s.updateCluster(ctx, ns, clusterInfo); err != nil {
		return err
	}
	s.EmitEvent(Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shardIdx,
		NodeID:    shard.Nodes[fastestSlaveIndex].ID,
		Type:      EventNode,
		Command:   CommandRemove,
	})
	return nil
}

// UpdateNode update exists node under the specified shard
func (s *Storage) UpdateNode(ctx context.Context, ns, cluster string, shardIdx int, node *metadata.NodeInfo) error {
	clusterInfo, err := s.GetClusterInfo(ctx, ns, cluster)
	if err != nil {
		return fmt.Errorf("get cluster: %w", err)
	}
	if shardIdx >= len(clusterInfo.Shards) || shardIdx < 0 {
		return metadata.ErrIndexOutOfRange
	}
	shard := clusterInfo.Shards[shardIdx]
	if shard.Nodes == nil {
		return metadata.ErrEntryNoExists
	}
	// TODO: check the role
	for idx, existedNode := range shard.Nodes {
		if existedNode.ID == node.ID {
			clusterInfo.Version++
			clusterInfo.Shards[shardIdx].Nodes[idx] = *node
			if err := s.updateCluster(ctx, ns, clusterInfo); err != nil {
				return err
			}
			s.EmitEvent(Event{
				Namespace: ns,
				Cluster:   cluster,
				Shard:     shardIdx,
				NodeID:    node.ID,
				Type:      EventNode,
				Command:   CommandUpdate,
			})
			return nil
		}
	}
	return metadata.ErrEntryNoExists
}
