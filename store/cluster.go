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
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"go.uber.org/atomic"

	"github.com/apache/kvrocks-controller/consts"
)

type Cluster struct {
	Name    string       `json:"name"`
	Version atomic.Int64 `json:"version"`
	Shards  []*Shard     `json:"shards"`
}

func NewCluster(name string, nodes []string, replicas int) (*Cluster, error) {
	if len(nodes) == 0 {
		return nil, errors.New("cluster nodes should NOT be empty")
	}
	if replicas < 0 {
		return nil, errors.New("replicas should NOT be less than 0")
	}
	if replicas == 0 {
		replicas = 1
	}
	if len(nodes)%replicas != 0 {
		return nil, errors.New("cluster nodes should be divisible by replicas")
	}
	shardCount := len(nodes) / replicas
	shards := make([]*Shard, 0)
	slotRanges := CalculateSlotRanges(shardCount)
	for i := 0; i < shardCount; i++ {
		shard := NewShard()
		shard.Nodes = make([]Node, 0)
		for j := 0; j < replicas; j++ {
			addr := nodes[i*replicas+j]
			role := RoleMaster
			if j != 0 {
				role = RoleSlave
			}
			node := NewClusterNode(addr, "")
			node.SetRole(role)
			shard.Nodes = append(shard.Nodes, node)
		}
		shard.SlotRanges = append(shard.SlotRanges, slotRanges[i])
		shards = append(shards, shard)
	}

	cluster := &Cluster{Name: name, Shards: shards}
	cluster.Version.Store(1)
	return cluster, nil
}

func (cluster *Cluster) Clone() *Cluster {
	clone := &Cluster{
		Name:   cluster.Name,
		Shards: make([]*Shard, 0),
	}
	clone.Version.Store(cluster.Version.Load())
	for _, shard := range cluster.Shards {
		clone.Shards = append(clone.Shards, shard.Clone())
	}
	return clone
}

// SetPassword will set the password for all nodes in the cluster.
func (cluster *Cluster) SetPassword(password string) {
	for i := 0; i < len(cluster.Shards); i++ {
		for j := 0; j < len(cluster.Shards[i].Nodes); j++ {
			cluster.Shards[i].Nodes[j].SetPassword(password)
		}
	}
}

func (cluster *Cluster) ToSlotString() (string, error) {
	var builder strings.Builder
	for i, shard := range cluster.Shards {
		shardSlotsString, err := shard.ToSlotsString()
		if err != nil {
			return "", fmt.Errorf("found err at shard[%d]: %w", i, err)
		}
		builder.WriteString(shardSlotsString)
	}
	return builder.String(), nil
}

func (cluster *Cluster) GetShard(shardIndex int) (*Shard, error) {
	if shardIndex < 0 || shardIndex >= len(cluster.Shards) {
		return nil, consts.ErrIndexOutOfRange
	}
	return cluster.Shards[shardIndex], nil
}

func (cluster *Cluster) AddNode(shardIndex int, addr, role, password string) error {
	if shardIndex < 0 || shardIndex >= len(cluster.Shards) {
		return consts.ErrIndexOutOfRange
	}
	return cluster.Shards[shardIndex].addNode(addr, role, password)
}

func (cluster *Cluster) RemoveNode(shardIndex int, nodeID string) error {
	if shardIndex < 0 || shardIndex >= len(cluster.Shards) {
		return consts.ErrIndexOutOfRange
	}
	return cluster.Shards[shardIndex].removeNode(nodeID)
}

func (cluster *Cluster) PromoteNewMaster(ctx context.Context,
	shardIdx int, masterNodeID, preferredNodeID string) (string, error) {
	shard, err := cluster.GetShard(shardIdx)
	if err != nil {
		return "", err
	}
	newMasterNodeID, err := shard.promoteNewMaster(ctx, masterNodeID, preferredNodeID)
	if err != nil {
		return "", err
	}
	cluster.Shards[shardIdx] = shard
	return newMasterNodeID, nil
}

func (cluster *Cluster) SyncToNodes(ctx context.Context) error {
	for i := 0; i < len(cluster.Shards); i++ {
		for _, node := range cluster.Shards[i].Nodes {
			if err := node.SyncClusterInfo(ctx, cluster); err != nil {
				return err
			}
		}
	}
	return nil
}

func (cluster *Cluster) GetNodes() []Node {
	nodes := make([]Node, 0)
	for i := 0; i < len(cluster.Shards); i++ {
		nodes = append(nodes, cluster.Shards[i].Nodes...)
	}
	return nodes
}

func (cluster *Cluster) Reset(ctx context.Context) error {
	for i := 0; i < len(cluster.Shards); i++ {
		for _, node := range cluster.Shards[i].Nodes {
			if err := node.Reset(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (cluster *Cluster) findShardIndexBySlot(slot int) (int, error) {
	if slot < 0 || slot > MaxSlotID {
		return -1, consts.ErrSlotOutOfRange
	}
	sourceShardIdx := -1
	for i := 0; i < len(cluster.Shards); i++ {
		slotRanges := cluster.Shards[i].SlotRanges
		for _, slotRange := range slotRanges {
			if slotRange.Contains(slot) {
				sourceShardIdx = i
				break
			}
		}
	}
	if sourceShardIdx == -1 {
		return -1, consts.ErrSlotNotBelongToAnyShard
	}
	return sourceShardIdx, nil
}

func (cluster *Cluster) MigrateSlot(ctx context.Context, slot int, targetShardIdx int, slotOnly bool) error {
	if targetShardIdx < 0 || targetShardIdx >= len(cluster.Shards) {
		return consts.ErrIndexOutOfRange
	}
	sourceShardIdx, err := cluster.findShardIndexBySlot(slot)
	if err != nil {
		return err
	}
	if sourceShardIdx == targetShardIdx {
		return consts.ErrShardIsSame
	}
	if slotOnly {
		cluster.Shards[sourceShardIdx].SlotRanges = RemoveSlotFromSlotRanges(cluster.Shards[sourceShardIdx].SlotRanges, slot)
		cluster.Shards[targetShardIdx].SlotRanges = AddSlotToSlotRanges(cluster.Shards[targetShardIdx].SlotRanges, slot)
		return nil
	}

	if cluster.Shards[sourceShardIdx].IsMigrating() || cluster.Shards[targetShardIdx].IsMigrating() {
		return consts.ErrShardSlotIsMigrating
	}
	// Send the migration command to the source node
	sourceMasterNode := cluster.Shards[sourceShardIdx].GetMasterNode()
	if sourceMasterNode == nil {
		return consts.ErrNotFound
	}
	targetNodeID := cluster.Shards[targetShardIdx].GetMasterNode().ID()
	if err := sourceMasterNode.MigrateSlot(ctx, slot, targetNodeID); err != nil {
		return err
	}

	// Will start the data migration in the background
	cluster.Shards[sourceShardIdx].MigratingSlot = slot
	cluster.Shards[sourceShardIdx].TargetShardIndex = targetShardIdx
	return nil
}

func (cluster *Cluster) SetSlot(ctx context.Context, slot int, targetNodeID string) error {
	version := cluster.Version.Inc()
	for i := 0; i < len(cluster.Shards); i++ {
		for _, node := range cluster.Shards[i].Nodes {
			clusterNode, ok := node.(*ClusterNode)
			if !ok {
				continue
			}
			err := clusterNode.GetClient().Do(ctx, "CLUSTERX", "SETSLOT", slot, "NODE", targetNodeID, version).Err()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ParseCluster will parse the cluster string into cluster topology.
func ParseCluster(clusterStr string) (*Cluster, error) {
	if len(clusterStr) == 0 {
		return nil, errors.New("cluster nodes string error")
	}
	nodeStrings := strings.Split(clusterStr, "\n")
	if len(nodeStrings) == 0 {
		return nil, errors.New("cluster nodes string parser error")
	}

	var clusterVer int64 = -1
	var shards Shards
	slaveNodes := make(map[string][]Node)
	for _, nodeString := range nodeStrings {
		fields := strings.Split(nodeString, " ")
		if len(fields) < 7 {
			return nil, fmt.Errorf("require at least 7 fields, node info[%s]", nodeString)
		}
		node := &ClusterNode{
			id:   fields[0],
			addr: strings.Split(fields[1], "@")[0],
		}

		if strings.Contains(fields[2], ",") {
			node.role = strings.Split(fields[2], ",")[1]
		} else {
			node.role = fields[2]
		}

		var err error
		clusterVer, err = strconv.ParseInt(fields[6], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("node version error, node info[%q]", nodeString)
		}

		if node.role == RoleMaster {
			shard := NewShard()
			shard.Nodes = append(shard.Nodes, node)
			// remain fields are slot ranges
			for i := 8; i < len(fields); i++ {
				slotRange, err := ParseSlotRange(fields[i])
				if err != nil {
					return nil, fmt.Errorf("parse slots error for node[%s]: %w", nodeString, err)
				}
				shard.SlotRanges = append(shard.SlotRanges, *slotRange)
			}
			shards = append(shards, shard)
		} else if node.role == RoleSlave {
			slaveNodes[fields[3]] = append(slaveNodes[fields[3]], node)
		} else {
			return nil, fmt.Errorf("node role error, node info[%q]", nodeString)
		}
	}
	if clusterVer == -1 {
		return nil, fmt.Errorf("no cluster version, cluster info[%q]", clusterStr)
	}
	sort.Sort(shards)
	for i := 0; i < len(shards); i++ {
		masterNode := shards[i].Nodes[0]
		shards[i].Nodes = append(shards[i].Nodes, slaveNodes[masterNode.ID()]...)
	}
	return &Cluster{
		Version: *atomic.NewInt64(clusterVer),
		Shards:  shards,
	}, nil
}
