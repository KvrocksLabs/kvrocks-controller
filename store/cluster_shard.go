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
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/apache/kvrocks-controller/logger"

	"github.com/apache/kvrocks-controller/consts"
)

type Shard struct {
	Nodes            []Node      `json:"nodes"`
	SlotRanges       []SlotRange `json:"slot_ranges"`
	TargetShardIndex int         `json:"target_shard_index"`
	MigratingSlot    int         `json:"migrating_slot"`
}

type Shards []*Shard

func (s Shards) Len() int {
	return len(s)
}
func (s Shards) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s Shards) Less(i, j int) bool {
	if len(s[i].SlotRanges) == 0 {
		return false
	} else if len(s[j].SlotRanges) == 0 {
		return true
	}
	return s[i].SlotRanges[0].Start < s[j].SlotRanges[0].Start
}

func NewShard() *Shard {
	return &Shard{
		Nodes:            make([]Node, 0),
		SlotRanges:       make([]SlotRange, 0),
		MigratingSlot:    -1,
		TargetShardIndex: -1,
	}
}

func (shard *Shard) ClearMigrateState() {
	shard.MigratingSlot = -1
	shard.TargetShardIndex = -1
}

func (shard *Shard) IsServicing() bool {
	for _, slotRange := range shard.SlotRanges {
		if slotRange.Start != -1 || slotRange.Stop != -1 {
			return true
		}
	}
	return shard.IsMigrating()
}

func (shard *Shard) addNode(addr, role, password string) error {
	if role != RoleMaster && role != RoleSlave {
		return fmt.Errorf("%w: role", consts.ErrInvalidArgument)
	}
	for _, node := range shard.Nodes {
		if node.Addr() == addr {
			return consts.ErrAlreadyExists
		}
	}
	if role == RoleMaster && len(shard.Nodes) > 0 {
		return fmt.Errorf("master node %w", consts.ErrAlreadyExists)
	}
	node := NewClusterNode(addr, password)
	node.SetRole(role)
	shard.Nodes = append(shard.Nodes, node)
	return nil
}

func (shard *Shard) IsMigrating() bool {
	return shard.MigratingSlot != -1 && shard.TargetShardIndex != -1
}

func (shard *Shard) GetMasterNode() Node {
	for _, node := range shard.Nodes {
		if node.IsMaster() {
			return node
		}
	}
	return nil
}

func (shard *Shard) removeNode(nodeID string) error {
	isFound := false
	for i, node := range shard.Nodes {
		if node.ID() != nodeID {
			continue
		}
		if node.IsMaster() {
			return fmt.Errorf("cannot remove master node: %w", consts.ErrInvalidArgument)
		}
		shard.Nodes = append(shard.Nodes[:i], shard.Nodes[i+1:]...)
		isFound = true
	}
	if !isFound {
		return consts.ErrNotFound
	}
	return nil
}

func (shard *Shard) getNewMasterNodeIndex(ctx context.Context, masterNodeIndex int, preferredNodeID string) int {
	newMasterNodeIndex := -1
	var newestOffset uint64
	for i, node := range shard.Nodes {
		// don't promote the current master node
		if i == masterNodeIndex {
			continue
		}
		clusterNodeInfo, err := node.GetClusterNodeInfo(ctx)
		if err != nil {
			logger.Get().With(
				zap.Error(err),
				zap.String("id", node.ID()),
				zap.String("addr", node.Addr()),
			).Warn("Skip the node due to failed to get cluster info")
			continue
		}
		// If the preferredNodeID is not empty, we will use it as the new master node.
		if preferredNodeID != "" && node.ID() == preferredNodeID {
			newMasterNodeIndex = i
			break
		}
		if clusterNodeInfo.Sequence >= newestOffset {
			newMasterNodeIndex = i
			newestOffset = clusterNodeInfo.Sequence
		}
	}
	return newMasterNodeIndex
}

// PromoteNewMaster promotes a new master node in the shard,
// it will return the new master node ID.
//
// The masterNodeID is used to check if the node is the current master node if it's not empty.
// The preferredNodeID is used to specify the preferred node to be promoted as the new master node,
// it will choose the node with the highest sequence number if the preferredNodeID is empty.
func (shard *Shard) promoteNewMaster(ctx context.Context, masterNodeID, preferredNodeID string) (string, error) {
	if len(shard.Nodes) <= 1 {
		return "", consts.ErrShardNoReplica
	}

	oldMasterNodeIndex := -1
	for i, node := range shard.Nodes {
		if node.IsMaster() {
			oldMasterNodeIndex = i
			break
		}
	}
	if oldMasterNodeIndex == -1 {
		return "", consts.ErrOldMasterNodeNotFound
	}
	if masterNodeID != "" && shard.Nodes[oldMasterNodeIndex].ID() != masterNodeID {
		return "", consts.ErrNodeIsNotMaster
	}
	newMasterNodeIndex := shard.getNewMasterNodeIndex(ctx, oldMasterNodeIndex, preferredNodeID)
	if newMasterNodeIndex == -1 {
		return "", consts.ErrShardNoMatchNewMaster
	}
	shard.Nodes[oldMasterNodeIndex].SetRole(RoleSlave)
	shard.Nodes[newMasterNodeIndex].SetRole(RoleMaster)
	preferredNewMasterNode := shard.Nodes[newMasterNodeIndex]
	return preferredNewMasterNode.ID(), nil
}

func (shard *Shard) HasOverlap(slotRange *SlotRange) bool {
	for _, shardSlotRange := range shard.SlotRanges {
		if shardSlotRange.HasOverlap(slotRange) {
			return true
		}
	}
	return false
}

func (shard *Shard) ToSlotsString() (string, error) {
	var builder strings.Builder
	masterNodeIndex := -1
	for i, node := range shard.Nodes {
		if node.IsMaster() {
			masterNodeIndex = i
			break
		}
	}
	if masterNodeIndex == -1 {
		return "", errors.New("missing master node")
	}

	for i, node := range shard.Nodes {
		builder.WriteString(node.ID())
		builder.WriteByte(' ')
		builder.WriteString(strings.Replace(node.Addr(), ":", " ", 1))
		builder.WriteByte(' ')
		if i == masterNodeIndex {
			builder.WriteString(RoleMaster)
			builder.WriteByte(' ')
			builder.WriteByte('-')
			builder.WriteByte(' ')
			for j, slotRange := range shard.SlotRanges {
				builder.WriteString(slotRange.String())
				if j != len(shard.SlotRanges)-1 {
					builder.WriteByte(' ')
				}
			}
		} else {
			builder.WriteString(RoleSlave)
			builder.WriteByte(' ')
			builder.WriteString(shard.Nodes[masterNodeIndex].ID())
		}
		builder.WriteByte('\n')
	}
	return builder.String(), nil
}

// UnmarshalJSON unmarshal a Shard from JSON bytes,
// it's required since Shard.Nodes is an interface slice.
// So we need to take into a concrete type.
func (shard *Shard) UnmarshalJSON(bytes []byte) error {
	var data struct {
		SlotRanges       []SlotRange    `json:"slot_ranges"`
		TargetShardIndex int            `json:"target_shard_index"`
		MigratingSlot    int            `json:"migrating_slot"`
		Nodes            []*ClusterNode `json:"nodes"`
	}
	if err := json.Unmarshal(bytes, &data); err != nil {
		return err
	}
	shard.SlotRanges = data.SlotRanges
	shard.TargetShardIndex = data.TargetShardIndex
	shard.MigratingSlot = data.MigratingSlot
	shard.Nodes = make([]Node, len(data.Nodes))
	for i, node := range data.Nodes {
		shard.Nodes[i] = node
	}
	return nil
}
