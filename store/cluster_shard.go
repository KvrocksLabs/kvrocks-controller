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
	"strings"

	"go.uber.org/zap"

	"github.com/apache/kvrocks-controller/logger"

	"github.com/apache/kvrocks-controller/consts"
)

type Shard struct {
	Nodes         []Node      `json:"nodes"`
	SlotRanges    []SlotRange `json:"slot_ranges"`
	ImportSlot    int         `json:"import_slot"`
	MigratingSlot int         `json:"migrating_slot"`
}

type Shards []*Shard

func (s Shards) Len() int {
	return len(s)
}
func (s Shards) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s Shards) Less(i, j int) bool {
	return s[i].SlotRanges[0].Start < s[j].SlotRanges[0].Start
}

func NewShard() *Shard {
	return &Shard{
		Nodes:         make([]Node, 0),
		SlotRanges:    make([]SlotRange, 0),
		ImportSlot:    -1,
		MigratingSlot: -1,
	}
}

func (shard *Shard) promoteNewMaster(ctx context.Context, oldMasterNodeID string) (string, error) {
	if len(shard.Nodes) <= 1 {
		return "", consts.ErrShardNoReplica
	}

	oldMasterNodeIndex := -1
	preferredNewMasterIndex := -1
	var newestOffset uint64
	for i, node := range shard.Nodes {
		if node.ID() == oldMasterNodeID {
			oldMasterNodeIndex = i
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
		if clusterNodeInfo.Sequence >= newestOffset {
			preferredNewMasterIndex = i
			newestOffset = clusterNodeInfo.Sequence
		}
	}

	if oldMasterNodeIndex == -1 {
		return "", consts.ErrOldMasterNodeNotFound
	}
	if !shard.Nodes[oldMasterNodeIndex].IsMaster() {
		return "", consts.ErrNodeIsNotMaster
	}
	if preferredNewMasterIndex == -1 {
		return "", consts.ErrShardNoMatchPromoteNode
	}
	shard.Nodes[oldMasterNodeIndex].SetRole(RoleSlave)
	shard.Nodes[preferredNewMasterIndex].SetRole(RoleMaster)
	preferredNewMasterNode := shard.Nodes[preferredNewMasterIndex]
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
		SlotRanges    []SlotRange    `json:"slot_ranges"`
		ImportSlot    int            `json:"import_slot"`
		MigratingSlot int            `json:"migrating_slot"`
		Nodes         []*ClusterNode `json:"nodes"`
	}
	if err := json.Unmarshal(bytes, &data); err != nil {
		return err
	}
	shard.SlotRanges = data.SlotRanges
	shard.ImportSlot = data.ImportSlot
	shard.MigratingSlot = data.MigratingSlot
	shard.Nodes = make([]Node, len(data.Nodes))
	for i, node := range data.Nodes {
		shard.Nodes[i] = node
	}
	return nil
}
