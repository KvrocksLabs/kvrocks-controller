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

	"github.com/apache/kvrocks-controller/consts"
)

type Cluster struct {
	Name    string   `json:"name"`
	Version int64    `json:"version"`
	Shards  []*Shard `json:"shards"`
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

func (cluster *Cluster) GetShard(shardIdx int) (*Shard, error) {
	if shardIdx < 0 || shardIdx >= len(cluster.Shards) {
		return nil, consts.ErrIndexOutOfRange
	}
	return cluster.Shards[shardIdx], nil
}

func (cluster *Cluster) PromoteNewMaster(ctx context.Context, shardIdx int, oldMasterNodeID string) (string, error) {
	if oldMasterNodeID == "" {
		return "", consts.ErrEmptyNodeID
	}
	shard, err := cluster.GetShard(shardIdx)
	if err != nil {
		return "", err
	}
	newMasterNodeID, err := shard.promoteNewMaster(ctx, oldMasterNodeID)
	if err != nil {
		return "", err
	}
	cluster.Shards[shardIdx] = shard
	return newMasterNodeID, nil
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
			if len(fields) < 9 {
				return nil, fmt.Errorf("master node element less 9, node info[%q]", nodeString)
			}
			slots, err := ParseSlotRange(fields[8])
			if err != nil {
				return nil, fmt.Errorf("master node parser slot error, node info[%q]", nodeString)
			}
			shard := NewShard()
			shard.Nodes = append(shard.Nodes, node)
			shard.SlotRanges = append(shard.SlotRanges, *slots)
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
		Version: clusterVer,
		Shards:  shards,
	}, nil
}
