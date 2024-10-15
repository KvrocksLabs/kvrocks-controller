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
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShard_HasOverlap(t *testing.T) {
	shard := NewShard()
	slotRange := &SlotRange{Start: 0, Stop: 100}
	shard.SlotRanges = append(shard.SlotRanges, *slotRange)
	require.True(t, shard.HasOverlap(slotRange))
	require.True(t, shard.HasOverlap(&SlotRange{Start: 50, Stop: 150}))
	require.False(t, shard.HasOverlap(&SlotRange{Start: 101, Stop: 150}))
}

func TestShard_Sort(t *testing.T) {
	shard0 := NewShard()
	shard0.SlotRanges = []SlotRange{{Start: 201, Stop: 300}}
	shard1 := NewShard()
	shard1.SlotRanges = []SlotRange{{Start: 0, Stop: 400}}
	shard2 := NewShard()
	shard2.SlotRanges = []SlotRange{{Start: 101, Stop: 500}}
	shard3 := NewShard()
	shard3.SlotRanges = []SlotRange{}
	shards := Shards{shard0, shard1, shard2, shard3}
	sort.Sort(shards)
	require.EqualValues(t, 0, shards[0].SlotRanges[0].Start)
	require.EqualValues(t, 101, shards[1].SlotRanges[0].Start)
	require.EqualValues(t, 201, shards[2].SlotRanges[0].Start)
	require.EqualValues(t, 0, len(shards[3].SlotRanges))
}

func TestShard_IsServicing(t *testing.T) {
	shard := NewShard()
	shard.TargetShardIndex = 0
	shard.MigratingSlot = -1
	require.False(t, shard.IsServicing())

	shard.TargetShardIndex = 0
	shard.MigratingSlot = 0
	require.True(t, shard.IsServicing())

	shard.TargetShardIndex = -1
	shard.MigratingSlot = -1
	shard.SlotRanges = []SlotRange{{Start: 0, Stop: 100}}
	require.True(t, shard.IsServicing())

	shard.SlotRanges = []SlotRange{{Start: -1, Stop: -1}}
	require.False(t, shard.IsServicing())
}
