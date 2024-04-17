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
	shards := Shards{shard0, shard1, shard2}
	sort.Sort(shards)
	require.EqualValues(t, 0, shards[0].SlotRanges[0].Start)
	require.EqualValues(t, 101, shards[1].SlotRanges[0].Start)
	require.EqualValues(t, 201, shards[2].SlotRanges[0].Start)
}
