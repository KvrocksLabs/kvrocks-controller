package metadata

import (
	"errors"
	"strings"
)

type Shard struct {
	Nodes         []NodeInfo  `json:"nodes"`
	SlotRanges    []SlotRange `json:"slot_ranges"`
	ImportSlot    int         `json:"import_slot"`
	MigratingSlot int         `json:"migrating_slot"`
}

type Shards []Shard

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
		Nodes:         make([]NodeInfo, 0),
		SlotRanges:    make([]SlotRange, 0),
		ImportSlot:    -1,
		MigratingSlot: -1,
	}
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
		if node.Role == RoleMaster {
			masterNodeIndex = i
			break
		}
	}
	if masterNodeIndex == -1 {
		return "", errors.New("missing master node")
	}

	for i, node := range shard.Nodes {
		builder.WriteString(node.ID)
		builder.WriteByte(' ')
		builder.WriteString(strings.Replace(node.Address, ":", " ", 1))
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
			builder.WriteString(shard.Nodes[masterNodeIndex].ID)
		}
		builder.WriteByte('\n')
	}
	return builder.String(), nil
}
