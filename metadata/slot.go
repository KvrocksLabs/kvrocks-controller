package metadata

import (
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
)

const (
	MinSlotID = 0
	MaxSlotID = 16383
)

var ErrSlotOutOfRange = errors.New("slot id was out of range, should be between 0 and 65535")

type SlotRange struct {
	Start int `json:"start"`
	Stop  int `json:"stop"`
}

func NewSlotRange(start, stop int) (*SlotRange, error) {
	if start > stop {
		return nil, errors.New("start was larger than Stop")
	}
	if (start < MinSlotID || start > MaxSlotID) ||
		(stop < MinSlotID || stop > MaxSlotID) {
		return nil, ErrSlotOutOfRange
	}
	return &SlotRange{
		Start: start,
		Stop:  stop,
	}, nil
}

func (slotRange *SlotRange) HasOverlap(that *SlotRange) bool {
	return !(slotRange.Stop < that.Start || slotRange.Start > that.Stop)
}

func (slotRange *SlotRange) String() string {
	if slotRange.Start == slotRange.Stop {
		return strconv.Itoa(slotRange.Start)
	}
	return strconv.Itoa(slotRange.Start) + "-" + strconv.Itoa(slotRange.Stop)
}

func (slotRange *SlotRange) MarshalJSON() ([]byte, error) {
	return json.Marshal(slotRange.String())
}

func (slotRange *SlotRange) UnmarshalJSON(data []byte) error {
	var slotsString string
	if err := json.Unmarshal(data, &slotsString); err != nil {
		return err
	}
	slotObject, err := ParseSlotRange(slotsString)
	if err != nil {
		return err
	}
	*slotRange = *slotObject
	return nil
}

func ParseSlotRange(s string) (*SlotRange, error) {
	index := strings.IndexByte(s, '-')
	if index == -1 {
		start, err := strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
		if start < MinSlotID || start > MaxSlotID {
			return nil, ErrSlotOutOfRange
		}
		return &SlotRange{
			Start: start,
			Stop:  start,
		}, nil
	}

	start, err := strconv.Atoi(s[0:index])
	if err != nil {
		return nil, err
	}
	stop, err := strconv.Atoi(s[index+1:])
	if err != nil {
		return nil, err
	}
	if start > stop {
		return nil, errors.New("start slot id greater than Stop slot id")
	}
	if (start < MinSlotID || start > MaxSlotID) ||
		(stop < MinSlotID || stop > MaxSlotID) {
		return nil, ErrSlotOutOfRange
	}
	return &SlotRange{
		Start: start,
		Stop:  stop,
	}, nil
}

func MergeSlotRanges(source []SlotRange, target []SlotRange) []SlotRange {
	source = append(source, target...)
	if len(source) == 1 {
		return source
	}
	sort.Slice(source, func(i, j int) bool {
		return source[i].Start < source[j].Start
	})
	merged := make([]SlotRange, 0)
	start := source[0].Start
	stop := source[0].Stop
	for i := 1; i < len(source); i++ {
		if stop + 1 < source[i].Start {
			merged = append(merged, SlotRange{Start: start, Stop: stop,})
			start = source[i].Start
			stop = source[i].Stop
		} else if stop < source[i].Stop {
			stop = source[i].Stop
		} 
	}
	merged = append(merged, SlotRange{Start: start, Stop: stop,})
	return merged
}

func RemoveSlotRanges(source []SlotRange, target []SlotRange) []SlotRange {
	for delIdx := 0; delIdx < len(target); {
		deleteSlotRange := target[delIdx]
		sort.Slice(source, func(i, j int) bool {
			return source[i].Start < source[j].Start
		})
		skip := true
		for i, slotRange := range source {
			if !slotRange.HasOverlap(&deleteSlotRange) {
				continue
			}
			skip = false
			source = append(source[0:i], source[i+1:]...)
			if deleteSlotRange.Start == slotRange.Start && deleteSlotRange.Stop < slotRange.Stop {
				source = append(source, SlotRange{Start: deleteSlotRange.Stop + 1, Stop: slotRange.Stop})
			} else if  deleteSlotRange.Stop == slotRange.Stop && deleteSlotRange.Start > slotRange.Start {
				source = append(source, SlotRange{Start: slotRange.Start, Stop: deleteSlotRange.Start - 1})
			} else if deleteSlotRange.Start < slotRange.Start && deleteSlotRange.Stop < slotRange.Stop {
				source = append(source, SlotRange{Start: deleteSlotRange.Stop + 1, Stop: slotRange.Stop})
			} else if deleteSlotRange.Start > slotRange.Start && deleteSlotRange.Stop > slotRange.Stop {
				source = append(source, SlotRange{Start: slotRange.Start, Stop: deleteSlotRange.Start - 1})
			} else if deleteSlotRange.Start > slotRange.Start && deleteSlotRange.Stop < slotRange.Stop {
				source = append(source, SlotRange{Start: slotRange.Start, Stop: deleteSlotRange.Start - 1})
				source = append(source, SlotRange{Start: deleteSlotRange.Stop + 1., Stop: slotRange.Stop})
			}
			break
		}
		if skip {
			delIdx++
		}
	}
	return source
}

func SpiltSlotRange(number int) []SlotRange {
	var slots []SlotRange
	rangeSize := (MaxSlotID + 1) / number
	for i := 0; i < number; i++ {
		if i != number - 1 {
			slots = append(slots, SlotRange{Start: i * rangeSize, Stop: (i+1)*rangeSize - 1, })
		} else {
			slots = append(slots, SlotRange{Start: i * rangeSize, Stop: MaxSlotID, })
		}
	}
	return slots
}
