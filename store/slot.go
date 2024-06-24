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

var ErrSlotOutOfRange = errors.New("slot id was out of range, should be between 0 and 16383")

type SlotRange struct {
	Start int `json:"start"`
	Stop  int `json:"stop"`
}

type SlotRanges []SlotRange

func NewSlotRange(start, stop int) (*SlotRange, error) {
	if start > stop {
		return nil, errors.New("start was larger than Shutdown")
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

func (slotRange *SlotRange) Contains(slot int) bool {
	return slot >= slotRange.Start && slot <= slotRange.Stop
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
		return nil, errors.New("start slot id greater than Shutdown slot id")
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

func (SlotRanges *SlotRanges) Contains(slot int) bool {
	for _, slotRange := range *SlotRanges {
		if slotRange.Contains(slot) {
			return true
		}
	}
	return false
}

func AddSlotToSlotRanges(source SlotRanges, slot int) SlotRanges {
	sort.Slice(source, func(i, j int) bool {
		return source[i].Start < source[j].Start
	})
	if len(source) == 0 {
		return append(source, SlotRange{Start: slot, Stop: slot})
	}
	if source[0].Start-1 > slot {
		return append([]SlotRange{{Start: slot, Stop: slot}}, source...)
	}
	if source[len(source)-1].Stop+1 < slot {
		return append(source, SlotRange{Start: slot, Stop: slot})
	}

	// first run is to find the fittest slot range and create a new one if necessary
	for i, slotRange := range source {
		if slotRange.Contains(slot) {
			return source
		}
		// check next slot range, it won't be the last one since we have checked it before
		if slotRange.Stop+1 < slot {
			continue
		}
		if slotRange.Start == slot+1 {
			source[i].Start = slot
		} else if slotRange.Stop == slot-1 {
			source[i].Stop = slot
		} else if slotRange.Start > slot {
			// no suitable slot range, create a new one before the current slot range
			tmp := make(SlotRanges, len(source)+1)
			copy(tmp, source[0:i])
			tmp[i] = SlotRange{Start: slot, Stop: slot}
			copy(tmp[i+1:], source[i:])
			source = tmp
		} else {
			// should not reach here
			panic("should not reach here")
		}
		break
	}
	// merge the slot ranges if necessary
	for i := 0; i < len(source)-1; i++ {
		if source[i].Stop+1 == source[i+1].Start {
			source[i].Stop = source[i+1].Stop
			if i+1 == len(source)-1 {
				// remove the last slot range
				source = source[:i+1]
			} else {
				source = append(source[:i+1], source[i+2:]...)
			}
		}
	}
	return source
}

func RemoveSlotFromSlotRanges(source SlotRanges, slot int) SlotRanges {
	sort.Slice(source, func(i, j int) bool {
		return source[i].Start < source[j].Start
	})
	if !source.Contains(slot) {
		return source
	}
	for i, slotRange := range source {
		if slotRange.Contains(slot) {
			if slotRange.Start == slot && slotRange.Stop == slot {
				source = append(source[0:i], source[i+1:]...)
			} else if slotRange.Start == slot {
				source[i].Start = slot + 1
			} else if slotRange.Stop == slot {
				source[i].Stop = slot - 1
			} else {
				tmp := make(SlotRanges, len(source)+1)
				copy(tmp, source[0:i])
				tmp[i] = SlotRange{Start: slotRange.Start, Stop: slot - 1}
				tmp[i+1] = SlotRange{Start: slot + 1, Stop: slotRange.Stop}
				copy(tmp[i+2:], source[i+1:])
				source = tmp
			}
		}
	}
	return source
}

func CalculateSlotRanges(n int) SlotRanges {
	var slots []SlotRange
	rangeSize := (MaxSlotID + 1) / n
	for i := 0; i < n; i++ {
		if i != n-1 {
			slots = append(slots, SlotRange{Start: i * rangeSize, Stop: (i+1)*rangeSize - 1})
		} else {
			slots = append(slots, SlotRange{Start: i * rangeSize, Stop: MaxSlotID})
		}
	}
	return slots
}
