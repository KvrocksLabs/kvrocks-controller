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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSlotRange_String(t *testing.T) {
	sr, err := NewSlotRange(1, 100)
	require.Nil(t, err)
	assert.Equal(t, sr.String(), "1-100")

	_, err = NewSlotRange(100, 1)
	assert.NotNil(t, err)

	_, err = NewSlotRange(-1, 100)
	assert.Equal(t, ErrSlotOutOfRange, err)

	_, err = NewSlotRange(-1, 65536)
	assert.Equal(t, ErrSlotOutOfRange, err)
}

func TestSlotRange_Parse(t *testing.T) {
	sr, err := ParseSlotRange("1-12")
	assert.Nil(t, err)
	assert.Equal(t, 1, sr.Start)
	assert.Equal(t, 12, sr.Stop)

	_, err = ParseSlotRange("1-65536")
	assert.Equal(t, ErrSlotOutOfRange, err)

	_, err = ParseSlotRange("-11-65536")
	assert.NotNil(t, err)

	_, err = ParseSlotRange("12-1")
	assert.NotNil(t, err)
}

func TestSlotRange_MergeSlotRanges(t *testing.T) {
	range1 := SlotRange{
		Start: 0,
		Stop:  8191,
	}
	range2 := SlotRange{
		Start: 8192,
		Stop:  16383,
	}
	newSlot := MergeSlotRanges([]SlotRange{range1}, []SlotRange{range2})
	assert.Equal(t, 1, len(newSlot))
	assert.Equal(t, 0, newSlot[0].Start)
	assert.Equal(t, 16383, newSlot[0].Stop)

	range3 := SlotRange{
		Start: 0,
		Stop:  8199,
	}
	range4 := SlotRange{
		Start: 8192,
		Stop:  16383,
	}
	newSlot = MergeSlotRanges([]SlotRange{range3}, []SlotRange{range4})
	assert.Equal(t, 1, len(newSlot))
	assert.Equal(t, 0, newSlot[0].Start)
	assert.Equal(t, 16383, newSlot[0].Stop)
}

func TestSlotRange_RemoveSlotRanges(t *testing.T) {
	range1 := SlotRange{
		Start: 0,
		Stop:  8191,
	}
	range2 := SlotRange{
		Start: 0,
		Stop:  0,
	}
	range3 := SlotRange{
		Start: 8191,
		Stop:  8191,
	}
	newSlot := RemoveSlotRanges([]SlotRange{range1}, []SlotRange{range2})
	assert.Equal(t, 1, newSlot[0].Start)
	assert.Equal(t, 8191, newSlot[0].Stop)
	newSlot = RemoveSlotRanges([]SlotRange{range1}, []SlotRange{range3})
	assert.Equal(t, 0, newSlot[0].Start)
	assert.Equal(t, 8190, newSlot[0].Stop)

	range1 = SlotRange{
		Start: 0,
		Stop:  8191,
	}
	range2 = SlotRange{
		Start: 8192,
		Stop:  16383,
	}
	range3 = SlotRange{
		Start: 0,
		Stop:  8192,
	}
	newSlot = RemoveSlotRanges([]SlotRange{range1, range2}, []SlotRange{range3})
	assert.Equal(t, 8193, newSlot[0].Start)
	assert.Equal(t, 16383, newSlot[0].Stop)

	range1 = SlotRange{
		Start: 0,
		Stop:  8191,
	}
	range2 = SlotRange{
		Start: 8192,
		Stop:  16383,
	}
	range3 = SlotRange{
		Start: 1,
		Stop:  8192,
	}
	newSlot = RemoveSlotRanges([]SlotRange{range1, range2}, []SlotRange{range3})
	assert.Equal(t, 0, newSlot[0].Start)
	assert.Equal(t, 0, newSlot[0].Stop)
	assert.Equal(t, 8193, newSlot[1].Start)
	assert.Equal(t, 16383, newSlot[1].Stop)

	range1 = SlotRange{
		Start: 0,
		Stop:  8191,
	}
	range2 = SlotRange{
		Start: 8192,
		Stop:  16383,
	}
	range3 = SlotRange{
		Start: 1,
		Stop:  8192,
	}
	range4 := SlotRange{
		Start: 8194,
		Stop:  16383,
	}
	newSlot = RemoveSlotRanges([]SlotRange{range1, range2}, []SlotRange{range3, range4})
	assert.Equal(t, 0, newSlot[0].Start)
	assert.Equal(t, 0, newSlot[0].Stop)
	assert.Equal(t, 8193, newSlot[1].Start)
	assert.Equal(t, 8193, newSlot[1].Stop)
}

func TestSlotRange_SpiltSlotRange(t *testing.T) {
	slots := SpiltSlotRange(5)
	assert.Equal(t, 0, slots[0].Start)
	assert.Equal(t, 3275, slots[0].Stop)
	assert.Equal(t, 13104, slots[4].Start)
	assert.Equal(t, 16383, slots[4].Stop)
}
