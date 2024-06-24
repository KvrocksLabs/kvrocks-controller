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

func TestAddSlotToSlotRanges(t *testing.T) {
	slotRanges := SlotRanges{
		{Start: 1, Stop: 20},
		{Start: 101, Stop: 199},
		{Start: 201, Stop: 300},
	}
	slotRanges = AddSlotToSlotRanges(slotRanges, 0)
	require.Equal(t, 3, len(slotRanges))
	require.EqualValues(t, SlotRange{Start: 0, Stop: 20}, slotRanges[0])

	slotRanges = AddSlotToSlotRanges(slotRanges, 21)
	require.Equal(t, 3, len(slotRanges))
	require.EqualValues(t, SlotRange{Start: 0, Stop: 21}, slotRanges[0])

	slotRanges = AddSlotToSlotRanges(slotRanges, 50)
	require.Equal(t, 4, len(slotRanges))
	require.EqualValues(t, SlotRange{Start: 50, Stop: 50}, slotRanges[1])

	slotRanges = AddSlotToSlotRanges(slotRanges, 200)
	require.Equal(t, 3, len(slotRanges))
	require.EqualValues(t, SlotRange{Start: 101, Stop: 300}, slotRanges[2])

	slotRanges = AddSlotToSlotRanges(slotRanges, 400)
	require.Equal(t, 4, len(slotRanges))
	require.EqualValues(t, SlotRange{Start: 400, Stop: 400}, slotRanges[3])
}

func TestRemoveSlotRanges(t *testing.T) {
	slotRanges := SlotRanges{
		{Start: 1, Stop: 20},
		{Start: 101, Stop: 199},
		{Start: 201, Stop: 300},
	}
	slotRanges = RemoveSlotFromSlotRanges(slotRanges, 0)
	require.Equal(t, 3, len(slotRanges))
	require.EqualValues(t, SlotRange{Start: 1, Stop: 20}, slotRanges[0])

	slotRanges = RemoveSlotFromSlotRanges(slotRanges, 21)
	require.Equal(t, 3, len(slotRanges))
	require.EqualValues(t, SlotRange{Start: 1, Stop: 20}, slotRanges[0])

	slotRanges = RemoveSlotFromSlotRanges(slotRanges, 20)
	require.Equal(t, 3, len(slotRanges))
	require.EqualValues(t, SlotRange{Start: 1, Stop: 19}, slotRanges[0])

	slotRanges = RemoveSlotFromSlotRanges(slotRanges, 150)
	require.Equal(t, 4, len(slotRanges))
	require.EqualValues(t, SlotRange{Start: 101, Stop: 149}, slotRanges[1])

	slotRanges = RemoveSlotFromSlotRanges(slotRanges, 101)
	require.Equal(t, 4, len(slotRanges))
	require.EqualValues(t, SlotRange{Start: 102, Stop: 149}, slotRanges[1])

	slotRanges = RemoveSlotFromSlotRanges(slotRanges, 199)
	require.Equal(t, 4, len(slotRanges))
	require.EqualValues(t, SlotRange{Start: 151, Stop: 198}, slotRanges[2])

	slotRanges = RemoveSlotFromSlotRanges(slotRanges, 300)
	require.Equal(t, 4, len(slotRanges))
	require.EqualValues(t, SlotRange{Start: 201, Stop: 299}, slotRanges[3])

	slotRanges = RemoveSlotFromSlotRanges(slotRanges, 298)
	require.Equal(t, 5, len(slotRanges))
	require.EqualValues(t, SlotRange{Start: 201, Stop: 297}, slotRanges[3])
	require.EqualValues(t, SlotRange{Start: 299, Stop: 299}, slotRanges[4])

	slotRanges = RemoveSlotFromSlotRanges(slotRanges, 299)
	require.Equal(t, 4, len(slotRanges))
	require.EqualValues(t, SlotRange{Start: 201, Stop: 297}, slotRanges[3])
}

func TestCalculateSlotRanges(t *testing.T) {
	slots := CalculateSlotRanges(5)
	assert.Equal(t, 0, slots[0].Start)
	assert.Equal(t, 3275, slots[0].Stop)
	assert.Equal(t, 13104, slots[4].Start)
	assert.Equal(t, 16383, slots[4].Stop)
}
