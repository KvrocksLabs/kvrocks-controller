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

package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestDataStore(t *testing.T) {
	dir := "/tmp/kvrocks/raft/test-datastore"
	store := NewDataStore(dir)
	require.NotNil(t, store)

	defer func() {
		store.Close()
		os.RemoveAll(dir)
	}()

	_, err := store.replayWAL()
	require.NoError(t, err)

	t.Run("reply WAL from the disk", func(t *testing.T) {
		entries := make([]raftpb.Entry, 0)
		for i := 0; i < 3; i++ {
			payload, err := json.Marshal(Event{Op: opSet, Key: fmt.Sprintf("key-%d", i), Value: []byte(fmt.Sprintf("value-%d", i))})
			require.NoError(t, err)
			entries = append(entries, raftpb.Entry{Term: 1, Index: uint64(i + 1), Type: raftpb.EntryNormal, Data: payload})
		}
		require.NoError(t, store.wal.Save(raftpb.HardState{Term: 1, Vote: 1}, entries))
		store.Close()

		store = NewDataStore(dir)
		snapshot, err := store.replayWAL()
		require.NoError(t, err)
		require.NotNil(t, snapshot)

		firstIndex, err := store.raftStorage.FirstIndex()
		require.NoError(t, err)
		require.EqualValues(t, 1, firstIndex)

		lastIndex, err := store.raftStorage.LastIndex()
		require.NoError(t, err)
		require.EqualValues(t, 3, lastIndex)

		term, err := store.raftStorage.Term(1)
		require.NoError(t, err)
		require.EqualValues(t, 1, term)

		for i := 0; i < 3; i++ {
			v, err := store.Get(fmt.Sprintf("key-%d", i))
			require.NoError(t, err)
			require.Equal(t, []byte(fmt.Sprintf("value-%d", i)), v)
		}
	})

	t.Run("Basic GET/SET/DELETE/LIST", func(t *testing.T) {
		store.Set("bar-1", []byte("v1"))
		store.Set("bar-2", []byte("v2"))
		store.Set("baz-3", []byte("v3"))
		store.Set("ba-4", []byte("v4"))
		store.Set("foo", []byte("v5"))

		v, err := store.Get("bar-2")
		require.NoError(t, err)
		require.Equal(t, []byte("v2"), v)

		entries := store.List("bar")
		require.Len(t, entries, 2)

		entries = store.List("baz")
		require.Len(t, entries, 1)

		entries = store.List("ba")
		require.Len(t, entries, 4)

		entries = store.List("bar")
		require.Len(t, entries, 2)

		entries = store.List("fo")
		require.Len(t, entries, 1)

		store.Delete("bar-2")
		_, err = store.Get("bar-2")
		require.ErrorIs(t, err, ErrKeyNotFound)

		entries = store.List("bar")
		require.Len(t, entries, 1)
	})
}
