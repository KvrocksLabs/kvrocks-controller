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
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"

	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/store/engine"

	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

var ErrKeyNotFound = errors.New("key not found")

type DataStore struct {
	walDir      string
	snapshotDir string

	snapshotter *snap.Snapshotter
	wal         *wal.WAL

	raftStorage *raft.MemoryStorage

	mu  sync.RWMutex
	kvs map[string][]byte
}

func NewDataStore(dir string) *DataStore {
	snapshotDir := fmt.Sprintf("%s/snapshot", dir)
	snapshotter := snap.New(logger.Get(), snapshotDir)
	return &DataStore{
		walDir:      fmt.Sprintf("%s/wal", dir),
		snapshotDir: snapshotDir,
		snapshotter: snapshotter,
		raftStorage: raft.NewMemoryStorage(),
		kvs:         make(map[string][]byte),
	}
}

func (ds *DataStore) walExists() bool {
	return wal.Exist(ds.walDir)
}

func (ds *DataStore) loadSnapshotFromDisk() (*raftpb.Snapshot, error) {
	if !fileutil.Exist(ds.snapshotDir) {
		if err := os.MkdirAll(ds.snapshotDir, 0750); err != nil {
			return nil, err
		}
	}

	emptySnapshot := &raftpb.Snapshot{}
	if !ds.walExists() {
		return emptySnapshot, nil
	}

	snapshots, err := wal.ValidSnapshotEntries(logger.Get(), ds.walDir)
	if err != nil {
		return nil, err
	}
	latestSnapshot, err := ds.snapshotter.LoadNewestAvailable(snapshots)
	if err != nil {
		if errors.Is(err, snap.ErrNoSnapshot) {
			return emptySnapshot, nil
		}
		return nil, err
	}
	return latestSnapshot, nil
}

func (ds *DataStore) reloadSnapshot() error {
	snapshot, err := ds.snapshotter.Load()
	if errors.Is(err, snap.ErrNoSnapshot) {
		return nil
	}
	if err != nil {
		return err
	}

	var m map[string][]byte
	if err := json.Unmarshal(snapshot.Data, &m); err != nil {
		return err
	}

	ds.mu.Lock()
	ds.kvs = m
	ds.mu.Unlock()
	return nil
}

func (ds *DataStore) openWAL(snapshot *raftpb.Snapshot) (*wal.WAL, error) {
	if !ds.walExists() {
		if err := os.MkdirAll(ds.walDir, 0750); err != nil {
			return nil, err
		}
		w, err := wal.Create(logger.Get(), ds.walDir, nil)
		if err != nil {
			return nil, err
		}
		w.Close()
	}
	walSnapshot := walpb.Snapshot{}
	if snapshot != nil {
		walSnapshot.Index = snapshot.Metadata.Index
		walSnapshot.Term = snapshot.Metadata.Term
	}
	return wal.Open(logger.Get(), ds.walDir, walSnapshot)
}

func (ds *DataStore) replayWAL() (*raftpb.Snapshot, error) {
	snapshot, err := ds.loadSnapshotFromDisk()
	if err != nil {
		return nil, fmt.Errorf("failed to load newest snapshot: %w", err)
	}

	w, err := ds.openWAL(snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}
	ds.wal = w

	_, hardState, entries, err := w.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL: %w", err)
	}
	if snapshot != nil {
		_ = ds.raftStorage.ApplySnapshot(*snapshot)
	}
	if err := ds.raftStorage.SetHardState(hardState); err != nil {
		return nil, fmt.Errorf("failed to set hard state: %w", err)
	}
	if err := ds.raftStorage.Append(entries); err != nil {
		return nil, fmt.Errorf("failed to append entries: %w", err)
	}

	if err := ds.reloadSnapshot(); err != nil {
		return nil, fmt.Errorf("failed to reload snapshot: %w", err)
	}
	for _, entry := range entries {
		if err := ds.applyDataEntry(entry); err != nil {
			return nil, fmt.Errorf("failed to apply data entry: %w", err)
		}
	}
	return snapshot, nil
}

func (ds *DataStore) saveSnapshot(snapshot raftpb.Snapshot) error {
	walSnap := walpb.Snapshot{
		Index:     snapshot.Metadata.Index,
		Term:      snapshot.Metadata.Term,
		ConfState: &snapshot.Metadata.ConfState,
	}
	if err := ds.snapshotter.SaveSnap(snapshot); err != nil {
		return err
	}
	if err := ds.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	return ds.wal.ReleaseLockTo(snapshot.Metadata.Index)
}

func (ds *DataStore) applyDataEntry(entry raftpb.Entry) error {
	if entry.Type != raftpb.EntryNormal || len(entry.Data) == 0 {
		return nil
	}

	var e Event
	if err := json.Unmarshal(entry.Data, &e); err != nil {
		return err
	}
	switch e.Op {
	case opSet:
		ds.Set(e.Key, e.Value)
	case opDelete:
		ds.Delete(e.Key)
	case opGet:
		// do nothing
	default:
		return fmt.Errorf("unknown operation type: %d", e.Op)
	}
	return nil
}

func (ds *DataStore) Set(key string, value []byte) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.kvs[key] = value
}

func (ds *DataStore) Get(key string) ([]byte, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	if v, ok := ds.kvs[key]; ok {
		return v, nil
	}
	return nil, ErrKeyNotFound
}

func (ds *DataStore) Delete(key string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	delete(ds.kvs, key)
}

func (ds *DataStore) List(prefix string) []engine.Entry {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	entries := make([]engine.Entry, 0)
	for key := range ds.kvs {
		if !strings.HasPrefix(key, prefix) || key == prefix {
			continue
		}
		trimmedKey := strings.TrimLeft(key[len(prefix)+1:], "/")
		if strings.ContainsRune(trimmedKey, '/') {
			continue
		}

		entries = append(entries, engine.Entry{
			Key:   trimmedKey,
			Value: ds.kvs[trimmedKey],
		})
	}
	slices.SortFunc(entries, func(i, j engine.Entry) int {
		return strings.Compare(i.Key, j.Key)
	})
	return entries
}

func (ds *DataStore) GetDataStoreSnapshot() ([]byte, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return json.Marshal(ds.kvs)
}

func (ds *DataStore) Close() {
	if ds.wal != nil {
		ds.wal.Close()
	}
}
