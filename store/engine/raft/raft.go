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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/apache/kvrocks-controller/store/engine"

	"github.com/apache/kvrocks-controller/logger"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"

	"go.uber.org/zap"
)

const SnapshotThreshold = 10000

type Node struct {
	config        *Config
	raftNode      raft.Node
	transport     *rafthttp.Transport
	httpServer    *http.Server
	dataStore     *DataStore
	leaderChanged chan bool

	appliedIndex  uint64
	snapshotIndex uint64
	confState     raftpb.ConfState

	wg       sync.WaitGroup
	shutdown chan struct{}
}

var _ engine.Engine = (*Node)(nil)

func New(config *Config) (*Node, error) {
	config.init()
	if err := config.validate(); err != nil {
		return nil, err
	}

	n := &Node{
		config:        config,
		dataStore:     NewDataStore(config.DataDir),
		leaderChanged: make(chan bool),
		shutdown:      make(chan struct{}),
	}
	if err := n.run(); err != nil {
		return nil, err
	}
	return n, nil
}

func (n *Node) run() error {
	peers := make([]raft.Peer, len(n.config.Peers))
	for i, peer := range n.config.Peers {
		peers[i] = raft.Peer{
			ID:      uint64(i + 1),
			Context: []byte(peer),
		}
	}
	raftConfig := &raft.Config{
		ID:              n.config.ID,
		HeartbeatTick:   n.config.HeartbeatSeconds,
		ElectionTick:    n.config.ElectionSeconds,
		MaxInflightMsgs: 128,
		MaxSizePerMsg:   10 * 1024 * 1024, // 10 MiB
		Storage:         n.dataStore.raftStorage,
	}

	// WAL existing check must be done before replayWAL since it will create a new WAL if not exists
	walExists := n.dataStore.walExists()
	if err := n.dataStore.replayWAL(); err != nil {
		return err
	}

	if n.config.Join || walExists {
		n.raftNode = raft.RestartNode(raftConfig)
	} else {
		n.raftNode = raft.StartNode(raftConfig, peers)
	}

	if err := n.runTransport(); err != nil {
		return err
	}
	return n.runRaftMessages()
}

func (n *Node) runTransport() error {
	logger := logger.Get()
	idString := fmt.Sprintf("%d", n.config.ID)
	transport := &rafthttp.Transport{
		ID:     types.ID(n.config.ID),
		Logger: logger,
		// FIXME: use a proper cluster id here
		ClusterID:   0x1234,
		Raft:        n,
		LeaderStats: stats.NewLeaderStats(logger, idString),
		ServerStats: stats.NewServerStats("raft", idString),
		ErrorC:      make(chan error),
	}
	if err := transport.Start(); err != nil {
		return fmt.Errorf("failed to start transport: %w", err)
	}
	for i, peer := range n.config.Peers {
		// Don't add self to transport
		if uint64(i+1) != n.config.ID {
			transport.AddPeer(types.ID(i+1), []string{peer})
		}
	}

	url, err := url.Parse(n.config.Peers[n.config.ID-1])
	if err != nil {
		return err
	}
	httpServer := &http.Server{
		Addr:    url.Host,
		Handler: transport.Handler(),
	}

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("failed to start http server", zap.Error(err))
			os.Exit(1)
		}
	}()

	n.transport = transport
	n.httpServer = httpServer
	return nil
}

func (n *Node) runRaftMessages() error {
	snapshot, err := n.dataStore.loadSnapshotFromDisk()
	if err != nil {
		return err
	}

	// Load the snapshot into the key-value store.
	if err := n.dataStore.reloadSnapshot(); err != nil {
		return err
	}
	n.appliedIndex = snapshot.Metadata.Index
	n.snapshotIndex = snapshot.Metadata.Index
	n.confState = snapshot.Metadata.ConfState

	n.wg.Add(1)
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer func() {
			ticker.Stop()
			n.wg.Done()
		}()

		for {
			select {
			case <-ticker.C:
				n.raftNode.Tick()
			case rd := <-n.raftNode.Ready():
				// Save to wal and storage first
				if !raft.IsEmptySnap(rd.Snapshot) {
					if err := n.dataStore.saveSnapshot(rd.Snapshot); err != nil {
						logger.Get().Error("Failed to save snapshot", zap.Error(err))
					}
				}
				if err := n.dataStore.wal.Save(rd.HardState, rd.Entries); err != nil {
					logger.Get().Error("Failed to save to wal", zap.Error(err))
				}

				// Replay the entries into the raft storage
				if err := n.applySnapshot(rd.Snapshot); err != nil {
					logger.Get().Error("Failed to apply snapshot", zap.Error(err))
				}
				if len(rd.Entries) > 0 {
					_ = n.dataStore.raftStorage.Append(rd.Entries)
				}

				// TODO: check if it's the leader role and send messages to peers if needed
				n.transport.Send(rd.Messages)

				// Apply the committed entries to the state machine
				n.applyEntries(rd.CommittedEntries)
				if err := n.triggerSnapshotIfNeed(); err != nil {
					logger.Get().Error("Failed to trigger snapshot", zap.Error(err))
				}
				n.raftNode.Advance()
			case err := <-n.transport.ErrorC:
				logger.Get().Fatal("transport error", zap.Error(err))
				return
			case <-n.shutdown:
				logger.Get().Info("shutting down raft node")
				return
			}
		}
	}()
	return nil
}

func (n *Node) triggerSnapshotIfNeed() error {
	if n.appliedIndex-n.snapshotIndex <= SnapshotThreshold {
		return nil
	}
	snapshotBytes, err := n.dataStore.GetDataStoreSnapshot()
	if err != nil {
		return err
	}
	snap, err := n.dataStore.raftStorage.CreateSnapshot(n.appliedIndex, &n.confState, snapshotBytes)
	if err != nil {
		return err
	}
	if err := n.dataStore.saveSnapshot(snap); err != nil {
		return err
	}

	compactIndex := uint64(1)
	if n.appliedIndex > 10000 {
		compactIndex = n.appliedIndex - 10000
	}
	if err := n.dataStore.raftStorage.Compact(compactIndex); err != nil && !errors.Is(err, raft.ErrCompacted) {
		return err
	}
	n.snapshotIndex = n.appliedIndex
	return nil
}

func (n *Node) Set(ctx context.Context, key string, value []byte) error {
	bytes, err := json.Marshal(&engine.Entry{
		Key:   key,
		Value: value,
	})
	if err != nil {
		return err
	}
	return n.raftNode.Propose(ctx, bytes)
}

func (n *Node) ID() string {
	return fmt.Sprintf("%d", n.config.ID)
}

func (n *Node) Leader() string {
	lead := n.raftNode.Status().Lead
	return fmt.Sprintf("%d", lead)
}

func (n *Node) IsReady(_ context.Context) bool {
	return n.raftNode.Status().Lead != raft.None
}

func (n *Node) LeaderChange() <-chan bool {
	return n.leaderChanged
}

func (n *Node) Get(_ context.Context, key string) ([]byte, error) {
	return n.dataStore.Get(key)
}

func (n *Node) Exists(_ context.Context, key string) (bool, error) {
	_, err := n.dataStore.Get(key)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (n *Node) Delete(ctx context.Context, key string) error {
	bytes, err := json.Marshal(&engine.Entry{
		Key: key,
	})
	if err != nil {
		return err
	}
	return n.raftNode.Propose(ctx, bytes)
}

func (n *Node) List(_ context.Context, prefix string) ([]engine.Entry, error) {
	n.dataStore.List(prefix)
	return nil, nil
}

func (n *Node) applySnapshot(snapshot raftpb.Snapshot) error {
	if raft.IsEmptySnap(snapshot) {
		return nil
	}

	_ = n.dataStore.raftStorage.ApplySnapshot(snapshot)
	if n.appliedIndex >= snapshot.Metadata.Index {
		return fmt.Errorf("snapshot index [%d] should be greater than applied index [%d]", snapshot.Metadata.Index, n.appliedIndex)
	}

	// Load the snapshot into the key-value store.
	if err := n.dataStore.reloadSnapshot(); err != nil {
		return err
	}
	n.confState = snapshot.Metadata.ConfState
	n.appliedIndex = snapshot.Metadata.Index
	n.snapshotIndex = snapshot.Metadata.Index
	return nil
}

func (n *Node) applyEntries(entries []raftpb.Entry) {
	if len(entries) == 0 || entries[0].Index > n.appliedIndex+1 {
		return
	}

	firstEntryIndex := entries[0].Index
	// remove entries that have been applied
	if n.appliedIndex-firstEntryIndex+1 < uint64(len(entries)) {
		entries = entries[n.appliedIndex-firstEntryIndex+1:]
	}
	for _, entry := range entries {
		if err := n.applyEntry(entry); err != nil {
			logger.Get().Error("failed to apply entry", zap.Error(err))
		}
	}
	n.appliedIndex = entries[len(entries)-1].Index
}

func (n *Node) applyEntry(entry raftpb.Entry) error {
	switch entry.Type {
	case raftpb.EntryNormal:
		// apply entry to the state machine
		if len(entry.Data) == 0 {
			// empty message, skip it.
			return nil
		}
		var e engine.Entry
		if err := json.Unmarshal(entry.Data, &e); err != nil {
			return err
		}
		if len(e.Value) == 0 {
			n.dataStore.Delete(e.Key)
		} else {
			n.dataStore.Set(e.Key, e.Value)
		}
	case raftpb.EntryConfChangeV2, raftpb.EntryConfChange:
		// apply config change to the state machine
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(entry.Data); err != nil {
			return err
		}
		n.confState = *n.raftNode.ApplyConfChange(cc)
		switch cc.Type {
		case raftpb.ConfChangeAddNode:
			// FIXME: change the config change state's id
			if cc.NodeID != n.config.ID && len(cc.Context) > 0 {
				logger.Get().Info("Add the new peer",
					zap.Uint64("nodeID", cc.NodeID),
					zap.String("context", string(cc.Context)),
				)
				n.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
			}
		case raftpb.ConfChangeRemoveNode:
			// TODO: check if it's itself. If so, shutdown the node.
			n.transport.RemovePeer(types.ID(cc.NodeID))
		case raftpb.ConfChangeUpdateNode:
			// TODO: update the peer
		case raftpb.ConfChangeAddLearnerNode:
			// TODO: add the learner node
		}
	}
	return nil
}

func (n *Node) Process(ctx context.Context, m raftpb.Message) error {
	return n.raftNode.Step(ctx, m)
}

func (n *Node) IsIDRemoved(_ uint64) bool {
	return false
}

func (n *Node) ReportUnreachable(id uint64) {
	n.raftNode.ReportUnreachable(id)
}

func (n *Node) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	n.raftNode.ReportSnapshot(id, status)
}

func (n *Node) Close() error {
	close(n.shutdown)
	n.raftNode.Stop()
	n.transport.Stop()
	if err := n.httpServer.Close(); err != nil {
		return err
	}

	n.dataStore.Close()
	n.wg.Wait()
	return nil
}
