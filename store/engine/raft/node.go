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

	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/store/engine"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"

	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	defaultSnapshotThreshold = 10000
	defaultCompactThreshold  = 1024
)

const (
	opGet = iota + 1
	opSet
	opDelete
)

type Event struct {
	Op    int    `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type Node struct {
	config *Config

	addr          string
	raftNode      raft.Node
	transport     *rafthttp.Transport
	httpServer    *http.Server
	dataStore     *DataStore
	leaderChanged chan bool
	logger        *zap.Logger
	peers         sync.Map

	mu                sync.Mutex
	leader            uint64
	appliedIndex      uint64
	snapshotIndex     uint64
	confState         raftpb.ConfState
	snapshotThreshold uint64
	compactThreshold  uint64

	wg       sync.WaitGroup
	shutdown chan struct{}

	isRunning atomic.Bool
}

var _ engine.Engine = (*Node)(nil)

func New(config *Config) (*Node, error) {
	config.init()
	if err := config.validate(); err != nil {
		return nil, err
	}

	logger := logger.Get().With(zap.Uint64("node_id", config.ID))
	n := &Node{
		config:            config,
		leader:            raft.None,
		dataStore:         NewDataStore(config.DataDir),
		leaderChanged:     make(chan bool),
		snapshotThreshold: defaultSnapshotThreshold,
		compactThreshold:  defaultCompactThreshold,
		logger:            logger,
	}
	if err := n.run(); err != nil {
		return nil, err
	}
	return n, nil
}

func (n *Node) Addr() string {
	return n.addr
}

func (n *Node) Peers() []string {
	peers := make([]string, 0)
	n.peers.Range(func(key, value interface{}) bool {
		peer, _ := value.(string)
		peers = append(peers, peer)
		return true
	})
	return peers
}

func (n *Node) SetSnapshotThreshold(threshold uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.snapshotThreshold = threshold
}

func (n *Node) run() error {
	// The node is already running
	if !n.isRunning.CAS(false, true) {
		return nil
	}
	n.shutdown = make(chan struct{})

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
		ID:          types.ID(n.config.ID),
		Logger:      logger,
		ClusterID:   0x6666,
		Raft:        n,
		LeaderStats: stats.NewLeaderStats(logger, idString),
		ServerStats: stats.NewServerStats("raft", idString),
		ErrorC:      make(chan error),
	}
	if err := transport.Start(); err != nil {
		return fmt.Errorf("unable to start transport: %w", err)
	}
	for i, peer := range n.config.Peers {
		// Don't add self to transport
		if uint64(i+1) != n.config.ID {
			transport.AddPeer(types.ID(i+1), []string{peer})
		}
		n.peers.Store(uint64(i+1), peer)
	}

	n.addr = n.config.Peers[n.config.ID-1]
	url, err := url.Parse(n.addr)
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
			n.logger.Fatal("Unable to start http server", zap.Error(err))
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
						n.logger.Error("Failed to save snapshot", zap.Error(err))
					}
				}
				if err := n.dataStore.wal.Save(rd.HardState, rd.Entries); err != nil {
					n.logger.Error("Failed to save to wal", zap.Error(err))
				}

				// Replay the entries into the raft storage
				if err := n.applySnapshot(rd.Snapshot); err != nil {
					n.logger.Error("Failed to apply snapshot", zap.Error(err))
				}
				if len(rd.Entries) > 0 {
					_ = n.dataStore.raftStorage.Append(rd.Entries)
				}

				for _, msg := range rd.Messages {
					if msg.Type == raftpb.MsgApp {
						msg.Snapshot.Metadata.ConfState = n.confState
					}
				}
				n.transport.Send(rd.Messages)

				// Apply the committed entries to the state machine
				n.applyEntries(rd.CommittedEntries)
				if err := n.triggerSnapshotIfNeed(); err != nil {
					n.logger.Error("Failed to trigger snapshot", zap.Error(err))
				}
				n.raftNode.Advance()
			case err := <-n.transport.ErrorC:
				n.logger.Fatal("Found transport error", zap.Error(err))
				return
			case <-n.shutdown:
				n.logger.Info("Shutting down raft node")
				return
			}
		}
	}()
	return nil
}

func (n *Node) triggerSnapshotIfNeed() error {
	if n.appliedIndex-n.snapshotIndex <= n.snapshotThreshold {
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
	if n.appliedIndex > n.compactThreshold {
		compactIndex = n.appliedIndex - n.compactThreshold
	}
	if err := n.dataStore.raftStorage.Compact(compactIndex); err != nil && !errors.Is(err, raft.ErrCompacted) {
		return err
	}
	n.snapshotIndex = n.appliedIndex
	return nil
}

func (n *Node) Set(ctx context.Context, key string, value []byte) error {
	bytes, err := json.Marshal(&Event{
		Op:    opSet,
		Key:   key,
		Value: value,
	})
	if err != nil {
		return err
	}
	return n.raftNode.Propose(ctx, bytes)
}

func (n *Node) AddPeer(ctx context.Context, nodeID uint64, peer string) error {
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  nodeID,
		Context: []byte(peer),
	}
	return n.raftNode.ProposeConfChange(ctx, cc)
}

func (n *Node) RemovePeer(ctx context.Context, nodeID uint64) error {
	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: nodeID,
	}
	return n.raftNode.ProposeConfChange(ctx, cc)
}

func (n *Node) ID() string {
	return fmt.Sprintf("%d", n.config.ID)
}

func (n *Node) Leader() string {
	return fmt.Sprintf("%d", n.GetRaftLead())
}

func (n *Node) GetRaftLead() uint64 {
	return n.raftNode.Status().Lead
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
	bytes, err := json.Marshal(&Event{
		Op:  opDelete,
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
			n.logger.Error("failed to apply entry", zap.Error(err))
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

		var e Event
		if err := json.Unmarshal(entry.Data, &e); err != nil {
			return err
		}
		switch e.Op {
		case opSet:
			n.dataStore.Set(e.Key, e.Value)
			return nil
		case opDelete:
			n.dataStore.Delete(e.Key)
		case opGet:
			// do nothing
		default:
			return fmt.Errorf("unknown operation type: %d", e.Op)
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
			if cc.NodeID != n.config.ID && len(cc.Context) > 0 {
				n.logger.Info("Add the new peer", zap.String("context", string(cc.Context)))
				n.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				n.peers.Store(cc.NodeID, string(cc.Context))
			}
		case raftpb.ConfChangeRemoveNode:
			n.peers.Delete(cc.NodeID)
			n.transport.RemovePeer(types.ID(cc.NodeID))
			if cc.NodeID == n.config.ID {
				n.Close()
				n.logger.Info("Node removed from the cluster")
				return nil
			}
		case raftpb.ConfChangeUpdateNode:
			n.transport.UpdatePeer(types.ID(cc.NodeID), []string{string(cc.Context)})
			if _, ok := n.peers.Load(cc.NodeID); ok {
				n.peers.Store(cc.NodeID, string(cc.Context))
			}
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
	if !n.isRunning.CAS(true, false) {
		return nil
	}
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
