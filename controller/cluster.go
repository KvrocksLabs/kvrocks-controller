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
package controller

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/store"
)

var (
	ErrClusterNotInitialized = errors.New("CLUSTERDOWN The cluster is not initialized")
	ErrRestoringBackUp       = errors.New("LOADING kvrocks is restoring the db from backup")
)

type ClusterCheckOptions struct {
	pingInterval    time.Duration
	maxFailureCount int64
}

type ClusterChecker struct {
	options      ClusterCheckOptions
	clusterStore store.Store
	clusterMu    sync.Mutex
	cluster      *store.Cluster

	namespace   string
	clusterName string

	failureMu     sync.Mutex
	failureCounts map[string]int64
	syncCh        chan struct{}

	ctx      context.Context
	cancelFn context.CancelFunc

	wg sync.WaitGroup
}

func NewClusterChecker(s store.Store, ns, cluster string) *ClusterChecker {
	ctx, cancel := context.WithCancel(context.Background())
	c := &ClusterChecker{
		namespace:   ns,
		clusterName: cluster,

		clusterStore: s,
		options: ClusterCheckOptions{
			pingInterval:    time.Second * 3,
			maxFailureCount: 5,
		},
		failureCounts: make(map[string]int64),
		syncCh:        make(chan struct{}, 1),

		ctx:      ctx,
		cancelFn: cancel,
	}
	return c
}

func (c *ClusterChecker) Start() {
	c.wg.Add(1)
	go c.probeLoop()
	c.wg.Add(1)
	go c.migrationLoop()
}

func (c *ClusterChecker) WithPingInterval(interval time.Duration) *ClusterChecker {
	c.options.pingInterval = interval
	if c.options.pingInterval < 200*time.Millisecond {
		c.options.pingInterval = 200 * time.Millisecond
	}
	return c
}

func (c *ClusterChecker) WithMaxFailureCount(count int64) *ClusterChecker {
	c.options.maxFailureCount = count
	if c.options.maxFailureCount < 1 {
		c.options.maxFailureCount = 5
	}
	return c
}

func (c *ClusterChecker) probeNode(ctx context.Context, node store.Node) (int64, error) {
	clusterInfo, err := node.GetClusterInfo(ctx)
	if err != nil {
		// We need to use the string contains to check the error message
		// since Kvrocks wrongly returns the error message with `ERR` prefix.
		// And it's fixed in PR: https://github.com/apache/kvrocks/pull/2362,
		// but we need to be compatible with the old version here.
		if strings.Contains(err.Error(), ErrRestoringBackUp.Error()) {
			return -1, nil
		} else if strings.Contains(err.Error(), ErrClusterNotInitialized.Error()) {
			return -1, ErrClusterNotInitialized
		} else {
			return -1, err
		}
	}
	return clusterInfo.CurrentEpoch, nil
}

func (c *ClusterChecker) increaseFailureCount(shardIndex int, node store.Node) int64 {
	id := node.ID()
	c.failureMu.Lock()
	if _, ok := c.failureCounts[id]; !ok {
		c.failureCounts[id] = 0
	}
	c.failureCounts[id] += 1
	count := c.failureCounts[id]
	c.failureMu.Unlock()

	// don't add the node into the failover candidates if it's not a master node
	if !node.IsMaster() {
		return count
	}

	log := logger.Get().With(
		zap.String("id", node.ID()),
		zap.Bool("is_master", node.IsMaster()),
		zap.String("addr", node.Addr()))
	if count%c.options.maxFailureCount == 0 {
		cluster, err := c.clusterStore.GetCluster(c.ctx, c.namespace, c.clusterName)
		if err != nil {
			log.Error("Failed to get the clusterName info", zap.Error(err))
			return count
		}
		newMasterID, err := cluster.PromoteNewMaster(c.ctx, shardIndex, node.ID(), "")
		if err == nil {
			// the node is normal if it can be elected as the new master,
			// because it requires the node is healthy.
			c.resetFailureCount(newMasterID)
			err = c.clusterStore.UpdateCluster(c.ctx, c.namespace, cluster)
		}
		if err != nil {
			log.Error("Failed to promote the new master", zap.Error(err))
		} else {
			log.With(zap.String("new_master_id", newMasterID)).Info("Promote the new master")
		}
	}
	return count
}

func (c *ClusterChecker) resetFailureCount(nodeID string) {
	c.failureMu.Lock()
	delete(c.failureCounts, nodeID)
	c.failureMu.Unlock()
}

func (c *ClusterChecker) sendSyncEvent() {
	select {
	case c.syncCh <- struct{}{}:
	case <-c.ctx.Done():
		return
	}
}

func (c *ClusterChecker) syncClusterToNodes(ctx context.Context) error {
	clusterInfo, err := c.clusterStore.GetCluster(ctx, c.namespace, c.clusterName)
	if err != nil {
		return err
	}
	for _, shard := range clusterInfo.Shards {
		for _, node := range shard.Nodes {
			// sync the clusterName to the latest version
			if err := node.SyncClusterInfo(ctx, clusterInfo); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *ClusterChecker) parallelProbeNodes(ctx context.Context, cluster *store.Cluster) {
	for i, shard := range cluster.Shards {
		for _, node := range shard.Nodes {
			go func(shardIdx int, n store.Node) {
				log := logger.Get().With(
					zap.String("id", n.ID()),
					zap.Bool("is_master", n.IsMaster()),
					zap.String("addr", n.Addr()),
				)
				version, err := c.probeNode(ctx, n)
				if err != nil && !errors.Is(err, ErrClusterNotInitialized) {
					failureCount := c.increaseFailureCount(shardIdx, n)
					log.With(zap.Error(err),
						zap.Int64("failure_count", failureCount),
					).Warn("Failed to probe the node")
					return
				}
				log.Debug("Probe the clusterName node")

				clusterVersion := cluster.Version.Load()
				if version < clusterVersion {
					// sync the clusterName to the latest version
					if err := n.SyncClusterInfo(ctx, cluster); err != nil {
						log.With(zap.Error(err)).Error("Failed to sync the clusterName info")
					}
				} else if version > cluster.Version.Load() {
					log.With(
						zap.Int64("node.version", version),
						zap.Int64("clusterName.version", clusterVersion),
					).Warn("The node is in a higher version")
				}
				c.resetFailureCount(n.ID())
			}(i, node)
		}
	}
}

func (c *ClusterChecker) probeLoop() {
	defer c.wg.Done()
	log := logger.Get().With(
		zap.String("namespace", c.namespace),
		zap.String("clusterName", c.clusterName),
	)

	probeTicker := time.NewTicker(c.options.pingInterval)
	defer probeTicker.Stop()
	for {
		select {
		case <-probeTicker.C:
			clusterInfo, err := c.clusterStore.GetCluster(c.ctx, c.namespace, c.clusterName)
			if err != nil {
				log.Error("Failed to get the clusterName info from the clusterStore", zap.Error(err))
				break
			}
			c.clusterMu.Lock()
			c.cluster = clusterInfo
			c.clusterMu.Unlock()
			c.parallelProbeNodes(c.ctx, clusterInfo)
		case <-c.syncCh:
			if err := c.syncClusterToNodes(c.ctx); err != nil {
				log.Error("Failed to sync the clusterName to the nodes", zap.Error(err))
			}
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *ClusterChecker) tryUpdateMigrationStatus(ctx context.Context, cluster *store.Cluster) {
	log := logger.Get().With(
		zap.String("namespace", c.namespace),
		zap.String("cluster", c.clusterName))

	for i, shard := range cluster.Shards {
		if !shard.IsMigrating() {
			continue
		}

		sourceNodeClusterInfo, err := shard.GetMasterNode().GetClusterInfo(ctx)
		if err != nil {
			log.Error("Failed to get the cluster info from the source node", zap.Error(err))
			return
		}
		if sourceNodeClusterInfo.MigratingSlot != shard.MigratingSlot {
			log.Error("Mismatch migrate slot", zap.Int("slot", shard.MigratingSlot))
		}
		if shard.TargetShardIndex < 0 || shard.TargetShardIndex >= len(cluster.Shards) {
			log.Error("Invalid target shard index", zap.Int("index", shard.TargetShardIndex))
		}
		targetMasterNode := cluster.Shards[shard.TargetShardIndex].GetMasterNode()

		switch sourceNodeClusterInfo.MigratingState {
		case "none", "start":
			continue
		case "fail":
			c.clusterMu.Lock()
			cluster.Shards[i].ClearMigrateState()
			c.clusterMu.Unlock()
			if err := c.clusterStore.SetCluster(ctx, c.namespace, cluster); err != nil {
				log.Error("Failed to clear the migrate state", zap.Error(err))
			}
			log.Warn("Failed to migrate the slot", zap.Int("slot", shard.MigratingSlot))
		case "success":
			err := cluster.SetSlot(ctx, shard.MigratingSlot, targetMasterNode.ID())
			if err != nil {
				log.Error("Failed to set the slot", zap.Error(err))
				return
			}
			cluster.Shards[i].SlotRanges = store.RemoveSlotFromSlotRanges(cluster.Shards[i].SlotRanges, shard.MigratingSlot)
			cluster.Shards[shard.TargetShardIndex].SlotRanges = store.AddSlotToSlotRanges(
				cluster.Shards[shard.TargetShardIndex].SlotRanges, shard.MigratingSlot)
			cluster.Shards[i].ClearMigrateState()
			if err := c.clusterStore.UpdateCluster(ctx, c.namespace, cluster); err != nil {
				log.Error("Failed to update the cluster", zap.Error(err))
			} else {
				log.Info("Migrate the slot successfully", zap.Int("slot", shard.MigratingSlot))
			}
		default:
			log.Error("Unknown migrating state", zap.String("state", sourceNodeClusterInfo.MigratingState))
		}
	}
}

func (c *ClusterChecker) migrationLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.clusterMu.Lock()
			cluster := c.cluster
			c.clusterMu.Unlock()
			if cluster == nil {
				continue
			}
			c.tryUpdateMigrationStatus(c.ctx, cluster)
		}
	}
}

func (c *ClusterChecker) Close() {
	c.cancelFn()
	c.wg.Wait()
}
