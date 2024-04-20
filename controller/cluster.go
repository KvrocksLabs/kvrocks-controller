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
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/store"
)

var (
	ErrClusterNotInitialized = errors.New("ERR CLUSTERDOWN The cluster is not initialized")
	ErrRestoringBackUp       = errors.New("ERR LOADING kvrocks is restoring the db from backup")
)

type ClusterOptions struct {
	pingInterval    time.Duration
	maxFailureCount int64
}

type Cluster struct {
	options *ClusterOptions
	storage *store.ClusterStore

	namespace string
	cluster   string

	failureMu     sync.Mutex
	failureCounts map[string]int64

	ctx      context.Context
	cancelFn context.CancelFunc
}

func NewCluster(storage *store.ClusterStore, ns, cluster string) *Cluster {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Cluster{
		namespace: ns,
		cluster:   cluster,

		storage:       storage,
		failureCounts: make(map[string]int64),

		ctx:      ctx,
		cancelFn: cancel,
	}
	go c.probeLoop()
	return c
}

func (c *Cluster) probeNode(ctx context.Context, node store.Node) (int64, error) {
	clusterInfo, err := node.GetClusterInfo(ctx)
	if err != nil {
		switch err.Error() {
		case ErrRestoringBackUp.Error():
			// The node is restoring from backup, just skip it
			return -1, nil
		case ErrClusterNotInitialized.Error():
			return -1, ErrClusterNotInitialized
		default:
			return -1, err
		}
	}
	return clusterInfo.CurrentEpoch, nil
}

func (c *Cluster) increaseFailureCount(shardIndex int, node store.Node) int64 {
	addr := node.Addr()
	c.failureMu.Lock()
	if _, ok := c.failureCounts[addr]; !ok {
		c.failureCounts[addr] = 0
	}
	c.failureCounts[addr] += 1
	count := c.failureCounts[addr]
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
		cluster, err := c.storage.GetCluster(c.ctx, c.namespace, c.cluster)
		if err != nil {
			log.Error("Failed to get the cluster info", zap.Error(err))
			return count
		}
		newMasterID, err := cluster.PromoteNewMaster(c.ctx, shardIndex, node.ID())
		if err != nil {
			log.Error("Failed to promote the new master", zap.Error(err))
		} else {
			log.With(zap.String("new_master_id", newMasterID)).Info("Promote the new master")
		}
	}
	return count
}

func (c *Cluster) resetFailureCount(node store.Node) {
	c.failureMu.Lock()
	delete(c.failureCounts, node.Addr())
	c.failureMu.Unlock()
}

func (c *Cluster) parallelProbeNodes(ctx context.Context, cluster *store.Cluster) {
	for i, shard := range cluster.Shards {
		for _, node := range shard.Nodes {
			go func(shardIdx int, node store.Node) {
				log := logger.Get().With(
					zap.String("id", node.ID()),
					zap.Bool("is_master", node.IsMaster()),
					zap.String("addr", node.Addr()),
				)
				version, err := c.probeNode(ctx, node)
				if err != nil && !errors.Is(err, ErrClusterNotInitialized) {
					failureCount := c.increaseFailureCount(shardIdx, node)
					log.With(zap.Error(err),
						zap.Int64("failure_count", failureCount),
					).Error("Failed to parallelProbeNodes the node")
					return
				}
				log.Debug("Probe the cluster node")

				if version < cluster.Version {
					// sync the cluster to the latest version
					if err := node.SyncClusterInfo(ctx, cluster); err != nil {
						log.With(zap.Error(err)).Error("Failed to sync the cluster info")
					}
				} else if version > cluster.Version {
					log.With(
						zap.Int64("node.version", version),
						zap.Int64("cluster.version", cluster.Version),
					).Warn("The node is in a higher version")
				}
				c.resetFailureCount(node)
			}(i, node)
		}
	}
}

func (c *Cluster) probeLoop() {
	log := logger.Get().With(
		zap.String("namespace", c.namespace),
		zap.String("cluster", c.cluster),
	)

	probeTicker := time.NewTicker(c.options.pingInterval)
	defer probeTicker.Stop()
	for {
		select {
		case <-probeTicker.C:
			clusterInfo, err := c.storage.GetCluster(c.ctx, c.namespace, c.cluster)
			if err != nil {
				log.Error("Failed to get the cluster info from the store", zap.Error(err))
				break
			}
			c.parallelProbeNodes(c.ctx, clusterInfo)
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Cluster) Close() {
	c.cancelFn()
}
