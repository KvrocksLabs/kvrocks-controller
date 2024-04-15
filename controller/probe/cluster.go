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
package probe

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/apache/kvrocks-controller/controller/failover"
	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/metadata"
	"github.com/apache/kvrocks-controller/storage"
	"github.com/apache/kvrocks-controller/util"
	"go.uber.org/zap"
)

var (
	ErrClusterNotInitialized = errors.New("ERR CLUSTERDOWN The cluster is not initialized")
	ErrRestoringBackUp       = errors.New("ERR LOADING kvrocks is restoring the db from backup")
)

type Cluster struct {
	namespace     string
	cluster       string
	storage       *storage.Storage
	failOver      *failover.FailOver
	failureMu     sync.Mutex
	failureCounts map[string]int64
	stopCh        chan struct{}
}

func NewCluster(ns, cluster string, storage *storage.Storage, failOver *failover.FailOver) *Cluster {
	return &Cluster{
		namespace:     ns,
		cluster:       cluster,
		storage:       storage,
		failOver:      failOver,
		failureCounts: make(map[string]int64),
		stopCh:        make(chan struct{}),
	}
}

func (c *Cluster) start() {
	go c.loop()
}

func (c *Cluster) probeNode(ctx context.Context, node *metadata.NodeInfo) (int64, error) {
	info, err := util.ClusterInfoCmd(ctx, node)
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
	return info.ClusterCurrentEpoch, nil
}

func (c *Cluster) increaseFailureCount(index int, node *metadata.NodeInfo) int64 {
	log := logger.Get().With(
		zap.String("id", node.ID),
		zap.String("role", node.Role),
		zap.String("addr", node.Addr),
	)

	c.failureMu.Lock()
	if _, ok := c.failureCounts[node.Addr]; !ok {
		c.failureCounts[node.Addr] = 0
	}
	c.failureCounts[node.Addr] += 1
	count := c.failureCounts[node.Addr]
	c.failureMu.Unlock()

	/// don't add the node into the failover candidates if it's not a master node
	if node.Role != metadata.RoleMaster {
		return count
	}

	if count%c.failOver.Config().MaxPingCount == 0 {
		err := c.failOver.AddNode(c.namespace, c.cluster, index, *node, failover.AutoType)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failed to add the node into the fail over candidates")
		} else {
			log.With(zap.Int64("failure_count", count)).Info("Add the node into the fail over candidates")
		}
	}
	return count
}

func (c *Cluster) resetFailureCount(node *metadata.NodeInfo) {
	c.failureMu.Lock()
	delete(c.failureCounts, node.Addr)
	c.failureMu.Unlock()
}

func (c *Cluster) probe(ctx context.Context, cluster *metadata.Cluster) {
	for i, shard := range cluster.Shards {
		for _, node := range shard.Nodes {
			go func(shardIdx int, node metadata.NodeInfo) {
				log := logger.Get().With(
					zap.String("id", node.ID),
					zap.String("role", node.Role),
					zap.String("addr", node.Addr),
				)
				version, err := c.probeNode(ctx, &node)
				if err != nil && !errors.Is(err, ErrClusterNotInitialized) {
					failureCount := c.increaseFailureCount(shardIdx, &node)
					log.With(zap.Error(err),
						zap.Int64("failure_count", failureCount),
					).Error("Failed to probe the node")
					return
				}
				log.Debug("Probe the cluster node")

				if version < cluster.Version {
					// sync the cluster to the latest version
					err := util.SyncClusterInfo2Node(ctx, &node, cluster)
					if err != nil {
						log.With(zap.Error(err)).Error("Failed to sync the cluster info")
					}
				} else if version > cluster.Version {
					log.With(
						zap.Int64("node.version", version),
						zap.Int64("cluster.version", cluster.Version),
					).Warn("The node is in a higher version")
				}
				c.resetFailureCount(&node)
			}(i, node)
		}
	}
}

func (c *Cluster) loop() {
	log := logger.Get().With(
		zap.String("namespace", c.namespace),
		zap.String("cluster", c.cluster),
	)
	ctx := context.Background()
	probeTicker := time.NewTicker(time.Duration(c.failOver.Config().PingIntervalSeconds) * time.Second)
	defer probeTicker.Stop()
	for {
		select {
		case <-probeTicker.C:
			clusterInfo, err := c.storage.GetClusterInfo(ctx, c.namespace, c.cluster)
			if err != nil {
				log.With(
					zap.Error(err),
				).Error("Failed to get the cluster info from the storage")
				break
			}
			c.probe(ctx, clusterInfo)
		case <-c.stopCh:
			return
		}
	}
}

func (c *Cluster) stop() {
	close(c.stopCh)
}
