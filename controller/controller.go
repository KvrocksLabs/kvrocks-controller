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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/apache/kvrocks-controller/config"
	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/store"
)

const (
	stateInit = iota + 1
	stateRunning
	stateClosed
)

type Controller struct {
	config       *config.ControllerConfig
	clusterStore *store.ClusterStore

	mu       sync.Mutex
	clusters map[string]*ClusterChecker

	wg      sync.WaitGroup
	state   atomic.Int32
	readyCh chan struct{}
	closeCh chan struct{}
}

func New(s *store.ClusterStore, config *config.ControllerConfig) (*Controller, error) {
	c := &Controller{
		config:       config,
		clusterStore: s,
		clusters:     make(map[string]*ClusterChecker),
		readyCh:      make(chan struct{}, 1),
		closeCh:      make(chan struct{}),
	}
	c.state.Store(stateInit)
	return c, nil
}

func (c *Controller) Start(ctx context.Context) error {
	if !c.state.CompareAndSwap(stateInit, stateRunning) {
		return nil
	}

	c.wg.Add(1)
	go c.syncLoop(ctx)
	c.wg.Add(1)
	go c.leaderEventLoop()
	return nil
}

func (c *Controller) WaitForReady() {
	<-c.readyCh
}

// suspend stops the controller from processing events if it's not the leader
func (c *Controller) suspend() {
	c.mu.Lock()
	for key, cluster := range c.clusters {
		cluster.Close()
		delete(c.clusters, key)
	}
	c.mu.Unlock()
}

// resume starts the controller to process events
func (c *Controller) resume(ctx context.Context) error {
	namespaces, err := c.clusterStore.ListNamespace(ctx)
	if err != nil {
		return fmt.Errorf("failed to list namespaces: %w", err)
	}
	for _, ns := range namespaces {
		clusters, err := c.clusterStore.ListCluster(ctx, ns)
		if err != nil {
			return fmt.Errorf("failed to list clusters: %w", err)
		}
		for _, cluster := range clusters {
			c.addCluster(ns, cluster)
			logger.Get().Debug("Resume the cluster", zap.String("namespace", ns), zap.String("cluster", cluster))
		}
	}
	return nil
}

func (c *Controller) becomeLeader(ctx context.Context, prevTermLeader string) {
	if prevTermLeader == c.clusterStore.ID() {
		return
	}
	if err := c.resume(ctx); err != nil {
		logger.Get().Error("Failed to resume the controller", zap.Error(err))
		return
	}
	logger.Get().Info("Became the leader, resume the controller")
}

func (c *Controller) syncLoop(ctx context.Context) {
	defer c.wg.Done()

	prevTermLeader := ""
	if c.clusterStore.IsLeader() {
		c.becomeLeader(ctx, prevTermLeader)
		prevTermLeader = c.clusterStore.ID()
	}

	c.readyCh <- struct{}{}
	for {
		select {
		case <-c.clusterStore.LeaderChange():
			if c.clusterStore.IsLeader() {
				if prevTermLeader != c.clusterStore.ID() {
					c.becomeLeader(ctx, prevTermLeader)
					prevTermLeader = c.clusterStore.ID()
				}
			} else {
				if prevTermLeader != c.clusterStore.ID() {
					continue
				}
				c.suspend()
				logger.Get().Warn("Lost the leader, suspend the controller")
			}
		case <-c.closeCh:
			return
		}
	}
}

func (c *Controller) leaderEventLoop() {
	defer c.wg.Done()
	for {
		select {
		case event := <-c.clusterStore.Notify():
			if !c.clusterStore.IsLeader() || event.Type != store.EventCluster {
				continue
			}
			switch event.Command {
			case store.CommandCreate:
				c.addCluster(event.Namespace, event.Cluster)
			case store.CommandRemove:
				c.removeCluster(event.Namespace, event.Cluster)
			case store.CommandUpdate:
				c.updateCluster(event.Namespace, event.Cluster)
			default:
				logger.Get().Error("Unknown command", zap.Any("event", event))
			}
		case <-c.closeCh:
			return
		}
	}
}

func (c *Controller) buildClusterKey(namespace, clusterName string) string {
	return namespace + "/" + clusterName
}

func (c *Controller) addCluster(namespace, clusterName string) {
	key := c.buildClusterKey(namespace, clusterName)
	if cluster, err := c.getCluster(namespace, clusterName); err == nil && cluster != nil {
		return
	}

	cluster := NewClusterChecker(c.clusterStore, namespace, clusterName).
		WithPingInterval(time.Duration(c.config.FailOver.PingIntervalSeconds) * time.Second).
		WithMaxFailureCount(c.config.FailOver.MaxPingCount)
	cluster.Start()

	c.mu.Lock()
	c.clusters[key] = cluster
	c.mu.Unlock()
}

func (c *Controller) getCluster(namespace, clusterName string) (*ClusterChecker, error) {
	key := c.buildClusterKey(namespace, clusterName)

	c.mu.Lock()
	defer c.mu.Unlock()
	cluster, ok := c.clusters[key]
	if !ok {
		return nil, consts.ErrNotFound
	}
	return cluster, nil
}

func (c *Controller) removeCluster(namespace, clusterName string) {
	key := c.buildClusterKey(namespace, clusterName)
	c.mu.Lock()
	if cluster, ok := c.clusters[key]; ok {
		cluster.Close()
		delete(c.clusters, key)
	}
	c.mu.Unlock()
}

func (c *Controller) updateCluster(namespace, clusterName string) {
	key := c.buildClusterKey(namespace, clusterName)
	c.mu.Lock()
	cluster, ok := c.clusters[key]
	c.mu.Unlock()
	if !ok {
		logger.Get().With(
			zap.String("namespace", namespace),
			zap.String("clusterName", clusterName),
		).Error("Cluster not found")
		return
	}
	cluster.sendSyncEvent()
}

func (c *Controller) Close() {
	if !c.state.CompareAndSwap(stateRunning, stateClosed) {
		return
	}

	c.suspend()
	close(c.readyCh)
	close(c.closeCh)
	c.wg.Wait()
}
