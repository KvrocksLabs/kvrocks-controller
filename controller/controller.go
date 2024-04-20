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
	"sync"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/apache/kvrocks-controller/config"
	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/store"
)

const (
	stateInit = iota + 1
	stateRunning
	stateClosed
)

type Controller struct {
	storage  *store.ClusterStore
	mu       sync.Mutex
	clusters map[string]*Cluster

	state atomic.Int32

	wg      sync.WaitGroup
	closeCh chan struct{}
}

func New(s *store.ClusterStore, config *config.Config) (*Controller, error) {
	c := &Controller{
		storage:  s,
		clusters: make(map[string]*Cluster),
		closeCh:  make(chan struct{}),
	}
	c.state.Store(stateInit)
	return c, nil
}

func (c *Controller) Start() error {
	if !c.state.CAS(stateInit, stateRunning) {
		return nil
	}

	c.wg.Add(1)
	go c.syncLoop()
	return nil
}

func (c *Controller) syncLoop() {
	defer c.wg.Done()

	prevTermLeader := ""
	go c.leaderEventLoop()
	for {
		select {
		case <-c.storage.LeaderChange():
			if c.storage.IsLeader() {
				// TODO: load clusters
				currentTermLeader := c.storage.Leader()
				if prevTermLeader == "" {
					logger.Get().Info("Start as the leader")
				} else if prevTermLeader != currentTermLeader {
					logger.Get().With(
						zap.String("prev", prevTermLeader),
					).Info("Become the leader")
				}
				prevTermLeader = currentTermLeader
			} else {
				// TODO: Close all clusters
			}
		case <-c.closeCh:
			return
		}
	}
}

func (c *Controller) leaderEventLoop() {
	for {
		select {
		case event := <-c.storage.Notify():
			if !c.storage.IsLeader() {
				continue
			}
			switch event.Type { // nolint
			case store.EventCluster:
				switch event.Command {
				case store.CommandCreate:
					c.addCluster(event.Namespace, event.Cluster)
				case store.CommandRemove:
					c.removeCluster(event.Namespace, event.Cluster)
				default:
					// TODO: update cluster & remove namespace
				}
			default:
			}
		case <-c.closeCh:
			return
		}
	}
}

func (c *Controller) addCluster(namespace, clusterName string) {
	key := namespace + "/" + clusterName
	cluster := NewCluster(c.storage, namespace, clusterName)
	c.mu.Lock()
	c.clusters[key] = cluster
	c.mu.Unlock()
}

func (c *Controller) removeCluster(namespace, clusterName string) {
	key := namespace + "/" + clusterName
	c.mu.Lock()
	if cluster, ok := c.clusters[key]; ok {
		cluster.Close()
	}
	delete(c.clusters, key)
	c.mu.Unlock()
}

func (c *Controller) Close() error {
	if !c.state.CAS(stateRunning, stateClosed) {
		return nil
	}
	c.mu.Lock()
	for _, cluster := range c.clusters {
		cluster.Close()
	}
	c.mu.Unlock()
	close(c.closeCh)
	c.wg.Wait()
	return nil
}
