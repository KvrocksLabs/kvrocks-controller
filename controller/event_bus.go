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

	"go.uber.org/zap"

	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/store"
)

// EventBus would sync the cluster topology information
// to cluster nodes when it's changed.
type EventBus struct {
	storage  *store.ClusterStore
	wg       sync.WaitGroup
	shutdown chan struct{}
	notifyCh chan store.Event
}

func NewEventBus(s *store.ClusterStore) *EventBus {
	syncer := &EventBus{
		storage:  s,
		shutdown: make(chan struct{}, 0),
		notifyCh: make(chan store.Event, 8),
	}
	go syncer.loop()
	return syncer
}

func (syncer *EventBus) Notify(event *store.Event) {
	syncer.notifyCh <- *event
}

func (syncer *EventBus) handleEvent(event *store.Event) error {
	switch event.Type {
	case store.EventCluster, store.EventShard, store.EventNode:
		return syncer.handleClusterEvent(event)
	default:
		return nil
	}
}

func (syncer *EventBus) handleClusterEvent(event *store.Event) error {
	if event.Command != store.CommandRemove {
		cluster, err := syncer.storage.GetCluster(context.Background(), event.Namespace, event.Cluster)
		if err != nil {
			return fmt.Errorf("failed to get cluster: %w", err)
		}
		return syncClusterInfoToAllNodes(context.Background(), cluster)
	}
	// TODO: Remove related cluster tasks
	return nil
}

func (syncer *EventBus) loop() {
	defer syncer.wg.Done()
	syncer.wg.Add(1)
	for {
		select {
		case event := <-syncer.notifyCh:
			if err := syncer.handleEvent(&event); err != nil {
				logger.Get().With(
					zap.Error(err),
					zap.Any("event", event),
				).Error("Failed to handle event")
			}
		case <-syncer.shutdown:
			return
		}
	}
}

func (syncer *EventBus) Close() {
	close(syncer.shutdown)
	close(syncer.notifyCh)
	syncer.wg.Wait()
}

func syncClusterInfoToAllNodes(ctx context.Context, cluster *store.Cluster) error {
	// FIXME: should keep retry in separate routine to prevent occurring error
	// and cause update failure.
	var errs []error
	for _, shard := range cluster.Shards {
		for _, node := range shard.Nodes {
			if err := node.SyncClusterInfo(ctx, cluster); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if errs != nil {
		return fmt.Errorf("%v", errs)
	}
	return nil
}
