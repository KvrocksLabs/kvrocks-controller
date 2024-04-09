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
package failover

import (
	"sync"
	"time"

	"go.uber.org/atomic"
	"golang.org/x/net/context"

	"go.uber.org/zap"

	"github.com/apache/kvrocks-controller/config"
	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/metadata"
	"github.com/apache/kvrocks-controller/storage"
	"github.com/apache/kvrocks-controller/util"
)

type ClusterConfig struct {
	PingInterval    int
	MaxPingCount    int
	MinAliveSize    int
	MaxFailureRatio float64
}

type Cluster struct {
	namespace string
	cluster   string
	config    *config.FailOverConfig
	storage   *storage.Storage
	tasks     map[string]*storage.FailoverTask
	tasksIdx  []string

	closed   atomic.Bool
	ctx      context.Context
	cancelFn context.CancelFunc

	rw sync.RWMutex
	wg sync.WaitGroup
}

// NewCluster return a Cluster instance and start schedule goroutine
func NewCluster(ns, cluster string, stor *storage.Storage, cfg *config.FailOverConfig) *Cluster {
	ctx, cancelFn := context.WithCancel(context.Background())
	c := &Cluster{
		namespace: ns,
		cluster:   cluster,
		storage:   stor,
		tasks:     make(map[string]*storage.FailoverTask),
		config:    cfg,
		ctx:       ctx,
		cancelFn:  cancelFn,
	}

	c.wg.Add(1)
	go c.loop()
	return c
}

// Close will release the resource when closing
func (c *Cluster) Close() {
	if !c.closed.CAS(false, true) {
		return
	}
	c.cancelFn()
}

func (c *Cluster) AddTask(task *storage.FailoverTask) error {
	c.rw.Lock()
	defer c.rw.Unlock()
	if task == nil {
		return nil
	}
	if _, ok := c.tasks[task.Node.Addr]; ok {
		return nil
	}
	task.Status = TaskQueued
	c.tasks[task.Node.Addr] = task
	c.tasksIdx = append(c.tasksIdx, task.Node.Addr)
	return nil
}

func (c *Cluster) RemoveNodeTask(addr string) {
	c.rw.Lock()
	defer c.rw.Unlock()
	if _, ok := c.tasks[addr]; !ok {
		return
	}
	targetIndex := -1
	for i, nodeAddr := range c.tasksIdx {
		if addr == nodeAddr {
			targetIndex = i
			break
		}
	}
	c.removeTask(targetIndex)
}

func (c *Cluster) GetTasks() ([]*storage.FailoverTask, error) {
	c.rw.RLock()
	defer c.rw.RUnlock()
	var tasks []*storage.FailoverTask
	for _, task := range c.tasks {
		tasks = append(tasks, task)
	}
	return tasks, nil
}

// IsEmpty return an indicator whether the tasks queue has tasks, callend gcClusters
func (c *Cluster) IsEmpty() bool {
	c.rw.Lock()
	defer c.rw.Unlock()
	return len(c.tasksIdx) == 0
}

// removeTask is not goroutine safety, assgin caller hold mutex
func (c *Cluster) removeTask(idx int) {
	if idx < 0 || idx >= len(c.tasksIdx) {
		return
	}
	node := c.tasksIdx[idx]
	c.tasksIdx = append(c.tasksIdx[:idx], c.tasksIdx[idx+1:]...)
	delete(c.tasks, node)
}

func (c *Cluster) purgeTasks() {
	c.rw.Lock()
	defer c.rw.Unlock()
	for node := range c.tasks {
		delete(c.tasks, node)
	}
	c.tasksIdx = c.tasksIdx[0:0]
	return
}

func (c *Cluster) loop() {
	defer c.wg.Done()

	ticker := time.NewTicker(time.Duration(c.config.PingIntervalSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.rw.RLock()
			nodesCount, err := c.storage.ClusterNodesCounts(c.ctx, c.namespace, c.cluster)
			if err != nil {
				c.rw.RUnlock()
				break
			}
			if nodesCount > c.config.MinAliveSize && float64(len(c.tasks))/float64(nodesCount) > c.config.MaxFailureRatio {
				logger.Get().Sugar().Warnf("safe mode, loop ratio %.2f, allnodes: %d, failnodes: %d",
					c.config.MaxFailureRatio, nodesCount, len(c.tasks),
				)
				c.purgeTasks()
				c.rw.RUnlock()
				break
			}
			for idx, nodeAddr := range c.tasksIdx {
				if _, ok := c.tasks[nodeAddr]; !ok {
					continue
				}
				task := c.tasks[nodeAddr]
				c.removeTask(idx)
				if task.Type == ManualType {
					c.promoteMaster(c.ctx, task)
					continue
				}
				if err := util.PingCmd(c.ctx, &task.Node); err == nil {
					continue
				}
				c.promoteMaster(c.ctx, task)
			}
			c.rw.RUnlock()
		}
	}
}

func (c *Cluster) promoteMaster(ctx context.Context, task *storage.FailoverTask) {
	task.Status = TaskStarted
	task.StartTime = time.Now().Unix()
	var err error
	if task.Node.Role == metadata.RoleMaster {
		err = c.storage.PromoteNewMaster(ctx, c.namespace, c.cluster, task.ShardIdx, task.Node.ID)
	}
	if err != nil {
		task.Status = TaskFailed
		task.Err = err.Error()
		logger.Get().With(
			zap.Error(err),
			zap.Any("task", task),
		).Error("Abort the fail over task")
	} else {
		task.Status = TaskSuccess
		logger.Get().With(zap.Any("task", task)).Info("Finish the fail over task")
	}

	task.FinishTime = time.Now().Unix()
}
