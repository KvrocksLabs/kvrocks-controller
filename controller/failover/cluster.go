package failover

import (
	"fmt"
	"sync"
	"time"

	"golang.org/x/net/context"

	"go.uber.org/zap"

	"github.com/KvrocksLabs/kvrocks_controller/logger"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/KvrocksLabs/kvrocks_controller/util"
)

type Cluster struct {
	namespace string
	cluster   string
	storage   *storage.Storage
	tasks     map[string]*storage.FailOverTask
	tasksIdx  []string

	quitCh    chan struct{}
	closeOnce sync.Once
	rw        sync.RWMutex
}

// NewCluster return a Cluster instance and start schedule goroutine
func NewCluster(ns, cluster string, stor *storage.Storage) *Cluster {
	fn := &Cluster{
		namespace: ns,
		cluster:   cluster,
		storage:   stor,
		tasks:     make(map[string]*storage.FailOverTask),
		quitCh:    make(chan struct{}),
	}
	go fn.loop()
	return fn
}

// Close will release the resource when closing
func (c *Cluster) Close() error {
	c.closeOnce.Do(func() {
		close(c.quitCh)
	})
	return nil
}

func (c *Cluster) AddTask(task *storage.FailOverTask) error {
	c.rw.Lock()
	defer c.rw.Unlock()
	if task == nil {
		return nil
	}
	if _, ok := c.tasks[task.Node.Address]; ok {
		return nil
	}
	task.Status = TaskQueued
	c.tasks[task.Node.Address] = task
	c.tasksIdx = append(c.tasksIdx, task.Node.Address)
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

func (c *Cluster) GetTasks() ([]*storage.FailOverTask, error) {
	c.rw.RLock()
	defer c.rw.RUnlock()
	var tasks []*storage.FailOverTask
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
	ctx := context.Background()
	ticker := time.NewTicker(time.Duration(PingInterval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.rw.RLock()
			nodesCount, err := c.storage.ClusterNodesCounts(ctx, c.namespace, c.cluster)
			if err != nil {
				c.rw.RUnlock()
				break
			}
			if nodesCount > MinAliveSize && float64(len(c.tasks))/float64(nodesCount) > MaxFailureRatio {
				logger.Get().Warn(fmt.Sprintf("safe mode, loop ratio %.2f, allnodes: %d, failnodes: %d",
					MaxFailureRatio, nodesCount, len(c.tasks)))
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
					c.failover(ctx, task)
					continue
				}
				if err := util.PingCmd(nodeAddr); err == nil {
					break
				}
				c.failover(ctx, task)
			}
			c.rw.RUnlock()
		case <-c.quitCh:
			return
		}
	}
}

func (c *Cluster) failover(ctx context.Context, task *storage.FailOverTask) {
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
	_ = c.storage.AddFailOverHistory(ctx, task)
}
