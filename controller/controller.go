package controller

import (
	"context"
	"fmt"
	"os"
	"sync"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/RocksLabs/kvrocks_controller/controller/migrate"

	"github.com/RocksLabs/kvrocks_controller/controller/failover"

	"github.com/RocksLabs/kvrocks_controller/controller/probe"
	"github.com/RocksLabs/kvrocks_controller/logger"
	"github.com/RocksLabs/kvrocks_controller/storage"
	"github.com/RocksLabs/kvrocks_controller/util"
	"github.com/RocksLabs/kvrocks_controller/config"
)

type Controller struct {
	storage  *storage.Storage
	probe    *probe.Probe
	failover *failover.Failover
	migrator *migrate.Migrator

	mu       sync.Mutex
	syncers  map[string]*Syncer
	isLoaded atomic.Bool

	stopCh    chan struct{}
	closeOnce sync.Once
}

func New(s *storage.Storage, config *config.Config) (*Controller, error) {
	failover := failover.New(s, config.Controller.FailOver)
	return &Controller{
		storage:  s,
		failover: failover,
		migrator: migrate.New(s),
		probe:    probe.New(s, failover),
		syncers:  make(map[string]*Syncer, 0),
		stopCh:   make(chan struct{}),
	}, nil
}

func (c *Controller) Start() error {
	go c.syncLoop()
	return nil
}

func (c *Controller) loadModules() error {
	if c.isLoaded.CAS(false, true) {
		ctx := context.Background()
		if err := c.failover.Load(); err != nil {
			return fmt.Errorf("load failover module: %w", err)
		}
		if err := c.probe.Load(ctx); err != nil {
			return fmt.Errorf("load probe module: %w", err)
		}
		if err := c.migrator.Load(ctx); err != nil {
			return fmt.Errorf("load migration module: %w", err)
		}
	}
	return nil
}

func (c *Controller) unloadModules() {
	c.probe.Shutdown()
	c.failover.Shutdown()
	c.migrator.Shutdown()
	c.isLoaded.Store(false)
}

func (c *Controller) syncLoop() {
	prevTermLeader := ""
	go c.leaderEventLoop()
	for {
		select {
		case <-c.storage.LeaderChange():
			if c.storage.IsLeader() {
				if err := c.loadModules(); err != nil {
					logger.Get().With(zap.Error(err)).Error("Failed to load module, will exit")
					os.Exit(1)
				}
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
				c.unloadModules()
				logger.Get().Info("Lost the leader campaign, will unload modules")
			}
		case <-c.stopCh:
			return
		}
	}
}

func (c *Controller) handleEvent(event *storage.Event) {
	if event.Namespace == "" || event.Cluster == "" {
		return
	}
	key := util.BuildClusterKey(event.Namespace, event.Cluster)
	c.mu.Lock()
	if _, ok := c.syncers[key]; !ok {
		c.syncers[key] = NewSyncer(c.storage)
	}
	syncer := c.syncers[key]
	c.mu.Unlock()

	syncer.Notify(event)
}

func (c *Controller) leaderEventLoop() {
	for {
		select {
		case event := <-c.storage.Notify():
			if !c.storage.IsLeader() {
				continue
			}
			c.handleEvent(&event)
			switch event.Type { // nolint
			case storage.EventCluster:
				switch event.Command {
				case storage.CommandCreate:
					c.probe.AddCluster(event.Namespace, event.Cluster)
				case storage.CommandRemove:
					c.probe.RemoveCluster(event.Namespace, event.Cluster)
				default:
				}
			default:
			}
		case <-c.stopCh:
			return
		}
	}
}

func (c *Controller) GetFailOver() *failover.Failover {
	return c.failover
}

func (c *Controller) GetMigrate() *migrate.Migrator {
	return c.migrator
}

func (c *Controller) Stop() error {
	c.closeOnce.Do(func() {
		for _, syncer := range c.syncers {
			syncer.Close()
		}
		close(c.stopCh)
		util.CloseRedisClients()
		c.unloadModules()
	})
	return nil
}
