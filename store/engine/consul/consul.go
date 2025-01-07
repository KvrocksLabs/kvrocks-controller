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
package consul

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/store/engine"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	"go.uber.org/zap"
)

const (
	sessionTTL          = 10 * time.Second
	lockDelay           = time.Millisecond
	configSchemeWithTLS = "https"
	defaultElectPath    = "kvrocks/controller/leader"
)

type Config struct {
	Addrs []string `yaml:"addrs"`
	TLS   struct {
		Enable   bool   `yaml:"enable"`
		CertFile string `yaml:"cert_file"`
		KeyFile  string `yaml:"key_file"`
		CAFile   string `yaml:"ca_file"`
	} `yaml:"tls"`
	ElectPath string `yaml:"elect_path"`
}

type Consul struct {
	client    *api.Client
	watchPlan *watch.Plan

	leaderMu  sync.RWMutex
	leaderID  string
	myID      string
	electPath string
	isReady   atomic.Bool

	leaderChangeCh chan bool
	electionCh     chan bool
	lockReleaseCh  chan bool
	quitCh         chan bool
	wg             sync.WaitGroup
}

func New(id string, cfg *Config) (*Consul, error) {
	if len(id) == 0 {
		return nil, errors.New("id must NOT be a empty string")
	}

	if len(cfg.Addrs) == 0 {
		return nil, errors.New("Consul address must be provided")
	}

	clientConfig := &api.Config{
		Address: cfg.Addrs[0],
	}

	if cfg.TLS.Enable {
		clientConfig.Scheme = configSchemeWithTLS
		tlsConfig := api.TLSConfig{
			CertFile: cfg.TLS.CertFile,
			KeyFile:  cfg.TLS.KeyFile,
			CAFile:   cfg.TLS.CAFile,
		}
		clientConfig.TLSConfig = tlsConfig
	}

	client, err := api.NewClient(clientConfig)
	if err != nil {
		return nil, err
	}

	electPath := defaultElectPath
	if cfg.ElectPath != "" {
		electPath = cfg.ElectPath
	}

	watchPlanParams := map[string]interface{}{
		"type": "key",
		"key":  electPath,
	}

	watchPlan, err := watch.Parse(watchPlanParams)
	if err != nil {
		return nil, err
	}

	c := &Consul{
		myID:           id,
		electPath:      electPath,
		client:         client,
		watchPlan:      watchPlan,
		leaderChangeCh: make(chan bool),
		lockReleaseCh:  make(chan bool),
		electionCh:     make(chan bool),
		quitCh:         make(chan bool),
	}
	c.watchPlan.Handler = c.watchHandler
	c.isReady.Store(false)
	c.wg.Add(2)
	go c.electLoop()
	go c.runWatch()
	return c, nil
}

func (c *Consul) ID() string {
	return c.myID
}

func (c *Consul) Leader() string {
	c.leaderMu.RLock()
	defer c.leaderMu.RUnlock()
	return c.leaderID
}

func (c *Consul) LeaderChange() <-chan bool {
	return c.leaderChangeCh
}

func (c *Consul) IsReady(ctx context.Context) bool {
	for {
		select {
		case <-c.quitCh:
			return false
		case <-time.After(100 * time.Millisecond):
			if c.isReady.Load() {
				return true
			}
		case <-ctx.Done():
			return c.isReady.Load()
		}
	}
}

func (c *Consul) Get(ctx context.Context, key string) ([]byte, error) {
	rsp, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return nil, err
	}
	if rsp == nil {
		return nil, consts.ErrNotFound
	}
	return rsp.Value, nil
}

func (c *Consul) Exists(ctx context.Context, key string) (bool, error) {
	_, err := c.Get(ctx, key)
	if err != nil {
		if errors.Is(err, consts.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *Consul) Set(ctx context.Context, key string, value []byte) error {
	kvPair := &api.KVPair{
		Key:   key,
		Value: value,
	}
	_, err := c.client.KV().Put(kvPair, nil)
	return err
}

func (c *Consul) Delete(ctx context.Context, key string) error {
	_, err := c.client.KV().Delete(key, nil)
	return err
}

func (c *Consul) List(ctx context.Context, prefix string) ([]engine.Entry, error) {
	rsp, _, err := c.client.KV().List(prefix, nil)
	if err != nil {
		return nil, err
	}

	prefixLen := len(prefix)
	entries := make([]engine.Entry, 0)
	for _, kv := range rsp {
		if string(kv.Key) == prefix {
			continue
		}
		key := strings.TrimLeft(string(kv.Key[prefixLen+1]), "/")
		if strings.ContainsRune(key, '/') {
			continue
		}
		entries = append(entries, engine.Entry{
			Key:   key,
			Value: kv.Value,
		})
	}
	return entries, nil
}

func (c *Consul) electLoop() {
	defer c.wg.Done()
	for {
		select {
		case <-c.quitCh:
			return
		default:
		}

		sessionID, _, err := c.client.Session().Create(&api.SessionEntry{
			Name:      c.electPath,
			Behavior:  "release",
			TTL:       fmt.Sprintf("%v", sessionTTL),
			LockDelay: lockDelay,
		}, nil)

		if err != nil {
			logger.Get().With(
				zap.Error(err),
			).Error("Failed to create session")
			time.Sleep(sessionTTL / 3)
			continue
		}

		kvPair := &api.KVPair{
			Key:     c.electPath,
			Value:   []byte(c.myID),
			Session: sessionID,
		}

		if c.leaderElection(kvPair) {
			return
		}
	}
}

func (c *Consul) leaderElection(kvPair *api.KVPair) bool {
	for {
		if _, _, err := c.client.KV().Acquire(kvPair, nil); err != nil {
			logger.Get().With(
				zap.Error(err),
			).Error("Failed to acquire the leader campaign")
			continue
		}

		select {
		case <-c.lockReleaseCh:
			return false
		case <-c.quitCh:
			logger.Get().Info("Exit the leader election loop")
			return true
		}
	}
}

func (c *Consul) runWatch() {
	defer c.wg.Done()
	if err := c.watchPlan.RunWithClientAndHclog(c.client, nil); err != nil {
		errMsg := fmt.Sprintf("Error running watch plan: %s", err.Error())
		logger.Get().Error(errMsg)
	}
}

func (c *Consul) watchHandler(index uint64, data interface{}) {
	if data == nil {
		return
	}

	c.isReady.Store(true)
	if kvPair, ok := data.(*api.KVPair); ok {

		if kvPair.Session == "" {
			c.lockReleaseCh <- true
			return
		}

		newLeaderID := string(kvPair.Value)
		c.leaderMu.Lock()
		c.leaderID = newLeaderID
		c.leaderMu.Unlock()
		c.leaderChangeCh <- true
	}
}

func (c *Consul) Close() error {
	close(c.quitCh)
	c.watchPlan.Stop()
	c.wg.Wait()
	c.client = nil
	return nil
}
