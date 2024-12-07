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
package etcd

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/apache/kvrocks-controller/consts"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/pkg/transport"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/store/engine"
)

const (
	sessionTTL         = 6
	defaultDialTimeout = 5 * time.Second
)

const defaultElectPath = "/kvrocks/controller/leader"

type Config struct {
	Addrs    []string `yaml:"addrs"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
	TLS      struct {
		Enable        bool   `yaml:"enable"`
		CertFile      string `yaml:"cert_file"`
		KeyFile       string `yaml:"key_file"`
		TrustedCAFile string `yaml:"ca_file"`
	} `yaml:"tls"`
	ElectPath string `yaml:"elect_path"`
}

type Etcd struct {
	client *clientv3.Client
	kv     clientv3.KV

	leaderMu  sync.RWMutex
	leaderID  string
	myID      string
	electPath string
	isReady   atomic.Bool

	quitCh         chan struct{}
	wg             sync.WaitGroup
	electionCh     chan *concurrency.Election
	leaderChangeCh chan bool
}

func New(id string, cfg *Config) (*Etcd, error) {
	if len(id) == 0 {
		return nil, errors.New("id must NOT be a empty string")
	}

	clientConfig := clientv3.Config{
		Endpoints:   cfg.Addrs,
		DialTimeout: defaultDialTimeout,
		Logger:      logger.Get(),
	}

	if cfg.TLS.Enable {
		tlsInfo := transport.TLSInfo{
			CertFile:      cfg.TLS.CertFile,
			KeyFile:       cfg.TLS.KeyFile,
			TrustedCAFile: cfg.TLS.TrustedCAFile,
		}
		tlsConfig, err := tlsInfo.ClientConfig()
		if err != nil {
			return nil, err
		}

		clientConfig.TLS = tlsConfig
	}
	if cfg.Username != "" && cfg.Password != "" {
		clientConfig.Username = cfg.Username
		clientConfig.Password = cfg.Password
	}

	client, err := clientv3.New(clientConfig)
	if err != nil {
		return nil, err
	}

	electPath := defaultElectPath
	if cfg.ElectPath != "" {
		electPath = cfg.ElectPath
	}
	e := &Etcd{
		myID:           id,
		electPath:      electPath,
		client:         client,
		kv:             clientv3.NewKV(client),
		quitCh:         make(chan struct{}),
		electionCh:     make(chan *concurrency.Election),
		leaderChangeCh: make(chan bool),
	}
	e.isReady.Store(false)
	e.wg.Add(2)
	go e.electLoop(context.Background())
	go e.observeLeaderEvent(context.Background())
	return e, nil
}

func (e *Etcd) ID() string {
	return e.myID
}

func (e *Etcd) Leader() string {
	e.leaderMu.RLock()
	defer e.leaderMu.RUnlock()
	return e.leaderID
}

func (e *Etcd) LeaderChange() <-chan bool {
	return e.leaderChangeCh
}

func (e *Etcd) IsReady(ctx context.Context) bool {
	for {
		select {
		case <-e.quitCh:
			return false
		case <-time.After(100 * time.Millisecond):
			if e.isReady.Load() {
				return true
			}
		case <-ctx.Done():
			return e.isReady.Load()
		}
	}
}

func (e *Etcd) Get(ctx context.Context, key string) ([]byte, error) {
	rsp, err := e.kv.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(rsp.Kvs) == 0 {
		return nil, consts.ErrNotFound
	}
	return rsp.Kvs[0].Value, nil
}

func (e *Etcd) Exists(ctx context.Context, key string) (bool, error) {
	_, err := e.Get(ctx, key)
	if err != nil {
		if errors.Is(err, consts.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (e *Etcd) Set(ctx context.Context, key string, value []byte) error {
	_, err := e.kv.Put(ctx, key, string(value))
	return err
}

func (e *Etcd) Delete(ctx context.Context, key string) error {
	_, err := e.kv.Delete(ctx, key)
	return err
}

func (e *Etcd) List(ctx context.Context, prefix string) ([]engine.Entry, error) {
	rsp, err := e.kv.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	prefixLen := len(prefix)
	entries := make([]engine.Entry, 0)
	for _, kv := range rsp.Kvs {
		if string(kv.Key) == prefix {
			continue
		}
		key := strings.TrimLeft(string(kv.Key[prefixLen+1:]), "/")
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

func (e *Etcd) electLoop(ctx context.Context) {
	defer e.wg.Done()
	for {
		select {
		case <-e.quitCh:
			return
		default:
		}

	reset:
		session, err := concurrency.NewSession(e.client, concurrency.WithTTL(sessionTTL))
		if err != nil {
			logger.Get().With(
				zap.Error(err),
			).Error("Failed to create session")
			time.Sleep(sessionTTL / 3)
			continue
		}
		election := concurrency.NewElection(session, e.electPath)
		e.electionCh <- election
		for {
			if err := election.Campaign(ctx, e.myID); err != nil {
				logger.Get().With(
					zap.Error(err),
				).Error("Failed to acquire the leader campaign")
				continue
			}
			select {
			case <-session.Done():
				logger.Get().Warn("Leader session is done")
				goto reset
			case <-e.quitCh:
				logger.Get().Info("Exit the leader election loop")
				return
			}
		}
	}
}

func (e *Etcd) observeLeaderEvent(ctx context.Context) {
	defer e.wg.Done()
	var election *concurrency.Election
	select {
	case elect := <-e.electionCh:
		election = elect
	case <-e.quitCh:
		return
	}

	ch := election.Observe(ctx)
	for {
		select {
		case resp := <-ch:
			e.isReady.Store(true)
			if len(resp.Kvs) > 0 {
				newLeaderID := string(resp.Kvs[0].Value)
				e.leaderMu.Lock()
				e.leaderID = newLeaderID
				e.leaderMu.Unlock()
				e.leaderChangeCh <- true
				if newLeaderID != "" && newLeaderID == e.leaderID {
					continue
				}
			} else {
				ch = election.Observe(ctx)
				e.leaderChangeCh <- false
			}
		case elect := <-e.electionCh:
			election = elect
			ch = election.Observe(ctx)
		case <-e.quitCh:
			logger.Get().Info("Exit the leader change observe loop")
			return
		}
	}
}

func (e *Etcd) Close() error {
	close(e.quitCh)
	e.wg.Wait()
	return e.client.Close()
}
