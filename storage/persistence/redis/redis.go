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
package redis

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/storage/persistence"
	"github.com/go-redis/redis/v8"
	"go.uber.org/atomic"
)

const (
	sessionTTL       = 6 * time.Second
	defaultElectPath = "/kvrocks/controller/leader"
)

type Config struct {
	Addrs     string `yaml:"addrs"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
	DB        int    `yaml:"db"`
	ElectPath string `yaml:"elect_path"`
}

type Redis struct {
	client *redis.Client

	leaderMu  sync.RWMutex
	leaderID  string
	myID      string
	electPath string
	isReady   atomic.Bool

	quitCh         chan struct{}
	wg             sync.WaitGroup
	leaderChangeCh chan bool
}

func New(id string, cfg *Config) (*Redis, error) {
	if len(id) == 0 {
		return nil, errors.New("id must NOT be a empty string")
	}

	clientConfig := &redis.Options{
		Addr:     cfg.Addrs,
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       cfg.DB,
	}

	client := redis.NewClient(clientConfig)

	electPath := defaultElectPath
	if cfg.ElectPath != "" {
		electPath = cfg.ElectPath
	}
	e := &Redis{
		myID:           id,
		electPath:      electPath,
		client:         client,
		quitCh:         make(chan struct{}),
		leaderChangeCh: make(chan bool),
	}
	e.isReady.Store(false)
	e.wg.Add(1)
	go e.electLoop(context.Background())
	return e, nil
}

func (e *Redis) ID() string {
	return e.myID
}

func (e *Redis) Leader() string {
	e.leaderMu.RLock()
	defer e.leaderMu.RUnlock()
	return e.leaderID
}

func (e *Redis) LeaderChange() <-chan bool {
	return e.leaderChangeCh
}

func (e *Redis) IsReady(ctx context.Context) bool {
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

func (e *Redis) Get(ctx context.Context, key string) ([]byte, error) {
	resp := e.client.Get(ctx, key)
	if resp.Err() != nil {
		return nil, resp.Err()
	}
	return resp.Bytes()
}

func (e *Redis) Exists(ctx context.Context, key string) (bool, error) {
	resp := e.client.Exists(ctx, key)
	if resp.Err() != nil {
		return false, resp.Err()
	}
	return resp.Val() == 1, nil
}

func (e *Redis) Set(ctx context.Context, key string, value []byte) error {
	resp := e.client.Set(ctx, key, value, 0)
	if resp.Err() != nil {
		return resp.Err()
	}
	return nil
}

func (e *Redis) Delete(ctx context.Context, key string) error {
	resp := e.client.Del(ctx, key)
	if resp.Err() != nil {
		return resp.Err()
	}
	return nil
}

// use scan to list all keys with prefix
func (e *Redis) List(ctx context.Context, prefix string) ([]persistence.Entry, error) {
	var entries []persistence.Entry
	cursor := uint64(0)
	for {
		resp := e.client.Scan(ctx, cursor, prefix+"/*", 100)
		if resp.Err() != nil {
			return nil, resp.Err()
		}
		keys, cursor := resp.Val()
		prefixLen := len(prefix)
		for _, key := range keys {
			value, err := e.Get(ctx, key)
			if err != nil {
				return nil, err
			}
			key := strings.TrimLeft(string(key[prefixLen+1:]), "/")
			if strings.ContainsRune(key, '/') {
				continue
			}
			entries = append(entries, persistence.Entry{
				Key:   key,
				Value: value,
			})
		}
		if cursor == 0 {
			break
		}
	}
	return entries, nil
}

// we use reids to implement the leader election
func (e *Redis) electLoop(ctx context.Context) {
	defer e.wg.Done()

reset:
	select {
	case <-e.quitCh:
		logger.Get().Info(e.myID + " Exit the leader elect loop")
		return
	default:
	}
	res := e.client.SetNX(ctx, e.electPath, e.myID, sessionTTL)
	if res.Err() != nil {
		time.Sleep(sessionTTL / 3)
		goto reset
	}
	for {
		time.Sleep(sessionTTL / 3)
		resp := e.client.Get(ctx, e.electPath)
		if resp.Err() != nil {
			// if the key is not exist or error, goto reset
			goto reset
		}
		e.SetLeaderID(resp.Val())
		if resp.Val() == e.myID {
			// if the key is set by myself, set ex electPath myId
			res := e.client.Set(ctx, e.electPath, e.myID, sessionTTL)
			if res.Err() != nil {
				goto reset
			}
		}
		select {
		case <-e.quitCh:
			logger.Get().Info(e.myID + " Exit the leader elect loop")
			return
		default:
		}
	}
}

func (e *Redis) SetLeaderID(newleaderID string) {
	if newleaderID != "" && newleaderID != e.leaderID {
		if !e.isReady.Load() {
			e.isReady.Store(true)
		}
		e.leaderMu.Lock()
		e.leaderID = newleaderID
		e.leaderMu.Unlock()
		e.leaderChangeCh <- true
	}
}

func (e *Redis) Close() error {
	close(e.quitCh)
	e.wg.Wait()
	return e.client.Close()
}
