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

package zookeeper

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/store/engine"
	"github.com/go-zookeeper/zk"
)

const (
	sessionTTL       = 6 * time.Second
	defaultElectPath = "/kvrocks/controller/leader"
)

type Config struct {
	Addrs     []string `yaml:"addrs"`
	Scheme    string   `yaml:"scheme"`
	Auth      string   `yaml:"auth"`
	ElectPath string   `yaml:"elect_path"`
}

type Zookeeper struct {
	conn           *zk.Conn
	acl            []zk.ACL // We will set this ACL for the node we have created
	leaderMu       sync.RWMutex
	leaderID       string
	myID           string
	electPath      string
	isReady        atomic.Bool
	quitCh         chan struct{}
	leaderChangeCh chan bool
	wg             sync.WaitGroup
}

func New(id string, cfg *Config) (*Zookeeper, error) {
	if len(id) == 0 {
		return nil, errors.New("id must NOT be a empty string")
	}
	conn, _, err := zk.Connect(cfg.Addrs, sessionTTL)
	if err != nil {
		return nil, err
	}
	electPath := defaultElectPath
	if cfg.ElectPath != "" {
		electPath = cfg.ElectPath
	}
	acl := zk.WorldACL(zk.PermAll)
	if cfg.Scheme != "" && cfg.Auth != "" {
		err := conn.AddAuth(cfg.Scheme, []byte(cfg.Auth))
		if err == nil {
			acl = []zk.ACL{{Perms: zk.PermAll, Scheme: cfg.Scheme, ID: cfg.Auth}}
		} else {
			logger.Get().Warn("Zookeeper addAuth fail: " + err.Error())
		}
	}
	e := &Zookeeper{
		myID:           id,
		acl:            acl,
		electPath:      electPath,
		conn:           conn,
		quitCh:         make(chan struct{}),
		leaderChangeCh: make(chan bool),
		wg:             sync.WaitGroup{},
	}
	e.isReady.Store(false)
	e.wg.Add(1)
	go e.electLoop(context.Background())
	return e, nil
}

func (e *Zookeeper) ID() string {
	return e.myID
}

func (e *Zookeeper) Leader() string {
	e.leaderMu.RLock()
	defer e.leaderMu.RUnlock()
	return e.leaderID
}

func (e *Zookeeper) LeaderChange() <-chan bool {
	return e.leaderChangeCh
}

func (e *Zookeeper) IsReady(ctx context.Context) bool {
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

func (e *Zookeeper) Get(ctx context.Context, key string) ([]byte, error) {
	data, _, err := e.conn.Get(key)
	if err != nil {
		if errors.Is(err, zk.ErrNoNode) {
			return nil, nil // Key does not exist
		}
		return nil, err
	}

	return data, nil
}

func (e *Zookeeper) Exists(ctx context.Context, key string) (bool, error) {
	exists, _, err := e.conn.Exists(key)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// Set sets the value for the key. If the key exists, it will be set; if not, it will be created.
func (e *Zookeeper) Set(ctx context.Context, key string, value []byte) error {
	exist, _ := e.Exists(ctx, key)
	if exist {
		_, err := e.conn.Set(key, value, -1)
		return err
	}

	return e.Create(ctx, key, value, 0)
}

func (e *Zookeeper) Create(ctx context.Context, key string, value []byte, flags int32) error {
	lastSlashIndex := strings.LastIndex(key, "/")
	if lastSlashIndex > 0 {
		substring := key[:lastSlashIndex]
		// If the parent node does not exist, create the parent node recursively
		exist, _ := e.Exists(ctx, substring)
		if !exist {
			err := e.Create(ctx, substring, []byte{}, 0)
			if err != nil {
				return err
			}
		}
	}
	_, err := e.conn.Create(key, value, flags, e.acl)
	return err
}

func (e *Zookeeper) Delete(ctx context.Context, key string) error {
	err := e.conn.Delete(key, -1)
	if errors.Is(err, zk.ErrNoNode) {
		return nil // Key does not exist
	}
	return err
}

func (e *Zookeeper) List(ctx context.Context, prefix string) ([]engine.Entry, error) {
	children, _, err := e.conn.Children(prefix)
	if errors.Is(err, zk.ErrNoNode) {
		return []engine.Entry{}, nil
	} else if err != nil {
		return nil, err
	}

	entries := make([]engine.Entry, 0)
	for _, child := range children {
		key := prefix + "/" + child
		data, _, err := e.conn.Get(key)
		if err != nil {
			return nil, err
		}

		entry := engine.Entry{
			Key:   child,
			Value: data,
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func (e *Zookeeper) SetleaderID(newLeaderID string) {
	if newLeaderID != "" && newLeaderID != e.leaderID {
		if !e.isReady.Load() {
			// we set ready flag when leaderID first changed
			e.isReady.Store(true)
		}
		e.leaderMu.Lock()
		e.leaderID = newLeaderID
		e.leaderMu.Unlock()
		e.leaderChangeCh <- true
	}
}

func (e *Zookeeper) electLoop(ctx context.Context) {
	defer e.wg.Done()
reset:
	select {
	case <-e.quitCh:
		return
	default:
	}
	err := e.Create(ctx, e.electPath, []byte(e.myID), zk.FlagEphemeral)
	if err != nil && !errors.Is(err, zk.ErrNodeExists) {
		time.Sleep(sessionTTL / 3)
		goto reset
	}
	data, _, ch, err := e.conn.GetW(e.electPath)
	if err != nil {
		time.Sleep(sessionTTL / 3)
		goto reset
	}
	e.SetleaderID(string(data))

	for {
		select {
		case resp := <-ch:
			if resp.Type == zk.EventNodeDeleted {
				err := e.Create(ctx, e.electPath, []byte(e.myID), zk.FlagEphemeral)
				if err != nil && !errors.Is(err, zk.ErrNodeExists) {
					time.Sleep(sessionTTL / 3)
					goto reset
				}
			}
			data, _, ch, err = e.conn.GetW(e.electPath)
			if err != nil {
				time.Sleep(sessionTTL / 3)
				goto reset
			}
			e.SetleaderID(string(data))
		case <-e.quitCh:
			logger.Get().Info(e.myID + " Exit the leader election loop")
			return
		}

	}

}

func (e *Zookeeper) Close() error {
	close(e.quitCh)
	e.wg.Wait()
	e.conn.Close()
	return nil
}
