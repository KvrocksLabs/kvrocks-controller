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
package postgresql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/store/engine"
	"github.com/lib/pq"
	"go.uber.org/zap"
)

const (
	// Need to modify the cron schedule timeout accordingly in init.sql before changing the lockTTL
	lockTTL                      = 6 * time.Second
	listenerMinReconnectInterval = 10 * time.Second
	listenerMaxReconnectInterval = 1 * time.Minute
	defaultElectPath             = "/kvrocks/controller/leader"
)

type Config struct {
	Addrs         []string `yaml:"addrs"`
	Username      string   `yaml:"username"`
	Password      string   `yaml:"password"`
	DBName        string   `yaml:"db_name"`
	NotifyChannel string   `yaml:"notify_channel"`
	ElectPath     string   `yaml:"elect_path"`
}

type Postgresql struct {
	db       *sql.DB
	listener *pq.Listener

	leaderMu  sync.Mutex
	leaderID  string
	myID      string
	electPath string
	isReady   atomic.Bool

	quitCh         chan struct{}
	wg             sync.WaitGroup
	lockReleaseCh  chan bool
	leaderChangeCh chan bool
}

func New(id string, cfg *Config) (*Postgresql, error) {
	if len(id) == 0 {
		return nil, errors.New("id must NOT be a empty string")
	}

	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", cfg.Username, cfg.Password, cfg.Addrs[0], cfg.DBName)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	listener := pq.NewListener(connStr, listenerMinReconnectInterval, listenerMaxReconnectInterval, nil)
	err = listener.Listen(cfg.NotifyChannel)
	if err != nil {
		return nil, err
	}

	electPath := defaultElectPath
	if cfg.ElectPath != "" {
		electPath = defaultElectPath
	}

	p := &Postgresql{
		myID:           id,
		electPath:      electPath,
		db:             db,
		listener:       listener,
		quitCh:         make(chan struct{}),
		lockReleaseCh:  make(chan bool),
		leaderChangeCh: make(chan bool),
	}
	err = p.initLeaderId()
	if err != nil {
		return nil, err
	}
	p.isReady.Store(false)
	p.wg.Add(2)
	go p.electLoop()
	go p.observeLeaderEvent()
	return p, nil
}

func (p *Postgresql) ID() string {
	return p.myID
}

func (p *Postgresql) Leader() string {
	p.leaderMu.Lock()
	defer p.leaderMu.Unlock()
	return p.leaderID
}

func (p *Postgresql) LeaderChange() <-chan bool {
	return p.leaderChangeCh
}

func (p *Postgresql) IsReady(ctx context.Context) bool {
	for {
		select {
		case <-p.quitCh:
			return false
		case <-time.After(100 * time.Millisecond):
			if p.isReady.Load() {
				return true
			}
		case <-ctx.Done():
			return p.isReady.Load()
		}
	}
}

func (p *Postgresql) Get(ctx context.Context, key string) ([]byte, error) {
	var value []byte
	query := "SELECT value FROM kv WHERE key = $1"

	row := p.db.QueryRow(query, key)
	err := row.Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, consts.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (p *Postgresql) Exists(ctx context.Context, key string) (bool, error) {
	_, err := p.Get(ctx, key)
	if err != nil {
		if errors.Is(err, consts.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (p *Postgresql) Set(ctx context.Context, key string, value []byte) error {
	query := "INSERT INTO kv (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
	_, err := p.db.Exec(query, key, value)
	return err
}

func (p *Postgresql) Delete(ctx context.Context, key string) error {
	query := "DELETE FROM kv WHERE key = $1"
	_, err := p.db.Exec(query, key)
	return err
}

func (p *Postgresql) List(ctx context.Context, prefix string) ([]engine.Entry, error) {
	prefixWithWildcard := prefix + "%"
	query := "SELECT key, value from kv WHERE key LIKE $1"
	rows, err := p.db.Query(query, prefixWithWildcard)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	prefixLen := len(prefix)
	entries := make([]engine.Entry, 0)
	for rows.Next() {
		var key string
		var value []byte

		err := rows.Scan(&key, &value)
		if err != nil {
			return nil, err
		}

		if key == prefix {
			continue
		}

		key = strings.TrimLeft(key[prefixLen+1:], "/")
		if strings.ContainsRune(key, '/') {
			continue
		}
		entries = append(entries, engine.Entry{
			Key:   key,
			Value: value,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}

func (p *Postgresql) electLoop() {
	defer p.wg.Done()
	for {
		select {
		case <-p.quitCh:
			return
		default:
		}

		query := "INSERT INTO locks (name, leaderID) VALUES ($1, $2) ON CONFLICT DO NOTHING"
		_, err := p.db.Exec(query, p.electPath, p.myID)
		if err != nil {
			time.Sleep(lockTTL / 3)
			continue
		}

		select {
		case <-p.lockReleaseCh:
			continue
		case <-p.quitCh:
			return
		}
	}
}

func (p *Postgresql) observeLeaderEvent() {
	defer p.wg.Done()

	for {
		select {
		case <-p.quitCh:
			return
		case notification := <-p.listener.Notify:
			p.isReady.Store(true)

			data := strings.SplitN(notification.Extra, ":", 2)
			if len(data) != 2 {
				logger.Get().With(
					zap.Error(fmt.Errorf("failed to parse notification data: expected two parts separated by a colon")),
				).Error("Failed to parse notification data")
			}

			operation := data[0]
			leaderID := data[1]

			if operation == "INSERT" {
				p.leaderMu.Lock()
				p.leaderID = leaderID
				p.leaderMu.Unlock()
				p.leaderChangeCh <- true
			} else {
				p.lockReleaseCh <- true
			}
		}
	}
}

func (p *Postgresql) initLeaderId() error {
	var leaderId string
	query := "SELECT leaderID FROM locks WHERE name = $1"
	row := p.db.QueryRow(query, p.electPath)
	err := row.Scan(&leaderId)
	if errors.Is(err, sql.ErrNoRows) {
		p.leaderID = ""
		return nil
	}
	if err != nil {
		return err
	}
	p.leaderID = leaderId
	return nil
}

func (p *Postgresql) Close() error {
	close(p.quitCh)
	p.wg.Wait()
	p.listener.Close()
	return p.db.Close()
}
