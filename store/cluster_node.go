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
package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/kvrocks-controller/util"

	"github.com/go-playground/validator/v10"
	"github.com/go-redis/redis/v8"
)

const (
	RoleMaster = "master"
	RoleSlave  = "slave"

	NodeIDLen = 40
)

const (
	dialTimeout  = 3200 * time.Millisecond
	readTimeout  = 3 * time.Second
	writeTimeout = 3 * time.Second
	minIdleConns = 3
)

var _validator = validator.New()

type Node interface {
	ID() string
	Password() string
	Addr() string
	IsMaster() bool

	SetRole(string)
	SetPassword(string)

	GetClusterInfo(ctx context.Context) (*ClusterInfo, error)
	SyncClusterInfo(ctx context.Context, cluster *Cluster) error
}

type ClusterNode struct {
	mu     sync.RWMutex
	client *redis.Client

	id        string
	addr      string
	role      string
	password  string
	createdAt int64
}

type ClusterInfo struct {
	CurrentEpoch   int64  `json:"cluster_current_epoch"`
	MigratingSlot  int    `json:"migrating_slot"`
	MigratingState string `json:"migrating_state"`
}

type ClusterNodeInfo struct {
	Sequence uint64 `json:"sequence"`
}

func NewClusterNode(addr, password string) *ClusterNode {
	return &ClusterNode{
		id:        util.GenerateNodeID(),
		addr:      addr,
		password:  password,
		role:      RoleMaster,
		createdAt: time.Now().Unix(),
	}
}

func (n *ClusterNode) ID() string {
	return n.id
}

func (n *ClusterNode) Password() string {
	return n.password
}

func (n *ClusterNode) SetPassword(password string) {
	n.password = password
}

func (n *ClusterNode) SetRole(role string) {
	n.role = role
}

func (n *ClusterNode) Addr() string {
	return n.addr
}

func (n *ClusterNode) Validate() error {
	if len(n.id) == 0 {
		return errors.New("node id shouldn't be empty")
	}
	if len(n.id) != NodeIDLen {
		return errors.New("the length of node id must be 40")
	}
	if n.role != RoleMaster && n.role != RoleSlave {
		return errors.New("node role should be 'master' or 'slave'")
	}
	return _validator.Struct(n)
}

func (n *ClusterNode) IsMaster() bool {
	return n.role == RoleMaster
}

func (n *ClusterNode) GetClient() *redis.Client {
	n.mu.RLock()
	if n.client != nil {
		n.mu.RUnlock()
		return n.client
	}
	n.mu.RUnlock()

	n.mu.Lock()
	n.client = redis.NewClient(&redis.Options{
		Addr:         n.addr,
		Password:     n.password,
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		MaxRetries:   -1, // don't retry inside the client
		MinIdleConns: minIdleConns,
	})
	n.mu.Unlock()
	return n.client
}

func (n *ClusterNode) CheckClusterMode(ctx context.Context) (int64, error) {
	clusterInfo, err := n.GetClusterInfo(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "cluster is not initialized") {
			return -1, nil
		}
		return -1, fmt.Errorf("error while checking node(%s) cluster mode: %w", n.addr, err)
	}
	return clusterInfo.CurrentEpoch, nil
}

func (n *ClusterNode) GetClusterInfo(ctx context.Context) (*ClusterInfo, error) {
	infoStr, err := n.GetClient().ClusterInfo(ctx).Result()
	if err != nil {
		return nil, err
	}

	clusterInfo := &ClusterInfo{CurrentEpoch: -1}
	lines := strings.Split(infoStr, "\r\n")
	for _, line := range lines {
		fields := strings.Split(line, ":")
		if len(fields) != 2 {
			continue
		}
		switch fields[0] {
		case "cluster_current_epoch":
			clusterInfo.CurrentEpoch, err = strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				return nil, err
			}
		case "migrating_slot":
			clusterInfo.MigratingSlot, err = strconv.Atoi(fields[1])
			if err != nil {
				return nil, err
			}
		case "migrating_state":
			clusterInfo.MigratingState = fields[1]
		}
	}
	return clusterInfo, nil
}

func (n *ClusterNode) GetClusterNodeInfo(ctx context.Context) (*ClusterNodeInfo, error) {
	infoStr, err := n.GetClient().Info(ctx).Result()
	if err != nil {
		return nil, err
	}

	clusterNodeInfo := &ClusterNodeInfo{}
	lines := strings.Split(infoStr, "\r\n")
	for _, line := range lines {
		fields := strings.Split(line, ":")
		if len(fields) != 2 {
			continue
		}
		switch fields[0] {
		case "sequence":
			clusterNodeInfo.Sequence, err = strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return nil, err
			}
		}
	}
	return clusterNodeInfo, nil
}

func (n *ClusterNode) GetClusterNodesString(ctx context.Context) (string, error) {
	clusterNodesStr, err := n.GetClient().ClusterNodes(ctx).Result()
	if err != nil {
		return "", err
	}
	return strings.TrimRight(clusterNodesStr, "\n"), nil
}

func (n *ClusterNode) SyncClusterInfo(ctx context.Context, cluster *Cluster) error {
	clusterStr, err := cluster.ToSlotString()
	if err != nil {
		return err
	}
	redisCli := n.GetClient()
	err = redisCli.Do(ctx, "CLUSTERX", "SETNODEID", n.id).Err()
	if err != nil {
		return err
	}
	return redisCli.Do(ctx, "CLUSTERX", "SETNODES", clusterStr, cluster.Version).Err()
}
