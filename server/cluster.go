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

package server

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/RocksLabs/kvrocks_controller/util"
	"golang.org/x/sync/errgroup"

	"github.com/RocksLabs/kvrocks_controller/controller/failover"
	"github.com/RocksLabs/kvrocks_controller/controller/migrate"

	"github.com/gin-gonic/gin"

	"github.com/RocksLabs/kvrocks_controller/consts"
	"github.com/RocksLabs/kvrocks_controller/metadata"
	"github.com/RocksLabs/kvrocks_controller/storage"
)

type CreateClusterRequest struct {
	Name     string   `json:"name"`
	Nodes    []string `json:"nodes"`
	Password string   `json:"password"`
	Replicas int      `json:"replicas"`
}

func (req *CreateClusterRequest) validate() error {
	if len(req.Name) == 0 {
		return fmt.Errorf("cluster name should NOT be empty")
	}
	if len(req.Nodes) == 0 {
		return errors.New("cluster nodes should NOT be empty")
	}
	if !util.IsUniqueSlice(req.Nodes) {
		return errors.New("cluster nodes should NOT be duplicated")
	}

	invalidNodes := make([]string, 0)
	for _, node := range req.Nodes {
		if !util.IsIPPort(node) {
			invalidNodes = append(invalidNodes, node)
		}
	}
	if len(invalidNodes) > 0 {
		return fmt.Errorf("invalid node addresses: %v", invalidNodes)
	}

	if req.Replicas == 0 {
		req.Replicas = 1
	}
	if len(req.Nodes)%req.Replicas != 0 {
		return errors.New("cluster nodes should be divisible by replica")
	}
	return nil
}

type ClusterHandler struct {
	storage *storage.Storage
}

func (handler *ClusterHandler) List(c *gin.Context) {
	namespace := c.Param("namespace")
	clusters, err := handler.storage.ListCluster(c, namespace)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, gin.H{"clusters": clusters})
}

func (handler *ClusterHandler) Get(c *gin.Context) {
	namespace := c.Param("namespace")
	clusterName := c.Param("cluster")
	cluster, err := handler.storage.GetClusterInfo(c, namespace, clusterName)
	if err != nil {
		responseError(c, metadata.ErrEntryNoExists)
		return
	}
	responseOK(c, gin.H{"cluster": cluster})
}

func (handler *ClusterHandler) Create(c *gin.Context) {
	namespace := c.Param("namespace")

	var req CreateClusterRequest
	if err := c.BindJSON(&req); err != nil {
		responseBadRequest(c, err)
		return
	}
	if err := req.validate(); err != nil {
		responseBadRequest(c, err)
		return
	}

	if c.GetHeader(consts.HeaderDontDetectHost) != "true" {
		var (
			batcher = errgroup.Group{}
			once    = sync.Once{}
		)

		// limits max number of concurrent goroutines.
		batcher.SetLimit(10)
		for _, node := range req.Nodes {
			node := node
			batcher.Go(func() error {
				_, err := util.ClusterInfoCmd(c, &metadata.NodeInfo{
					Addr:     node,
					Password: req.Password,
				})
				if err != nil && !strings.Contains(err.Error(), "cluster is not initialized") {
					once.Do(func() {
						responseBadRequest(c, fmt.Errorf("error while checking node(%s) cluster mode: %w", node, err))
					})
				}
				return err
			})
		}
		_ = batcher.Wait()
	}

	replicas := req.Replicas
	shards := make([]metadata.Shard, len(req.Nodes)/replicas)
	slotRanges := metadata.SpiltSlotRange(len(shards))
	for i := range shards {
		shards[i].Nodes = make([]metadata.NodeInfo, 0)
		for j := 0; j < replicas; j++ {
			nodeAddr := req.Nodes[i*replicas+j]
			role := metadata.RoleMaster
			if j != 0 {
				role = metadata.RoleSlave
			}
			shards[i].Nodes = append(shards[i].Nodes, metadata.NodeInfo{
				ID:       util.GenerateNodeID(),
				Addr:     nodeAddr,
				Password: req.Password,
				Role:     role,
			})
		}
		shards[i].SlotRanges = append(shards[i].SlotRanges, slotRanges[i])
		shards[i].MigratingSlot = -1
		shards[i].ImportSlot = -1
	}
	err := handler.storage.CreateCluster(c, namespace, &metadata.Cluster{
		Name:   req.Name,
		Shards: shards,
	})
	if err != nil {
		responseError(c, err)
		return
	}
	responseCreated(c, "created")
}

func (handler *ClusterHandler) Remove(c *gin.Context) {
	namespace := c.Param("namespace")
	cluster := c.Param("cluster")
	err := handler.storage.RemoveCluster(c, namespace, cluster)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, "ok")
}

func (handler *ClusterHandler) GetFailOverTasks(c *gin.Context) {
	namespace := c.Param("namespace")
	cluster := c.Param("cluster")
	typ := c.Param("type")
	failover, _ := c.MustGet(consts.ContextKeyFailover).(*failover.FailOver)
	tasks, err := failover.GetTasks(c, namespace, cluster, typ)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, gin.H{"tasks": tasks})
}

func (handler *ClusterHandler) GetMigratingTasks(c *gin.Context) {
	namespace := c.Param("namespace")
	cluster := c.Param("cluster")
	typ := c.Param("type")

	migration, _ := c.MustGet(consts.ContextKeyMigrate).(*migrate.Migrator)
	tasks, err := migration.GetMigrateTasks(c, namespace, cluster, typ)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, gin.H{"tasks": tasks})
}
