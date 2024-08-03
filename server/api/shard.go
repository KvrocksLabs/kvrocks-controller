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
package api

import (
	"encoding/json"
	"errors"
	"io"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/server/helper"
	"github.com/apache/kvrocks-controller/store"
)

type ShardHandler struct {
	s store.Store
}

type SlotsRequest struct {
	Slots []string `json:"slots" validate:"required"`
}

type CreateShardRequest struct {
	Master *store.ClusterNode  `json:"master"`
	Slaves []store.ClusterNode `json:"slaves"`
}

func (handler *ShardHandler) List(c *gin.Context) {
	cluster, _ := c.MustGet(consts.ContextKeyCluster).(*store.Cluster)
	helper.ResponseOK(c, gin.H{"shards": cluster.Shards})
}

func (handler *ShardHandler) Get(c *gin.Context) {
	shard, _ := c.MustGet(consts.ContextKeyClusterShard).(*store.Shard)
	helper.ResponseOK(c, gin.H{"shard": shard})
}

func (handler *ShardHandler) Create(c *gin.Context) {
	ns := c.Param("namespace")
	var req struct {
		Nodes    []string `json:"nodes" validate:"required"`
		Password string   `json:"password"`
	}
	if err := c.BindJSON(&req); err != nil {
		helper.ResponseBadRequest(c, err)
		return
	}
	if len(req.Nodes) == 0 {
		helper.ResponseBadRequest(c, errors.New("nodes should NOT be empty"))
		return
	}
	nodes := make([]store.Node, 0, len(req.Nodes))
	for i, addr := range req.Nodes {
		node := store.NewClusterNode(addr, req.Password)
		if i == 0 {
			node.SetRole(store.RoleMaster)
		} else {
			node.SetRole(store.RoleSlave)
		}
		nodes = append(nodes, node)
	}
	cluster, _ := c.MustGet(consts.ContextKeyCluster).(*store.Cluster)
	newShard := store.NewShard()
	newShard.Nodes = nodes
	cluster.Shards = append(cluster.Shards, newShard)
	if err := handler.s.UpdateCluster(c, ns, cluster); err != nil {
		helper.ResponseError(c, err)
		return
	}
	helper.ResponseCreated(c, gin.H{"shard": newShard})
}

func (handler *ShardHandler) Remove(c *gin.Context) {
	ns := c.Param("namespace")
	shardIdx, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		helper.ResponseBadRequest(c, err)
		return
	}
	cluster, _ := c.MustGet(consts.ContextKeyCluster).(*store.Cluster)

	if shardIdx < 0 || shardIdx >= len(cluster.Shards) {
		helper.ResponseBadRequest(c, consts.ErrIndexOutOfRange)
		return
	}
	if cluster.Shards[shardIdx].IsServicing() {
		helper.ResponseBadRequest(c, consts.ErrShardIsServicing)
		return
	}
	cluster.Shards = append(cluster.Shards[:shardIdx], cluster.Shards[shardIdx+1:]...)
	if err := handler.s.UpdateCluster(c, ns, cluster); err != nil {
		helper.ResponseError(c, err)
		return
	}
	helper.ResponseNoContent(c)
}

func (handler *ShardHandler) Failover(c *gin.Context) {
	ns := c.Param("namespace")
	cluster, _ := c.MustGet(consts.ContextKeyCluster).(*store.Cluster)

	var req struct {
		PreferredNodeID string `json:"preferred_node_id"`
	}
	if c.Request.Body != nil {
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			helper.ResponseBadRequest(c, err)
			return
		}
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				helper.ResponseBadRequest(c, err)
				return
			}
		}
	}
	// We have checked this if statement in middleware.RequiredClusterShard
	shardIndex, _ := strconv.Atoi(c.Param("shard"))
	newMasterNodeID, err := cluster.PromoteNewMaster(c, shardIndex, "", req.PreferredNodeID)
	if err != nil {
		helper.ResponseError(c, err)
		return
	}
	if err := handler.s.UpdateCluster(c, ns, cluster); err != nil {
		helper.ResponseError(c, err)
		return
	}
	helper.ResponseOK(c, gin.H{"new_master_id": newMasterNodeID})
}
