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
	"errors"
	"strconv"

	"github.com/apache/kvrocks-controller/server/helper"

	"github.com/gin-gonic/gin"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/store"
)

type ShardHandler struct {
	s store.Store
}

type SlotsRequest struct {
	Slots []string `json:"slots" validate:"required"`
}

type MigrateSlotDataRequest struct {
	Source int `json:"source" validate:"required"`
	Target int `json:"target" validate:"required"`
	Slot   int `json:"slot"`
}

type MigrateSlotOnlyRequest struct {
	Source int               `json:"source" validate:"required"`
	Target int               `json:"target" validate:"required"`
	Slots  []store.SlotRange `json:"slots"`
}

type CreateShardRequest struct {
	Master *store.ClusterNode  `json:"master"`
	Slaves []store.ClusterNode `json:"slaves"`
}

func (handler *ShardHandler) List(c *gin.Context) {
	ns := c.Param("namespace")
	cluster, err := handler.s.GetCluster(c, ns, c.Param("cluster"))
	if err != nil {
		helper.ResponseError(c, err)
		return
	}
	helper.ResponseOK(c, gin.H{"shards": cluster.Shards})
}

func (handler *ShardHandler) Get(c *gin.Context) {
	shard, _ := c.MustGet(consts.ContextKeyClusterShard).(*store.Shard)
	helper.ResponseOK(c, gin.H{"shard": shard})
}

func (handler *ShardHandler) Create(c *gin.Context) {
	ns := c.Param("namespace")
	clusterName := c.Param("cluster")

	var req struct {
		Nodes    []string `json:"nodes"`
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
	nodes := make([]store.Node, len(req.Nodes))
	for i, addr := range req.Nodes {
		node := store.NewClusterNode(addr, req.Password)
		if i == 0 {
			node.SetRole(store.RoleMaster)
		} else {
			node.SetRole(store.RoleSlave)
		}
	}
	cluster, err := handler.s.GetCluster(c, ns, clusterName)
	if err != nil {
		helper.ResponseError(c, err)
		return
	}
	newShard := store.NewShard()
	newShard.Nodes = nodes
	cluster.Shards = append(cluster.Shards, newShard)
	if err := handler.s.UpdateCluster(c, ns, cluster); err != nil {
		helper.ResponseError(c, err)
		return
	}
	helper.ResponseCreated(c, "created")
}

func (handler *ShardHandler) Remove(c *gin.Context) {
	ns := c.Param("namespace")
	clusterName := c.Param("cluster")
	shardIdx, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		helper.ResponseBadRequest(c, err)
		return
	}

	cluster, err := handler.s.GetCluster(c, ns, clusterName)
	if err != nil {
		helper.ResponseError(c, err)
		return
	}
	if shardIdx < 0 || shardIdx >= len(cluster.Shards) {
		helper.ResponseBadRequest(c, consts.ErrIndexOutOfRange)
		return
	}
	cluster.Shards = append(cluster.Shards[:shardIdx], cluster.Shards[shardIdx+1:]...)
	if err := handler.s.UpdateCluster(c, ns, cluster); err != nil {
		helper.ResponseError(c, err)
		return
	}
	helper.ResponseOK(c, "ok")
}

func (handler *ShardHandler) MigrateSlotData(c *gin.Context) {
	var req MigrateSlotDataRequest
	if err := c.BindJSON(&req); err != nil {
		helper.ResponseBadRequest(c, err)
		return
	}
	// TODO: add task to the task queue
	helper.ResponseOK(c, "ok")
}

func (handler *ShardHandler) MigrateSlotOnly(c *gin.Context) {
	//var req MigrateSlotOnlyRequest
	//if err := c.BindJSON(&req); err != nil {
	//	ResponseBadRequest(c, err)
	//	return
	//}
	//ns := c.Param("namespace")
	//cluster := c.Param("cluster")
	//if err := handler.s.UpdateMigrateSlotInfo(c, ns, cluster, req.Source, req.Target, req.Slots); err != nil {
	//	responseError(c, err)
	//	return
	//}
	helper.ResponseOK(c, "ok")
}

func (handler *ShardHandler) Failover(c *gin.Context) {
	//ns := c.Param("namespace")
	//cluster := c.Param("cluster")
	//shard, err := strconv.Atoi(c.Param("shard"))
	//if err != nil {
	//	ResponseBadRequest(c, err)
	//	return
	//}
	//
	//nodes, err := handler.s.ListNodes(c, ns, cluster, shard)
	//if err != nil {
	//	return
	//}
	//if len(nodes) <= 1 {
	//	ResponseBadRequest(c, errors.New("no node to be failover"))
	//	return
	//}
	//var failoverNode *s.ClusterNode
	//for i, node := range nodes {
	//	if node.role == s.RoleMaster {
	//		failoverNode = nodes[i]
	//		break
	//	}
	//}
	//if failoverNode == nil {
	//	ResponseBadRequest(c, consts.ErrNotFound)
	//	return
	//}

	// TODO: failover
	helper.ResponseOK(c, "ok")
}
