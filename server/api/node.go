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
	"strconv"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/server/helper"
	"github.com/gin-gonic/gin"

	"github.com/apache/kvrocks-controller/store"
)

type NodeHandler struct {
	s store.Store
}

func (handler *NodeHandler) List(c *gin.Context) {
	shard, _ := c.MustGet(consts.ContextKeyClusterShard).(*store.Shard)
	helper.ResponseOK(c, gin.H{"nodes": shard.Nodes})
}

func (handler *NodeHandler) Create(c *gin.Context) {
	ns := c.Param("namespace")
	cluster, _ := c.MustGet(consts.ContextKeyCluster).(*store.Cluster)
	var req struct {
		Addr     string `json:"addr" binding:"required"`
		Role     string `json:"role"`
		Password string `json:"password"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		helper.ResponseBadRequest(c, err)
		return
	}
	if req.Role == "" {
		req.Role = store.RoleSlave
	}
	shardIndex, _ := strconv.Atoi(c.Param("shard"))
	err := cluster.AddNode(shardIndex, req.Addr, req.Role, req.Password)
	if err != nil {
		helper.ResponseError(c, err)
		return
	}
	if err := handler.s.UpdateCluster(c, ns, cluster); err != nil {
		helper.ResponseError(c, err)
		return
	}
	helper.ResponseCreated(c, nil)
}

func (handler *NodeHandler) Remove(c *gin.Context) {
	ns := c.Param("namespace")
	cluster, _ := c.MustGet(consts.ContextKeyCluster).(*store.Cluster)
	shardIndex, _ := strconv.Atoi(c.Param("shard"))
	err := cluster.RemoveNode(shardIndex, c.Param("id"))
	if err != nil {
		helper.ResponseError(c, err)
		return
	}
	if err := handler.s.UpdateCluster(c, ns, cluster); err != nil {
		helper.ResponseError(c, err)
		return
	}
	helper.ResponseNoContent(c)
}
