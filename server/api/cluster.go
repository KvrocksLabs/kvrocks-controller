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
	"fmt"

	"github.com/gin-gonic/gin"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/server/helper"
	"github.com/apache/kvrocks-controller/store"
	"github.com/apache/kvrocks-controller/util"
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
		if !util.IsHostPort(node) {
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
	s store.Store
}

func (handler *ClusterHandler) List(c *gin.Context) {
	namespace := c.Param("namespace")
	clusters, err := handler.s.ListCluster(c, namespace)
	if err != nil {
		helper.ResponseError(c, err)
		return
	}
	helper.ResponseOK(c, gin.H{"clusters": clusters})
}

func (handler *ClusterHandler) Get(c *gin.Context) {
	cluster, _ := c.MustGet(consts.ContextKeyCluster).(*store.Cluster)
	helper.ResponseOK(c, gin.H{"cluster": cluster})
}

func (handler *ClusterHandler) Create(c *gin.Context) {
	namespace := c.Param("namespace")

	var req CreateClusterRequest
	if err := c.BindJSON(&req); err != nil {
		helper.ResponseBadRequest(c, err)
		return
	}
	if err := req.validate(); err != nil {
		helper.ResponseBadRequest(c, err)
		return
	}

	replicas := req.Replicas
	shardCount := len(req.Nodes) / replicas
	shards := make([]*store.Shard, 0)
	slotRanges := store.SpiltSlotRange(shardCount)
	for i := 0; i < shardCount; i++ {
		shard := store.NewShard()
		shard.Nodes = make([]store.Node, 0)
		for j := 0; j < replicas; j++ {
			addr := req.Nodes[i*replicas+j]
			role := store.RoleMaster
			if j != 0 {
				role = store.RoleSlave
			}
			node := store.NewClusterNode(addr, req.Password)
			node.SetRole(role)
			shard.Nodes = append(shard.Nodes, node)
		}
		shard.SlotRanges = append(shard.SlotRanges, slotRanges[i])
		shard.MigratingSlot = -1
		shard.ImportSlot = -1
		shards = append(shards, shard)
	}

	cluster := &store.Cluster{Version: 1, Name: req.Name, Shards: shards}
	err := handler.s.CreateCluster(c, namespace, cluster)
	if err != nil {
		helper.ResponseError(c, err)
		return
	}
	helper.ResponseCreated(c, gin.H{"cluster": cluster})
}

func (handler *ClusterHandler) Remove(c *gin.Context) {
	namespace := c.Param("namespace")
	cluster := c.Param("cluster")
	err := handler.s.RemoveCluster(c, namespace, cluster)
	if err != nil {
		helper.ResponseError(c, err)
		return
	}
	helper.ResponseNoContent(c)
}

func (handler *ClusterHandler) Import(c *gin.Context) {
	namespace := c.Param("namespace")
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

	firstNode := store.NewClusterNode(req.Nodes[0], req.Password)
	clusterNodesStr, err := firstNode.GetClusterNodesString(c)
	if err != nil {
		helper.ResponseError(c, err)
		return
	}
	cluster, err := store.ParseCluster(clusterNodesStr)
	if err != nil {
		helper.ResponseError(c, err)
		return
	}
	cluster.SetPassword(req.Password)

	cluster.Name = clusterName
	if err := handler.s.CreateCluster(c, namespace, cluster); err != nil {
		helper.ResponseError(c, err)
		return
	}
	helper.ResponseOK(c, gin.H{"cluster": cluster})
}
