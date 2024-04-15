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
	"time"

	"github.com/apache/kvrocks-controller/controller/failover"
	"github.com/apache/kvrocks-controller/util"
	"github.com/gin-gonic/gin"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/metadata"
	"github.com/apache/kvrocks-controller/storage"
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
		for _, node := range req.Nodes {
			if err := util.DetectClusterNode(c, &metadata.NodeInfo{
				Addr:     node,
				Password: req.Password,
			}); err != nil {
				responseBadRequest(c, err)
				return
			}
		}
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
				ID:        util.GenerateNodeID(),
				Addr:      nodeAddr,
				Password:  req.Password,
				Role:      role,
				CreatedAt: time.Now().Unix(),
			})
		}
		shards[i].SlotRanges = append(shards[i].SlotRanges, slotRanges[i])
		shards[i].MigratingSlot = -1
		shards[i].ImportSlot = -1
	}
	err := handler.storage.CreateCluster(c, namespace, &metadata.Cluster{
		Version: 1,
		Name:    req.Name,
		Shards:  shards,
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

func (handler *ClusterHandler) Import(c *gin.Context) {
	namespace := c.Param("namespace")
	clusterName := c.Param("cluster")
	var req struct {
		Nodes    []string `json:"nodes"`
		Password string   `json:"password"`
	}
	if err := c.BindJSON(&req); err != nil {
		responseBadRequest(c, err)
		return
	}
	if len(req.Nodes) == 0 {
		responseBadRequest(c, errors.New("nodes should NOT be empty"))
		return
	}

	clusterNodesStr, err := util.ClusterNodesCmd(c, &metadata.NodeInfo{
		Addr:     req.Nodes[0],
		Password: req.Password,
	})
	if err != nil {
		responseError(c, err)
		return
	}
	clusterInfo, err := metadata.ParseCluster(clusterNodesStr)
	if err != nil {
		responseError(c, err)
		return
	}
	clusterInfo.SetPassword(req.Password)

	clusterInfo.Name = clusterName
	if err := handler.storage.CreateCluster(c, namespace, clusterInfo); err != nil {
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
