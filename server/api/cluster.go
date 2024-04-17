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

	"github.com/gin-gonic/gin"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/server/helper"
	"github.com/apache/kvrocks-controller/store"
)

type MigrateSlotRequest struct {
	Target   int  `json:"target" validate:"required"`
	Slot     int  `json:"slot" validate:"required"`
	SlotOnly bool `json:"slot_only"`
}

type CreateClusterRequest struct {
	Name     string   `json:"name"`
	Nodes    []string `json:"nodes"`
	Password string   `json:"password"`
	Replicas int      `json:"replicas"`
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

	cluster, err := store.NewCluster(req.Name, req.Nodes, req.Replicas)
	if err != nil {
		helper.ResponseBadRequest(c, err)
		return
	}
	cluster.SetPassword(req.Password)
	if err := handler.s.CreateCluster(c, namespace, cluster); err != nil {
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

func (handler *ClusterHandler) MigrateSlot(c *gin.Context) {
	namespace := c.Param("namespace")
	cluster, _ := c.MustGet(consts.ContextKeyCluster).(*store.Cluster)

	var req MigrateSlotRequest
	if err := c.BindJSON(&req); err != nil {
		helper.ResponseBadRequest(c, err)
		return
	}

	err := cluster.MigrateSlot(c, req.Slot, req.Target, req.SlotOnly)
	if err != nil {
		helper.ResponseError(c, err)
		return
	}

	if req.SlotOnly {
		err = handler.s.UpdateCluster(c, namespace, cluster)
	} else {
		// The version should be increased after the slot migration is done
		err = handler.s.SetCluster(c, namespace, cluster)
	}
	if err != nil {
		helper.ResponseError(c, err)
		return
	}
	helper.ResponseOK(c, gin.H{"cluster": cluster})
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
