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
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/apache/kvrocks-controller/server/helper"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/server/api"
	"github.com/apache/kvrocks-controller/server/middleware"
)

func (srv *Server) initHandlers() {
	engine := srv.engine
	engine.Use(middleware.CollectMetrics, func(c *gin.Context) {
		c.Set(consts.ContextKeyStore, srv.store)
		c.Next()
	}, middleware.RedirectIfNotLeader)
	handler := api.NewHandler(srv.store)

	engine.Any("/debug/pprof/*profile", PProf)
	engine.GET("/metrics", gin.WrapH(promhttp.Handler()))
	engine.NoRoute(func(c *gin.Context) {
		helper.ResponseError(c, consts.ErrNotFound)
		c.Abort()
	})

	apiV1 := engine.Group("/api/v1/")
	{
		raftAPI := apiV1.Group("raft")
		{
			raftAPI.Use(middleware.RequiredRaftEngine)
			raftAPI.POST("/peers", handler.Raft.UpdatePeer)
			raftAPI.GET("/peers", handler.Raft.ListPeers)
		}

		namespaces := apiV1.Group("namespaces")
		{
			namespaces.GET("", handler.Namespace.List)
			namespaces.GET("/:namespace", handler.Namespace.Exists)
			namespaces.POST("", handler.Namespace.Create)
			namespaces.DELETE("/:namespace", handler.Namespace.Remove)
		}

		clusters := namespaces.Group("/:namespace/clusters")
		{
			clusters.GET("", middleware.RequiredNamespace, handler.Cluster.List)
			clusters.POST("", middleware.RequiredNamespace, handler.Cluster.Create)
			clusters.POST("/:cluster/import", middleware.RequiredNamespace, handler.Cluster.Import)
			clusters.GET("/:cluster", middleware.RequiredCluster, handler.Cluster.Get)
			clusters.DELETE("/:cluster", middleware.RequiredCluster, handler.Cluster.Remove)
			clusters.POST("/:cluster/migrate", middleware.RequiredCluster, handler.Cluster.MigrateSlot)
		}

		shards := clusters.Group("/:cluster/shards")
		{
			shards.GET("", middleware.RequiredCluster, handler.Shard.List)
			shards.POST("", middleware.RequiredCluster, handler.Shard.Create)
			shards.GET("/:shard", middleware.RequiredClusterShard, handler.Shard.Get)
			shards.DELETE("/:shard", middleware.RequiredCluster, handler.Shard.Remove)
			shards.POST("/:shard/failover", middleware.RequiredClusterShard, handler.Shard.Failover)
		}

		nodes := shards.Group("/:shard/nodes")
		{
			nodes.GET("", middleware.RequiredClusterShard, handler.Node.List)
			nodes.POST("", middleware.RequiredClusterShard, handler.Node.Create)
			nodes.DELETE("/:id", middleware.RequiredClusterShard, handler.Node.Remove)
		}
	}
}
