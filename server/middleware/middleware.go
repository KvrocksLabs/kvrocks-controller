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

package middleware

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/apache/kvrocks-controller/store/engine/raft"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/metrics"
	"github.com/apache/kvrocks-controller/server/helper"
	"github.com/apache/kvrocks-controller/store"
)

func CollectMetrics(c *gin.Context) {
	startTime := time.Now()
	c.Next()
	latency := time.Since(startTime).Milliseconds()

	uri := c.FullPath()
	// uri was empty means not found routes, so rewrite it to /not_found here
	if c.Writer.Status() == http.StatusNotFound && uri == "" {
		uri = "/not_found"
	}
	labels := prometheus.Labels{
		"host":   c.Request.Host,
		"uri":    uri,
		"method": c.Request.Method,
		"code":   strconv.Itoa(c.Writer.Status()),
	}
	metrics.Get().HTTPCodes.With(labels).Inc()
	metrics.Get().Latencies.With(labels).Observe(float64(latency))
	size := c.Writer.Size()
	if size > 0 {
		metrics.Get().Payload.With(labels).Add(float64(size))
	}
}

func RedirectIfNotLeader(c *gin.Context) {
	storage, _ := c.MustGet(consts.ContextKeyStore).(*store.ClusterStore)
	if storage.Leader() == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no leader now, please retry later"})
		c.Abort()
		return
	}

	_, isRaftMode := storage.GetEngine().(*raft.Node)
	// Raft engine will forward the request to the leader node under the hood,
	// so we don't need to do the redirect.
	if !storage.IsLeader() && !isRaftMode {
		if !c.GetBool(consts.HeaderIsRedirect) {
			c.Set(consts.HeaderIsRedirect, true)
			peerAddr := helper.ExtractAddrFromSessionID(storage.Leader())
			c.Redirect(http.StatusTemporaryRedirect, "http://"+peerAddr+c.Request.RequestURI)
			c.Redirect(http.StatusTemporaryRedirect, "http://"+storage.Leader()+c.Request.RequestURI)
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": "no leader now, please retry later"})
			c.Abort()
		}
		return
	}
	c.Next()
}

func RequiredNamespace(c *gin.Context) {
	s, _ := c.MustGet(consts.ContextKeyStore).(*store.ClusterStore)
	ok, err := s.ExistsNamespace(c, c.Param("namespace"))
	if err != nil {
		helper.ResponseError(c, err)
		return
	}
	if !ok {
		helper.ResponseBadRequest(c, errors.New("namespace not found"))
		c.Abort()
	} else {
		c.Next()
	}
}

func RequiredCluster(c *gin.Context) {
	s, _ := c.MustGet(consts.ContextKeyStore).(*store.ClusterStore)
	cluster, err := s.GetCluster(c, c.Param("namespace"), c.Param("cluster"))
	if err != nil {
		helper.ResponseError(c, err)
		return
	}

	c.Set(consts.ContextKeyCluster, cluster)
	c.Next()
}

func RequiredClusterShard(c *gin.Context) {
	s, _ := c.MustGet(consts.ContextKeyStore).(*store.ClusterStore)
	cluster, err := s.GetCluster(c, c.Param("namespace"), c.Param("cluster"))
	if err != nil {
		helper.ResponseError(c, err)
		return
	}

	shardIndex, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		helper.ResponseBadRequest(c, err)
		return
	}
	shard, err := cluster.GetShard(shardIndex)
	if err != nil {
		helper.ResponseError(c, err)
		return
	}

	c.Set(consts.ContextKeyCluster, cluster)
	c.Set(consts.ContextKeyClusterShard, shard)
	c.Next()
}

func RequiredRaftEngine(c *gin.Context) {
	storage, _ := c.MustGet(consts.ContextKeyStore).(*store.ClusterStore)
	raftNode, ok := storage.GetEngine().(*raft.Node)
	if !ok {
		helper.ResponseBadRequest(c, errors.New("raft engine is not enabled"))
		c.Abort()
		return
	}
	c.Set(consts.ContextKeyRaftNode, raftNode)
	c.Next()
}
