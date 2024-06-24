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
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/server/middleware"
	"github.com/apache/kvrocks-controller/store"
	"github.com/apache/kvrocks-controller/store/engine"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestNodeBasics(t *testing.T) {
	ns := "test-ns"
	cluster, err := store.NewCluster("test-cluster", []string{"127.0.0.1:1234", "127.0.0.1:1235"}, 2)
	require.NoError(t, err)

	handler := &NodeHandler{s: store.NewClusterStore(engine.NewMock())}
	require.NoError(t, handler.s.CreateCluster(context.Background(), ns, cluster))

	runCreate := func(t *testing.T, addr, role string, expectedStatusCode int) {
		var req struct {
			Addr string `json:"addr"`
			Role string `json:"role"`
		}
		req.Addr = addr
		req.Role = role

		recorder := httptest.NewRecorder()
		ctx := GetTestContext(recorder)
		body, err := json.Marshal(req)
		require.NoError(t, err)

		ctx.Set(consts.ContextKeyStore, handler.s)
		ctx.Request.Body = io.NopCloser(bytes.NewBuffer(body))
		ctx.Params = []gin.Param{
			{Key: "namespace", Value: ns},
			{Key: "cluster", Value: cluster.Name},
			{Key: "shard", Value: "0"}}

		middleware.RequiredClusterShard(ctx)
		require.Equal(t, http.StatusOK, recorder.Code)
		handler.Create(ctx)
		require.Equal(t, expectedStatusCode, recorder.Code)
	}

	runRemove := func(t *testing.T, nodeID string, expectedStatusCode int) {
		recorder := httptest.NewRecorder()
		ctx := GetTestContext(recorder)
		ctx.Set(consts.ContextKeyStore, handler.s)
		ctx.Params = []gin.Param{{Key: "namespace", Value: ns},
			{Key: "cluster", Value: cluster.Name},
			{Key: "shard", Value: "0"},
			{Key: "id", Value: nodeID}}
		middleware.RequiredClusterShard(ctx)
		require.Equal(t, http.StatusOK, recorder.Code)
		handler.Remove(ctx)
		require.Equal(t, expectedStatusCode, recorder.Code)
	}

	t.Run("create node", func(t *testing.T) {
		runCreate(t, "127.0.0.1:1236", "slave", http.StatusCreated)
		runCreate(t, "127.0.0.1:1236", "master", http.StatusConflict)
		runCreate(t, "127.0.0.1:1237", "master", http.StatusConflict)
	})

	t.Run("list node", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ctx := GetTestContext(recorder)
		ctx.Set(consts.ContextKeyStore, handler.s)
		ctx.Params = []gin.Param{{Key: "namespace", Value: ns},
			{Key: "cluster", Value: cluster.Name},
			{Key: "shard", Value: "0"}}
		middleware.RequiredClusterShard(ctx)
		require.Equal(t, http.StatusOK, recorder.Code)
		handler.List(ctx)
		require.Equal(t, http.StatusOK, recorder.Code)

		var rsp struct {
			Data struct {
				Nodes []*store.ClusterNode `json:"nodes"`
			} `json:"data"`
		}
		err := json.Unmarshal(recorder.Body.Bytes(), &rsp)
		require.NoError(t, err)
		require.Len(t, rsp.Data.Nodes, 3)
	})

	t.Run("remove node", func(t *testing.T) {
		runRemove(t, cluster.Shards[0].Nodes[0].ID(), http.StatusBadRequest)
		runRemove(t, cluster.Shards[0].Nodes[1].ID(), http.StatusNoContent)
	})
}
