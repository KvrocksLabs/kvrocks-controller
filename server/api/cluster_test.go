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

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/server/middleware"
	"github.com/apache/kvrocks-controller/store"
	"github.com/apache/kvrocks-controller/store/engine"
)

func TestClusterBasics(t *testing.T) {
	ns := "test-ns"
	handler := &ClusterHandler{s: store.NewClusterStore(engine.NewMock())}

	runCreate := func(t *testing.T, name string, expectedStatusCode int) {
		testCreateRequest := &CreateClusterRequest{
			Name:     name,
			Nodes:    []string{"127.0.0.1:1234", "127.0.0.1:1235", "127.0.0.1:1236", "127.0.0.1:1237"},
			Replicas: 2,
		}
		recorder := httptest.NewRecorder()
		ctx := GetTestContext(recorder)
		body, _ := json.Marshal(testCreateRequest)
		ctx.Request.Body = io.NopCloser(bytes.NewBuffer(body))
		ctx.Params = []gin.Param{{Key: "namespace", Value: ns}}

		handler.Create(ctx)
		require.Equal(t, expectedStatusCode, recorder.Code)
	}

	runGet := func(t *testing.T, name string, expectedStatusCode int) {
		recorder := httptest.NewRecorder()
		ctx := GetTestContext(recorder)
		ctx.Set(consts.ContextKeyStore, handler.s)
		ctx.Params = []gin.Param{{Key: "namespace", Value: ns}, {Key: "cluster", Value: name}}

		middleware.RequiredCluster(ctx)
		if recorder.Code != http.StatusOK {
			return
		}
		handler.Get(ctx)
		require.Equal(t, expectedStatusCode, recorder.Code)
	}

	runRemove := func(t *testing.T, name string, expectedStatusCode int) {
		recorder := httptest.NewRecorder()
		ctx := GetTestContext(recorder)
		ctx.Set(consts.ContextKeyStore, handler.s)
		ctx.Params = []gin.Param{{Key: "namespace", Value: ns}, {Key: "cluster", Value: name}}

		middleware.RequiredCluster(ctx)
		if recorder.Code != http.StatusOK {
			return
		}
		handler.Remove(ctx)
		require.Equal(t, expectedStatusCode, recorder.Code)
	}

	t.Run("create cluster", func(t *testing.T) {
		runCreate(t, "test-cluster", http.StatusCreated)
		runCreate(t, "test-cluster", http.StatusConflict)
	})

	t.Run("get cluster", func(t *testing.T) {
		runGet(t, "test-cluster", http.StatusOK)
		runGet(t, "not-exist", http.StatusNotFound)
	})

	t.Run("list cluster", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ctx := GetTestContext(recorder)
		ctx.Set(consts.ContextKeyStore, handler.s)
		ctx.Params = []gin.Param{{Key: "namespace", Value: ns}}

		handler.List(ctx)
		require.Equal(t, http.StatusOK, recorder.Code)

		var rsp struct {
			Data struct {
				Clusters []string `json:"clusters"`
			} `json:"data"`
		}
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &rsp))
		require.ElementsMatch(t, []string{"test-cluster"}, rsp.Data.Clusters)
	})

	t.Run("remove cluster", func(t *testing.T) {
		runRemove(t, "test-cluster", http.StatusNoContent)
		runRemove(t, "not-exist", http.StatusNotFound)
	})
}

func TestClusterImport(t *testing.T) {
	ns := "test-ns"
	clusterName := "test-cluster-import"
	handler := &ClusterHandler{s: store.NewClusterStore(engine.NewMock())}
	testNodeAddr := "127.0.0.1:7770"

	clusterNode := store.NewClusterNode(testNodeAddr, "")
	shard := store.NewShard()
	shard.Nodes = []store.Node{clusterNode}
	shard.SlotRanges = []store.SlotRange{{Start: 0, Stop: 16383}}

	cluster := &store.Cluster{
		Name:    clusterName,
		Version: 1,
		Shards:  []*store.Shard{shard},
	}
	ctx := context.Background()
	require.NoError(t, clusterNode.GetClient().ClusterResetHard(ctx).Err())
	require.NoError(t, clusterNode.SyncClusterInfo(ctx, cluster))
	defer func() {
		// clean up the cluster information to avoid affecting other tests
		require.NoError(t, clusterNode.GetClient().ClusterResetHard(ctx).Err())
	}()

	var req struct {
		Nodes []string `json:"nodes"`
	}
	req.Nodes = []string{testNodeAddr}

	recorder := httptest.NewRecorder()
	testCtx := GetTestContext(recorder)
	body, _ := json.Marshal(req)
	testCtx.Request.Body = io.NopCloser(bytes.NewBuffer(body))
	testCtx.Params = []gin.Param{{Key: "namespace", Value: ns}, {Key: "cluster", Value: "test-cluster-import"}}
	handler.Import(testCtx)
	require.Equal(t, http.StatusOK, recorder.Code)

	var rsp struct {
		Data struct {
			Cluster *store.Cluster `json:"cluster"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &rsp))
	require.Len(t, rsp.Data.Cluster.Shards, 1)
	require.Len(t, rsp.Data.Cluster.Shards[0].Nodes, 1)
	require.Equal(t, testNodeAddr, rsp.Data.Cluster.Shards[0].Nodes[0].Addr())
}
