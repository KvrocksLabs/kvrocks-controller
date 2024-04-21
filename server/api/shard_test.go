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
	"strconv"
	"testing"

	"github.com/apache/kvrocks-controller/consts"

	"github.com/apache/kvrocks-controller/server/middleware"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks-controller/store"
	"github.com/apache/kvrocks-controller/store/engine"
)

func TestShardBasics(t *testing.T) {
	ns := "test-ns"
	clusterName := "test-cluster"
	handler := &ShardHandler{s: store.NewClusterStore(engine.NewMock())}

	// create a test cluster
	shard := store.NewShard()
	shard.SlotRanges = []store.SlotRange{{Start: 0, Stop: 16383}}
	shard.Nodes = []store.Node{store.NewClusterNode("127.0.0.1:1234", "")}
	err := handler.s.CreateCluster(context.Background(), ns, &store.Cluster{
		Name:    clusterName,
		Version: 1,
		Shards:  []*store.Shard{shard},
	})
	require.NoError(t, err)

	runCreate := func(t *testing.T, expectedStatusCode int) {
		var req struct {
			Nodes []string `json:"nodes"`
		}
		req.Nodes = []string{"127.0.0.1:1235", "127.0.0.1:1236"}

		recorder := httptest.NewRecorder()
		ctx := GetTestContext(recorder)
		ctx.Set(consts.ContextKeyStore, handler.s)
		ctx.Params = []gin.Param{{Key: "namespace", Value: ns}, {Key: "cluster", Value: clusterName}}
		body, err := json.Marshal(req)
		require.NoError(t, err)
		ctx.Request.Body = io.NopCloser(bytes.NewBuffer(body))

		middleware.RequiredCluster(ctx)
		require.Equal(t, http.StatusOK, recorder.Code)
		handler.Create(ctx)
	}

	runRemove := func(t *testing.T, shardIndex, expectedStatusCode int) {
		recorder := httptest.NewRecorder()
		ctx := GetTestContext(recorder)
		ctx.Set(consts.ContextKeyStore, handler.s)
		ctx.Params = []gin.Param{
			{Key: "namespace", Value: ns},
			{Key: "cluster", Value: clusterName},
			{Key: "shard", Value: strconv.Itoa(shardIndex)}}

		middleware.RequiredClusterShard(ctx)
		require.Equal(t, http.StatusOK, recorder.Code)
		handler.Remove(ctx)
		require.Equal(t, expectedStatusCode, recorder.Code)
	}

	t.Run("create shard", func(t *testing.T) {
		runCreate(t, http.StatusOK)
	})

	t.Run("get shard", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ctx := GetTestContext(recorder)
		ctx.Set(consts.ContextKeyStore, handler.s)
		ctx.Params = []gin.Param{
			{Key: "namespace", Value: ns},
			{Key: "cluster", Value: clusterName},
			{Key: "shard", Value: "1"}}

		middleware.RequiredClusterShard(ctx)
		require.Equal(t, http.StatusOK, recorder.Code)
		handler.Get(ctx)
		require.Equal(t, http.StatusOK, recorder.Code)

		var rsp struct {
			Data struct {
				Shard *store.Shard `json:"shard"`
			} `json:"data"`
		}
		err := json.Unmarshal(recorder.Body.Bytes(), &rsp)
		require.NoError(t, err)
		require.Len(t, rsp.Data.Shard.Nodes, 2)

		var nodeAddrs []string
		for _, node := range rsp.Data.Shard.Nodes {
			nodeAddrs = append(nodeAddrs, node.Addr())
		}
		require.ElementsMatch(t, []string{"127.0.0.1:1235", "127.0.0.1:1236"}, nodeAddrs)
		require.EqualValues(t, -1, rsp.Data.Shard.ImportSlot)
		require.EqualValues(t, -1, rsp.Data.Shard.MigratingSlot)
	})

	t.Run("list shards", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ctx := GetTestContext(recorder)
		ctx.Set(consts.ContextKeyStore, handler.s)
		ctx.Params = []gin.Param{{Key: "namespace", Value: ns}, {Key: "cluster", Value: clusterName}}

		middleware.RequiredCluster(ctx)
		require.Equal(t, http.StatusOK, recorder.Code)
		handler.List(ctx)
		require.Equal(t, http.StatusOK, recorder.Code)

		var rsp struct {
			Data struct {
				Shards []*store.Shard `json:"shards"`
			} `json:"data"`
		}
		err := json.Unmarshal(recorder.Body.Bytes(), &rsp)
		require.NoError(t, err)
		require.Len(t, rsp.Data.Shards, 2)
	})

	t.Run("remove shard", func(t *testing.T) {
		// shard 0 is servicing
		runRemove(t, 0, http.StatusBadRequest)
		runRemove(t, 1, http.StatusNoContent)
	})
}
