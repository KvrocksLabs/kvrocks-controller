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
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks-controller/config"
	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/controller"
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
		body, err := json.Marshal(testCreateRequest)
		require.NoError(t, err)

		ctx.Header(consts.HeaderDontCheckClusterMode, "yes")
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

	t.Run("migrate slot only", func(t *testing.T) {
		handler := &ClusterHandler{s: store.NewClusterStore(engine.NewMock())}
		clusterName := "test-migrate-slot-only-cluster"
		recorder := httptest.NewRecorder()
		ctx := GetTestContext(recorder)
		ctx.Set(consts.ContextKeyStore, handler.s)
		ctx.Params = []gin.Param{{Key: "namespace", Value: ns}, {Key: "cluster", Value: clusterName}}
		testMigrateReq := &MigrateSlotRequest{
			Slot:     3,
			SlotOnly: true,
			Target:   1,
		}
		body, err := json.Marshal(testMigrateReq)
		require.NoError(t, err)
		ctx.Request.Body = io.NopCloser(bytes.NewBuffer(body))

		cluster, err := store.NewCluster(clusterName, []string{"127.0.0.1:1111", "127.0.0.1:2222"}, 1)
		require.NoError(t, err)
		require.NoError(t, handler.s.CreateCluster(ctx, ns, cluster))

		before, err := handler.s.GetCluster(ctx, ns, clusterName)
		require.NoError(t, err)
		require.EqualValues(t, store.SlotRange{Start: 0, Stop: 8191}, before.Shards[0].SlotRanges[0])
		require.EqualValues(t, store.SlotRange{Start: 8192, Stop: store.MaxSlotID}, before.Shards[1].SlotRanges[0])

		middleware.RequiredCluster(ctx)
		handler.MigrateSlot(ctx)
		require.Equal(t, http.StatusOK, recorder.Code)
		after, err := handler.s.GetCluster(ctx, ns, clusterName)
		require.NoError(t, err)

		require.EqualValues(t, before.Version.Add(1), after.Version.Load())
		require.Len(t, after.Shards[0].SlotRanges, 2)
		require.EqualValues(t, store.SlotRange{Start: 0, Stop: 2}, after.Shards[0].SlotRanges[0])
		require.EqualValues(t, store.SlotRange{Start: 4, Stop: 8191}, after.Shards[0].SlotRanges[1])
		require.Len(t, after.Shards[1].SlotRanges, 2)
		require.EqualValues(t, store.SlotRange{Start: 3, Stop: 3}, after.Shards[1].SlotRanges[0])
		require.EqualValues(t, store.SlotRange{Start: 8192, Stop: store.MaxSlotID}, after.Shards[1].SlotRanges[1])
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
	// cluster import must be done on a real cluster
	testNodeAddr := "127.0.0.1:7770"
	clusterNode := store.NewClusterNode(testNodeAddr, "")
	cluster, err := store.NewCluster(clusterName, []string{testNodeAddr}, 1)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, cluster.Reset(ctx))
	require.NoError(t, clusterNode.SyncClusterInfo(ctx, cluster))
	defer func() {
		// clean up the cluster information to avoid affecting other tests
		require.NoError(t, cluster.Reset(ctx))
	}()

	var req struct {
		Nodes []string `json:"nodes"`
	}
	req.Nodes = []string{testNodeAddr}

	recorder := httptest.NewRecorder()
	testCtx := GetTestContext(recorder)
	body, err := json.Marshal(req)
	require.NoError(t, err)

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

// TestClusterMigrateData only test if the API works well here
// and for the migration status will be tested in the controller
func TestClusterMigrateData(t *testing.T) {
	ns := "test-ns"
	clusterName := "test-cluster"
	handler := &ClusterHandler{s: store.NewClusterStore(engine.NewMock())}
	cluster, err := store.NewCluster(clusterName, []string{"127.0.0.1:7770", "127.0.0.1:7771"}, 1)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, cluster.Reset(ctx))
	defer func() {
		require.NoError(t, cluster.Reset(ctx))
	}()
	for _, shard := range cluster.Shards {
		for _, node := range shard.Nodes {
			require.NoError(t, node.SyncClusterInfo(ctx, cluster))
		}
	}
	handler.s.CreateCluster(ctx, ns, cluster)

	recorder := httptest.NewRecorder()
	reqCtx := GetTestContext(recorder)
	reqCtx.Set(consts.ContextKeyStore, handler.s)
	reqCtx.Params = []gin.Param{{Key: "namespace", Value: ns}, {Key: "cluster", Value: clusterName}}
	testMigrateReq := &MigrateSlotRequest{
		Slot:   0,
		Target: 1,
	}
	body, err := json.Marshal(testMigrateReq)
	require.NoError(t, err)
	reqCtx.Request.Body = io.NopCloser(bytes.NewBuffer(body))
	middleware.RequiredCluster(reqCtx)
	handler.MigrateSlot(reqCtx)
	require.Equal(t, http.StatusOK, recorder.Code)

	gotCluster, err := handler.s.GetCluster(ctx, ns, clusterName)
	require.NoError(t, err)
	require.EqualValues(t, 1, gotCluster.Version.Load())
	require.Len(t, gotCluster.Shards[0].SlotRanges, 1)
	require.EqualValues(t, 0, gotCluster.Shards[0].MigratingSlot)
	require.EqualValues(t, 1, gotCluster.Shards[0].TargetShardIndex)

	ctrl, err := controller.New(handler.s.(*store.ClusterStore), &config.ControllerConfig{
		FailOver: &config.FailOverConfig{
			PingIntervalSeconds: 1,
			MaxPingCount:        3,
		}})
	require.NoError(t, err)
	require.NoError(t, ctrl.Start(ctx))
	ctrl.WaitForReady()
	defer ctrl.Close()

	// Migration will be failed due to the source node cannot connect to the target node,
	// we just use it to confirm if the migration loop took effected.
	require.Eventually(t, func() bool {
		gotCluster, err := handler.s.GetCluster(ctx, ns, "test-cluster")
		if err != nil {
			return false
		}
		return gotCluster.Shards[0].MigratingSlot == -1
	}, 10*time.Second, 100*time.Millisecond)
}
