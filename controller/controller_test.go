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

package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks-controller/config"
	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/store"
	"github.com/apache/kvrocks-controller/store/engine"
)

func TestController_Basics(t *testing.T) {
	ctx := context.Background()
	ns := "test-ns"
	cluster0, err := store.NewCluster("test-cluster-0", []string{"127.0.0.1:7770"}, 1)
	require.NoError(t, err)
	cluster1, err := store.NewCluster("test-cluster-1", []string{"127.0.0.1:7771"}, 1)
	require.NoError(t, err)

	s := store.NewClusterStore(engine.NewMock())
	require.True(t, s.IsLeader())
	require.NoError(t, s.CreateCluster(ctx, ns, cluster0))
	require.NoError(t, s.CreateCluster(ctx, ns, cluster1))

	c, err := New(s, &config.ControllerConfig{
		FailOver: &config.FailOverConfig{
			PingIntervalSeconds: 1,
		},
	})
	require.NoError(t, err)
	require.NoError(t, c.Start(ctx))
	defer func() {
		c.Close()
	}()

	c.WaitReady()

	t.Run("get cluster", func(t *testing.T) {
		cluster, err := c.getCluster(ns, "test-cluster-0")
		require.NoError(t, err)
		require.NotNil(t, cluster)
		cluster, err = c.getCluster(ns, "test-cluster-1")
		require.NoError(t, err)
		require.NotNil(t, cluster)
		_, err = c.getCluster(ns, "test-cluster-2")
		require.ErrorIs(t, err, consts.ErrNotFound)
		require.NotNil(t, cluster)
	})

	t.Run("remove cluster", func(t *testing.T) {
		c.removeCluster(ns, "test-cluster-1")
		_, err = c.getCluster(ns, "test-cluster-1")
		require.ErrorIs(t, err, consts.ErrNotFound)
	})
}
