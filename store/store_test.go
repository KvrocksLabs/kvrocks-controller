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
package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/store/engine"
)

func TestClusterStore(t *testing.T) {
	ctx := context.Background()
	store := NewClusterStore(engine.NewMock())

	t.Run("create/get/list/delete namespace", func(t *testing.T) {
		namespaces := []string{"ns0", "ns1", "ns2"}
		for _, ns := range namespaces {
			err := store.CreateNamespace(ctx, ns)
			require.NoError(t, err)
			require.ErrorIs(t, store.CreateNamespace(ctx, ns), consts.ErrAlreadyExists)
			exists, err := store.ExistsNamespace(ctx, ns)
			require.NoError(t, err)
			require.True(t, exists)
		}

		exists, err := store.ExistsNamespace(ctx, "not-exits-ns")
		require.NoError(t, err)
		require.False(t, exists)

		gotNamespaces, err := store.ListNamespace(ctx)
		require.NoError(t, err)
		require.ElementsMatch(t, namespaces, gotNamespaces)

		for _, ns := range namespaces {
			err := store.RemoveNamespace(ctx, ns)
			require.NoError(t, err)
			exists, err := store.ExistsNamespace(ctx, ns)
			require.NoError(t, err)
			require.False(t, exists)
		}
	})

	t.Run("create/get/list/delete cluster", func(t *testing.T) {
		ns := "ns0"
		cluster0 := &Cluster{Name: "cluster0", Shards: Shards{NewShard()}}
		cluster1 := &Cluster{Name: "cluster1", Shards: Shards{NewShard()}}
		cluster0.Version.Store(2)
		cluster1.Version.Store(3)

		require.NoError(t, store.CreateCluster(ctx, ns, cluster0))
		require.ErrorIs(t, store.CreateCluster(ctx, ns, cluster0), consts.ErrAlreadyExists)
		require.NoError(t, store.CreateCluster(ctx, ns, cluster1))

		gotCluster, err := store.GetCluster(ctx, ns, "cluster0")
		require.NoError(t, err)
		require.Equal(t, cluster0.Name, gotCluster.Name)
		require.Equal(t, cluster0.Version, gotCluster.Version)

		gotClusters, err := store.ListCluster(ctx, ns)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"cluster0", "cluster1"}, gotClusters)

		require.NoError(t, store.UpdateCluster(ctx, ns, cluster0))
		gotCluster, err = store.GetCluster(ctx, ns, "cluster0")
		require.NoError(t, err)
		require.Equal(t, cluster0.Name, gotCluster.Name)
		require.EqualValues(t, 3, gotCluster.Version.Load())

		for _, name := range []string{"cluster0", "cluster1"} {
			require.NoError(t, store.RemoveCluster(ctx, ns, name))
			_, err = store.GetCluster(ctx, ns, name)
			require.ErrorIs(t, err, consts.ErrNotFound)
		}
	})

	t.Run("check nodes", func(t *testing.T) {
		testCluster, err := NewCluster("test-cluster-another",
			[]string{"127.0.0.1:1111", "127.0.0.1:2222", "127.0.0.1:3333"}, 1)
		require.NoError(t, err)

		require.NoError(t, store.CreateCluster(ctx, "test-ns", testCluster))
		require.NoError(t, store.CheckNewNodes(ctx, []string{"127.0.0.1:4444", "127.0.0.1:5555"}))
		require.NotNil(t, store.CheckNewNodes(ctx, []string{"127.0.0.1:3333", "127.0.0.1:4444"}))
		require.NotNil(t, store.CheckNewNodes(ctx, []string{"127.0.0.1:2222", "127.0.0.1:3333"}))
	})
}
