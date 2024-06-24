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

import "context"

// ClusterMockNode is a mock implementation of the Node interface,
// it is used for testing purposes.
type ClusterMockNode struct {
	*ClusterNode

	Sequence uint64
}

var _ Node = (*ClusterMockNode)(nil)

func NewClusterMockNode() *ClusterMockNode {
	return &ClusterMockNode{
		ClusterNode: NewClusterNode("", ""),
	}
}

func (mock *ClusterMockNode) GetClusterNodeInfo(ctx context.Context) (*ClusterNodeInfo, error) {
	return &ClusterNodeInfo{Sequence: mock.Sequence}, nil
}

func (mock *ClusterMockNode) GetClusterInfo(ctx context.Context) (*ClusterInfo, error) {
	return &ClusterInfo{}, nil
}

func (mock *ClusterMockNode) SyncClusterInfo(ctx context.Context, cluster *Cluster) error {
	return nil
}

func (mock *ClusterMockNode) Reset(ctx context.Context) error {
	return nil
}
