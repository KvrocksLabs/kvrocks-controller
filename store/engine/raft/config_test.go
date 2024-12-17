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

package raft

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	c := &Config{}
	c.init()

	// missing ID
	require.ErrorContains(t, c.validate(), "ID cannot be 0")
	// missing peers
	c.ID = 1
	require.ErrorContains(t, c.validate(), "peers cannot be empty")
	// valid
	c.Peers = []string{"http://127.0.0.1:12345"}
	require.NoError(t, c.validate())
	// ID greater than the number of peers
	c.ID = 2
	require.ErrorContains(t, c.validate(), "ID cannot be greater than the number of peers")

	c.ID = 1
	c.ClusterState = "invalid"
	require.ErrorContains(t, c.validate(), "cluster state must be one of [new, existing]")
	c.ClusterState = ClusterStateNew
	require.NoError(t, c.validate())
}

func TestConfig_Init(t *testing.T) {
	c := &Config{}
	c.init()
	require.Equal(t, ".", c.DataDir)
	require.Equal(t, 2, c.HeartbeatSeconds)
	require.Equal(t, 20, c.ElectionSeconds)

	c.DataDir = "/tmp"
	c.HeartbeatSeconds = 3
	c.ElectionSeconds = 30
	c.init()
	require.Equal(t, "/tmp", c.DataDir)
	require.Equal(t, 3, c.HeartbeatSeconds)
	require.Equal(t, 30, c.ElectionSeconds)
}
