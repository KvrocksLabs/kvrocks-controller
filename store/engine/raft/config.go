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

import "errors"

type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64 `yaml:"id"`
	// DataDir is the directory to store the raft data which includes snapshot and WALs.
	DataDir string `yaml:"data_dir"`
	// Join should be set to true if the node is joining an existing cluster.
	Join bool `yaml:"join"`
	// Peers is the list of raft peers.
	Peers []string `yaml:"peers"`
	// HeartbeatSeconds is the interval to send heartbeat message. Default is 2 seconds.
	HeartbeatSeconds int `yaml:"heartbeat_seconds"`
	// ElectionSeconds is the interval to start an election. Default is 10 * HeartBeat.
	ElectionSeconds int `yaml:"election_seconds"`
}

func (c *Config) validate() error {
	if c.ID == 0 {
		return errors.New("ID cannot be 0")
	}
	if len(c.Peers) == 0 {
		return errors.New("peers cannot be empty")
	}
	if c.ID > uint64(len(c.Peers)) {
		return errors.New("ID cannot be greater than the number of peers")
	}
	return nil
}

func (c *Config) init() {
	if c.DataDir == "" {
		c.DataDir = "."
	}
	if c.HeartbeatSeconds == 0 {
		c.HeartbeatSeconds = 2
	}
	if c.ElectionSeconds == 0 {
		c.ElectionSeconds = c.HeartbeatSeconds * 10
	}
}
