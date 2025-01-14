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

package command

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"

	"github.com/spf13/cobra"
)

const (
	raftCommandList   = "list"
	raftCommandAdd    = "add"
	raftCommandRemove = "remove"
)

var RaftCommand = &cobra.Command{
	Use:   "raft",
	Short: "Raft operations",
	Example: `
# Display all memberships in the cluster
kvctl raft list peers

# Add a node to the cluster
kvctl raft add peer <node_id> <node_address>

# Remove a node from the cluster
kvctl raft remove peer <node_id>
`,
	ValidArgs: []string{raftCommandList, raftCommandAdd, raftCommandRemove},
	RunE: func(cmd *cobra.Command, args []string) error {
		host, _ := cmd.Flags().GetString("host")
		client := newClient(host)
		switch strings.ToLower(args[0]) {
		case raftCommandList:
			if len(args) < 2 || args[1] != "peers" {
				return fmt.Errorf("unsupported openeration: '%s' in raft command", args[1])
			}
			return listRaftPeers(client)
		case raftCommandAdd, raftCommandRemove:
			if len(args) < 2 {
				return errors.New("missing 'peer' in raft command")
			}
			if args[1] != "peer" {
				return fmt.Errorf("unsupported openeration: '%s' in raft command", args[1])
			}
			if len(args) < 3 {
				return fmt.Errorf("missing node_id and node_address")
			}
			id, err := strconv.ParseUint(args[2], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid node_id: %s", args[1])
			}
			if args[0] == raftCommandAdd {
				if len(args) < 4 {
					return fmt.Errorf("missing node_address")
				}
				address := args[3]
				if _, err := url.Parse(address); err != nil {
					return fmt.Errorf("invalid node_address: %s", address)
				}
				return addRaftPeer(client, id, address)
			} else {
				return removeRaftPeer(client, id)
			}
		default:
			return fmt.Errorf("unsupported openeration: '%s' in raft command", args[0])
		}
	},
	SilenceUsage:  true,
	SilenceErrors: true,
}

func listRaftPeers(cli *client) error {
	rsp, err := cli.restyCli.R().Get("/raft/peers")
	if err != nil {
		return err
	}
	if rsp.IsError() {
		return unmarshalError(rsp.Body())
	}

	var result struct {
		Leader uint64            `json:"leader"`
		Peers  map[uint64]string `json:"peers"`
	}
	if err := unmarshalData(rsp.Body(), &result); err != nil {
		return err
	}
	writer := tablewriter.NewWriter(os.Stdout)
	printLine("")
	writer.SetHeader([]string{"NODE_ID", "NODE_ADDRESS", "IS_LEADER"})
	writer.SetCenterSeparator("|")
	for id, addr := range result.Peers {
		isLeader := "NO"
		if id == result.Leader {
			isLeader = "YES"
		}
		columns := []string{fmt.Sprintf("%d", id), addr, isLeader}
		writer.Append(columns)
	}
	writer.Render()
	return nil
}

func addRaftPeer(cli *client, id uint64, address string) error {
	var request struct {
		ID        uint64 `json:"id"`
		Peer      string `json:"peer"`
		Operation string `json:"operation"`
	}
	request.ID = id
	request.Peer = address
	request.Operation = "add"

	rsp, err := cli.restyCli.R().
		SetBody(&request).
		Post("/raft/peers")
	if err != nil {
		return err
	}
	if rsp.IsError() {
		return unmarshalError(rsp.Body())
	}

	printLine("Add node '%d' with address '%s' successfully", id, address)
	return nil
}

func removeRaftPeer(cli *client, id uint64) error {
	var request struct {
		ID        uint64 `json:"id"`
		Operation string `json:"operation"`
	}
	request.ID = id
	request.Operation = "remove"

	rsp, err := cli.restyCli.R().
		SetBody(&request).
		Post("/raft/peers")
	if err != nil {
		return err
	}
	if rsp.IsError() {
		return unmarshalError(rsp.Body())
	}

	printLine("Remove node '%d' successfully", id)
	return nil
}
