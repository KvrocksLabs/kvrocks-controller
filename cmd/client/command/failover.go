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
	"fmt"
	"strconv"

	"github.com/spf13/cobra"
)

type FailoverOptions struct {
	namespace string
	cluster   string
	preferred string
}

var failoverOptions FailoverOptions

var FailoverCommand = &cobra.Command{
	Use:   "failover",
	Short: "Failover the master of a shard",
	Example: `
# Failover the master of a shard
kvctl failover shard <shard_index> -n <namespace> -c <cluster>

# Failover the master of a shard with preferred slave
kvctl failover shard <shard_index> --preferred <node_id> -n <namespace> -c <cluster>
`,
	PreRunE: failoverPreRun,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("missing shard index, plese specify the shard index")
		}
		host, _ := cmd.Flags().GetString("host")
		client := newClient(host)
		resource := args[0]
		switch resource {
		case "shard":
			shardIndex, err := strconv.Atoi(args[1])
			if err != nil {
				return fmt.Errorf("invalid shard index: %s", args[1])
			}
			return failoverShard(client, &failoverOptions, shardIndex)
		default:
			return fmt.Errorf("unsupported resource type: %s", resource)
		}
	},
}

func failoverPreRun(cmd *cobra.Command, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("missing resource name, must be [shard]")
	}
	if failoverOptions.namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	if failoverOptions.cluster == "" {
		return fmt.Errorf("cluster is required")
	}
	return nil
}

func failoverShard(client *client, options *FailoverOptions, shardIndex int) error {
	rsp, err := client.restyCli.R().
		SetPathParam("namespace", options.namespace).
		SetPathParam("cluster", options.cluster).
		SetPathParam("shard", strconv.Itoa(shardIndex)).
		Post("/namespaces/{namespace}/clusters/{cluster}/shards/{shard}/failover")
	if err != nil {
		return err
	}
	if rsp.IsError() {
		return unmarshalError(rsp.Body())
	}
	var result struct {
		NewMasterID string `json:"new_master_id"`
	}
	if err := unmarshalData(rsp.Body(), &result); err != nil {
		return err
	}
	printLine("failover shard %d successfully, new master id: %s.", shardIndex, result.NewMasterID)
	return nil
}

func init() {
	FailoverCommand.Flags().StringVarP(&failoverOptions.namespace, "namespace", "n", "", "The namespace of the cluster")
	FailoverCommand.Flags().StringVarP(&failoverOptions.cluster, "cluster", "c", "", "The name of the cluster")
	FailoverCommand.Flags().StringVarP(&failoverOptions.preferred, "preferred", "", "", "The preferred slave node id")
}
