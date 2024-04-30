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
	"strings"

	"github.com/spf13/cobra"
)

type DeleteOptions struct {
	namespace string
	cluster   string
	shard     int
}

var deleteOptions DeleteOptions

var DeleteCommand = &cobra.Command{
	Use:   "delete",
	Short: "Delete a resource",
	Example: `
# Delete a namespace
kvctl delete namespace <namespace>

# Delete a cluster in the namespace
kvctl delete cluster <cluster> -n <namespace>

# Delete a shard in the cluster
kvctl delete shard <shard> -n <namespace> -c <cluster>

# Delete a node in the cluster
kvctl delete node <node_id> -n <namespace> -c <cluster> --shard <shard>
`,
	PreRunE: deletePreRun,
	RunE: func(cmd *cobra.Command, args []string) error {
		resource := strings.ToLower(args[0])
		if len(args) < 2 {
			return fmt.Errorf("missing resource name")
		}
		host, _ := cmd.Flags().GetString("host")
		client := newClient(host)
		switch resource {
		case ResourceNamespace:
			namespace := args[1]
			return deleteNamespace(client, namespace)
		case ResourceCluster:
			deleteOptions.cluster = args[1]
			return deleteCluster(client, &deleteOptions)
		case ResourceShard:
			shard, err := strconv.Atoi(args[1])
			if err != nil {
				return err
			}
			deleteOptions.shard = shard
			return deleteShard(client, &deleteOptions)
		case ResourceNode:
			nodeID := args[1]
			return deleteNode(client, &deleteOptions, nodeID)
		default:
			return fmt.Errorf("unsupported resource type %s", resource)
		}
	},
	SilenceUsage:  true,
	SilenceErrors: true,
}

func deletePreRun(_ *cobra.Command, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing resource type")
	}

	resource := strings.ToLower(args[0])
	if resource == "namespace" {
		return nil
	}
	if deleteOptions.namespace == "" {
		return fmt.Errorf("missing namespace, please specify the namespace via -n or --namespace option")
	}
	if resource == "cluster" {
		return nil
	}
	if deleteOptions.cluster == "" {
		return fmt.Errorf("missing cluster, please specify the cluster via -c or --cluster option")
	}
	if resource == "shard" {
		return nil
	}
	if deleteOptions.shard == -1 {
		return fmt.Errorf("missing shard, please specify the shard via -s or --shard option")
	}
	if deleteOptions.shard < 0 {
		return fmt.Errorf("invalid shard %d", deleteOptions.shard)
	}
	return nil
}

func deleteNamespace(client *client, namespace string) error {
	rsp, err := client.restyCli.R().
		SetPathParam("namespace", namespace).
		Delete("/namespaces/{namespace}")
	if err != nil {
		return err
	}
	if rsp.IsError() {
		return unmarshalError(rsp.Body())
	}
	printLine("delete namespace: %s successfully.", namespace)
	return nil
}

func deleteCluster(client *client, options *DeleteOptions) error {
	rsp, err := client.restyCli.R().
		SetPathParams(map[string]string{
			"namespace": options.namespace,
			"cluster":   options.cluster,
		}).Delete("/namespaces/{namespace}/clusters/{cluster}")
	if err != nil {
		return err
	}
	if rsp.IsError() {
		return unmarshalError(rsp.Body())
	}
	printLine("delete cluster: %s successfully.", options.cluster)
	return nil
}

func deleteShard(client *client, options *DeleteOptions) error {
	rsp, err := client.restyCli.R().
		SetPathParam("namespace", options.namespace).
		SetPathParam("cluster", options.cluster).
		SetPathParam("shard", strconv.Itoa(options.shard)).
		Delete("/namespaces/{namespace}/clusters/{cluster}/shards/{shard}")
	if err != nil {
		return err
	}
	if rsp.IsError() {
		return unmarshalError(rsp.Body())
	}
	printLine("delete shard %d successfully.", options.shard)
	return nil
}

func deleteNode(client *client, options *DeleteOptions, nodeID string) error {
	rsp, err := client.restyCli.R().
		SetPathParam("namespace", options.namespace).
		SetPathParam("cluster", options.cluster).
		SetPathParam("shard", strconv.Itoa(options.shard)).
		SetPathParam("node", nodeID).
		Delete("/namespaces/{namespace}/clusters/{cluster}/shards/{shard}/nodes/{node}")
	if err != nil {
		return err
	}
	if rsp.IsError() {
		return unmarshalError(rsp.Body())
	}
	printLine("delete node: %s successfully.", nodeID)
	return nil
}

func init() {
	DeleteCommand.Flags().StringVarP(&deleteOptions.namespace, "namespace", "n", "", "The namespace")
	DeleteCommand.Flags().StringVarP(&deleteOptions.cluster, "cluster", "c", "", "The cluster")
	DeleteCommand.Flags().IntVarP(&deleteOptions.shard, "shard", "s", -1, "The shard")
}
