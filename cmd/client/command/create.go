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
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

type CreateOptions struct {
	namespace string
	cluster   string
	shard     int
	replica   int
	nodes     []string
	password  string
}

var createOptions CreateOptions

var CreateCommand = &cobra.Command{
	Use:   "create",
	Short: "Create a resource",
	Example: `
# Create a namespace
kvctl create namespace <namespace>

# Create a cluster in the namespace
kvctl create cluster <cluster> -n <namespace> --replica 1 --nodes 127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381

# Create a shard in the cluster
kvctl create shard -n <namespace> -c <cluster> --nodes 127.0.0.1:6379,127.0.0.1:6380

# Create nodes in the cluster
kvctl create node 127.0.0.1:6379 -n <namespace> -c <cluster> --shard <shard>
`,
	PreRunE: createPreRun,
	RunE: func(cmd *cobra.Command, args []string) error {
		host, _ := cmd.Flags().GetString("host")
		client := newClient(host)
		switch strings.ToLower(args[0]) {
		case ResourceNamespace:
			if len(args) < 2 {
				return errors.New("missing namespace name")
			}
			return createNamespace(client, args[1])
		case ResourceCluster:
			if len(args) < 2 {
				return errors.New("missing cluster name")
			}
			createOptions.cluster = args[1]
			return createCluster(client, &createOptions)
		case ResourceShard:
			return createShard(client, &createOptions)
		case ResourceNode:
			if len(args) < 2 {
				return errors.New("missing node address")
			}
			createOptions.nodes = []string{args[1]}
			return createNodes(client, &createOptions)
		default:
			return errors.New("unsupported resource type, please specify one of [namespace, cluster, shard, nodes]")
		}
	},
	SilenceUsage:  true,
	SilenceErrors: true,
}

func createPreRun(_ *cobra.Command, args []string) error {
	if len(args) == 0 {
		return errors.New("missing resource type, please specify one of [namespace, cluster, shard, node]")
	}
	resource := strings.ToLower(args[0])
	if resource == ResourceNamespace {
		return nil
	}
	if createOptions.namespace == "" {
		return errors.New("missing namespace, please specify the namespace via -n or --namespace option")
	}
	if resource != ResourceNode && createOptions.nodes == nil {
		return errors.New("missing nodes, please specify the nodes via --nodes option")
	}
	if resource == ResourceCluster {
		return nil
	}
	if createOptions.cluster == "" {
		return errors.New("missing cluster, please specify the cluster via -c or --cluster option")
	}
	if resource == ResourceShard {
		return nil
	}
	if createOptions.shard == -1 {
		return errors.New("missing shard, please specify the shard via -s or --shard option")
	}
	if createOptions.shard < 0 {
		return errors.New("shard must be a positive number")
	}
	return nil
}

func createNamespace(cli *client, name string) error {
	rsp, err := cli.restyCli.R().
		SetBody(map[string]string{"namespace": name}).
		Post("/namespaces")
	if err != nil {
		return err
	}

	if rsp.IsError() {
		return unmarshalError(rsp.Body())
	}
	printLine("create namespace: %s successfully.", name)
	return nil
}

func createCluster(cli *client, options *CreateOptions) error {
	rsp, err := cli.restyCli.R().
		SetPathParam("namespace", options.namespace).
		SetBody(map[string]interface{}{
			"name":     options.cluster,
			"replica":  options.replica,
			"nodes":    options.nodes,
			"password": options.password,
		}).
		Post("/namespaces/{namespace}/clusters")
	if err != nil {
		return err
	}

	if rsp.IsError() {
		return unmarshalError(rsp.Body())
	}
	printLine("create cluster: %s successfully.", options.cluster)
	return nil
}

func createShard(cli *client, options *CreateOptions) error {
	rsp, err := cli.restyCli.R().
		SetPathParam("namespace", options.namespace).
		SetPathParam("cluster", options.cluster).
		SetBody(map[string]interface{}{
			"name":     options.cluster,
			"nodes":    options.nodes,
			"password": options.password,
		}).
		Post("/namespaces/{namespace}/clusters/{cluster}/shards")
	if err != nil {
		return err
	}

	if rsp.IsError() {
		return unmarshalError(rsp.Body())
	}
	printLine("create the new shard successfully.")
	return nil
}

func createNodes(cli *client, options *CreateOptions) error {
	rsp, err := cli.restyCli.R().
		SetPathParam("namespace", options.namespace).
		SetPathParam("cluster", options.cluster).
		SetPathParam("shard", strconv.Itoa(options.shard)).
		SetBody(map[string]interface{}{
			"addr":     options.nodes[0],
			"password": options.password,
		}).
		Post("/namespaces/{namespace}/clusters/{cluster}/shards/{shard}/nodes")
	if err != nil {
		return err
	}

	if rsp.IsError() {
		return unmarshalError(rsp.Body())
	}
	printLine("create node: %v successfully.", options.nodes[0])
	return nil
}

func init() {
	CreateCommand.Flags().StringVarP(&createOptions.namespace, "namespace", "n", "", "The namespace")
	CreateCommand.Flags().StringVarP(&createOptions.cluster, "cluster", "c", "", "The cluster")
	CreateCommand.Flags().IntVarP(&createOptions.shard, "shard", "s", -1, "The shard number")
	CreateCommand.Flags().IntVarP(&createOptions.replica, "replica", "r", 1, "The replica number")
	CreateCommand.Flags().StringSliceVarP(&createOptions.nodes, "nodes", "", nil, "The node list")
	CreateCommand.Flags().StringVarP(&createOptions.password, "password", "", "", "The password")
}
