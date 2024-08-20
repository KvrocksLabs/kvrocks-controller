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
	"strings"

	"github.com/spf13/cobra"
)

var listOptions struct {
	namespace string
	cluster   string
}

var ListCommand = &cobra.Command{
	Use:   "list",
	Short: "Display all resources",
	Example: `
# Display all namespaces 
kvctl list namespaces

# Display all clusters in the namespace
kvctl list clusters -n <namespace>

# Display all nodes in the cluster
kvctl list nodes -n <namespace> -c <cluster>
`,
	ValidArgs: []string{"namespaces", "clusters", "shards", "nodes"},
	RunE: func(cmd *cobra.Command, args []string) error {
		host, _ := cmd.Flags().GetString("host")
		client := newClient(host)
		switch strings.ToLower(args[0]) {
		case "namespaces":
			return listNamespace(client)
		case "clusters":
			return listClusters(client)
		default:
			return fmt.Errorf("unsupported resource type %s", args[0])
		}
	},
	PreRunE:       listPreRun,
	SilenceUsage:  true,
	SilenceErrors: true,
}

func listPreRun(_ *cobra.Command, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing resource type, please specify one of [namespaces, clusters, nodes]")
	}

	resource := strings.ToLower(args[0])
	if resource == "namespaces" {
		return nil
	}
	if listOptions.namespace == "" {
		return fmt.Errorf("missing namespace, please specify the namespace via -n or --namespace option")
	}
	if resource == "nodes" && listOptions.cluster == "" {
		return fmt.Errorf("missing cluster, please specify the cluster via -c or --cluster option")
	}
	return nil
}

func listNamespace(cli *client) error {
	rsp, err := cli.restyCli.R().Get("/namespaces")
	if err != nil {
		return err
	}
	if rsp.IsError() {
		return unmarshalError(rsp.Body())
	}

	var result struct {
		Namespaces []string `json:"namespaces"`
	}
	if err := unmarshalData(rsp.Body(), &result); err != nil {
		return err
	}
	if len(result.Namespaces) == 0 {
		printLine("no namespace found.")
		return nil
	}
	for _, ns := range result.Namespaces {
		printLine(ns)
	}
	return nil
}

func listClusters(cli *client) error {
	rsp, err := cli.restyCli.R().
		SetPathParam("namespace", listOptions.namespace).
		Get("/namespaces/{namespace}/clusters")
	if err != nil {
		return err
	}
	if rsp.IsError() {
		return unmarshalError(rsp.Body())
	}

	var result struct {
		Clusters []string `json:"clusters"`
	}
	if err := unmarshalData(rsp.Body(), &result); err != nil {
		return err
	}
	if len(result.Clusters) == 0 {
		printLine("no cluster found.")
		return nil
	}
	for _, cluster := range result.Clusters {
		printLine(cluster)
	}
	return nil
}

func init() {
	ListCommand.Flags().StringVarP(&listOptions.namespace, "namespace", "n", "", "The namespace")
	ListCommand.Flags().StringVarP(&listOptions.cluster, "cluster", "c", "", "The cluster")
}
