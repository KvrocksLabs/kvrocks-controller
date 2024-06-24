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
	"strings"

	"github.com/spf13/cobra"
)

type ImportOptions struct {
	namespace string
	cluster   string
	nodes     []string
	password  string
}

var importOptions ImportOptions

var ImportCommand = &cobra.Command{
	Use:   "import",
	Short: "Import data from a cluster",
	Example: `
# Import a cluster from nodes
kvctl import cluster <cluster> --nodes 127.0.0.1:6379,127.0.0.1:6380
`,
	PreRunE: importPreRun,
	RunE: func(cmd *cobra.Command, args []string) error {
		host, _ := cmd.Flags().GetString("host")
		client := newClient(host)
		resource := strings.ToLower(args[0])
		switch resource {
		case ResourceCluster:
			importOptions.cluster = args[1]
			return importCluster(client, &importOptions)
		default:
			return fmt.Errorf("unsupported resource type: %s", resource)
		}
	},
	SilenceUsage:  true,
	SilenceErrors: true,
}

func importPreRun(_ *cobra.Command, args []string) error {
	if len(args) < 2 {
		return errors.New("missing resource name")
	}
	if importOptions.namespace == "" {
		return errors.New("missing namespace, please specify with -n or --namespace")
	}
	if len(importOptions.nodes) == 0 {
		return errors.New("missing nodes")
	}
	return nil
}

func importCluster(client *client, options *ImportOptions) error {
	rsp, err := client.restyCli.R().
		SetPathParam("namespace", options.namespace).
		SetPathParam("cluster", options.cluster).
		SetBody(map[string]interface{}{
			"nodes":    options.nodes,
			"password": options.password,
		}).
		Post("/namespaces/{namespace}/clusters/{cluster}/import")
	if err != nil {
		return err
	}
	if rsp.IsError() {
		return errors.New(rsp.String())
	}
	printLine("import cluster: %s successfully.", options.cluster)
	return nil
}

func init() {
	ImportCommand.Flags().StringVarP(&importOptions.namespace, "namespace", "n", "", "The namespace of the cluster")
	ImportCommand.Flags().StringVarP(&importOptions.cluster, "cluster", "c", "", "The cluster name")
	ImportCommand.Flags().StringSliceVarP(&importOptions.nodes, "nodes", "", nil, "The nodes to import from")
	ImportCommand.Flags().StringVarP(&importOptions.password, "password", "p", "", "The password of the cluster")
}
