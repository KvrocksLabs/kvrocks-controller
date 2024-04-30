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

	"github.com/apache/kvrocks-controller/store"
)

type GetOptions struct {
	namespace string
	cluster   string
	shard     int
}

var getOptions GetOptions

var GetCommand = &cobra.Command{
	Use:   "get",
	Short: "Get a resource",
	Example: `
# Get a cluster 
kvctl get cluster <cluster> -n <namespace>
`,
	PreRunE: getPreRun,
	RunE: func(cmd *cobra.Command, args []string) error {
		host, _ := cmd.Flags().GetString("host")
		client := newClient(host)
		if len(args) < 2 {
			return fmt.Errorf("missing resource name, must be one of [cluster, shard]")
		}
		resource := strings.ToLower(args[0])
		switch resource {
		case "cluster":
			getOptions.cluster = args[1]
			return getCluster(client, &getOptions)
		default:
			return fmt.Errorf("unsupported resource type %s", resource)
		}
	},
	SilenceUsage:  true,
	SilenceErrors: true,
}

func getPreRun(_ *cobra.Command, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing resource type")
	}

	if getOptions.namespace == "" {
		return fmt.Errorf("missing namespace, please specify the namespace via -n or --namespace option")
	}
	return nil
}

func getCluster(client *client, options *GetOptions) error {
	rsp, err := client.restyCli.R().SetPathParams(map[string]string{
		"namespace": options.namespace,
		"cluster":   options.cluster,
	}).Get("/namespaces/{namespace}/clusters/{cluster}")
	if err != nil {
		return err
	}
	if rsp.IsError() {
		return unmarshalError(rsp.Body())
	}

	var result struct {
		Cluster *store.Cluster `json:"cluster"`
	}
	if err := unmarshalData(rsp.Body(), &result); err != nil {
		return err
	}
	printCluster(result.Cluster)
	return nil
}

func init() {
	GetCommand.Flags().StringVarP(&getOptions.namespace, "namespace", "n", "", "The namespace of the resource")
	GetCommand.Flags().StringVarP(&getOptions.cluster, "cluster", "c", "", "The cluster of the resource")
}
