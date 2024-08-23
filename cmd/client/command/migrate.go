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
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

type MigrationOptions struct {
	namespace string
	cluster   string
	slot      int
	target    int
	slotOnly  bool
}

var migrateOptions MigrationOptions

var MigrateCommand = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate slot to another node",
	Example: `
# Migrate slot between cluster shards 
kvctl migrate slot <slot> --target <target_shard_index> -n <namespace> -c <cluster>
`,
	PreRunE: migrationPreRun,
	RunE: func(cmd *cobra.Command, args []string) error {
		host, _ := cmd.Flags().GetString("host")
		user, _ := cmd.Flags().GetString("api-user")
		password, _ := cmd.Flags().GetString("api-password")
		client := newClient(host, user, password)
		resource := strings.ToLower(args[0])
		switch resource {
		case "slot":
			return migrateSlot(client, &migrateOptions)
		default:
			return fmt.Errorf("unsupported resource type: %s", resource)
		}
	},
	SilenceUsage:  true,
	SilenceErrors: true,
}

func migrationPreRun(_ *cobra.Command, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("resource type should be specified")
	}
	if len(args) < 2 {
		return fmt.Errorf("the slot number should be specified")
	}
	slot, err := strconv.Atoi(args[1])
	if err != nil {
		return fmt.Errorf("invalid slot number: %s", args[1])
	}
	if slot < 0 || slot > 16383 {
		return errors.New("slot number should be in range [0, 16383]")
	}
	migrateOptions.slot = slot

	if migrateOptions.namespace == "" {
		return fmt.Errorf("namespace is required, please specify with -n or --namespace")
	}
	if migrateOptions.cluster == "" {
		return fmt.Errorf("cluster is required, please specify with -c or --cluster")
	}
	if migrateOptions.target < 0 {
		return fmt.Errorf("target is required, please specify with --target")
	}
	return nil
}

func migrateSlot(client *client, options *MigrationOptions) error {
	rsp, err := client.restyCli.R().
		SetPathParam("namespace", options.namespace).
		SetPathParam("cluster", options.cluster).
		SetBody(map[string]interface{}{
			"slot":     options.slot,
			"target":   options.target,
			"slotOnly": strconv.FormatBool(options.slotOnly),
		}).
		Post("/namespaces/{namespace}/clusters/{cluster}/migrate")
	if err != nil {
		return err
	}
	if rsp.IsError() {
		return errors.New(rsp.String())
	}
	printLine("migrate slot[%d] task is submitted successfully.", options.slot)
	return nil
}

func init() {
	MigrateCommand.Flags().IntVar(&migrateOptions.slot, "slot", -1, "The slot to migrate")
	MigrateCommand.Flags().IntVar(&migrateOptions.target, "target", -1, "The target node")
	MigrateCommand.Flags().StringVarP(&migrateOptions.namespace, "namespace", "n", "", "The namespace")
	MigrateCommand.Flags().StringVarP(&migrateOptions.cluster, "cluster", "c", "", "The cluster")
	MigrateCommand.Flags().BoolVar(&migrateOptions.slotOnly, "slot-only", false, "Only migrate slot and ignore the existing data")
}
