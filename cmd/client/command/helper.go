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
	"os"
	"strings"

	"github.com/fatih/color"

	"github.com/olekukonko/tablewriter"

	"github.com/apache/kvrocks-controller/store"
)

func printLine(format string, a ...interface{}) {
	boldColor := color.New(color.Bold)
	_, _ = fmt.Fprintln(os.Stdout, boldColor.Sprintf(format, a...))
}

func printCluster(cluster *store.Cluster) {
	writer := tablewriter.NewWriter(os.Stdout)
	printLine("")
	printLine("cluster: %s", cluster.Name)
	printLine("version: %d\n", cluster.Version.Load())
	writer.SetHeader([]string{"SHARD", "NODE_ID", "ADDRESS", "ROLE", "MIGRATING"})
	writer.SetCenterSeparator("|")
	for i, shard := range cluster.Shards {
		for _, node := range shard.Nodes {
			role := strings.ToUpper(store.RoleSlave)
			if node.IsMaster() {
				role = strings.ToUpper(store.RoleMaster)
			}
			migratingStatus := "NO"
			if shard.MigratingSlot != -1 {
				migratingStatus = fmt.Sprintf("%d --> %d", shard.MigratingSlot, shard.TargetShardIndex)
			}
			columns := []string{fmt.Sprintf("%d", i), node.ID(), node.Addr(), role, migratingStatus}
			writer.Append(columns)
		}
	}
	writer.Render()
}
