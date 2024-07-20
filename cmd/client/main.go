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

package main

import (
	"os"

	"github.com/fatih/color"

	"github.com/spf13/cobra"

	"github.com/apache/kvrocks-controller/cmd/client/command"
)

var rootCommand = &cobra.Command{
	Use:   "kvctl",
	Short: "kvctl is a command line tool for the Kvrocks controller service",
	Run: func(cmd *cobra.Command, args []string) {
		_, _ = color.New(color.Bold).Println("Run 'kvctl --help' for usage.")
		os.Exit(0)
	},
}

func main() {
	if err := rootCommand.Execute(); err != nil {
		color.Red("error: %v", err)
		os.Exit(1)
	}
}

func init() {
	rootCommand.PersistentFlags().StringP("host", "H",
		"http://127.0.0.1:9379", "The host of the Kvrocks controller service")

	rootCommand.AddCommand(command.ListCommand)
	rootCommand.AddCommand(command.CreateCommand)
	rootCommand.AddCommand(command.GetCommand)
	rootCommand.AddCommand(command.DeleteCommand)
	rootCommand.AddCommand(command.ImportCommand)
	rootCommand.AddCommand(command.MigrateCommand)
	rootCommand.AddCommand(command.FailoverCommand)

	rootCommand.SilenceUsage = true
	rootCommand.SilenceErrors = true
}
