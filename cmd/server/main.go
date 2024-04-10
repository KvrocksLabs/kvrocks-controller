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
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/apache/kvrocks-controller/config"
	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/server"
	"github.com/apache/kvrocks-controller/version"

	"go.uber.org/zap"
	"gopkg.in/yaml.v1"
)

var configPath string

func init() {
	flag.StringVar(&configPath, "c", "config/config.yaml", "set config yaml file path")
}

func registerSignal(shutdown chan struct{}) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, []os.Signal{syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1}...)
	go func() {
		for sig := range c {
			if handleSignals(sig) {
				close(shutdown)
				return
			}
		}
	}()
}

func handleSignals(sig os.Signal) (exitNow bool) {
	switch sig {
	case syscall.SIGINT, syscall.SIGTERM:
		logger.Get().With(zap.String("signal", sig.String())).Info("Got signal to exit")
		return true
	default:
		return false
	}
}

func main() {
	// os signal handler
	shutdownCh := make(chan struct{})
	registerSignal(shutdownCh)

	flag.Parse()

	logger.Get().Info("Kvrocks controller is running with version: " + version.Version)
	cfg := config.Default()
	if len(configPath) != 0 {
		content, err := os.ReadFile(configPath)
		if err != nil {
			logger.Get().With(zap.Error(err)).Error("Failed to read the config file")
			return
		}
		if err := yaml.Unmarshal(content, cfg); err != nil {
			logger.Get().With(zap.Error(err)).Error("Failed to unmarshal the config file")
			return
		}
	}
	if err := cfg.Validate(); err != nil {
		logger.Get().With(zap.Error(err)).Error("Failed to validate the config file")
		return
	}
	hostPort := strings.Split(cfg.Addr, ":")
	if hostPort[0] == "0.0.0.0" || hostPort[0] == "127.0.0.1" {
		logger.Get().Warn("Leader forward may not work if the host is " + hostPort[0])
	}
	srv, err := server.NewServer(cfg)
	if err != nil {
		logger.Get().With(zap.Error(err)).Error("Failed to create the server")
		return
	}
	if err := srv.Start(); err != nil {
		logger.Get().With(zap.Error(err)).Error("Failed to start the server")
		return
	}

	// wait for the term signal
	<-shutdownCh
	if err := srv.Stop(); err != nil {
		logger.Get().With(zap.Error(err)).Error("Failed to close the server")
	} else {
		logger.Get().Info("Bye bye, Kvrocks controller was exited")
	}
}
