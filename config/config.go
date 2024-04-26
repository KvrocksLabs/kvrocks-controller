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
package config

import (
	"errors"
	"fmt"
	"net"
	"os"

	"github.com/apache/kvrocks-controller/storage/persistence/etcd"
	"github.com/apache/kvrocks-controller/storage/persistence/redis"
	"github.com/apache/kvrocks-controller/storage/persistence/zookeeper"
	"github.com/go-playground/validator/v10"
)

type AdminConfig struct {
	Addr string `yaml:"addr"`
}

type FailOverConfig struct {
	GCIntervalSeconds   int     `yaml:"gc_interval_seconds"`
	PingIntervalSeconds int     `yaml:"ping_interval_seconds"`
	MaxPingCount        int64   `yaml:"max_ping_count"`
	MinAliveSize        int     `yaml:"min_alive_size"`
	MaxFailureRatio     float64 `yaml:"max_failure_ratio"`
}

type ControllerConfig struct {
	FailOver *FailOverConfig `yaml:"failover"`
}

const defaultPort = 9379

type StorageConfig struct {
	StorageType string            `yaml:"type"`
	Etcd        *etcd.Config      `yaml:"etcd"`
	Zookeeper   *zookeeper.Config `yaml:"zookeeper"`
	Redis       *redis.Config     `yaml:"redis"`
}

type Config struct {
	Addr       string            `yaml:"addr"`
	Storage    *StorageConfig    `yaml:"storage"`
	Etcd       *etcd.Config      `yaml:"etcd"` // Deprecated, use Storage.Etcd.
	Admin      AdminConfig       `yaml:"admin"`
	Controller *ControllerConfig `yaml:"controller"`
}

func Default() *Config {
	c := &Config{
		Storage: &StorageConfig{
			StorageType: "etcd",
			Etcd: &etcd.Config{
				Addrs: []string{"127.0.0.1:2379"},
			},
		},
		Controller: &ControllerConfig{
			FailOver: DefaultFailOverConfig(),
		},
	}
	c.Addr = c.getAddr()
	return c
}

func (c *Config) Validate() error {
	if c.Controller.FailOver.MaxPingCount < 3 {
		return errors.New("max ping count required >= 3")
	}
	if c.Controller.FailOver.GCIntervalSeconds < 60 {
		return errors.New("gc interval required >= 1min")
	}
	if c.Controller.FailOver.PingIntervalSeconds < 1 {
		return errors.New("ping interval required >= 1s")
	}
	if c.Controller.FailOver.MinAliveSize < 2 {
		return errors.New("min alive size required >= 2")
	}
	return nil
}

func DefaultFailOverConfig() *FailOverConfig {
	return &FailOverConfig{
		GCIntervalSeconds:   3600,
		PingIntervalSeconds: 3,
		MaxPingCount:        5,
		MinAliveSize:        10,
		MaxFailureRatio:     0.6,
	}
}

func (c *Config) getAddr() string {
	// env has higher priority than configuration.
	// case: get addr from env
	checker := validator.New()
	host := os.Getenv("KVROCKS_CONTROLLER_HTTP_HOST")
	port := os.Getenv("KVROCKS_CONTROLLER_HTTP_PORT")
	addr := host + ":" + port
	err := checker.Var(addr, "required,tcp_addr")
	if err == nil {
		return fmt.Sprintf("%s:%s", host, port)
	}
	if c.Addr != "" {
		return c.Addr
	}

	// case: addr is empty
	ip := getLocalIP()
	if ip != "" {
		return fmt.Sprintf("%s:%d", ip, defaultPort)
	}
	return fmt.Sprintf("127.0.0.1:%d", defaultPort)
}

// getLocalIP returns the non loopback local IP of the host.
func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		ipnet, ok := address.(*net.IPNet)
		if ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
