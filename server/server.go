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

package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"

	"github.com/apache/kvrocks-controller/store/engine/consul"
	"github.com/apache/kvrocks-controller/store/engine/raft"

	"github.com/gin-gonic/gin"

	"github.com/apache/kvrocks-controller/config"
	"github.com/apache/kvrocks-controller/controller"
	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/server/helper"
	"github.com/apache/kvrocks-controller/store"
	"github.com/apache/kvrocks-controller/store/engine"
	"github.com/apache/kvrocks-controller/store/engine/etcd"
	"github.com/apache/kvrocks-controller/store/engine/zookeeper"
)

type Server struct {
	engine     *gin.Engine
	store      *store.ClusterStore
	controller *controller.Controller
	config     *config.Config
	httpServer *http.Server
}

func NewServer(cfg *config.Config) (*Server, error) {
	var persist engine.Engine
	var err error

	sessionID := helper.GenerateSessionID(cfg.Addr)
	storageType := strings.ToLower(cfg.StorageType)
	switch storageType {
	case "etcd":
		logger.Get().Info("Use Etcd as store")
		persist, err = etcd.New(sessionID, cfg.Etcd)
	case "zookeeper":
		logger.Get().Info("Use Zookeeper as store")
		persist, err = zookeeper.New(sessionID, cfg.Zookeeper)
	case "raft":
		logger.Get().Info("Use Raft as store")
		persist, err = raft.New(cfg.Raft)
	case "consul":
		logger.Get().Info("Use Consul as store")
		persist, err = consul.New(sessionID, cfg.Consul)
	default:
		logger.Get().Info("Use Etcd as default store")
		persist, err = etcd.New(sessionID, cfg.Etcd)
	}

	if err != nil {
		return nil, err
	}
	if persist == nil {
		return nil, fmt.Errorf("no found any store config")
	}

	clusterStore := store.NewClusterStore(persist)
	ctrl, err := controller.New(clusterStore, cfg.Controller)
	if err != nil {
		return nil, err
	}
	gin.SetMode(gin.ReleaseMode)
	return &Server{
		store:      clusterStore,
		controller: ctrl,
		config:     cfg,
		engine:     gin.New(),
	}, nil
}

func (srv *Server) startAPIServer() {
	srv.initHandlers()
	httpServer := &http.Server{
		Addr:    srv.config.Addr,
		Handler: srv.engine,
	}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil {
			if errors.Is(err, http.ErrServerClosed) {
				return
			}
			panic(fmt.Errorf("API server: %w", err))
		}
	}()
	srv.httpServer = httpServer
}

func PProf(c *gin.Context) {
	switch c.Param("profile") {
	case "/cmdline":
		pprof.Cmdline(c.Writer, c.Request)
	case "/symbol":
		pprof.Symbol(c.Writer, c.Request)
	case "/profile":
		pprof.Profile(c.Writer, c.Request)
	case "/trace":
		pprof.Trace(c.Writer, c.Request)
	default:
		pprof.Index(c.Writer, c.Request)
	}
}

func (srv *Server) Start(ctx context.Context) error {
	if ok := srv.store.IsReady(ctx); !ok {
		return fmt.Errorf("the cluster store is not ready")
	}
	if err := srv.controller.Start(ctx); err != nil {
		return err
	}
	srv.controller.WaitForReady()
	srv.startAPIServer()
	return nil
}

func (srv *Server) Stop() error {
	srv.controller.Close()
	gracefulCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return srv.httpServer.Shutdown(gracefulCtx)
}
