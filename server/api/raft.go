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

package api

import (
	"errors"
	"fmt"
	"strings"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/server/helper"
	"github.com/apache/kvrocks-controller/store/engine/raft"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

const (
	OperationAdd    = "add"
	OperationRemove = "remove"
)

type RaftHandler struct{}

type MemberRequest struct {
	ID        uint64 `json:"id" validate:"required,gt=0"`
	Operation string `json:"operation" validate:"required"`
	Peer      string `json:"peer"`
}

func (r *MemberRequest) validate() error {
	r.Operation = strings.ToLower(r.Operation)
	if r.Operation != OperationAdd && r.Operation != OperationRemove {
		return fmt.Errorf("operation must be one of [%s]",
			strings.Join([]string{OperationAdd, OperationRemove}, ","))
	}
	if r.Operation == OperationAdd && r.Peer == "" {
		return fmt.Errorf("peer should NOT be empty")
	}
	return nil
}

func (handler *RaftHandler) ListPeers(c *gin.Context) {
	raftNode, _ := c.MustGet(consts.ContextKeyRaftNode).(*raft.Node)
	helper.ResponseOK(c, gin.H{
		"leader": raftNode.GetRaftLead(),
		"peers":  raftNode.ListPeers(),
	})
}

func (handler *RaftHandler) UpdatePeer(c *gin.Context) {
	var req MemberRequest
	if err := c.BindJSON(&req); err != nil {
		helper.ResponseBadRequest(c, err)
		return
	}
	if err := req.validate(); err != nil {
		helper.ResponseBadRequest(c, err)
		return
	}

	raftNode, _ := c.MustGet(consts.ContextKeyRaftNode).(*raft.Node)
	peers := raftNode.ListPeers()

	var err error
	if req.Operation == OperationAdd {
		for _, peer := range peers {
			if peer == req.Peer {
				helper.ResponseError(c, fmt.Errorf("peer '%s' already exists", req.Peer))
				return
			}
		}
		err = raftNode.AddPeer(c, req.ID, req.Peer)
	} else {
		if _, ok := peers[req.ID]; !ok {
			helper.ResponseBadRequest(c, errors.New("peer not exists"))
			return
		}
		if len(peers) == 1 {
			helper.ResponseBadRequest(c, errors.New("can't remove the last peer"))
			return
		}
		err = raftNode.RemovePeer(c, req.ID)
	}
	if err != nil {
		helper.ResponseError(c, err)
	} else {
		logger.Get().With(zap.Any("request", req)).Info("Update peer success")
		helper.ResponseOK(c, nil)
	}
}
