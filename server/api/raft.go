package api

import (
	"fmt"
	"strings"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/server/helper"
	"github.com/apache/kvrocks-controller/store/engine/raft"
	"go.uber.org/zap"

	"github.com/gin-gonic/gin"
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
	peers := raftNode.ListPeers()
	helper.ResponseOK(c, gin.H{"peers": peers})
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
	var err error
	if req.Operation == OperationAdd {
		err = raftNode.AddPeer(c, req.ID, req.Peer)
	} else {
		err = raftNode.RemovePeer(c, req.ID)
	}
	if err != nil {
		helper.ResponseError(c, err)
	} else {
		logger.Get().With(zap.Any("request", req)).Info("Update peer success")
		helper.ResponseOK(c, nil)
	}
}
