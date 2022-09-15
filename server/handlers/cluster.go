package handlers

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/KvrocksLabs/kvrocks_controller/consts"
	"github.com/KvrocksLabs/kvrocks_controller/failover"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/migrate"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/KvrocksLabs/kvrocks_controller/util"
)

func (req *CreateClusterParam) validate() error {
	if len(req.Cluster) == 0 {
		return fmt.Errorf("cluster name should NOT be empty")
	}
	for i, shard := range req.Shards {
		if err := shard.validate(); err != nil {
			return fmt.Errorf("validate shard[%d] err: %w", i, err)
		}
	}
	return nil
}

func ListCluster(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	namespace := c.Param("namespace")
	clusters, err := stor.ListCluster(namespace)
	if err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			util.ResponseErrorWithCode(c, http.StatusNotFound, err.Error())
		} else {
			util.ResponseError(c, err.Error())
		}
		return
	}
	util.ResponseOK(c, clusters)
}

func GetCluster(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	namespace := c.Param("namespace")
	clusterName := c.Param("cluster")
	cluster, err := stor.GetClusterCopy(namespace, clusterName)
	if err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			util.ResponseErrorWithCode(c, http.StatusNotFound, err.Error())
		} else {
			util.ResponseError(c, err.Error())
		}
		return
	}
	util.ResponseOK(c, cluster)
}

func CreateCluster(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	namespace := c.Param("namespace")

	var req CreateClusterParam
	if err := c.BindJSON(&req); err != nil {
		util.ResponseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := req.validate(); err != nil {
		util.ResponseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	shards := make([]metadata.Shard, len(req.Shards))
	slotRanges := metadata.SpiltSlotRange(len(req.Shards))
	for i, createShard := range req.Shards {
		shard, err := createShard.toShard()
		if err != nil {
			util.ResponseErrorWithCode(c, http.StatusBadRequest, err.Error())
			return
		}
		shard.SlotRanges = append(shard.SlotRanges, slotRanges[i])
		shards[i] = *shard
	}

	if err := stor.CreateCluster(namespace, req.Cluster, &metadata.Cluster{Shards: shards}); err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeExisted {
			util.ResponseErrorWithCode(c, http.StatusConflict, err.Error())
		} else {
			util.ResponseError(c, err.Error())
		}
		return
	}
	util.ResponseCreated(c, "OK")
}

func RemoveCluster(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	namespace := c.Param("namespace")
	cluster := c.Param("cluster")
	if err := stor.RemoveCluster(namespace, cluster); err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			util.ResponseErrorWithCode(c, http.StatusNotFound, err.Error())
		} else {
			util.ResponseError(c, err.Error())
		}
		return
	}
	util.ResponseOK(c, "OK")
}

func GetFailoverTasks(c *gin.Context) {
	namespace := c.Param("namespace")
	cluster := c.Param("cluster")
	qtype := c.Param("querytype")
	failover, _ := c.MustGet(consts.ContextKeyFailover).(*failover.FailOver)
	tasks, err := failover.GetTasks(namespace, cluster, qtype)
	if err != nil {
		util.ResponseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	util.ResponseOK(c, tasks)
}

func GetMigrateTasks(c *gin.Context) {
	namespace := c.Param("namespace")
	cluster := c.Param("cluster")
	qtype := c.Param("querytype")

	migr := c.MustGet(consts.ContextKeyMigrate).(*migrate.Migrate)
	tasks, err := migr.GetMigrateTasks(namespace, cluster, qtype)
	if err != nil {
		util.ResponseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	util.ResponseOK(c, tasks)
}
