package server

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/apache/kvrocks-controller/metadata"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/metrics"
	"github.com/apache/kvrocks-controller/storage"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
)

func CollectMetrics(c *gin.Context) {
	startTime := time.Now()
	c.Next()
	latency := time.Since(startTime).Milliseconds()

	uri := c.FullPath()
	// uri was empty means not found routes, so rewrite it to /not_found here
	if c.Writer.Status() == http.StatusNotFound && uri == "" {
		uri = "/not_found"
	}
	labels := prometheus.Labels{
		"host":   c.Request.Host,
		"uri":    uri,
		"method": c.Request.Method,
		"code":   strconv.Itoa(c.Writer.Status()),
	}
	metrics.Get().HTTPCodes.With(labels).Inc()
	metrics.Get().Latencies.With(labels).Observe(float64(latency))
	size := c.Writer.Size()
	if size > 0 {
		metrics.Get().Payload.With(labels).Add(float64(size))
	}
}

func RedirectIfNotLeader(c *gin.Context) {
	storage, _ := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	if storage.Leader() == "" {
		responseBadRequest(c, errors.New("no leader now, please retry later"))
		c.Abort()
		return
	}
	if !storage.IsLeader() {
		if !c.GetBool(consts.HeaderIsRedirect) {
			c.Set(consts.HeaderIsRedirect, true)
			c.Redirect(http.StatusTemporaryRedirect, "http://"+storage.Leader()+c.Request.RequestURI)
		} else {
			responseBadRequest(c, errors.New("too many redirects"))
		}
		c.Abort()
		return
	}
	c.Next()
}

func requiredNamespace(c *gin.Context) {
	storage, _ := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	ok, err := storage.IsNamespaceExists(c, c.Param("namespace"))
	if err != nil {
		responseError(c, err)
		return
	}
	if !ok {
		responseBadRequest(c, metadata.ErrEntryNoExists)
		return
	}
	c.Next()
}

func requiredCluster(c *gin.Context) {
	storage, _ := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	ok, err := storage.IsClusterExists(c, c.Param("namespace"), c.Param("cluster"))
	if err != nil {
		responseError(c, err)
		return
	}
	if !ok {
		responseBadRequest(c, metadata.ErrEntryNoExists)
		return
	}
	c.Next()
}
