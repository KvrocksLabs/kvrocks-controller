package api

import "github.com/apache/kvrocks-controller/store"

type Handler struct {
	Namespace *NamespaceHandler
	Cluster   *ClusterHandler
	Shard     *ShardHandler
	Node      *NodeHandler
}

func NewHandler(s *store.ClusterStore) *Handler {
	return &Handler{
		Namespace: &NamespaceHandler{s: s},
		Cluster:   &ClusterHandler{storage: s},
		Shard:     &ShardHandler{s: s},
		Node:      &NodeHandler{s: s},
	}
}
