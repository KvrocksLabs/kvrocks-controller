package storage

import (
	"encoding/json"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"

	"golang.org/x/net/context"
)

type FailOverTask struct {
	Namespace  string            `json:"namespace"`
	Cluster    string            `json:"cluster"`
	ShardIdx   int               `json:"shard_idx"`
	Node       metadata.NodeInfo `json:"node"`
	Type       int               `json:"type"`
	ProbeCount int               `json:"probe_count"`

	QueuedTime int64 `json:"pending_time"`
	StartTime  int64 `json:"start_time"`
	FinishTime int64 `json:"finish_time"`

	Status int    `json:"status"`
	Err    string `json:"error"`
}

func (s *Storage) UpdateFailOverTask(ctx context.Context, task *FailOverTask) error {
	taskData, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return s.persist.Set(ctx, buildFailOverKey(task.Namespace, task.Cluster), taskData)
}

func (s *Storage) GetFailOverTask(ctx context.Context, ns, cluster string) (*FailOverTask, error) {
	taskKey := buildFailOverKey(ns, cluster)
	value, err := s.persist.Get(ctx, taskKey)
	if err != nil {
		return nil, err
	}
	if len(value) == 0 {
		return nil, nil // nolint
	}
	var task FailOverTask
	if err := json.Unmarshal(value, &task); err != nil {
		return nil, err
	}
	return &task, nil
}

func (s *Storage) AddFailOverHistory(ctx context.Context, task *FailOverTask) error {
	taskKey := buildFailOverHistoryKey(task.Namespace, task.Cluster, task.Node.ID, task.QueuedTime)
	taskData, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return s.persist.Set(ctx, taskKey, taskData)
}

func (s *Storage) GetFailOverHistory(ctx context.Context, ns, cluster string) ([]*FailOverTask, error) {
	prefixKey := buildFailOverHistoryPrefix(ns, cluster)
	entries, err := s.persist.List(ctx, prefixKey)
	if err != nil {
		return nil, err
	}
	tasks := make([]*FailOverTask, 0)
	for _, entry := range entries {
		var task FailOverTask
		if err = json.Unmarshal(entry.Value, &task); err != nil {
			return nil, err
		}
		tasks = append(tasks, &task)
	}
	return tasks, nil
}
