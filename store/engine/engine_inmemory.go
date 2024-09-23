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
package engine

import (
	"context"
	"strings"
	"sync"

	"github.com/apache/kvrocks-controller/consts"
)

var _ Engine = (*Mock)(nil)

type Mock struct {
	mu     sync.Mutex
	values map[string]string
}

func NewMock() *Mock {
	return &Mock{
		values: make(map[string]string),
	}
}

func (m *Mock) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.values[key]
	if !ok {
		return nil, consts.ErrNotFound
	}
	return []byte(v), nil
}

func (m *Mock) Exists(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.values[key]
	return ok, nil
}

func (m *Mock) Set(_ context.Context, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values[key] = string(value)
	return nil
}

func (m *Mock) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.values, key)
	return nil
}

func (m *Mock) List(_ context.Context, prefix string) ([]Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	exists := make(map[string]bool, 0)
	var entries []Entry
	for k, v := range m.values {
		if strings.HasPrefix(k, prefix) {
			k = strings.Trim(strings.TrimPrefix(k, prefix), "/")
			fields := strings.SplitN(k, "/", 2)
			if len(fields) == 2 {
				// only list the first level
				k = fields[0]
			}
			if _, ok := exists[k]; ok {
				continue
			}
			exists[k] = true
			entries = append(entries, Entry{
				Key:   k,
				Value: []byte(v),
			})
		}
	}
	return entries, nil
}

func (m *Mock) Close() error {
	return nil
}

func (m *Mock) ID() string {
	return "mock_store_engine"
}

func (m *Mock) Leader() string {
	return "mock_store_engine"
}

func (m *Mock) LeaderChange() <-chan bool {
	return make(chan bool)
}

func (m *Mock) IsReady(_ context.Context) bool {
	return true
}
