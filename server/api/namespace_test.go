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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/apache/kvrocks-controller/store"
	"github.com/apache/kvrocks-controller/store/engine"

	"github.com/stretchr/testify/require"

	"github.com/gin-gonic/gin"
)

func GetTestContext(recorder *httptest.ResponseRecorder) *gin.Context {
	gin.SetMode(gin.TestMode)
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = &http.Request{
		Header: make(http.Header),
		URL:    &url.URL{},
	}
	return ctx
}

func TestNamespace(t *testing.T) {
	handler := &NamespaceHandler{s: store.NewClusterStore(engine.NewMock())}

	runCreate := func(t *testing.T, ns string, expectedStatusCode int) {
		recorder := httptest.NewRecorder()
		ctx := GetTestContext(recorder)
		ctx.Request.Body = io.NopCloser(bytes.NewBufferString(fmt.Sprintf("{\"namespace\":\"%s\"}", ns)))
		handler.Create(ctx)
		require.Equal(t, expectedStatusCode, recorder.Code)
	}

	runExists := func(t *testing.T, ns string, expectedStatusCode int) {
		recorder := httptest.NewRecorder()
		ctx := GetTestContext(recorder)
		ctx.Params = []gin.Param{{Key: "namespace", Value: ns}}
		handler.Exists(ctx)
		require.Equal(t, expectedStatusCode, recorder.Code)
	}

	runRemove := func(t *testing.T, ns string, expectedStatusCode int) {
		recorder := httptest.NewRecorder()
		ctx := GetTestContext(recorder)
		ctx.Params = []gin.Param{{Key: "namespace", Value: ns}}
		handler.Remove(ctx)
		require.Equal(t, expectedStatusCode, recorder.Code)
	}

	t.Run("create namespace", func(t *testing.T) {
		runCreate(t, "test0", http.StatusCreated)
		runCreate(t, "test1", http.StatusCreated)
		runCreate(t, "test0", http.StatusConflict)
	})

	t.Run("exits namespace", func(t *testing.T) {
		runExists(t, "test0", http.StatusOK)
		runExists(t, "not-exists", http.StatusNotFound)
	})

	t.Run("list namespace", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ctx := GetTestContext(recorder)
		handler.List(ctx)
		require.Equal(t, http.StatusOK, recorder.Code)

		var rsp struct {
			Data map[string][]string `json:"data"`
		}
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &rsp))
		require.ElementsMatch(t, []string{"test0", "test1"}, rsp.Data["namespaces"])
	})

	t.Run("remove namespace", func(t *testing.T) {
		for _, ns := range []string{"test0", "test1"} {
			runRemove(t, ns, http.StatusNoContent)
			runRemove(t, ns, http.StatusNotFound)
		}
	})

}
