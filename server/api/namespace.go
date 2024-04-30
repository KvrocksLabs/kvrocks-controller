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

	"github.com/apache/kvrocks-controller/consts"

	"github.com/apache/kvrocks-controller/server/helper"

	"github.com/gin-gonic/gin"

	"github.com/apache/kvrocks-controller/store"
)

type NamespaceHandler struct {
	s store.Store
}

func (handler *NamespaceHandler) List(c *gin.Context) {
	namespaces, err := handler.s.ListNamespace(c)
	if err != nil {
		helper.ResponseError(c, err)
		return
	}
	helper.ResponseOK(c, gin.H{"namespaces": namespaces})
}

func (handler *NamespaceHandler) Exists(c *gin.Context) {
	namespace := c.Param("namespace")
	ok, err := handler.s.ExistsNamespace(c, namespace)
	if err != nil {
		helper.ResponseError(c, err)
		return
	}
	if !ok {
		helper.ResponseError(c, consts.ErrNotFound)
		return
	}
	helper.ResponseOK(c, nil)
}

func (handler *NamespaceHandler) Create(c *gin.Context) {
	var request struct {
		Namespace string `json:"namespace" validate:"required"`
	}
	if err := c.BindJSON(&request); err != nil {
		helper.ResponseBadRequest(c, err)
		return
	}

	if len(request.Namespace) == 0 {
		helper.ResponseBadRequest(c, errors.New("namespace should NOT be empty"))
		return
	}

	if err := handler.s.CreateNamespace(c, request.Namespace); err != nil {
		helper.ResponseError(c, err)
		return
	}
	helper.ResponseCreated(c, gin.H{"namespace": request.Namespace})
}

func (handler *NamespaceHandler) Remove(c *gin.Context) {
	namespace := c.Param("namespace")
	if err := handler.s.RemoveNamespace(c, namespace); err != nil {
		helper.ResponseError(c, err)
		return
	}
	helper.ResponseNoContent(c)
}
