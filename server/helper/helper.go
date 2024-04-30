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
package helper

import (
	"errors"
	"net/http"

	"github.com/apache/kvrocks-controller/consts"

	"github.com/gin-gonic/gin"
)

type Error struct {
	Message string `json:"message"`
}

type Response struct {
	Error *Error      `json:"error,omitempty"`
	Data  interface{} `json:"data"`
}

func ResponseOK(c *gin.Context, data interface{}) {
	responseData(c, http.StatusOK, data)
}

func ResponseCreated(c *gin.Context, data interface{}) {
	responseData(c, http.StatusCreated, data)
}

func ResponseNoContent(c *gin.Context) {
	c.JSON(http.StatusNoContent, nil)
}

func ResponseBadRequest(c *gin.Context, err error) {
	c.JSON(http.StatusBadRequest, Response{
		Error: &Error{Message: err.Error()},
	})
}

func responseData(c *gin.Context, code int, data interface{}) {
	c.JSON(code, Response{
		Data: data,
	})
}

func ResponseError(c *gin.Context, err error) {
	code := http.StatusInternalServerError
	if errors.Is(err, consts.ErrNotFound) {
		code = http.StatusNotFound
	} else if errors.Is(err, consts.ErrIndexOutOfRange) {
		code = http.StatusBadRequest
	} else if errors.Is(err, consts.ErrAlreadyExists) {
		code = http.StatusConflict
	} else if errors.Is(err, consts.ErrForbidden) {
		code = http.StatusForbidden
	} else if errors.Is(err, consts.ErrInvalidArgument) {
		code = http.StatusBadRequest
	}
	c.JSON(code, Response{
		Error: &Error{Message: err.Error()},
	})
	c.Abort()
}
