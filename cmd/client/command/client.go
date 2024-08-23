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

package command

import (
	"encoding/json"
	"errors"
	"reflect"

	"github.com/go-resty/resty/v2"
)

const (
	apiVersionV1 = "/api/v1"

	defaultHost = "http://127.0.0.1:9379"
)

type client struct {
	restyCli *resty.Client
	host     string
}

type ErrorMessage struct {
	Message string `json:"message"`
}

type response struct {
	Error *ErrorMessage `json:"error"`
	Data  any           `json:"data"`
}

func newClient(host, apiUser, apiPass string) *client {
	if host == "" {
		host = defaultHost
	}
	var restyCli *resty.Client
	if apiUser != "" && apiPass != "" {
		restyCli = resty.New().SetDisableWarn(true).SetBaseURL(host+apiVersionV1).SetBasicAuth(apiUser, apiPass)
	} else {
		restyCli = resty.New().SetBaseURL(host + apiVersionV1)
	}
	return &client{
		restyCli: restyCli,
		host:     host,
	}
}

func unmarshalData(body []byte, v any) error {
	if len(body) == 0 {
		return errors.New("empty response body")
	}

	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return errors.New("unmarshal receiver was non-pointer")
	}

	var rsp response
	rsp.Data = v
	return json.Unmarshal(body, &rsp)
}

func unmarshalError(body []byte) error {
	var rsp response
	if err := json.Unmarshal(body, &rsp); err != nil {
		return err
	}
	if rsp.Error != nil {
		return errors.New(rsp.Error.Message)
	}
	return nil
}
