# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
FROM golang:1.20 as build

WORKDIR /kvctl

# If you encounter some issues when pulling modules, \
# you can try to use GOPROXY, especially in China.
# ENV GOPROXY=https://goproxy.cn

COPY . .
RUN make


FROM ubuntu:focal

WORKDIR /kvctl

COPY --from=build /kvctl/_build/kvctl-server ./bin/
COPY --from=build /kvctl/_build/kvctl ./bin/

VOLUME /var/lib/kvctl

COPY ./LICENSE ./NOTICE ./licenses ./
COPY ./config/config.yaml /var/lib/kvctl/

EXPOSE 9379:9379
ENTRYPOINT ["./bin/kvctl-server", "-c", "/var/lib/kvctl/config.yaml"]
