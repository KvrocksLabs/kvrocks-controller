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

set -e

GO_PROJECT=github.com/apache/kvrocks-controller
BUILD_DIR=./_build
VERSION=`cat VERSION.txt`

SERVER_TARGET_NAME=kvctl-server
CLIENT_TARGET_NAME=kvctl

for TARGET_NAME in "$SERVER_TARGET_NAME" "$CLIENT_TARGET_NAME"; do
    if [[ "$TARGET_NAME" == "$SERVER_TARGET_NAME" ]]; then
        CMD_PATH="${GO_PROJECT}/cmd/server"
    else
        CMD_PATH="${GO_PROJECT}/cmd/client"
    fi

    CGO_ENABLED=0 go build -v -ldflags \
        "-X $GO_PROJECT/version.Version=$VERSION" \
        -o ${TARGET_NAME} ${CMD_PATH}

    if [[ $? -ne 0 ]]; then
        echo "Failed to build $TARGET_NAME"
        exit 1
    fi

    echo "Build $TARGET_NAME successfully"
done

rm -rf ${BUILD_DIR}
mkdir -p ${BUILD_DIR}
mv $SERVER_TARGET_NAME ${BUILD_DIR}
mv $CLIENT_TARGET_NAME ${BUILD_DIR}
