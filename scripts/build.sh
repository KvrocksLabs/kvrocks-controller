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
BUILDER_IMAGE=${1:-none}
if test -z "$TARGET_OS"; then
    uname_S=`uname -s`
    case "$uname_S" in
        Darwin)
            TARGET_OS=darwin
            ;;
        OpenBSD)
            TARGET_OS=openbsd
            ;;
        DragonFly)
            TARGET_OS=dragonfly
            ;;
        FreeBSD)
            TARGET_OS=freebsd
            ;;
        NetBSD)
            TARGET_OS=netbsd
            ;;
        SunOS)
            TARGET_OS=solaris
            ;;
        *)
            TARGET_OS=linux
            ;;
    esac
fi

if test -z "$TARGET_ARCH"; then
    uname_M=`uname -m`
    case "$uname_M" in
        x86_64)
            TARGET_ARCH=amd64
            ;;
        arm32)
            TARGET_ARCH=arm32
            ;;
        arm64)
            TARGET_ARCH=arm64
            ;;
        ppc64*)
            TARGET_ARCH=ppc64
            ;;
        aarch64)
            TARGET_ARCH=arm
            ;;
        i386)
            TARGET_ARCH=386
            ;;
        *)
            TARGET_ARCH=amd64
            ;;
    esac
fi

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

    if [[ "$BUILDER_IMAGE" == "none" ]]; then
        GOOS="$TARGET_OS" GOARCH="$TARGET_ARCH" CGO_ENABLED=0 go build -v -ldflags \
            "-X $GO_PROJECT/version.Version=$VERSION" \
            -o ${TARGET_NAME} ${CMD_PATH}
    else
        docker run --rm --privileged -it -v $(pwd):/${TARGET_NAME} -w /${TARGET_NAME} \
            -e GOOS="$TARGET_OS" -e GOARCH="$TARGET_ARCH" -e CGO_ENABLED=0 \
            $BUILDER_IMAGE go build -v -ldflags \
            "-X $GO_PROJECT/version.Version=$VERSION" \
            -o /${TARGET_NAME}/${TARGET_NAME} ${CMD_PATH}
    fi

    if [[ $? -ne 0 ]]; then
        echo "Failed to build $TARGET_NAME"
        exit 1
    fi

    echo "Build $TARGET_NAME, OS is $TARGET_OS, Arch is $TARGET_ARCH"
done

rm -rf ${BUILD_DIR}
mkdir -p ${BUILD_DIR}
mv $SERVER_TARGET_NAME ${BUILD_DIR}
mv $CLIENT_TARGET_NAME ${BUILD_DIR}
