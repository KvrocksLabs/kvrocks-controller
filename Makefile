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

PROGRAM=kvrocks-controller

CCCOLOR="\033[37;1m"
MAKECOLOR="\033[32;1m"
ENDCOLOR="\033[0m"

all: $(PROGRAM)

.PHONY: all


$(PROGRAM):
	@bash scripts/build.sh
	@echo ""
	@printf $(MAKECOLOR)"Hint: It's a good idea to run 'make test' ;)"$(ENDCOLOR)
	@echo ""

setup:
	@cd scripts && sh setup.sh && cd ..

teardown:
	@cd scripts && sh teardown.sh && cd ..

test:
	@cd scripts && sh setup.sh && cd ..
	@scripts/run-test.sh
	@cd scripts && sh teardown.sh && cd ..

lint:
	@printf $(CCCOLOR)"GolangCI Lint...\n"$(ENDCOLOR)
	@golangci-lint run
