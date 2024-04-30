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

package consts

import "errors"

var (
	ErrInvalidArgument         = errors.New("invalid argument")
	ErrNotFound                = errors.New("not found")
	ErrForbidden               = errors.New("forbidden")
	ErrAlreadyExists           = errors.New("already exists")
	ErrIndexOutOfRange         = errors.New("index out of range")
	ErrShardIsSame             = errors.New("source and target shard is same")
	ErrSlotOutOfRange          = errors.New("slot out of range")
	ErrSlotNotBelongToAnyShard = errors.New("slot not belong to any shard")
	ErrNodeIsNotMaster         = errors.New("the old node is not master")
	ErrOldMasterNodeNotFound   = errors.New("old master node not found")
	ErrShardNoReplica          = errors.New("no replica in shard")
	ErrShardIsServicing        = errors.New("shard is servicing")
	ErrShardSlotIsMigrating    = errors.New("shard slot is migrating")
	ErrShardNoMatchNewMaster   = errors.New("no match new master in shard")
)
