package consts

import "errors"

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")

	ErrIndexOutOfRange         = errors.New("index out of range")
	ErrShardNoMatchPromoteNode = errors.New("no match promote node in shard")
	ErrShardNoReplica          = errors.New("no replica in shard")
)
