# Apache Kvrocks Controller

[![Build Status](https://github.com/apache/kvrocks-controller/workflows/CI%20Actions/badge.svg)](https://github.com/apache/kvrocks-controller/actions) [![Go Report Card](https://goreportcard.com/badge/github.com/apache/kvrocks-controller)](https://goreportcard.com/report/github.com/apache/kvrocks-controller) [![codecov](https://codecov.io/gh/apache/kvrocks-controller/branch/unsteable/graph/badge.svg?token=EKU6KU5IWK)](https://codecov.io/gh/apache/kvrocks-controller)

Apache Kvrocks Controller is a cluster management tool for [Apache Kvrocks](https://github.com/apache/incubator-kvrocks), including the following key features:

* Failover - controller will failover or remove the master/slave node when probing failed
* Scale out the cluster in one line command
* Manage many clusters in one controller cluster
* Support multi metadata storages like etcd and so on

## Build and Running

### Requirements

* Go >= 1.16

### Build binaries 

```shell
$ git clone https://github.com/apache/kvrocks-controller
$ cd kvrocks-controller
$ make # You can find the binary file in the `_build` dir if all goes good
# ---
# If you do not have a suitable Golang compilation environment locally, you can also use 'make BUILDER_IMAGE=<golang:version>' to choose a Golang image for compilation.
# $ make BUILDER_IMAGE=golang:1.20.3
```
### Overview
![image](docs/images/overview.png)
For the storage, the ETCD is used as the default storage now. Welcome to contribute other storages like MySQL, Redis, Consul and so on. And what you need to do is to implement the [persistence interface](https://github.com/apache/kvrocks-controller/blob/unstable/storage/persistence/persistence.go).

### 1. Run the controller server 

```shell
# Use docker-compose to setup the etcd
$ make setup
# Run the controller server
$ ./_build/kvctl-server -c config/config.yaml
```
![image](docs/images/server.gif)

For the HTTP API, you can find the [HTTP API(work in progress)](docs/API.md) for more details.
