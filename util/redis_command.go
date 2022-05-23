package util

import (
	"strings"
	"context"
	"strconv"
)

type ClusterInfo struct {
	ClusterState         bool
	ClusterSlotsAssigned int
	ClusterSlotsOK       int
	ClusterSlotsPFail    int
	ClusterSlotsFail     int 
	ClusterKnownNodes    int
	ClusterSize          int 
	ClusterCurrentEpoch  int64
	ClusterMyEpoch       int64
	MigratingSlot        int 
	ImportingSlot        int
	DestinationNode      string
	MigratingState       string
	ImportingState       string
}

func ClusterInfoCmd(nodeAddr string) (*ClusterInfo, error) {
	cli, err := RedisPool(nodeAddr)
	if err != nil {
		return nil, err
	}
	res, err:= cli.Do(context.Background(), "CLUSTER", "info").Result()
	if err != nil {
		return nil, err
	}
	clusterInfo := &ClusterInfo{
		MigratingSlot: -1,
		ImportingSlot: -1,
		DestinationNode: "",
		MigratingState: "",
		ImportingState: "",
	}
	infos := strings.Split(res.(string), "\n")
	for _, info := range infos {
		kv := strings.Split(info, ":")
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(kv[1])
		switch key {
		case "cluster_state":
			if val == "ok" {
				clusterInfo.ClusterState = true
			} 
		case "cluster_slots_assigned":
			clusterInfo.ClusterSlotsAssigned, _ = strconv.Atoi(val)
		case "cluster_slots_ok":
			clusterInfo.ClusterSlotsOK, _ = strconv.Atoi(val)
		case "cluster_slots_pfail":
			clusterInfo.ClusterSlotsPFail, _ = strconv.Atoi(val)
		case "cluster_slots_fail":
			clusterInfo.ClusterSlotsFail, _ = strconv.Atoi(val)
		case "cluster_known_nodes":
			clusterInfo.ClusterKnownNodes, _ = strconv.Atoi(val)
		case "cluster_size":
			clusterInfo.ClusterSize, _ = strconv.Atoi(val)
		case "cluster_current_epoch":
			clusterInfo.ClusterCurrentEpoch, _ = strconv.ParseInt(val, 10, 64)
		case "cluster_my_epoch":
			clusterInfo.ClusterMyEpoch, _ = strconv.ParseInt(val, 10, 64)
		case "migrating_slot":
			clusterInfo.MigratingSlot, _ = strconv.Atoi(val)
		case "importing_slot":
			clusterInfo.ImportingSlot, _ = strconv.Atoi(val)
		case "destination_node":
			clusterInfo.DestinationNode = val
		case "migrating_state":
			clusterInfo.MigratingState = val
		case "import_state":
			clusterInfo.ImportingState = val
		}
	}
	return clusterInfo, nil
}

type ServerInfo struct {
	Version    string
	GitSha1    string
	OS         string
	GccVersion string
	ArchBits   string
	ProcessId  string
	TcpPort    string
	UpTime     string
}

type ClientInfo struct {
	MaxClients       string
	ConnectedClients string
	MonitorClients   string
}

type MemoryInfo struct {
	UsedMemoryRss      string
	UsedMemoryHuman    string
	UsedMemoryLua      string
	UsedMemoryLuaHuman string
}

type StatesInfo struct {
	TotalConnectionsReceived string
	TotalCommandsProcessed   string
	InstantaneousOps         string
	TotalNetInputBytes       string
	TotalNetOutputBytes      string
	InstantaneousInputKbps   string
	InstantaneousOutputKbps  string
	SyncFull                 string
	SyncPartialOk            string
	SyncPartialErr           string
	PubsubChannels           string
	PubsubPatterns           string
}

type SlaveReplicationInfo struct {
	Role                         string
	MasterHost                   string
	MasterPort                   string
	MasterLinkStatus             string
	MasterSyncUnrecoverableError string
	MasterSyncInProgress         string
	MasterLastIoSecondsAgo       string
	SlaveReplOffset              string
	SlavePriority                string
}

type MasterReplicationInfo struct {
	Role             string
	ConnectedSlaves  string
	MasterReplOffset string
}

type KeySpaceInfo struct {
	Sequence        string
	UsedDbSize      string
	MaxDbSize       string
	UsedPercent     string
	DiskCapacity    string
	UsedDiskSize    string
	UsedDiskPercent string
}

type NodeInfo struct {
	Server            ServerInfo
	Client            ClientInfo
	Mem               MemoryInfo
	States            StatesInfo
	SlaveReplication  SlaveReplicationInfo
	MasterReplication MasterReplicationInfo
	KeySpace          KeySpaceInfo
}

func NodeInfoCmd(nodeAddr string) (*NodeInfo, error){
	cli, err := RedisPool(nodeAddr)
	if err != nil {
		return nil, err
	}
	res, err:= cli.Do(context.Background(), "info").Result()
	if err != nil {
		return nil, err
	}

	nodeInfo := &NodeInfo{}
	infos := strings.Split(res.(string), "\n")
	for _, info := range infos {
		kv := strings.Split(info, ":")
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(kv[1])
		switch key {
		case "version":
			nodeInfo.Server.Version = val
		case "git_sha1":
			nodeInfo.Server.GitSha1 = val
		case "os":
			nodeInfo.Server.OS = val
		case "gcc_version":
			nodeInfo.Server.GccVersion = val
		case "arch_bits":
			nodeInfo.Server.ArchBits = val
		case "process_id":
			nodeInfo.Server.ProcessId = val
		case "tcp_port":
			nodeInfo.Server.TcpPort = val
		case "uptime_in_seconds":
			nodeInfo.Server.UpTime = val
		case "maxclients":
			nodeInfo.Client.MaxClients = val
		case "connected_clients":
			nodeInfo.Client.ConnectedClients = val
		case "monitor_clients":
			nodeInfo.Client.MonitorClients = val
		case "used_memory_rss":
			nodeInfo.Mem.UsedMemoryRss = val
		case "used_memory_human":
			nodeInfo.Mem.UsedMemoryHuman = val
		case "used_memory_lua":
			nodeInfo.Mem.UsedMemoryLua = val
		case "used_memory_lua_human":
			nodeInfo.Mem.UsedMemoryLuaHuman = val
		case "total_connections_received":
			nodeInfo.States.TotalConnectionsReceived = val
		case "total_commands_processed":
			nodeInfo.States.TotalCommandsProcessed = val
		case "instantaneous_ops_per_sec":
			nodeInfo.States.InstantaneousOps = val
		case "total_net_input_bytes":
			nodeInfo.States.TotalNetInputBytes = val
		case "total_net_output_bytes":
			nodeInfo.States.TotalNetOutputBytes = val
		case "instantaneous_input_kbps":
			nodeInfo.States.InstantaneousInputKbps = val
		case "instantaneous_output_kbps":
			nodeInfo.States.InstantaneousOutputKbps = val
		case "sync_full":
			nodeInfo.States.SyncFull = val
		case "sync_partial_ok":
			nodeInfo.States.SyncPartialOk = val
		case "sync_partial_err":
			nodeInfo.States.SyncPartialErr = val
		case "pubsub_channels":
			nodeInfo.States.PubsubChannels = val
		case "pubsub_patterns":
			nodeInfo.States.PubsubPatterns = val
		case "role":
			nodeInfo.SlaveReplication.Role = val
			nodeInfo.MasterReplication.Role = val
		case "master_host":
			nodeInfo.SlaveReplication.MasterHost = val
		case "master_port":
			nodeInfo.SlaveReplication.MasterPort = val
		case "master_link_status":
			nodeInfo.SlaveReplication.MasterLinkStatus = val
		case "master_sync_unrecoverable_error":
			nodeInfo.SlaveReplication.MasterSyncUnrecoverableError = val
		case "master_sync_in_progress":
			nodeInfo.SlaveReplication.MasterSyncInProgress = val
		case "master_last_io_seconds_ago":
			nodeInfo.SlaveReplication.MasterLastIoSecondsAgo = val
		case "slave_repl_offset":
			nodeInfo.SlaveReplication.SlaveReplOffset = val
		case "slave_priority":
			nodeInfo.SlaveReplication.SlavePriority = val
		case "connected_slaves":
			nodeInfo.MasterReplication.ConnectedSlaves = val
		case "master_repl_offset":
			nodeInfo.MasterReplication.MasterReplOffset = val
		case "sequence":
			nodeInfo.KeySpace.Sequence = val
		case "used_db_size":
			nodeInfo.KeySpace.UsedDbSize = val
		case "max_db_size":
			nodeInfo.KeySpace.MaxDbSize = val
		case "used_percent":
			nodeInfo.KeySpace.UsedPercent = val
		case "disk_capacity":
			nodeInfo.KeySpace.DiskCapacity = val
		case "used_disk_size":
			nodeInfo.KeySpace.UsedDiskSize = val
		case "used_disk_percent":
			nodeInfo.KeySpace.UsedDiskPercent = val
		}
	}
	return nodeInfo, nil
}

func SyncClusterInfo2Node(nodeAddr, nodeID , clusterStr string, ver int64) error {
	cli, err := RedisPool(nodeAddr)
	if err != nil {
		return err
	}
	err = cli.Do(context.TODO(), "CLUSTERX", "setnodeid", nodeID).Err()
	if err != nil {
		return err
	}
	err = cli.Do(context.TODO(), "CLUSTERX", "setnodes", clusterStr, ver).Err()
	if err != nil {
		return err
	}
	return nil
}