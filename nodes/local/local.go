package local

import (
	"github.com/lnsp/dkvs/nodes"
	"github.com/lnsp/dkvs/nodes/local/cluster"
	"github.com/lnsp/dkvs/nodes/local/replicas"
)

type Node interface {
	Listen() error
}

type Slave struct {
	PublicAddress string
	ReplicaSet    replicas.Set
	Latest        nodes.Revision
	KeepAlive     bool
	Entries       Map
	NodeStatus    nodes.Status
}

type Master struct {
	Slave
	ReplicationFactor int
	ClusterSet        cluster.Set
	Primary           bool
}
