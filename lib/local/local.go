package local

import (
	"github.com/lnsp/dkvs/lib"
	"github.com/lnsp/dkvs/lib/local/cluster"
	"github.com/lnsp/dkvs/lib/local/hashtable"
	"github.com/lnsp/dkvs/lib/local/replicas"
)

type Node interface {
	Listen() error
}

type Slave struct {
	PublicAddress string
	ReplicaSet    replicas.Set
	Latest        lib.Revision
	KeepAlive     bool
	Entries       hashtable.Map
	NodeStatus    lib.Status
}

type Master struct {
	Slave
	ReplicationFactor int
	ClusterSet        cluster.Set
	Primary           bool
}
