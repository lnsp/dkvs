package local

import (
	"github.com/lnsp/dkvs/lib"
	"github.com/lnsp/dkvs/lib/local/cluster"
	"github.com/lnsp/dkvs/lib/local/hashtable"
	"github.com/lnsp/dkvs/lib/local/replicas"
)

// Node is a local node that can listen for incoming connections.
type Node interface {
	Listen() error
}

// Slave is a local slave runtime.
type Slave struct {
	PublicAddress string
	ReplicaSet    replicas.Set
	Latest        lib.Revision
	KeepAlive     bool
	Entries       hashtable.Map
	NodeStatus    lib.Status
}

// Master is a local master runtime.
type Master struct {
	Slave
	ReplicationFactor int
	ClusterSet        cluster.Set
	Primary           bool
}
