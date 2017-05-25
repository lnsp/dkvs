package local

import (
	"github.com/lnsp/dkvs/nodes"
)

type Node interface {
	Listen() error
}

type Slave struct {
	PublicAddress string
	Replicas      *ReplicaSet
	Latest        nodes.Revision
	KeepAlive     bool
	Entries       Map
	NodeStatus    nodes.Status
}

type Master struct {
	Slave
}

func (m *Master) Listen() error {
	return nil
}

func NewMaster(local, remote string, replicas int) *Master {
	return nil
}
