package local

import (
	"errors"
	"sync"

	"github.com/lnsp/dkvs/nodes"
)

func NewReplicaSet(nodes ...nodes.Master) *ReplicaSet {
	return &ReplicaSet{
		replicas: nodes,
		lock:     sync.Mutex{},
	}
}

type ReplicaSet struct {
	replicas []nodes.Master
	lock     sync.Mutex
}

func (lr *ReplicaSet) Set(repl []nodes.Master) {
	lr.lock.Lock()
	lr.replicas = repl
	lr.lock.Unlock()
}

func (lr *ReplicaSet) Trial(f func(nodes.Master) error) error {
	if lr.replicas == nil {
		return errors.New("Replica set not initiated")
	}

	lr.lock.Lock()
	defer lr.lock.Unlock()
	for _, n := range lr.replicas {
		if err := f(n); err != nil {
			continue
		}
		return nil
	}
	return errors.New("No replicas available")
}
