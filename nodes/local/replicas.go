package local

import (
	"errors"
	"sync"

	"github.com/lnsp/dkvs/nodes"
)

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
		return errors.New("No replicas available")
	}

	lr.lock.Lock()
	defer lr.lock.Unlock()
	for _, n := range lr.replicas {
		if n.Status() != nodes.StatusDown {
			if err := f(n); err != nil {
				continue
			}
			return nil
		}
	}
	return errors.New("No replica available")
}
