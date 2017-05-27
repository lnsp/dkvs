package replicas

import (
	"errors"
	"sync"

	"github.com/lnsp/dkvs/nodes"
)

var (
	errNoReplicas   = errors.New("No replicas available")
	errNotInitiated = errors.New("Replica set not initiated")
)

type Set interface {
	Join(nodes.Master)
	Set([]nodes.Master)
	All(func(nodes.Master) error) error
	Has(nodes.Master) bool
	Item() nodes.Master
	Trial(func(nodes.Master) error) error
	Instance() []nodes.Master
	Collect() []string
	Size() int
}

func New(nodes ...nodes.Master) Set {
	return &replicaSet{
		replicas: nodes,
		lock:     sync.Mutex{},
	}
}

type replicaSet struct {
	replicas []nodes.Master
	lock     sync.Mutex
}

func (lr *replicaSet) Size() int {
	lr.lock.Lock()
	defer lr.lock.Unlock()
	return len(lr.replicas)
}

func (lr *replicaSet) Set(repl []nodes.Master) {
	lr.lock.Lock()
	copy := make([]nodes.Master, 0, len(lr.replicas)+len(repl))
	for _, n := range repl {
		var e nodes.Master
		for _, p := range lr.replicas {
			if n.Address() == p.Address() {
				e = p
			}
		}
		if e != nil {
			copy = append(copy, e)
		} else {
			copy = append(copy, n)
		}
	}
	lr.replicas = copy
	lr.lock.Unlock()
}

func (lr *replicaSet) Join(n nodes.Master) {
	lr.lock.Lock()
	lr.replicas = append(lr.replicas, n)
	lr.lock.Unlock()
}

func (lr *replicaSet) All(f func(n nodes.Master) error) error {
	count := 0
	copy := lr.Instance()
	e := errNoReplicas
	for _, n := range copy {
		if err := f(n); err != nil {
			e = err
			continue
		}
		count++
	}

	if count < 1 {
		return e
	}
	return nil
}

func (lr *replicaSet) Has(n nodes.Master) bool {
	copy := lr.Instance()
	addr := n.Address()
	for _, n := range copy {
		if n.Address() == addr {
			return true
		}
	}
	return false
}
func (lr *replicaSet) Item() nodes.Master {
	lr.lock.Lock()
	defer lr.lock.Unlock()
	if len(lr.replicas) < 1 {
		return nil
	}
	return lr.replicas[0]
}

func (lr *replicaSet) Trial(f func(nodes.Master) error) error {
	if lr.replicas == nil {
		return errNotInitiated
	}

	copy := lr.Instance()
	e := errNoReplicas
	for _, n := range copy {
		if err := f(n); err != nil {
			e = err
			continue
		}
		return nil
	}

	return e
}

func (lr *replicaSet) Collect() []string {
	copy := lr.Instance()
	addrs := []string{}
	for _, r := range copy {
		addrs = append(addrs, r.Address())
	}
	return addrs
}

func (lr *replicaSet) Instance() []nodes.Master {
	lr.lock.Lock()
	defer lr.lock.Unlock()
	copy := make([]nodes.Master, len(lr.replicas))
	for i, r := range lr.replicas {
		copy[i] = r
	}
	return copy
}
