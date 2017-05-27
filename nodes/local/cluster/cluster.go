package cluster

import (
	"errors"
	"sync"

	"github.com/lnsp/dkvs/nodes"
)

var (
	errNoNodes      = errors.New("No cluster nodes available")
	errNotInitiated = errors.New("Cluster set not initiated")
)

type Set interface {
	Join(nodes.Node)
	Set([]nodes.Node)
	Collect() []string
	Union(Set)
	All(func(nodes.Node) error) error
	FilterSelected([]int, func(nodes.Node) bool) Set
	Selected([]int, func(nodes.Node) error) error
	Trial(func(nodes.Node) error) error
	TrialSelected([]int, func(nodes.Node) error) error
	Size() int
	Has(nodes.Node) bool
	Instance() []nodes.Node
}

func New(slaves ...nodes.Node) Set {
	if slaves == nil {
		slaves = make([]nodes.Node, 0)
	}

	return &clusterSet{
		slaves: slaves,
		lock:   sync.Mutex{},
	}
}

type clusterSet struct {
	slaves      []nodes.Node
	lock        sync.Mutex
	replication int
}

func (lr *clusterSet) Union(set Set) {
	lr.lock.Lock()
	union := make(map[string]nodes.Node)
	for _, s := range lr.slaves {
		key := s.Address()
		if _, ok := union[key]; !ok {
			union[key] = s
		}
	}
	set.All(func(s nodes.Node) error {
		key := s.Address()
		if _, ok := union[key]; !ok {
			union[key] = s
		}
		return nil
	})
	keys := make([]nodes.Node, len(union))
	i := 0
	for _, s := range union {
		keys[i] = s
		i++
	}
	lr.lock.Unlock()
	lr.Set(keys)
}

func (lr *clusterSet) Join(slave nodes.Node) {
	lr.lock.Lock()
	lr.slaves = append(lr.slaves, slave)
	lr.lock.Unlock()
}

func (lr *clusterSet) Set(slaves []nodes.Node) {
	lr.lock.Lock()
	copy := make([]nodes.Node, 0, len(lr.slaves)+len(slaves))
	for _, n := range slaves {
		var e nodes.Node
		if lr.slaves != nil {
			for _, p := range lr.slaves {
				if n.Address() == p.Address() {
					e = p
				}
			}
		}
		if e != nil {
			copy = append(copy, e)
		} else {
			copy = append(copy, n)
		}
	}
	lr.slaves = copy
	lr.lock.Unlock()
}

func (lr *clusterSet) Collect() []string {
	lr.lock.Lock()
	defer lr.lock.Unlock()
	addrs := make([]string, len(lr.slaves))
	for i, s := range lr.slaves {
		addrs[i] = s.Address()
	}
	return addrs
}

func (lr *clusterSet) All(f func(nodes.Node) error) error {
	return lr.Selected(nil, f)
}

func (lr *clusterSet) FilterSelected(id []int, f func(nodes.Node) bool) Set {
	if lr.slaves == nil {
		return nil
	}
	copy := lr.Instance()
	results := make([]nodes.Node, 0, len(copy))
	for _, c := range copy {
		if f(c) {
			results = append(results, c)
		}
	}

	return New(results...)
}

func (lr *clusterSet) Selected(id []int, f func(nodes.Node) error) error {
	if lr.slaves == nil {
		return errNotInitiated
	}

	copy := lr.Instance()
	if id == nil {
		id = make([]int, len(copy))
		for i := range id {
			id[i] = i
		}
	}
	count := 0
	for _, n := range id {
		if err := f(copy[n]); err != nil {
			return err
		}
		count++
	}

	if count < 1 {
		return errors.New("No cluster nodes available")
	}
	return nil
}

func (lr *clusterSet) Trial(f func(nodes.Node) error) error {
	return lr.TrialSelected(nil, f)
}

func (lr *clusterSet) TrialSelected(id []int, f func(nodes.Node) error) error {
	if lr.slaves == nil {
		return errors.New("Cluster set not initiated")
	}

	copy := lr.Instance()
	if id == nil {
		id = make([]int, len(copy))
		for i := range id {
			id[i] = i
		}
	}

	var e error
	size := len(copy)
	for _, s := range id {
		if s >= size {
			return errors.New("Node index out of range")
		}
		if err := f(copy[s]); err != nil {
			e = err
			continue
		}
		return nil
	}

	if e != nil {
		return e
	}
	return errors.New("No cluster nodes available")
}

func (lr *clusterSet) Size() int {
	lr.lock.Lock()
	defer lr.lock.Unlock()
	return len(lr.slaves)
}

func (lr *clusterSet) Has(i nodes.Node) bool {
	copy := lr.Instance()
	addr := i.Address()
	for _, s := range copy {
		if s.Address() == addr {
			return true
		}
	}
	return false
}

func (lr *clusterSet) Instance() []nodes.Node {
	lr.lock.Lock()
	defer lr.lock.Unlock()
	copy := make([]nodes.Node, len(lr.slaves))
	for i, s := range lr.slaves {
		copy[i] = s
	}
	return copy
}
