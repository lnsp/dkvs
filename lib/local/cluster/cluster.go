// Copyright 2017 Lennart Espe. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Package cluster provides functionality for managing node cluster sets.
package cluster

import (
	"errors"
	"sync"

	"github.com/lnsp/dkvs/lib"
)

var (
	errNoNodes      = errors.New("No cluster nodes available")
	errNotInitiated = errors.New("Cluster set not initiated")
)

type Set interface {
	Join(lib.Node)
	Set([]lib.Node)
	Collect() []string
	Union(Set)
	All(func(lib.Node) error) error
	FilterSelected([]int, func(lib.Node) bool) Set
	Selected([]int, func(lib.Node) error) error
	Trial(func(lib.Node) error) error
	TrialSelected([]int, func(lib.Node) error) error
	Size() int
	Has(lib.Node) bool
	Instance() []lib.Node
}

func New(slaves ...lib.Node) Set {
	if slaves == nil {
		slaves = make([]lib.Node, 0)
	}

	return &clusterSet{
		slaves: slaves,
		lock:   sync.Mutex{},
	}
}

type clusterSet struct {
	slaves      []lib.Node
	lock        sync.Mutex
	replication int
}

func (lr *clusterSet) Union(set Set) {
	lr.lock.Lock()
	union := make(map[string]lib.Node)
	for _, s := range lr.slaves {
		key := s.Address()
		if _, ok := union[key]; !ok {
			union[key] = s
		}
	}
	set.All(func(s lib.Node) error {
		key := s.Address()
		if _, ok := union[key]; !ok {
			union[key] = s
		}
		return nil
	})
	keys := make([]lib.Node, len(union))
	i := 0
	for _, s := range union {
		keys[i] = s
		i++
	}
	lr.lock.Unlock()
	lr.Set(keys)
}

func (lr *clusterSet) Join(slave lib.Node) {
	if lr.Has(slave) {
		return
	}

	lr.lock.Lock()
	lr.slaves = append(lr.slaves, slave)
	lr.lock.Unlock()
}

func (lr *clusterSet) Set(slaves []lib.Node) {
	lr.lock.Lock()
	copy := make([]lib.Node, 0, len(lr.slaves)+len(slaves))
	for _, n := range slaves {
		var e lib.Node
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

func (lr *clusterSet) All(f func(lib.Node) error) error {
	return lr.Selected(nil, f)
}

func (lr *clusterSet) FilterSelected(id []int, f func(lib.Node) bool) Set {
	if lr.slaves == nil {
		return nil
	}
	copy := lr.Instance()
	results := make([]lib.Node, 0, len(copy))
	for _, c := range copy {
		if f(c) {
			results = append(results, c)
		}
	}

	return New(results...)
}

func (lr *clusterSet) Selected(id []int, f func(lib.Node) error) error {
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

func (lr *clusterSet) Trial(f func(lib.Node) error) error {
	return lr.TrialSelected(nil, f)
}

func (lr *clusterSet) TrialSelected(id []int, f func(lib.Node) error) error {
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

func (lr *clusterSet) Has(i lib.Node) bool {
	copy := lr.Instance()
	addr := i.Address()
	for _, s := range copy {
		if s.Address() == addr {
			return true
		}
	}
	return false
}

func (lr *clusterSet) Instance() []lib.Node {
	lr.lock.Lock()
	defer lr.lock.Unlock()
	copy := make([]lib.Node, len(lr.slaves))
	for i, s := range lr.slaves {
		copy[i] = s
	}
	return copy
}
