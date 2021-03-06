// Copyright 2017 Lennart Espe. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Package replicas provides functionality to manage master node sets.
package replicas

import (
	"errors"
	"sync"

	"github.com/lnsp/dkvs/lib"
)

var (
	errNoReplicas   = errors.New("No replicas available")
	errNotInitiated = errors.New("Replica set not initiated")
)

type Set interface {
	Join(lib.Master)
	Set([]lib.Master)
	All(func(lib.Master) error) error
	Has(lib.Master) bool
	Item() lib.Master
	Trial(func(lib.Master) error) error
	Instance() []lib.Master
	Collect() []string
	Size() int
}

func New(nodes ...lib.Master) Set {
	return &replicaSet{
		replicas: nodes,
		lock:     sync.Mutex{},
	}
}

type replicaSet struct {
	replicas []lib.Master
	lock     sync.Mutex
}

func (lr *replicaSet) Size() int {
	lr.lock.Lock()
	defer lr.lock.Unlock()
	return len(lr.replicas)
}

func (lr *replicaSet) Set(repl []lib.Master) {
	lr.lock.Lock()
	copy := make([]lib.Master, 0, len(lr.replicas)+len(repl))
	for _, n := range repl {
		var e lib.Master
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

func (lr *replicaSet) Join(n lib.Master) {
	lr.lock.Lock()
	lr.replicas = append(lr.replicas, n)
	lr.lock.Unlock()
}

func (lr *replicaSet) All(f func(n lib.Master) error) error {
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

func (lr *replicaSet) Has(n lib.Master) bool {
	copy := lr.Instance()
	addr := n.Address()
	for _, n := range copy {
		if n.Address() == addr {
			return true
		}
	}
	return false
}
func (lr *replicaSet) Item() lib.Master {
	lr.lock.Lock()
	defer lr.lock.Unlock()
	if len(lr.replicas) < 1 {
		return nil
	}
	return lr.replicas[0]
}

func (lr *replicaSet) Trial(f func(lib.Master) error) error {
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

func (lr *replicaSet) Instance() []lib.Master {
	lr.lock.Lock()
	defer lr.lock.Unlock()
	copy := make([]lib.Master, len(lr.replicas))
	for i, r := range lr.replicas {
		copy[i] = r
	}
	return copy
}
