// Copyright 2017 Lennart Espe. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package cluster

import (
	"reflect"
	"sync"
	"testing"

	"github.com/lnsp/dkvs/lib"
	"github.com/lnsp/dkvs/lib/remote"
)

func makeEmptyClusterSet() *clusterSet {
	cluster := &clusterSet{
		slaves:      []lib.Node{},
		lock:        sync.Mutex{},
		replication: 0,
	}
	return cluster
}

func makeFilledClusterSet(names ...string) *clusterSet {
	if len(names) == 0 {
		names = []string{
			"node1", "node2", "node3",
		}
	}

	cluster := &clusterSet{
		slaves:      []lib.Node{},
		lock:        sync.Mutex{},
		replication: 0,
	}
	for _, name := range names {
		cluster.slaves = append(cluster.slaves, remote.NewSlave(name))
	}
	return cluster
}

func equalSetsString(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for _, i := range a {
		found := false
		for _, j := range b {
			if i == j {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func equalSets(a, b []lib.Node) bool {
	if len(a) != len(b) {
		return false
	}

	for _, ax := range a {
		found := false
		for _, bx := range b {
			if ax.Address() == bx.Address() {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func TestNew(t *testing.T) {
	type args struct {
		slaves []lib.Node
	}
	tests := []struct {
		name string
		args args
		want Set
	}{
		{"Empty cluster set", args{[]lib.Node{}}, makeEmptyClusterSet()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := New(tt.args.slaves...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("New() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_clusterSet_Union(t *testing.T) {
	type args struct {
		set *clusterSet
	}
	tests := []struct {
		name   string
		fields *clusterSet
		args   *clusterSet
		want   *clusterSet
	}{
		{
			"Union two empty cluster sets", makeEmptyClusterSet(), makeEmptyClusterSet(), makeEmptyClusterSet(),
		},
		{
			"Union on one empty, one filled", makeEmptyClusterSet(), makeFilledClusterSet(), makeFilledClusterSet(),
		},
		{
			"Union on two filled cluster sets", makeFilledClusterSet(), makeFilledClusterSet(), makeFilledClusterSet(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := &clusterSet{
				slaves:      tt.fields.slaves,
				replication: tt.fields.replication,
			}
			lr.Union(tt.args)
			if !equalSets(lr.Instance(), tt.want.Instance()) {
				t.Errorf("clusterSet.Union(a, b) = %v, want %v", lr, tt.want)
			}
		})
	}
}

func Test_clusterSet_Join(t *testing.T) {
	tests := []struct {
		name   string
		fields *clusterSet
		args   lib.Node
		want   *clusterSet
	}{
		{"Join on empty set", makeEmptyClusterSet(), remote.NewSlave("node1"), makeFilledClusterSet("node1")},
		{"Join on existing entry", makeFilledClusterSet(), remote.NewSlave("node3"), makeFilledClusterSet()},
		{"Join on filled set", makeFilledClusterSet(), remote.NewSlave("node4"), makeFilledClusterSet("node1", "node2", "node3", "node4")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := &clusterSet{
				slaves:      tt.fields.slaves,
				replication: tt.fields.replication,
			}
			lr.Join(tt.args)
			if !equalSets(lr.Instance(), tt.want.Instance()) {
				t.Errorf("clusterSet.Join(a) = %v, want %v", lr, tt.want)
			}
		})
	}
}

func Test_clusterSet_Set(t *testing.T) {
	tests := []struct {
		name   string
		fields Set
		args   []lib.Node
		want   Set
	}{
		{"Set empty on empty set", makeEmptyClusterSet(), []lib.Node{}, makeEmptyClusterSet()},
		{"Set filled on empty set", makeEmptyClusterSet(), []lib.Node{
			remote.NewSlave("node1"), remote.NewSlave("node2"), remote.NewSlave("node3"),
		}, makeFilledClusterSet()},
		{"Set filled on filled set", makeFilledClusterSet(), []lib.Node{
			remote.NewSlave("node1"), remote.NewSlave("node2"), remote.NewSlave("node3"),
		}, makeFilledClusterSet()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := &clusterSet{
				slaves:      tt.fields.Instance(),
				lock:        sync.Mutex{},
				replication: 0,
			}
			lr.Set(tt.args)
			if !equalSets(lr.Instance(), tt.want.Instance()) {
				t.Errorf("clusterSet.Set(a) = %v, want %v", lr, tt.want)
			}
		})
	}
}

func Test_clusterSet_Collect(t *testing.T) {
	type fields struct {
		slaves      []lib.Node
		lock        sync.Mutex
		replication int
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{"Collect on empty set", fields(*makeEmptyClusterSet()), []string{}},
		{"Collect on filled set 1", fields(*makeFilledClusterSet("node1")), []string{"node1"}},
		{"Collect on filled set 2", fields(*makeFilledClusterSet()), []string{"node1", "node2", "node3"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := &clusterSet{
				slaves:      tt.fields.slaves,
				lock:        tt.fields.lock,
				replication: tt.fields.replication,
			}
			if got := lr.Collect(); !equalSetsString(got, tt.want) {
				t.Errorf("clusterSet.Collect() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_clusterSet_All(t *testing.T) {
	type fields struct {
		slaves      []lib.Node
		lock        sync.Mutex
		replication int
	}
	type args struct {
		f func(lib.Node) error
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := &clusterSet{
				slaves:      tt.fields.slaves,
				lock:        tt.fields.lock,
				replication: tt.fields.replication,
			}
			if err := lr.All(tt.args.f); (err != nil) != tt.wantErr {
				t.Errorf("clusterSet.All() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_clusterSet_FilterSelected(t *testing.T) {
	type fields struct {
		slaves      []lib.Node
		lock        sync.Mutex
		replication int
	}
	type args struct {
		id []int
		f  func(lib.Node) bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   Set
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := &clusterSet{
				slaves:      tt.fields.slaves,
				lock:        tt.fields.lock,
				replication: tt.fields.replication,
			}
			if got := lr.FilterSelected(tt.args.id, tt.args.f); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("clusterSet.FilterSelected() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_clusterSet_Selected(t *testing.T) {
	type fields struct {
		slaves      []lib.Node
		lock        sync.Mutex
		replication int
	}
	type args struct {
		id []int
		f  func(lib.Node) error
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := &clusterSet{
				slaves:      tt.fields.slaves,
				lock:        tt.fields.lock,
				replication: tt.fields.replication,
			}
			if err := lr.Selected(tt.args.id, tt.args.f); (err != nil) != tt.wantErr {
				t.Errorf("clusterSet.Selected() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_clusterSet_Trial(t *testing.T) {
	type fields struct {
		slaves      []lib.Node
		lock        sync.Mutex
		replication int
	}
	type args struct {
		f func(lib.Node) error
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := &clusterSet{
				slaves:      tt.fields.slaves,
				lock:        tt.fields.lock,
				replication: tt.fields.replication,
			}
			if err := lr.Trial(tt.args.f); (err != nil) != tt.wantErr {
				t.Errorf("clusterSet.Trial() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_clusterSet_TrialSelected(t *testing.T) {
	type fields struct {
		slaves      []lib.Node
		lock        sync.Mutex
		replication int
	}
	type args struct {
		id []int
		f  func(lib.Node) error
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := &clusterSet{
				slaves:      tt.fields.slaves,
				lock:        tt.fields.lock,
				replication: tt.fields.replication,
			}
			if err := lr.TrialSelected(tt.args.id, tt.args.f); (err != nil) != tt.wantErr {
				t.Errorf("clusterSet.TrialSelected() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_clusterSet_Size(t *testing.T) {
	type fields struct {
		slaves      []lib.Node
		lock        sync.Mutex
		replication int
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := &clusterSet{
				slaves:      tt.fields.slaves,
				lock:        tt.fields.lock,
				replication: tt.fields.replication,
			}
			if got := lr.Size(); got != tt.want {
				t.Errorf("clusterSet.Size() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_clusterSet_Has(t *testing.T) {
	type fields struct {
		slaves      []lib.Node
		lock        sync.Mutex
		replication int
	}
	type args struct {
		i lib.Node
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := &clusterSet{
				slaves:      tt.fields.slaves,
				lock:        tt.fields.lock,
				replication: tt.fields.replication,
			}
			if got := lr.Has(tt.args.i); got != tt.want {
				t.Errorf("clusterSet.Has() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_clusterSet_Instance(t *testing.T) {
	type fields struct {
		slaves      []lib.Node
		lock        sync.Mutex
		replication int
	}
	tests := []struct {
		name   string
		fields fields
		want   []lib.Node
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := &clusterSet{
				slaves:      tt.fields.slaves,
				lock:        tt.fields.lock,
				replication: tt.fields.replication,
			}
			if got := lr.Instance(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("clusterSet.Instance() = %v, want %v", got, tt.want)
			}
		})
	}
}
