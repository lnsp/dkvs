// Copyright 2017 Lennart Espe. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package cluster

import (
	"reflect"
	"sync"
	"testing"

	"github.com/lnsp/dkvs/lib"
)

func TestNew(t *testing.T) {
	type args struct {
		slaves []lib.Node
	}
	tests := []struct {
		name string
		args args
		want Set
	}{
	// TODO: Add test cases.
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
	type fields struct {
		slaves      []lib.Node
		lock        sync.Mutex
		replication int
	}
	type args struct {
		set Set
	}
	tests := []struct {
		name   string
		fields fields
		args   args
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
			lr.Union(tt.args.set)
		})
	}
}

func Test_clusterSet_Join(t *testing.T) {
	type fields struct {
		slaves      []lib.Node
		lock        sync.Mutex
		replication int
	}
	type args struct {
		slave lib.Node
	}
	tests := []struct {
		name   string
		fields fields
		args   args
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
			lr.Join(tt.args.slave)
		})
	}
}

func Test_clusterSet_Set(t *testing.T) {
	type fields struct {
		slaves      []lib.Node
		lock        sync.Mutex
		replication int
	}
	type args struct {
		slaves []lib.Node
	}
	tests := []struct {
		name   string
		fields fields
		args   args
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
			lr.Set(tt.args.slaves)
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
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := &clusterSet{
				slaves:      tt.fields.slaves,
				lock:        tt.fields.lock,
				replication: tt.fields.replication,
			}
			if got := lr.Collect(); !reflect.DeepEqual(got, tt.want) {
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
