package hashtable

import (
	"reflect"
	"sync"
	"testing"

	"github.com/lnsp/dkvs/lib"
)

func makeEmptyHashtable() *hashTable {
	return &hashTable{
		backend: map[string]hashTableEntry{},
		lock:    sync.RWMutex{},
	}
}

func makeFullHashtable() *hashTable {
	table := hashTable{
		backend: map[string]hashTableEntry{
			"key1": hashTableEntry{
				val: "val1",
				rev: []byte{},
			},
			"key2": hashTableEntry{
				val: "val2",
				rev: []byte{0},
			},
			"key3": hashTableEntry{
				val: "val3",
				rev: []byte{1},
			},
		},
		lock: sync.RWMutex{},
	}
	return &table
}

func Test_hashTable_Read(t *testing.T) {
	type fields struct {
		lock    sync.RWMutex
		backend map[string]hashTableEntry
	}
	type args struct {
		key string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
		want1  lib.Revision
		want2  bool
	}{
		{"Key exist, revision 0", fields(*makeFullHashtable()), args{"key1"}, "val1", []byte{}, true},
		{"Key exist, revision 1", fields(*makeFullHashtable()), args{"key2"}, "val2", []byte{0}, true},
		{"Key exist, revision 2", fields(*makeFullHashtable()), args{"key3"}, "val3", []byte{1}, true},
		{"Key not found, revision 0", fields(*makeFullHashtable()), args{"key4"}, "", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &hashTable{
				lock:    tt.fields.lock,
				backend: tt.fields.backend,
			}
			got, got1, got2 := m.Read(tt.args.key)
			if got != tt.want {
				t.Errorf("hashTable.Read() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("hashTable.Read() got1 = %v, want %v", got1, tt.want1)
			}
			if got2 != tt.want2 {
				t.Errorf("hashTable.Read() got2 = %v, want %v", got2, tt.want2)
			}
		})
	}
}

func Test_hashTable_Store(t *testing.T) {
	type fields struct {
		lock    sync.RWMutex
		backend map[string]hashTableEntry
	}
	type args struct {
		key string
		val string
		rev lib.Revision
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{"Store existing key, revision 0->1", fields(*makeFullHashtable()), args{"key1", "val1", []byte{0}}, true},
		{"Store existing key, revision 1->1", fields(*makeFullHashtable()), args{"key2", "val2", []byte{0}}, true},
		{"Store existing key, revision 1->0", fields(*makeFullHashtable()), args{"key3", "val3", []byte{0}}, false},
		{"Store new key, revision 0", fields(*makeFullHashtable()), args{"key4", "val4", []byte{0}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &hashTable{
				lock:    tt.fields.lock,
				backend: tt.fields.backend,
			}
			if got := m.Store(tt.args.key, tt.args.val, tt.args.rev); got != tt.want {
				t.Errorf("hashTable.Store() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNew(t *testing.T) {
	tests := []struct {
		name string
		want Map
	}{
		{
			"Initialized", makeEmptyHashtable(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := New(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("New() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_hashTable_Keys(t *testing.T) {
	type fields struct {
		lock    sync.RWMutex
		backend map[string]hashTableEntry
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{"Empty hashtable keys", fields(*makeEmptyHashtable()), []string{}},
		{"Hashtable with keys", fields(*makeFullHashtable()), []string{"key1", "key2", "key3"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &hashTable{
				lock:    tt.fields.lock,
				backend: tt.fields.backend,
			}
			if got := m.Keys(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("hashTable.Keys() = %v, want %v", got, tt.want)
			}
		})
	}
}
