// Package hashtable provides functionality for storing and retrieving key-value pairs associated with a specific revision.
package hashtable

import (
	"sync"

	"github.com/lnsp/dkvs/lib"
)

// Map stores key-value pairs associated with a revision.
// If an entry should be overriden, the map checks if the updated entry has a newer revision.
// If yes, it will copy the data, else it will discard the change.
type Map interface {
	// Read retrieves the key-value pair from the data storage.
	// It returns the key, the revision and if everything has gone ok.
	Read(string) (string, lib.Revision, bool)
	// Store puts the key-value pair in the data storage.
	// it returns if the change has been committed.
	Store(string, string, lib.Revision) bool
	// Keys returns a slice of keys stored in the key-value storage.
	Keys() []string
}

// New initializes a new hash table.
func New() Map {
	return &hashTable{
		backend: map[string]hashTableEntry{},
		lock:    sync.RWMutex{},
	}
}

type hashTable struct {
	lock    sync.RWMutex
	backend map[string]hashTableEntry
}

type hashTableEntry struct {
	val string
	rev lib.Revision
}

func (m *hashTable) Read(key string) (string, lib.Revision, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	entry, ok := m.backend[key]
	return entry.val, entry.rev, ok
}

func (m *hashTable) Store(key, val string, rev lib.Revision) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	if entry, ok := m.backend[key]; ok && rev.IsNewer(entry.rev) {
		m.backend[key] = hashTableEntry{val, rev}
	} else if !ok {
		m.backend[key] = hashTableEntry{val, rev}
	} else {
		return false
	}
	return true
}

func (m *hashTable) Keys() []string {
	m.lock.Lock()
	defer m.lock.Unlock()
	keys := make([]string, len(m.backend))
	i := 0
	for k := range m.backend {
		keys[i] = k
		i++
	}
	return keys
}
