package hashtable

import (
	"sync"

	"github.com/lnsp/dkvs/nodes"
)

type Map interface {
	Read(string) (string, nodes.Revision, bool)
	Store(string, string, nodes.Revision) bool
	Keys() []string
}

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
	rev nodes.Revision
}

func (m *hashTable) Read(key string) (string, nodes.Revision, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	entry, ok := m.backend[key]
	return entry.val, entry.rev, ok
}

func (m *hashTable) Store(key, val string, rev nodes.Revision) bool {
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
