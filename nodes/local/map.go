package local

import (
	"sync"

	"github.com/lnsp/dkvs/nodes"
)

func NewMap() Map {
	return Map{
		backend: map[string]MapEntry{},
		lock:    sync.RWMutex{},
	}
}

type Map struct {
	lock    sync.RWMutex
	backend map[string]MapEntry
}

type MapEntry struct {
	val string
	rev nodes.Revision
}

func (m *Map) Read(key string) (string, nodes.Revision, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	entry, ok := m.backend[key]
	return entry.val, entry.rev, ok
}

func (m *Map) Store(key, val string, rev nodes.Revision) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	if entry, ok := m.backend[key]; ok && rev.IsNewer(entry.rev) {
		m.backend[key] = MapEntry{val, rev}
	} else if !ok {
		m.backend[key] = MapEntry{val, rev}
	} else {
		return false
	}
	return true
}
