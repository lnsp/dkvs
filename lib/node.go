package lib

import (
	"encoding/base64"
)

// Status is a node's internal status.
type Status int

// Role is a node's role within the cluster.
type Role int

const (
	// StatusDown is the offline state of a node.
	StatusDown Status = iota
	// StatusReady is the online state of a node.
	StatusReady
	// StatusShutdown is the soon-down state of a node.
	StatusShutdown
	// StatusStartup is the soon-ready state of a node.
	StatusStartup
)

const (
	// RoleMasterPrimary is the primary cluster master role.
	RoleMasterPrimary Role = iota
	// RoleMaster is the secondary cluster master role.
	RoleMaster
	// RoleSlave is the passive cluster role.
	RoleSlave
)

// Node is a part of a DKVS cluster.
type Node interface {
	// Read searches the value and revision mapped to the key.
	Read(key string) (string, Revision, error)
	// Store puts the key-value pair in the data store.
	Store(key, value string, rev Revision) error
	// Keys collects all keys mapped to values.
	Keys() ([]string, error)
	// Mirror collects all key-value pairs from the peers and stores them locally.
	Mirror([]Node) error
	// Revision updates the internal revision and always returns the latest revision.
	Revision(rev Revision) (Revision, error)

	// Address returns this node's address.
	Address() string
	// Role returns the node's role.
	Role() (Role, error)

	// Rebuild forces the node to refetch master replicas and cluster nodes.
	Rebuild() error
	// Status returns the current actor state of the node.
	Status() Status
	// Shutdown stops the node.
	Shutdown() error

	// LocalKeys collects all local keys that are mapped to values.
	LocalKeys() ([]string, error)
	// LocalRead searches locally for the value-revision pair mapped to the key.
	LocalRead(key string) (string, Revision, error)
	// LocalStore puts the key-value pair in the local key store.
	LocalStore(key, value string, rev Revision) error
}

// Master is a superset of a cluster node with administrative abilities.
type Master interface {
	Node
	// Cluster collects all to this master connected nodes.
	Cluster() ([]Node, error)
	// Replicas collects all master replicas in this cluster.
	Replicas() ([]Master, error)
	// Join tells this master than the node wants to join its cluster.
	Join(n Node) error
	// Assist tells this master that another secondary master wants to join its cluster.
	Assist(m Master) error
}

// Revision stores a byte-encoded versioning ID.
type Revision []byte

// String converts the revision into a string representation.
func (rev Revision) String() string {
	return base64.StdEncoding.EncodeToString(rev)
}

// Increase pushes the revision ID one up.
func (rev Revision) Increase() Revision {
	if rev == nil {
		return []byte{0}
	}
	out := make([]byte, 0, len(rev)+1)
	incr := true
	for i := range rev {
		if incr {
			if rev[i] < 255 {
				out = append(out, rev[i]+1)
			} else {
				out = append(out, 0)
				out = append(out, 255)
			}
			incr = false
		} else {
			out = append(out, rev[i])
		}
	}
	return out
}

// ToRevision converts the string into a revision ID.
func ToRevision(s string) (Revision, error) {
	return base64.StdEncoding.DecodeString(s)
}

// IsNewer checks if this revision is newer than the given one.
func (rev Revision) IsNewer(than Revision) bool {
	if len(rev) > len(than) {
		return true
	} else if len(rev) < len(than) {
		return false
	}

	for index := range rev {
		if rev[index] > than[index] {
			return true
		} else if rev[index] < than[index] {
			return false
		}
	}

	return true
}
