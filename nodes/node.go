package nodes

import (
	"encoding/base64"
)

type Status int
type Role int

const (
	StatusDown Status = iota
	StatusReady
	StatusShutdown
	StatusStartup
)

const (
	RoleMasterPrimary Role = iota
	RoleMaster
	RoleSlave
)

type Node interface {
	// Storage commands
	Read(key string) (string, Revision, error)
	Store(key, value string, rev Revision) error
	Keys() ([]string, error)
	Mirror([]Slave) error
	Revision(rev Revision) (Revision, error)

	// Static commands
	Address() string
	Role() (Role, error)

	// Management commands
	Rebuild() error
	Status() Status
	Shutdown() error

	// Local commands
	LocalKeys() ([]string, error)
	LocalRead(key string) (string, Revision, error)
	LocalStore(key, value string, rev Revision) error
}

type Slave interface {
	Node
}

type Master interface {
	Node
	Cluster() ([]Slave, error)
	Replicas() ([]Master, error)
	Join(n Slave) error
	Assist(m Master) error
}

type Revision []byte

func (rev Revision) String() string {
	return base64.StdEncoding.EncodeToString(rev)
}

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
func ToRevision(s string) (Revision, error) {
	return base64.StdEncoding.DecodeString(s)
}

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
