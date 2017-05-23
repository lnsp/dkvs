package local

import (
	"errors"
	"net"

	"sync"

	"github.com/lnsp/dkvs/nodes"
	"github.com/lnsp/dkvs/nodes/remote"
)

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

type LocalSlave struct {
	PublicAddress string
	Replicas      []nodes.Master
	Latest        nodes.Revision
	KeepAlive     bool
	Entries       Map
	NodeStatus    nodes.Status
}

type LocalMaster struct {
}

func (slave *LocalSlave) handleSlave(s remote.Slave) error {
	defer s.Close()
	for slave.KeepAlive {

	}
	return nil
}

func (slave *LocalSlave) handlePrimaryMaster(m remote.Master) error {
	defer m.Close()
	replicas, err := m.Replicas()
	if err != nil {
		return err
	}
	slave.Replicas = replicas

	for slave.KeepAlive {
		cmd, err := m.Poll()
		if err != nil {
			return err
		}
		if cmd.KindOf(remote.CommandStore) {
			key, value, revString := cmd.Arg(0), cmd.Arg(1), cmd.Arg(2)
			rev, err := nodes.ToRevision(revString)
			if err != nil {
				m.Push(remote.CommandError.Param(err.Error()))
				continue
			}
			slave.Store(key, value, rev)
			if err := m.Push(cmd.Param(remote.StoreOK)); err != nil {
				return err
			}
		} else if cmd.KindOf(remote.CommandRead) {
			key := cmd.Arg(0)
			val, rev, err := slave.Read(key)
			if err != nil {
				m.Push(remote.CommandError.Param(err.Error()))
				continue
			}
			if err := m.Push(cmd.Param(val, rev.String())); err != nil {
				return err
			}
		} else if cmd.KindOf(remote.CommandStatus) {
			if cmd.ArgCount() > 0 {
				switch cmd.Arg(0) {
				case remote.StatusDown:
					slave.NodeStatus = nodes.StatusDown
				case remote.StatusReady:
					slave.NodeStatus = nodes.StatusReady
				case remote.StatusShutdown:
					slave.NodeStatus = nodes.StatusShutdown
				case remote.StatusStartup:
					slave.NodeStatus = nodes.StatusStartup
				}
			}
			stat := remote.StatusDown
			switch slave.Status() {
			case nodes.StatusDown:
				stat = remote.StatusDown
			case nodes.StatusReady:
				stat = remote.StatusReady
			case nodes.StatusShutdown:
				stat = remote.StatusShutdown
			case nodes.StatusStartup:
				stat = remote.StatusStartup
			}
			if err := m.Push(cmd.Param(stat)); err != nil {
				return err
			}
		} else if cmd.KindOf(remote.CommandShutdown) {
			err := slave.Shutdown()
			status := remote.ShutdownOK
			if err != nil {
				status = remote.ShutdownDenied
			}
			if err := m.Push(cmd.Param(status)); err != nil {
				return err
			}
		} else if cmd.KindOf(remote.CommandRevision) {
			revision, _ := slave.Revision()
			if err := m.Push(cmd.Param(revision.String())); err != nil {
				return err
			}
		} else if cmd.KindOf(remote.CommandRole) {
			remoteRole := remote.RoleSlave
			role, _ := slave.Role()
			switch role {
			case nodes.RoleMasterPrimary:
				remoteRole = remote.RoleMasterPrimary
			case nodes.RoleMaster:
				remoteRole = remote.RoleMaster
			case nodes.RoleSlave:
				remoteRole = remote.RoleSlave
			}
			if err := m.Push(cmd.Param(remoteRole)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (slave *LocalSlave) handleMaster(m remote.Master) error {
	defer m.Close()
	for slave.KeepAlive {
		cmd, err := m.Poll()
		if err != nil {
			return err
		}
		if cmd.KindOf(remote.CommandOwn) {
			return slave.handlePrimaryMaster(m)
		}
	}
	return nil
}

func (slave *LocalSlave) Store(key, value string, rev nodes.Revision) error {
	if rev.IsNewer(slave.Latest) {
		slave.Latest = rev
	}
	ok := slave.Entries.Store(key, value, rev)
	if !ok {
		return errors.New("Later revision already stored")
	}
	return nil
}

func (slave *LocalSlave) Read(key string) (string, nodes.Revision, error) {
	val, rev, ok := slave.Entries.Read(key)
	if !ok {
		return key, nil, errors.New("Key not found")
	}
	return val, rev, nil
}

func (slave *LocalSlave) Status() nodes.Status {
	return slave.NodeStatus
}

func (slave *LocalSlave) Shutdown() error {
	if !slave.KeepAlive {
		return errors.New("Already shutting down")
	}
	slave.KeepAlive = false
	return nil
}

func (slave *LocalSlave) Role() (nodes.Role, error) {
	return nodes.RoleSlave, nil
}

func (slave *LocalSlave) Revision() (nodes.Revision, error) {
	return slave.Latest, nil
}

func (slave *LocalSlave) Listen() error {
	listener, err := net.Listen("tcp", slave.PublicAddress)
	if err != nil {
		return err
	}
	for slave.KeepAlive {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		defer conn.Close()

		node := &remote.RemoteSlave{
			Connection:    conn,
			PublicAddress: conn.RemoteAddr().String(),
		}
		node.PublicAddress = node.Address()
		role, err := node.Role()
		if err != nil {
			node.Close()
			continue
		}
		switch role {
		case nodes.RoleMasterPrimary:
			go slave.handlePrimaryMaster(&remote.RemoteMaster{RemoteSlave: *node})
		case nodes.RoleMaster:
			go slave.handleMaster(&remote.RemoteMaster{RemoteSlave: *node})
		case nodes.RoleSlave:
			go slave.handleSlave(node)
		}
	}
	return nil
}

func (slave *LocalSlave) Address() string {
	return slave.PublicAddress
}
