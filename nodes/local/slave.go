package local

import (
	"errors"
	"net"

	"github.com/lnsp/dkvs/nodes"
	"github.com/lnsp/dkvs/nodes/remote"
)

func (slave *Slave) handleSlave(s *remote.Slave) error {
	defer s.Close()
	for slave.KeepAlive {
		cmd, err := s.Poll()
		if err != nil {
			return err
		}
		if cmd.KindOf(remote.CommandStore) {
			key, value, revString := cmd.Arg(0), cmd.Arg(1), cmd.Arg(2)
			var result nodes.Revision
			if revString != "" {
				storeRev, err := nodes.ToRevision(revString)
				if err != nil {
					s.Push(remote.CommandError.Param(err.Error()))
					continue
				}
				result = storeRev
			} else {
				if err := slave.Replicas.Trial(func(m nodes.Master) error {
					rev, err := m.Revision()
					if err != nil {
						return err
					}
					result = rev.Increase()
					return nil
				}); err != nil {
					s.Push(remote.CommandError.Param(err.Error()))
					continue
				}
			}
			if err := slave.Replicas.Trial(func(m nodes.Master) error {
				return m.Store(key, value, result)
			}); err != nil {
				s.Push(remote.CommandError.Param(err.Error()))
				continue
			}
			if err := s.Push(cmd.Param(remote.StoreOK)); err != nil {
				return err
			}
		} else if cmd.KindOf(remote.CommandRead) {
			key := cmd.Arg(0)
			var value string
			var revision nodes.Revision
			if err := slave.Replicas.Trial(func(m nodes.Master) error {
				val, rev, err := m.Read(key)
				if err != nil {
					return err
				}
				value = val
				revision = rev
				return nil
			}); err != nil {
				s.Push(remote.CommandError.Param(err.Error()))
				continue
			}
			if err := s.Push(cmd.Param(value, revision.String())); err != nil {
				return err
			}
		} else {
			if err := slave.handleLocalCommand(cmd, s); err != nil {
				s.Push(remote.CommandError.Param(err.Error()))
			}
		}
	}
	return nil
}

func (slave *Slave) handleLocalCommand(cmd *remote.Command, m remote.Node) error {
	if cmd.KindOf(remote.CommandStatus) {
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
		return nil
	} else if cmd.KindOf(remote.CommandShutdown) {
		err := slave.Shutdown()
		status := remote.ShutdownOK
		if err != nil {
			status = remote.ShutdownDenied
		}
		if err := m.Push(cmd.Param(status)); err != nil {
			return err
		}
		return nil
	} else if cmd.KindOf(remote.CommandRevision) {
		revision, _ := slave.Revision()
		if err := m.Push(cmd.Param(revision.String())); err != nil {
			return err
		}
		return nil
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
		return nil
	}
	return errors.New("Command context not local")
}

func (slave *Slave) handlePrimaryMaster(m *remote.Master, active bool) error {
	defer m.Close()
	if active {
		if err := m.Join(slave); err != nil {
			return err
		}
		if err := slave.Own(m); err != nil {
			return err
		}
	}

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
			if err := slave.Store(key, value, rev); err != nil {
				m.Push(remote.CommandError.Param(err.Error()))
				continue
			}
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
		} else {
			if err := slave.handleLocalCommand(cmd, m); err != nil {
				m.Push(remote.CommandError.Param(err.Error()))
			}
		}
	}
	return nil
}

func (slave *Slave) Own(m nodes.Master) error {
	replicas, err := m.Replicas()
	if err != nil {
		return err
	}
	slave.Replicas.Set(replicas)
	return nil
}

func (slave *Slave) handleMaster(m *remote.Master) error {
	defer m.Close()
	for slave.KeepAlive {
		cmd, err := m.Poll()
		if err != nil {
			return err
		}
		if cmd.KindOf(remote.CommandOwn) {
			return slave.handlePrimaryMaster(m, false)
		} else if err := m.Push(remote.CommandError); err != nil {
			return err
		}
	}
	return nil
}

func (slave *Slave) Store(key, value string, rev nodes.Revision) error {
	if rev.IsNewer(slave.Latest) {
		slave.Latest = rev
	}
	ok := slave.Entries.Store(key, value, rev)
	if !ok {
		return errors.New("Later revision already stored")
	}
	return nil
}

func (slave *Slave) Read(key string) (string, nodes.Revision, error) {
	val, rev, ok := slave.Entries.Read(key)
	if !ok {
		return key, nil, errors.New("Key not found")
	}
	return val, rev, nil
}

func (slave *Slave) Status() nodes.Status {
	return slave.NodeStatus
}

func (slave *Slave) Shutdown() error {
	if !slave.KeepAlive {
		return errors.New("Already shutting down")
	}
	slave.KeepAlive = false
	return nil
}

func (slave *Slave) Role() (nodes.Role, error) {
	return nodes.RoleSlave, nil
}

func (slave *Slave) Revision() (nodes.Revision, error) {
	return slave.Latest, nil
}

func (slave *Slave) ConnectToSocket(conn net.Conn, active bool) error {
	node := &remote.Slave{
		Connection:    conn,
		PublicAddress: conn.RemoteAddr().String(),
	}
	node.PublicAddress = node.Address()
	role, err := node.Role()
	if err != nil {
		node.Close()
		return err
	}
	switch role {
	case nodes.RoleMasterPrimary:
		return slave.handlePrimaryMaster(&remote.Master{node}, active)
	case nodes.RoleMaster:
		return slave.handleMaster(&remote.Master{node})
	case nodes.RoleSlave:
		return slave.handleSlave(node)
	}
	return nil
}

func (slave *Slave) ConnectTo(address string) error {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return err
	}
	return slave.ConnectToSocket(conn, true)
}

func (slave *Slave) Listen() error {
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
		go slave.ConnectToSocket(conn, false)
	}
	return nil
}

func (slave *Slave) Address() string {
	return slave.PublicAddress
}

func NewSlave(local, primary string) *Slave {
	node := &Slave{
		PublicAddress: local,
		Replicas:      &ReplicaSet{},
		Latest:        []byte{},
		KeepAlive:     true,
		Entries:       NewMap(),
		NodeStatus:    nodes.StatusStartup,
	}
	if primary != "" {
		go node.ConnectTo(primary)
	}
	return node
}
