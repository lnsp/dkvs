package local

import (
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/lnsp/dkvs/lib"
	"github.com/lnsp/dkvs/lib/local/hashtable"
	"github.com/lnsp/dkvs/lib/local/replicas"
	"github.com/lnsp/dkvs/lib/remote"
)

var (
	errNotAvailable = errors.New("This command is not available")
)

func (slave *Slave) ready() {
	slave.NodeStatus = lib.StatusReady
}

func (slave *Slave) joinExistingCluster() {
	slave.ready()
	master := slave.ReplicaSet.Item()
	if err := master.Join(slave); err != nil {
		return
	}
	if err := slave.Rebuild(); err != nil {
		return
	}
}

// local handles commands that share their behaviour on both slave and master instances.
func (slave *Slave) local(cmd *remote.Command, m remote.Node) error {
	if cmd.KindOf(remote.CommandStatus) {
		// handle STATUS, interpret local state and send it back
		// syntax STATUS -> STATUS#...
		// syntax STATUS#TAGRET -> STATUS#TARGET
		if cmd.ArgCount() > 0 {
			switch cmd.Arg(0) {
			case remote.StatusDown:
				slave.NodeStatus = lib.StatusDown
			case remote.StatusReady:
				slave.NodeStatus = lib.StatusReady
			case remote.StatusShutdown:
				slave.NodeStatus = lib.StatusShutdown
			case remote.StatusStartup:
				slave.NodeStatus = lib.StatusStartup
			}
		}
		stat := remote.StatusDown
		switch slave.Status() {
		case lib.StatusDown:
			stat = remote.StatusDown
		case lib.StatusReady:
			stat = remote.StatusReady
		case lib.StatusShutdown:
			stat = remote.StatusShutdown
		case lib.StatusStartup:
			stat = remote.StatusStartup
		}
		if err := m.Push(cmd.Param(stat)); err != nil {
			return err
		}
		return nil
	} else if cmd.KindOf(remote.CommandShutdown) {
		// shutdown stops listening for new connections and kills all active loops
		// SYNTAX: SHUTDOWN -> SHUTDOWN#OK
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
		// revision stores and retrieves local revision information
		// SYNTAX: revision -> REVISION#LATEST-REVISION
		// retrieve the latest revision
		// SYNTAX: revision#rev-code -> REVISION#LATEST-REVISION
		// retrieve the latest revision, if the given rev-code is newer, latest revision is updated
		var rev lib.Revision
		if cmd.ArgCount() > 0 {
			arg, err := lib.ToRevision(cmd.Arg(0))
			if err != nil {
				return err
			}
			rev = arg
		}
		rev, err := slave.Revision(rev)
		if err != nil {
			return err
		}
		if err := m.Push(cmd.Param(rev.String())); err != nil {
			return err
		}
		return nil
	} else if cmd.KindOf(remote.CommandRole) {
		// role retrieves the local role information
		// SYNTAX: role -> ROLE#NODE-ROLE
		remoteRole := remote.RoleSlave
		role, _ := slave.Role()
		switch role {
		case lib.RoleMasterPrimary:
			remoteRole = remote.RoleMasterPrimary
		case lib.RoleMaster:
			remoteRole = remote.RoleMaster
		case lib.RoleSlave:
			remoteRole = remote.RoleSlave
		}
		if err := m.Push(cmd.Param(remoteRole)); err != nil {
			return err
		}
		return nil
	} else if cmd.KindOf(remote.CommandRebuild) {
		// rebuild forces to refetch all replica addresses
		// SYNTAX: REBUILD -> REBUILD#OK
		if err := slave.Rebuild(); err != nil {
			return err
		}
		return m.Push(cmd.Param(remote.RebuildOK))
	} else if cmd.KindOf(remote.CommandReplicas) {
		// replicas returns all registered replicated masters in the cluster
		// SYNTAX: REPLICAS -> REPLICAS#HOST1.COM;HOST2.COM;...
		return m.Push(remote.CommandReplicas.Param(slave.ReplicaSet.Collect()...))
	} else if cmd.KindOf(remote.CommandAddress) {
		// address returns the node's host address
		// SYNTAX: ADDRESS -> ADDRESS#here.comes.it:5000
		return m.Push(cmd.Param(slave.Address()))
	} else if cmd.KindOf(remote.CommandMirror) {
		// mirror tells the node to mirror another node's local data
		// SYNTAX: MIRROR -> MIRROR#OK
		peers := make([]lib.Node, cmd.ArgCount())
		for i, host := range cmd.ArgList() {
			peers[i] = remote.NewSlave(host)
		}
		if err := slave.Mirror(peers); err != nil {
			return err
		}
		if err := m.Push(cmd.Param(remote.MirrorOK)); err != nil {
			return err
		}
		return nil
	} else if cmd.KindOf(remote.CommandLocalKeys) {
		// keys_local fetches all stored keys and returns them
		// SYNTAX: KEYS_LOCAL -> KEYS_LOCAL#KEY1;KEY2;...
		keys, err := slave.LocalKeys()
		if err != nil {
			return err
		}
		if err := m.Push(cmd.Param(keys...)); err != nil {
			return err
		}
		return nil
	} else if cmd.KindOf(remote.CommandLocalStore) {
		// store_local stores a key-value-revision set locally.
		// it refuses to store it if the local version has a higher revision number.
		// SYNTAX: STORE_LOCAL#KEY;VALUE;REVISION -> STORE_LOCAL#OK
		key, value, revString := cmd.Arg(0), cmd.Arg(1), cmd.Arg(2)
		rev, err := lib.ToRevision(revString)
		if err != nil {
			return err
		}
		if err := slave.LocalStore(key, value, rev); err != nil {
			return err
		}
		if err := m.Push(cmd.Param(remote.StoreOK)); err != nil {
			return err
		}
		return nil
	} else if cmd.KindOf(remote.CommandLocalRead) {
		// read_local reads from the local key-value store.
		// SYNTAX: READ_LOCAL#KEY -> READ_LOCAL#VALUE;REVISION
		key := cmd.Arg(0)
		val, rev, err := slave.LocalRead(key)
		if err != nil {
			return err
		}
		if err := m.Push(cmd.Param(val, rev.String())); err != nil {
			return err
		}
		return nil
	} else if cmd.KindOf(remote.CommandHandshake) {
		// handshake does nothing. it's there to check connection state.
		// SYNTAX: HANDSHAKE -> HANDSHAKE#OK
		if err := m.Push(cmd.Param(remote.HandshakeOK)); err != nil {
			return err
		}
		return nil
	}
	return errNotAvailable
}

func (slave *Slave) handle(m *remote.Slave) error {
	defer m.Close()
	for slave.KeepAlive {
		cmd, err := m.Poll()
		if err != nil {
			return err
		}
		if cmd.KindOf(remote.CommandStore) {
			// store tells the node to store a key, value pair.
			// on a slave it fetches the latest revision, increments it and tells a master about the change.
			// SYNTAX: STORE#KEY;VALUE -> STORE#OK
			// pulls the latest revision from a master, increments it and commits the change.
			// SYNTAX: STORE#KEY;VALUE;REVISION -> STORE#OK
			// uses the given revision information to commit the change
			key, value, revString := cmd.Arg(0), cmd.Arg(1), cmd.Arg(2)
			var result lib.Revision
			if revString != "" {
				storeRev, err := lib.ToRevision(revString)
				if err != nil {
					m.Push(remote.Error(err))
					continue
				}
				result = storeRev
			} else {
				if err := slave.ReplicaSet.Trial(func(m lib.Master) error {
					rev, err := m.Revision(nil)
					if err != nil {
						return err
					}
					result = rev.Increase()
					return nil
				}); err != nil {
					m.Push(remote.CommandError.Param("Could not pull latest revision from master"))
					continue
				}
			}
			if err := slave.Store(key, value, result); err != nil {
				m.Push(remote.Error(err))
				continue
			}
			if err := m.Push(cmd.Param(remote.StoreOK)); err != nil {
				return err
			}
		} else if cmd.KindOf(remote.CommandRead) {
			// read contacts one of the masters in the replica set and pulls the set data.
			// SYNTAX: READ#KEY -> READ#VALUE;REVISION
			key := cmd.Arg(0)
			value, revision, err := slave.Read(key)
			if err != nil {
				m.Push(remote.Error(err))
				continue
			}
			if err := m.Push(cmd.Param(value, revision.String())); err != nil {
				return err
			}
		} else if cmd.KindOf(remote.CommandJoin) {
			// join tells one of the masters that the call node wants to join the cluster as a store.
			// SYNTAX: JOIN#STORE-ADDRESS -> JOIN#OK
			slv := remote.NewSlave(cmd.Arg(0))
			if err := slave.ReplicaSet.Trial(func(master lib.Master) error {
				return master.Join(slv)
			}); err != nil {
				m.Push(remote.Error(err))
				continue
			}
			if err := m.Push(remote.CommandJoin.Param(remote.JoinOK)); err != nil {
				return err
			}
		} else if cmd.KindOf(remote.CommandAssist) {
			// assist tells one of the masters that the call node wants to join the replica set as a master.
			// SYNTAX: ASSIST#MASTER-ADDRESS -> ASSIST#OK
			ast := remote.NewMaster(cmd.Arg(0))
			if err := slave.ReplicaSet.Trial(func(master lib.Master) error {
				return master.Assist(ast)
			}); err != nil {
				m.Push(remote.Error(err))
				continue
			}
			if err := m.Push(remote.CommandAssist.Param(remote.AssistOK)); err != nil {
				return err
			}
		} else {
			// check for other common commands
			if err := slave.local(cmd, m); err != nil {
				m.Push(remote.Error(err))
				continue
			}
		}
	}
	return nil
}

func (slave *Slave) Rebuild() error {
	slave.ReplicaSet.Trial(func(n lib.Master) error {
		replicas, err := n.Replicas()
		if err != nil {
			return err
		}
		slave.ReplicaSet.Set(replicas)
		return nil
	})
	return nil
}

func (slave *Slave) Store(key, value string, rev lib.Revision) error {
	return slave.ReplicaSet.Trial(func(m lib.Master) error {
		return m.Store(key, value, rev)
	})
}

func (slave *Slave) LocalStore(key, value string, rev lib.Revision) error {
	if rev.IsNewer(slave.Latest) {
		slave.Latest = rev
	}
	ok := slave.Entries.Store(key, value, rev)
	if !ok {
		return errors.New("Later revision already stored")
	}
	return nil
}

func (slave *Slave) Read(key string) (string, lib.Revision, error) {
	var (
		value    string
		revision lib.Revision
	)
	if err := slave.ReplicaSet.Trial(func(m lib.Master) error {
		val, rev, err := m.Read(key)
		if err != nil {
			return err
		}
		value = val
		revision = rev
		return nil
	}); err != nil {
		return "", nil, err
	}
	return value, revision, nil
}

func (slave *Slave) LocalRead(key string) (string, lib.Revision, error) {
	val, rev, ok := slave.Entries.Read(key)
	if !ok {
		return key, nil, errors.New("Key not found")
	}
	return val, rev, nil
}

func (slave *Slave) Status() lib.Status {
	return slave.NodeStatus
}

func (slave *Slave) Shutdown() error {
	if !slave.KeepAlive {
		return errors.New("Already shutting down")
	}
	slave.KeepAlive = false
	return nil
}

func (slave *Slave) Role() (lib.Role, error) {
	return lib.RoleSlave, nil
}

func (slave *Slave) Revision(rev lib.Revision) (lib.Revision, error) {
	if rev != nil && rev.IsNewer(slave.Latest) {
		slave.Latest = rev
	}
	return slave.Latest, nil
}

func (slave *Slave) Listen() error {
	listener, err := net.Listen("tcp", slave.PublicAddress)
	if err != nil {
		return err
	}
	if slave.ReplicaSet.Size() > 0 {
		go slave.joinExistingCluster()
	}
	for slave.KeepAlive {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		defer conn.Close()
		go slave.handle(&remote.Slave{
			Connection: conn,
		})
	}
	return nil
}

func (slave *Slave) Log(tag string, args ...interface{}) {
	fmt.Print("["+strings.ToUpper(tag)+"] ", fmt.Sprintln(args...))
}

func (slave *Slave) Address() string {
	return slave.PublicAddress
}

func (slave *Slave) Mirror(peers []lib.Node) error {
	for _, peer := range peers {
		if peer.Address() == slave.Address() {
			continue
		}
		keys, err := peer.LocalKeys()
		if err != nil {
			return err
		}
		for _, key := range keys {
			if key == "" {
				continue
			}
			value, revision, err := peer.LocalRead(key)
			if err != nil {
				return err
			}
			slave.LocalStore(key, value, revision)
		}
	}

	return nil
}

func (slave *Slave) Keys() ([]string, error) {
	var keys []string
	if err := slave.ReplicaSet.Trial(func(master lib.Master) error {
		k, err := master.Keys()
		if err != nil {
			return err
		}
		keys = k
		return nil
	}); err != nil {
		return nil, err
	}
	return keys, nil
}

func (slave *Slave) LocalKeys() ([]string, error) {
	return slave.Entries.Keys(), nil
}

func NewSlave(local, rmt string) *Slave {
	slave := &Slave{
		PublicAddress: local,
		ReplicaSet:    replicas.New(),
		Latest:        []byte{},
		KeepAlive:     true,
		Entries:       hashtable.New(),
		NodeStatus:    lib.StatusStartup,
	}
	if rmt != "" {
		slave.ReplicaSet.Join(remote.NewMaster(rmt))
	} else {
		slave.ready()
	}
	return slave
}
