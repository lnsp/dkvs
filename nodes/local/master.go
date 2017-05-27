package local

import (
	"errors"
	"net"

	"hash/fnv"

	"github.com/lnsp/dkvs/nodes"
	"github.com/lnsp/dkvs/nodes/local/cluster"
	"github.com/lnsp/dkvs/nodes/local/hashtable"
	"github.com/lnsp/dkvs/nodes/local/replicas"
	"github.com/lnsp/dkvs/nodes/remote"
)

var (
	errSameInstance = errors.New("Same master instance")
)

func (master *Master) parseOptionalRevision(rev string) (nodes.Revision, error) {
	if rev != "" {
		given, err := nodes.ToRevision(rev)
		if err != nil {
			return nil, err
		}
		return given, nil
	}
	local, err := master.Revision(nil)
	if err != nil {
		return nil, err
	}
	return local.Increase(), nil
}

func (master *Master) keysInCluster() ([]string, error) {
	keyMap := make(map[string]bool)
	if err := master.ClusterSet.All(func(n nodes.Node) error {
		keys, err := n.LocalKeys()
		if err != nil {
			return err
		}
		for _, key := range keys {
			keyMap[key] = true
		}
		return nil
	}); err != nil {
		return nil, err
	}
	keys := make([]string, len(keyMap))
	i := 0
	for key := range keyMap {
		keys[i] = key
		i++
	}
	return keys, nil
}

func (master *Master) handle(m *remote.Slave) error {
	defer m.Close()
	for master.KeepAlive {
		cmd, err := m.Poll()
		if err != nil {
			return err
		}
		if cmd.KindOf(remote.CommandCluster) {
			if err := m.Push(cmd.Param(master.ClusterSet.Collect()...)); err != nil {
				m.Push(remote.Error(err))
			}
		} else if cmd.KindOf(remote.CommandKeys) {
			keys, err := master.keysInCluster()
			if err != nil {
				m.Push(remote.Error(err))
				continue
			}
			if err := m.Push(cmd.Param(keys...)); err != nil {
				return err
			}
		} else if cmd.KindOf(remote.CommandJoin) {
			slave := remote.NewSlave(cmd.Arg(0))
			if err := master.Join(slave); err != nil {
				m.Push(remote.Error(err))
				continue
			}
			if err := m.Push(cmd.Param(remote.JoinOK)); err != nil {
				return err
			}
		} else if cmd.KindOf(remote.CommandAssist) {
			peer := remote.NewMaster(cmd.Arg(0))
			if err := master.Assist(peer); err != nil {
				m.Push(remote.Error(err))
				continue
			}
			if err := m.Push(cmd.Param(remote.AssistOK)); err != nil {
				return err
			}
		} else if cmd.KindOf(remote.CommandRead) {
			key := cmd.Arg(0)
			value, revision, err := master.Read(key)
			if err != nil {
				m.Push(remote.Error(err))
				continue
			}
			if err := m.Push(cmd.Param(value, revision.String())); err != nil {
				return err
			}
		} else if cmd.KindOf(remote.CommandStore) {
			key, value, suggested := cmd.Arg(0), cmd.Arg(1), cmd.Arg(2)
			valid, err := master.parseOptionalRevision(suggested)
			if err != nil {
				m.Push(remote.Error(err))
				continue
			}
			if err := master.Store(key, value, valid); err != nil {
				m.Push(remote.Error(err))
				continue
			}
			if err := m.Push(cmd.Param(remote.StoreOK)); err != nil {
				return err
			}
		} else if cmd.KindOf(remote.CommandLocalStore) {
			key, value, revString := cmd.Arg(0), cmd.Arg(1), cmd.Arg(2)
			rev, err := nodes.ToRevision(revString)
			if err != nil {
				m.Push(remote.Error(err))
				continue
			}
			if err := master.LocalStore(key, value, rev); err != nil {
				m.Push(remote.Error(err))
				continue
			}
			if err := m.Push(cmd.Param(remote.StoreOK)); err != nil {
				return err
			}
		} else if cmd.KindOf(remote.CommandRead) {
			key := cmd.Arg(0)
			value, revision, err := master.Read(key)
			if err != nil {
				m.Push(remote.Error(err))
				continue
			}
			if err := m.Push(cmd.Param(value, revision.String())); err != nil {
				return err
			}
		} else if cmd.KindOf(remote.CommandLocalRead) {
			key := cmd.Arg(0)
			val, rev, err := master.LocalRead(key)
			if err != nil {
				m.Push(remote.Error(err))
				continue
			}
			if err := m.Push(cmd.Param(val, rev.String())); err != nil {
				return err
			}
		} else {
			if err := master.local(cmd, m); err != nil {
				m.Push(remote.Error(err))
			}
		}
	}
	return nil
}

func (master *Master) Read(key string) (string, nodes.Revision, error) {
	var value string
	var revision nodes.Revision
	if master.ReplicationFactor > 1 {
		hasher := fnv.New32()
		hasher.Write([]byte(key))
		sum := hasher.Sum32()

		targets := make([]int, master.ReplicationFactor)
		size := master.ClusterSet.Size()
		start := int(sum) % size
		for i, j := start, 0; j < master.ReplicationFactor; j++ {
			targets[j] = i
			i = (i + master.ReplicationFactor) % size
		}
		if err := master.ClusterSet.TrialSelected(targets, func(slave nodes.Node) error {
			val, rev, err := slave.LocalRead(key)
			if err != nil {
				return err
			}
			value = val
			revision = rev
			return nil
		}); err != nil {
			return "", nil, err
		}
	} else {
		if err := master.ClusterSet.Trial(func(slave nodes.Node) error {
			val, rev, err := slave.LocalRead(key)
			if err != nil {
				return err
			}
			value = val
			revision = rev
			return nil
		}); err != nil {
			return "", nil, err
		}
	}

	return value, revision, nil
}

func (master *Master) ClusterGroup(id int) []int {
	targets := make([]int, master.ReplicationFactor)
	size := master.ClusterSet.Size()
	for i, j := id, 0; j < master.ReplicationFactor; j++ {
		targets[j] = i
		i = (i + master.ReplicationFactor) % size
	}
	return targets
}

func (master *Master) Store(key, value string, rev nodes.Revision) error {
	if master.ReplicationFactor > 1 {
		hasher := fnv.New32()
		hasher.Write([]byte(key))
		sum := hasher.Sum32()

		size := master.ClusterSet.Size()
		group := int(sum) % size
		targets := master.ClusterGroup(group)
		if err := master.ClusterSet.Selected(targets, func(slave nodes.Node) error {
			return slave.LocalStore(key, value, rev)
		}); err != nil {
			return err
		}
	} else {
		if err := master.ClusterSet.All(func(slave nodes.Node) error {
			return slave.LocalStore(key, value, rev)
		}); err != nil {
			return err
		}
	}

	if err := master.ReplicaSet.All(func(m nodes.Master) error {
		_, err := m.Revision(rev)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (master *Master) joinExistingCluster(peer nodes.Master) error {
	master.ready()
	if err := peer.Assist(master); err != nil {
		return err
	}
	if err := peer.Join(master); err != nil {
		return err
	}
	if err := master.Rebuild(); err != nil {
		return err
	}
	return nil
}

func (master *Master) Listen() error {
	listener, err := net.Listen("tcp", master.PublicAddress)
	if err != nil {
		return err
	}

	if !master.Primary {
		go master.joinExistingCluster(master.ReplicaSet.Item())
	}

	for master.KeepAlive {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		defer conn.Close()
		go func(conn net.Conn) {
			err := master.handle(&remote.Slave{
				Connection: conn,
			})
			if err != nil {
				master.Log("error", err)
			}
		}(conn)
	}
	return nil
}

func (master *Master) Assist(p nodes.Master) error {
	if !master.ReplicaSet.Has(p) {
		master.ReplicaSet.Join(p)
		master.ReplicaSet.Trial(func(peer nodes.Master) error {
			if peer.Address() == master.Address() {
				return errSameInstance
			}
			if peer.Address() == p.Address() {
				return errSameInstance
			}
			return peer.Assist(p)
		})
	}

	if master.Primary {
		master.ClusterSet.Trial(func(peer nodes.Node) error {
			if peer.Address() == master.Address() {
				return errSameInstance
			}
			if peer.Address() == p.Address() {
				return errSameInstance
			}
			return peer.Rebuild()
		})
	}

	return nil
}

func (master *Master) Cluster() ([]nodes.Node, error) {
	return master.ClusterSet.Instance(), nil
}

func (master *Master) Join(p nodes.Node) error {
	if !master.ClusterSet.Has(p) {
		master.ClusterSet.Join(p)
		mirrors := cluster.New()
		if master.ReplicationFactor > 1 {
			for i := 0; i < master.ClusterSet.Size(); i++ {
				set := master.ClusterSet.FilterSelected(master.ClusterGroup(i), func(n nodes.Node) bool { return n.Address() != p.Address() })
				mirrors.Union(set)
			}
		} else {
			mirrors = master.ClusterSet
		}
		if err := p.Mirror(mirrors.Instance()); err != nil {
			return err
		}

		// Find peer and copy keys
		master.ReplicaSet.Trial(func(peer nodes.Master) error {
			if peer.Address() == master.Address() {
				return errSameInstance
			}
			if peer.Address() == p.Address() {
				return errSameInstance
			}
			return peer.Join(p)
		})
	}
	return nil
}

func (master *Master) Replicas() ([]nodes.Master, error) {
	return master.ReplicaSet.Instance(), nil
}

func (master *Master) Role() (nodes.Role, error) {
	if master.Primary {
		return nodes.RoleMasterPrimary, nil
	}
	return nodes.RoleMaster, nil
}

func (master *Master) Rebuild() error {
	if master.Primary {
		return nil
	}
	master.Slave.Rebuild()
	master.ReplicaSet.Trial(func(peer nodes.Master) error {
		if peer.Address() == master.Address() {
			return errSameInstance
		}
		nodes, err := peer.Cluster()
		if err != nil {
			return err
		}
		master.ClusterSet.Set(nodes)
		return nil
	})
	return nil
}

func (master *Master) ready() {
	master.NodeStatus = nodes.StatusReady
}

func NewMaster(local, rmt string, scale int) *Master {
	var master *Master
	master = &Master{
		Slave: Slave{
			PublicAddress: local,
			ReplicaSet:    replicas.New(),
			Latest:        []byte{0},
			KeepAlive:     true,
			Entries:       hashtable.New(),
			NodeStatus:    nodes.StatusStartup,
		},
		ReplicationFactor: scale,
		ClusterSet:        cluster.New(),
		Primary:           rmt == "",
	}
	if master.Primary {
		master.ReplicaSet.Join(master)
		master.ClusterSet.Join(master)
		master.ready()
	} else {
		master.ReplicaSet.Join(remote.NewMaster(rmt))
	}
	return master
}
