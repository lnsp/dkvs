// Copyright 2017 Lennart Espe. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package local

import (
	"errors"
	"net"

	"hash/fnv"

	log "github.com/Sirupsen/logrus"
	"github.com/lnsp/dkvs/lib"
	"github.com/lnsp/dkvs/lib/local/cluster"
	"github.com/lnsp/dkvs/lib/local/hashtable"
	"github.com/lnsp/dkvs/lib/local/replicas"
	"github.com/lnsp/dkvs/lib/remote"
)

var (
	errSameInstance = errors.New("Same master instance")
)

func (master *Master) parseOptionalRevision(rev string) (lib.Revision, error) {
	if rev != "" {
		given, err := lib.ToRevision(rev)
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
	if err := master.ClusterSet.All(func(n lib.Node) error {
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
			// cluster collects all cluster node IPs and replies them.
			// SYNTAX: CLUSTER -> CLUSTER#NODE1;NODE2;NODE3;...
			if err := m.Push(cmd.Param(master.ClusterSet.Collect()...)); err != nil {
				m.Push(remote.Error(err))
			}
		} else if cmd.KindOf(remote.CommandKeys) {
			// keys collects all keys in the cluster and replies them.
			// SYNTAX: KEYS -> KEYS#KEY1;KEY2;KEY3;...
			keys, err := master.keysInCluster()
			if err != nil {
				m.Push(remote.Error(err))
				continue
			}
			if err := m.Push(cmd.Param(keys...)); err != nil {
				return err
			}
		} else if cmd.KindOf(remote.CommandJoin) {
			// join tells the master that a slave wants to join in.
			// SYNTAX: JOIN#SLAVE-IP -> JOIN#OK
			slave := remote.NewSlave(cmd.Arg(0))
			if err := master.Join(slave); err != nil {
				m.Push(remote.Error(err))
				continue
			}
			if err := m.Push(cmd.Param(remote.JoinOK)); err != nil {
				return err
			}
		} else if cmd.KindOf(remote.CommandAssist) {
			// assist tells the master that a secondary master wants to help out.
			// SYNTAX: ASSIST#MASTER-IP -> ASSIST#OK
			peer := remote.NewMaster(cmd.Arg(0))
			if err := master.Assist(peer); err != nil {
				m.Push(remote.Error(err))
				continue
			}
			if err := m.Push(cmd.Param(remote.AssistOK)); err != nil {
				return err
			}
		} else if cmd.KindOf(remote.CommandRead) {
			// read pulls the key-value pair from the cluster and spills it out.
			// SYNTAX: READ#KEY -> READ#VALUE;REVISION
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
			// store puts the key-value pair in the cluster.
			// SYNTAX: STORE#KEY;VALUE -> STORE#OK
			// the revision key gets fetched from the cluster's state store.
			// SYNTAX: STORE#KEY;VALUE;REVISION -> STORE#OK
			// the given revision will be used to store the pair in the cluster.
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
		} else {
			if err := master.local(cmd, m); err != nil {
				m.Push(remote.Error(err))
			}
		}
	}
	return nil
}

func (master *Master) Read(key string) (string, lib.Revision, error) {
	var value string
	var revision lib.Revision
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
		if err := master.ClusterSet.TrialSelected(targets, func(slave lib.Node) error {
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
		if err := master.ClusterSet.Trial(func(slave lib.Node) error {
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

// ClusterGroup returns the ids of the nodes addressed by the hash.
func (master *Master) ClusterGroup(hash int) []int {
	targets := make([]int, master.ReplicationFactor)
	size := master.ClusterSet.Size()
	for i, j := hash*master.ReplicationFactor, 0; j < master.ReplicationFactor; j++ {
		targets[j] = i
		i = (i + master.ReplicationFactor) % size
	}
	return targets
}

// Store puts a key-value pair in the cluster. It looks up the matching cluster group
// for the key and tells every node in the group to store the key-value pair locally.
func (master *Master) Store(key, value string, rev lib.Revision) error {
	if master.ReplicationFactor > 1 {
		hasher := fnv.New32()
		hasher.Write([]byte(key))
		sum := hasher.Sum32()

		size := master.ClusterSet.Size()
		group := int(sum) % size
		targets := master.ClusterGroup(group)
		if err := master.ClusterSet.Selected(targets, func(slave lib.Node) error {
			return slave.LocalStore(key, value, rev)
		}); err != nil {
			return err
		}
	} else {
		if err := master.ClusterSet.All(func(slave lib.Node) error {
			return slave.LocalStore(key, value, rev)
		}); err != nil {
			return err
		}
	}

	if err := master.ReplicaSet.All(func(m lib.Master) error {
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

func (master *Master) joinExistingCluster(peer lib.Master) error {
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
	log.WithFields(log.Fields{
		"local": master.Address(),
		"peer":  peer.Address(),
	}).Info("Joined existing cluster")
	return nil
}

// Listen waits for new connections.
func (master *Master) Listen() error {
	listener, err := net.Listen("tcp", master.PublicAddress)
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"local": master.PublicAddress,
	}).Info("Waiting for peers")

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
			slave := &remote.Slave{
				Connection: conn,
			}
			err := master.handle(slave)
			if err != nil {
				log.WithFields(log.Fields{
					"error": err,
					"peer":  slave.Address(),
				}).Error("Failed to handle client")
			}
		}(conn)
	}
	return nil
}

// Assist puts the peer node in the replica set and notifies the other cluster masters.
func (master *Master) Assist(p lib.Master) error {
	if master.ReplicaSet.Has(p) {
		return nil
	}

	log.WithFields(log.Fields{
		"peer":    p.Address(),
		"local":   master.Address(),
		"primary": master.Primary,
	}).Info("Request to assist cluster")

	master.ReplicaSet.Join(p)
	master.ReplicaSet.Trial(func(peer lib.Master) error {
		if peer.Address() == master.Address() {
			return errSameInstance
		}
		if peer.Address() == p.Address() {
			return errSameInstance
		}
		log.WithFields(log.Fields{
			"peer":    p.Address(),
			"replica": peer.Address(),
		}).Debug("Forward assist request to replica")
		return peer.Assist(p)
	})
	if !master.Primary {
		return nil
	}

	master.ClusterSet.Trial(func(peer lib.Node) error {
		if peer.Address() == master.Address() {
			return errSameInstance
		}
		if peer.Address() == p.Address() {
			return errSameInstance
		}
		log.WithFields(log.Fields{
			"peer": p.Address(),
			"node": peer.Address(),
		}).Debug("Force rebuild on cluster node")
		return peer.Rebuild()
	})
	return nil
}

// Cluster returns a instance of the cluster set.
func (master *Master) Cluster() ([]lib.Node, error) {
	return master.ClusterSet.Instance(), nil
}

// Join puts the peer node in the cluster set, requests it to mirror one of its group peers
// and notifies the other masters in the replica set.
func (master *Master) Join(p lib.Node) error {
	if master.ClusterSet.Has(p) {
		return nil
	}
	log.WithFields(log.Fields{
		"primary": master.Primary,
		"local":   master.Address(),
		"peer":    p.Address(),
	}).Info("Request to join cluster")
	master.ClusterSet.Join(p)
	mirrors := cluster.New()
	if master.ReplicationFactor > 1 {
		for i := 0; i < master.ClusterSet.Size(); i++ {
			set := master.ClusterSet.FilterSelected(master.ClusterGroup(i), func(n lib.Node) bool { return n.Address() != p.Address() })
			mirrors.Union(set)
		}
	} else {
		mirrors = master.ClusterSet
	}
	if err := p.Mirror(mirrors.Instance()); err != nil {
		log.WithFields(log.Fields{
			"primary": master.Primary,
			"local":   master.Address(),
			"peer":    p.Address(),
			"mirrors": mirrors.Instance(),
		}).Error("Mirror rebuild failed on node")
		return err
	}
	// Find peer and copy keys
	master.ReplicaSet.Trial(func(peer lib.Master) error {
		if peer.Address() == master.Address() {
			return errSameInstance
		}
		if peer.Address() == p.Address() {
			return errSameInstance
		}
		return peer.Join(p)
	})
	return nil
}

// Replicas returns an instance of the replica set.
func (master *Master) Replicas() ([]lib.Master, error) {
	return master.ReplicaSet.Instance(), nil
}

// Role returns the master nodes role.
// If it returns primary, it had no peer on startup.
// if it returns secondary, it joined an existing cluster on startup.
func (master *Master) Role() (lib.Role, error) {
	if master.Primary {
		return lib.RoleMasterPrimary, nil
	}
	return lib.RoleMaster, nil
}

// Rebuild refetches the replica and cluster set.
func (master *Master) Rebuild() error {
	if master.Primary {
		return nil
	}

	master.Slave.Rebuild()
	master.ReplicaSet.Trial(func(peer lib.Master) error {
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
	log.WithFields(log.Fields{
		"nodes": master.ClusterSet.Instance(),
	}).Info("Refetched cluster nodes")
	return nil
}

func (master *Master) ready() {
	master.NodeStatus = lib.StatusReady
}

// NewMaster initializes a new local master runtime.
func NewMaster(local, rmt string, scale int) *Master {
	var master *Master
	master = &Master{
		Slave: Slave{
			PublicAddress: local,
			ReplicaSet:    replicas.New(),
			Latest:        []byte{0},
			KeepAlive:     true,
			Entries:       hashtable.New(),
			NodeStatus:    lib.StatusStartup,
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
