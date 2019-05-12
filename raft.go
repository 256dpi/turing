package turing

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

type raftNodeConfig struct {
	Dir    string
	Server route
	Peers  []route
}

type raftNode struct {
	config raftNodeConfig
	raft   *raft.Raft

	leaderMutex  sync.Mutex
	currentRoute *route
	lastLeader   string
}

func newRaftNode(rsm *rsm, config raftNodeConfig) (*raftNode, error) {
	// prepare raft config
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.Server.name)
	raftConfig.SnapshotThreshold = 1024
	raftConfig.LogOutput = os.Stdout

	// resolve local address for advertisements
	localAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", config.Server.raftPort()))
	if err != nil {
		return nil, err
	}

	// create raft transport
	transport, err := raft.NewTCPTransport(config.Server.raftAddr(), localAddr, 3, 10*time.Second, os.Stdout)
	if err != nil {
		return nil, err
	}

	// create raft file snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(config.Dir, 2, os.Stdout)
	if err != nil {
		return nil, err
	}

	// create bolt db based raft store
	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(config.Dir, "raft.db"))
	if err != nil {
		return nil, err
	}

	// check if already bootstrapped
	bootstrapped, err := raft.HasExistingState(boltStore, boltStore, snapshotStore)
	if err != nil {
		return nil, err
	}

	// create raft instance
	rft, err := raft.NewRaft(raftConfig, rsm, boltStore, boltStore, snapshotStore, transport)
	if err != nil {
		return nil, err
	}

	// bootstrap cluster
	if !bootstrapped {
		// prepare servers
		servers := []raft.Server{
			{
				ID:      raft.ServerID(config.Server.name),
				Address: raft.ServerAddress(config.Server.raftAddr()),
			},
		}

		// add raft peers
		for _, peer := range config.Peers {
			// check if self
			if peer.name == config.Server.name {
				continue
			}

			// add peer
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(peer.name),
				Address: raft.ServerAddress(peer.raftAddr()),
			})
		}

		// bootstrap raft node
		err = rft.BootstrapCluster(raft.Configuration{
			Servers: servers,
		}).Error()
		if err != nil {
			return nil, err
		}
	}

	// create raft node
	rn := &raftNode{
		config: config,
		raft:   rft,
	}

	return rn, nil
}

func (n *raftNode) apply(cmd []byte) error {
	return n.raft.Apply(cmd, 10*time.Second).Error()
}

func (n *raftNode) isLeader() bool {
	return n.raft.State() == raft.Leader
}

func (n *raftNode) leaderRoute() *route {
	// acquire mutex
	n.leaderMutex.Lock()
	defer n.leaderMutex.Unlock()

	// get leader address
	addr := string(n.raft.Leader())
	if addr == "" {
		return nil
	}

	// return existing route if leader has not changed
	if addr == n.lastLeader {
		println("fast path")
		return n.currentRoute
	}

	// parse addr
	host, portString, _ := net.SplitHostPort(addr)
	if portString == "" {
		return nil
	}

	// default to 0.0.0.0
	if host == "" {
		host = "0.0.0.0"
	}

	// parse port
	port, _ := strconv.Atoi(portString)
	if port == 0 {
		return nil
	}

	// select peer
	for _, peer := range n.config.Peers {
		if peer.host == host && peer.raftPort() == port {
			// set current route
			n.currentRoute = &peer
			n.lastLeader = addr

			return &peer
		}
	}

	return nil
}

func (n *raftNode) state() string {
	switch n.raft.State() {
	case raft.Follower:
		return "follower"
	case raft.Candidate:
		return "candidate"
	case raft.Leader:
		return "leader"
	case raft.Shutdown:
		return "shutdown"
	default:
		return "unknown"
	}
}
