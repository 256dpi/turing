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

type coordinatorConfig struct {
	directory string
	server    route
	peers     []route
}

type coordinator struct {
	config coordinatorConfig
	raft   *raft.Raft

	leaderRouteCache struct {
		sync.Mutex
		current  *route
		lastAddr string
	}
}

func createCoordinator(sm *stateMachine, cfg coordinatorConfig) (*coordinator, error) {
	// prepare raft config
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.server.name)
	raftConfig.SnapshotThreshold = 1024
	raftConfig.LogOutput = os.Stdout

	// resolve local address for advertisements
	localAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", cfg.server.raftPort()))
	if err != nil {
		return nil, err
	}

	// create raft transport
	transport, err := raft.NewTCPTransport(cfg.server.raftAddr(), localAddr, 3, 10*time.Second, os.Stdout)
	if err != nil {
		return nil, err
	}

	// create raft file snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(cfg.directory, 2, os.Stdout)
	if err != nil {
		return nil, err
	}

	// create bolt db based raft store
	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.directory, "coordinator.db"))
	if err != nil {
		return nil, err
	}

	// check if already bootstrapped
	bootstrapped, err := raft.HasExistingState(boltStore, boltStore, snapshotStore)
	if err != nil {
		return nil, err
	}

	// create raft instance
	rft, err := raft.NewRaft(raftConfig, sm, boltStore, boltStore, snapshotStore, transport)
	if err != nil {
		return nil, err
	}

	// bootstrap cluster
	if !bootstrapped {
		// prepare servers
		servers := []raft.Server{
			{
				ID:      raft.ServerID(cfg.server.name),
				Address: raft.ServerAddress(cfg.server.raftAddr()),
			},
		}

		// add raft peers
		for _, peer := range cfg.peers {
			// check if self
			if peer.name == cfg.server.name {
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
	rn := &coordinator{
		config: cfg,
		raft:   rft,
	}

	return rn, nil
}

func (n *coordinator) apply(cmd []byte) error {
	return n.raft.Apply(cmd, 10*time.Second).Error()
}

func (n *coordinator) isLeader() bool {
	return n.raft.State() == raft.Leader
}

func (n *coordinator) leaderRoute() *route {
	// acquire mutex
	n.leaderRouteCache.Lock()
	defer n.leaderRouteCache.Unlock()

	// get leader address
	addr := string(n.raft.Leader())
	if addr == "" {
		return nil
	}

	// return existing route if leader has not changed
	if addr == n.leaderRouteCache.lastAddr {
		println("fast path")
		return n.leaderRouteCache.current
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
	for _, peer := range n.config.peers {
		if peer.host == host && peer.raftPort() == port {
			// set current route
			n.leaderRouteCache.current = &peer
			n.leaderRouteCache.lastAddr = addr

			return &peer
		}
	}

	return nil
}

func (n *coordinator) state() string {
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
