package turing

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

type coordinator struct {
	raft  *raft.Raft
	peers []Route

	leaderCache struct {
		sync.Mutex
		current  *Route
		lastAddr string
	}
}

func createCoordinator(sm *stateMachine, dir string, server Route, peers []Route, logger io.Writer) (*coordinator, error) {
	// prepare raft config
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(server.Name)
	raftConfig.SnapshotThreshold = 1024
	raftConfig.Logger = log.New(logger, "", log.LstdFlags)

	// resolve local address for advertisements
	localAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", server.raftPort()))
	if err != nil {
		return nil, err
	}

	// create raft transport
	transport, err := raft.NewTCPTransport(server.raftAddr(), localAddr, 3, 10*time.Second, logger)
	if err != nil {
		return nil, err
	}

	// create raft file snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(dir, 2, os.Stdout)
	if err != nil {
		return nil, err
	}

	// create bolt db based raft store
	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(dir, "raft.db"))
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
				ID:      raft.ServerID(server.Name),
				Address: raft.ServerAddress(server.raftAddr()),
			},
		}

		// add raft peers
		for _, peer := range peers {
			// check if self
			if peer.Name == server.Name {
				continue
			}

			// add peer
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(peer.Name),
				Address: raft.ServerAddress(peer.raftAddr()),
			})
		}

		// bootstrap cluster
		err = rft.BootstrapCluster(raft.Configuration{
			Servers: servers,
		}).Error()
		if err != nil {
			return nil, err
		}
	}

	// create coordinator
	rn := &coordinator{
		raft:  rft,
		peers: peers,
	}

	return rn, nil
}

func (n *coordinator) apply(cmd []byte) error {
	return n.raft.Apply(cmd, 10*time.Second).Error()
}

func (n *coordinator) isLeader() bool {
	return n.raft.State() == raft.Leader
}

func (n *coordinator) leader() *Route {
	// acquire mutex
	n.leaderCache.Lock()
	defer n.leaderCache.Unlock()

	// get leader address
	addr := string(n.raft.Leader())
	if addr == "" {
		return nil
	}

	// return existing route if leader has not changed
	if addr == n.leaderCache.lastAddr {
		return n.leaderCache.current
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
	for _, peer := range n.peers {
		if peer.Host == host && peer.raftPort() == port {
			// set current route
			n.leaderCache.current = &peer
			n.leaderCache.lastAddr = addr

			return &peer
		}
	}

	return nil
}

func (n *coordinator) state() string {
	return n.raft.State().String()
}
