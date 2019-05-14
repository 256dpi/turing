package turing

import (
	"context"
	"time"

	"github.com/lni/dragonboat"
	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/statemachine"
)

const clusterID uint64 = 1

type coordinator struct {
	node   *dragonboat.NodeHost
	server Route
	peers  []Route
}

func createCoordinator(cfg MachineConfig) (*coordinator, error) {
	// prepare peers
	peers := make(map[uint64]string)
	for _, peer := range cfg.Peers {
		peers[peer.ID] = peer.raftAddr()
	}

	// calculate rrt in ms
	var rttMS = uint64(cfg.RoundTripTime / time.Millisecond)

	// prepare config
	rc := config.Config{
		NodeID:             cfg.Server.ID,
		ClusterID:          1,
		CheckQuorum:        true,
		ElectionRTT:        10000 / rttMS, // 10s
		HeartbeatRTT:       1000 / rttMS,  // 1s
		SnapshotEntries:    10000,
		CompactionOverhead: 10000,
	}

	// prepare node host config
	nhc := config.NodeHostConfig{
		DeploymentID:   clusterID,
		WALDir:         cfg.raftDir(),
		NodeHostDir:    cfg.raftDir(),
		RTTMillisecond: rttMS,
		RaftAddress:    cfg.Server.raftAddr(),
	}

	// create node host
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, err
	}

	// prepare replicator factory
	factory := func(uint64, uint64) statemachine.IOnDiskStateMachine {
		return newReplicator(cfg)
	}

	// start cluster
	err = nh.StartOnDiskCluster(peers, false, factory, rc)
	if err != nil {
		return nil, err
	}

	// create coordinator
	rn := &coordinator{
		node:   nh,
		server: cfg.Server,
		peers:  cfg.Peers,
	}

	return rn, nil
}

func (c *coordinator) update(cmd []byte) ([]byte, error) {
	// prepare context
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// get session
	session := c.node.GetNoOPSession(clusterID)

	// update data
	result, err := c.node.SyncPropose(ctx, session, cmd)
	if err != nil {
		return nil, err
	}

	return result.Data, nil
}

func (c *coordinator) lookup(cmd []byte) ([]byte, error) {
	// prepare context
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// lookup data
	result, err := c.node.SyncRead(ctx, clusterID, cmd)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (c *coordinator) status() Status {
	// get info
	info := c.node.GetNodeHostInfo()

	// prepare id and role
	var id uint64
	role := Follower

	// prepare peers
	var peers []Route

	// check info
	if len(info.ClusterInfoList) > 0 {
		// set id
		id = info.ClusterInfoList[0].NodeID

		// set observer
		if info.ClusterInfoList[0].IsObserver {
			role = Observer
		}

		// set leader
		if info.ClusterInfoList[0].IsLeader {
			role = Leader
		}

		// parse peers
		for id, addr := range info.ClusterInfoList[0].Nodes {
			peer, _ := ParseRoute(addr)
			peer.ID = id
			peers = append(peers, peer)
		}
	}

	// prepare leader
	var leader *Route

	// get leader id
	lid, ok, _ := c.node.GetLeaderID(clusterID)
	if ok {
		// find route
		for _, peer := range c.peers {
			if peer.ID == lid {
				leader = &peer
			}
		}
	}

	// create status
	status := Status{
		ID:     id,
		Role:   role,
		Leader: leader,
		Peers:  peers,
	}

	return status
}

func (c *coordinator) close() {
	// stop node
	c.node.Stop()
}
