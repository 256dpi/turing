package turing

import (
	"context"
	"sync"
	"time"

	"github.com/lni/dragonboat"
	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/logger"
	"github.com/lni/dragonboat/statemachine"
)

type coordinator struct {
	raft *dragonboat.NodeHost

	server Route
	peers  []Route

	leaderCache struct {
		sync.Mutex
		current  *Route
		lastAddr string
	}
}

func createCoordinator(cfg MachineConfig) (*coordinator, error) {
	// prepare peers
	peers := make(map[uint64]string)
	for _, peer := range cfg.Peers {
		peers[peer.ID] = peer.raftAddr()
	}

	// change the log verbosity
	logger.GetLogger("dragonboat").SetLevel(logger.WARNING)
	logger.GetLogger("raft").SetLevel(logger.WARNING)
	logger.GetLogger("rsm").SetLevel(logger.WARNING)
	logger.GetLogger("transport").SetLevel(logger.WARNING)
	logger.GetLogger("grpc").SetLevel(logger.WARNING)
	logger.GetLogger("logdb").SetLevel(logger.WARNING)
	logger.GetLogger("config").SetLevel(logger.WARNING)
	logger.GetLogger("server").SetLevel(logger.WARNING)

	// prepare config
	rc := config.Config{
		NodeID:             cfg.Server.ID,
		ClusterID:          1,
		CheckQuorum:        true,
		ElectionRTT:        10,
		HeartbeatRTT:       1,
		SnapshotEntries:    1000,
		CompactionOverhead: 1000,
	}

	// prepare node host config
	nhc := config.NodeHostConfig{
		DeploymentID:   1,
		WALDir:         cfg.raftDir(),
		NodeHostDir:    cfg.raftDir(),
		RTTMillisecond: 50,
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
		raft:   nh,
		server: cfg.Server,
		peers:  cfg.Peers,
	}

	return rn, nil
}

func (n *coordinator) update(cmd []byte) ([]byte, error) {
	// prepare context
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// get session
	session := n.raft.GetNoOPSession(1)

	// update data
	result, err := n.raft.SyncPropose(ctx, session, cmd)
	if err != nil {
		return nil, err
	}

	return result.Data, nil
}

func (n *coordinator) lookup(cmd []byte) ([]byte, error) {
	// prepare context
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// lookup data
	result, err := n.raft.SyncRead(ctx, 1, cmd)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (n *coordinator) isLeader() bool {
	id, ok, _ := n.raft.GetLeaderID(1)
	return ok && id == n.server.ID
}

func (n *coordinator) state() string {
	if n.isLeader() {
		return "leader"
	} else {
		return "follower"
	}
}
