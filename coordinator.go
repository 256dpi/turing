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
	node *dragonboat.NodeHost
}

func createCoordinator(cfg Config) (*coordinator, error) {
	// prepare members
	members := make(map[uint64]string)
	for _, member := range cfg.Members {
		members[member.ID] = member.raftAddr()
	}

	// calculate rrt in ms
	var rttMS = uint64(cfg.RoundTripTime / time.Millisecond)

	// prepare config
	rc := config.Config{
		NodeID:             cfg.ID,
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
		RaftAddress:    cfg.Local().raftAddr(),
	}

	// create node
	node, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, err
	}

	// prepare replicator factory
	factory := func(uint64, uint64) statemachine.IOnDiskStateMachine {
		return newReplicator(cfg)
	}

	// start cluster
	err = node.StartOnDiskCluster(members, false, factory, rc)
	if err != nil {
		return nil, err
	}

	// create coordinator
	rn := &coordinator{
		node: node,
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

	// prepare members
	var members []Member

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

		// parse members
		for id, addr := range info.ClusterInfoList[0].Nodes {
			member, _ := ParseMember(addr)
			member.ID = id
			members = append(members, member)
		}
	}

	// prepare leader
	var leader *Member

	// get leader
	lid, ok, _ := c.node.GetLeaderID(clusterID)
	if ok {
		for _, member := range members {
			if member.ID == lid {
				leader = &member
			}
		}
	}

	// create status
	status := Status{
		ID:      id,
		Role:    role,
		Leader:  leader,
		Members: members,
	}

	return status
}

func (c *coordinator) close() {
	// stop node
	c.node.Stop()
}
