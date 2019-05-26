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

func createCoordinator(cfg Config, manager *manager) (*coordinator, error) {
	// observe
	defer observe(operationMetrics.WithLabelValues("createCoordinator"))()

	// prepare members
	members := make(map[uint64]string)
	for _, member := range cfg.Members {
		members[member.ID] = member.raftAddr()
	}

	// calculate rrt in ms
	var rttMS = uint64(cfg.RoundTripTime / time.Millisecond)

	// prepare node config
	nodeConfig := config.Config{
		NodeID:             cfg.Member,
		ClusterID:          1,
		CheckQuorum:        true,
		ElectionRTT:        10000 / rttMS, // 10s
		HeartbeatRTT:       1000 / rttMS,  // 1s
		SnapshotEntries:    10000,
		CompactionOverhead: 10000,
	}

	// prepare node host config
	hostConfig := config.NodeHostConfig{
		DeploymentID:   clusterID,
		WALDir:         cfg.raftDir(),
		NodeHostDir:    cfg.raftDir(),
		RTTMillisecond: rttMS,
		RaftAddress:    cfg.Local().raftAddr(),
	}

	// create node host
	node, err := dragonboat.NewNodeHost(hostConfig)
	if err != nil {
		return nil, err
	}

	// prepare replicator factory
	factory := func(uint64, uint64) statemachine.IOnDiskStateMachine {
		return newReplicator(cfg, manager)
	}

	// start cluster
	err = node.StartOnDiskCluster(members, false, factory, nodeConfig)
	if err != nil {
		return nil, err
	}

	// create coordinator
	coordinator := &coordinator{
		node: node,
	}

	return coordinator, nil
}

func (c *coordinator) update(cmd []byte) ([]byte, error) {
	// observe
	defer observe(operationMetrics.WithLabelValues("coordinator.update"))()

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

func (c *coordinator) lookup(instruction Instruction, nonLinear bool) (e error) {
	// observe
	defer observe(operationMetrics.WithLabelValues("coordinator.lookup"))()

	// prepare context
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// use faster non linear read if available
	if nonLinear {
		_, err := c.node.StaleRead(clusterID, instruction)
		if err != nil {
			return err
		}

		return nil
	}

	// lookup data
	_, err := c.node.SyncRead(ctx, clusterID, instruction)
	if err != nil {
		return err
	}

	return nil
}

func (c *coordinator) status() Status {
	// observe
	defer observe(operationMetrics.WithLabelValues("coordinator.status"))()

	// get info
	info := c.node.GetNodeHostInfo()

	// prepare id and role
	var id uint64
	role := RoleFollower

	// prepare members
	var members []Member

	// check info
	if len(info.ClusterInfoList) > 0 {
		// set id
		id = info.ClusterInfoList[0].NodeID

		// set observer
		if info.ClusterInfoList[0].IsObserver {
			role = RoleObserver
		}

		// set leader
		if info.ClusterInfoList[0].IsLeader {
			role = RoleLeader
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
		MemberID: id,
		Role:     role,
		Leader:   leader,
		Members:  members,
	}

	return status
}

func (c *coordinator) close() {
	// observe
	defer observe(operationMetrics.WithLabelValues("coordinator.close"))()

	// stop node
	c.node.Stop()
}
