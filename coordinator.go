package turing

import (
	"context"
	"time"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/plugin/pebble"
	"github.com/lni/dragonboat/v3/statemachine"
)

const clusterID uint64 = 1

type coordinator struct {
	node  *dragonboat.NodeHost
	reads *bundler
}

func createCoordinator(cfg Config, registry *registry, manager *manager) (*coordinator, error) {
	// prepare members
	members := make(map[uint64]string)
	for _, member := range cfg.Members {
		members[member.ID] = member.raftAddr()
	}

	// calculate rrt in ms
	var rttMS = uint64(cfg.RoundTripTime / time.Millisecond)

	// prepare node config
	nodeConfig := config.Config{
		NodeID:             cfg.ID,
		ClusterID:          clusterID,
		CheckQuorum:        true,
		ElectionRTT:        rttMS * 1000, // 1000ms @ 1ms RTT
		HeartbeatRTT:       rttMS * 100,  // 100ms @ 1ms RTT
		SnapshotEntries:    10000,
		CompactionOverhead: 10000,
	}

	// prepare node host config
	hostConfig := config.NodeHostConfig{
		DeploymentID:   clusterID,
		FS:             cfg.raftFS(),
		WALDir:         cfg.raftDir(),
		NodeHostDir:    cfg.raftDir(),
		LogDBFactory:   pebble.NewBatchedLogDB,
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
		return newReplicator(cfg, registry, manager)
	}

	// start cluster
	err = node.StartOnDiskCluster(members, false, factory, nodeConfig)
	if err != nil {
		return nil, err
	}

	// create coordinator
	coordinator := &coordinator{
		node: node,
		reads: newBundler(1000, 200, 4, func(list []Instruction) error {
			_, err := node.StaleRead(clusterID, list)
			return err
		}),
	}

	return coordinator, nil
}

func (c *coordinator) update(instruction Instruction) error {
	// observe
	timer := observe(operationMetrics, "coordinator.update")
	defer timer.ObserveDuration()

	// TODO: Make timeout configurable.

	// create context
	var cancel context.CancelFunc
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// get session
	session := c.node.GetNoOPSession(clusterID)

	// encode instruction
	encodedInstruction, err := instruction.Encode()
	if err != nil {
		return err
	}

	// prepare command
	cmd := Command{
		Name: instruction.Describe().Name,
		Data: encodedInstruction,
	}

	// encode command
	encodedCommand, err := EncodeCommand(cmd)
	if err != nil {
		return err
	}

	// TODO: Retry on ErrTimeout.

	// perform update
	result, err := c.node.SyncPropose(ctx, session, encodedCommand)
	if err != nil {
		return err
	}

	// decode result
	if result.Data != nil {
		err = instruction.Decode(result.Data)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *coordinator) lookup(ins Instruction, rc ReadConcern) error {
	// observe
	timer := observe(operationMetrics, "coordinator.lookup")
	defer timer.ObserveDuration()

	// immediately queue local ready
	if rc == Local {
		return c.reads.process(ins)
	}

	// otherwise read for linear reads

	// read index
	req, err := c.node.ReadIndex(clusterID, 10*time.Second)
	if err != nil {
		return err
	}

	// ensure release
	defer req.Release()

	// TODO: Retry on Timeout?

	// await completion
	res := <-req.CompletedC
	if !res.Completed() {
		if res.Timeout() {
			return dragonboat.ErrTimeout
		} else if res.Terminated() {
			return dragonboat.ErrClusterClosed
		} else if res.Dropped() {
			return dragonboat.ErrClusterNotReady
		}
	}

	// lookup data
	err = c.reads.process(ins)
	if err != nil {
		return err
	}

	return nil
}

func (c *coordinator) status() Status {
	// observe
	timer := observe(operationMetrics, "coordinator.status")
	defer timer.ObserveDuration()

	// get info
	info := c.node.GetNodeHostInfo(dragonboat.NodeHostInfoOption{
		SkipLogInfo: true,
	})

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
