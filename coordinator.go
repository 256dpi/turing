package turing

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/statemachine"

	"github.com/256dpi/turing/wire"
)

const clusterID uint64 = 1

type coordinator struct {
	config      Config
	node        *dragonboat.NodeHost
	staleReads  *bundler
	linearReads *bundler
	writes      *bundler
	session     *client.Session
	operations  []wire.Operation
}

func createCoordinator(cfg Config, registry *registry, manager *manager) (*coordinator, error) {
	// prepare members
	members := make(map[uint64]string)
	for _, member := range cfg.Members {
		members[member.ID] = member.Address()
	}

	// calculate rrt in ms
	var rttMS = uint64(cfg.RoundTripTime / time.Millisecond)

	// TODO: Allow node host tuning.

	// prepare node config
	nodeConfig := config.Config{
		NodeID:             cfg.ID,
		ClusterID:          clusterID,
		CheckQuorum:        true,
		ElectionRTT:        rttMS * 1000, // 1000ms @ 1ms RTT
		HeartbeatRTT:       rttMS * 100,  // 100ms @ 1ms RTT
		SnapshotEntries:    10000,
		CompactionOverhead: 20000,
	}

	// prepare node host config
	hostConfig := config.NodeHostConfig{
		DeploymentID:   clusterID,
		WALDir:         cfg.RaftDir(),
		NodeHostDir:    cfg.RaftDir(),
		RTTMillisecond: rttMS,
		RaftAddress:    cfg.Local().Address(),
		Expert: config.ExpertConfig{
			FS: cfg.RaftFS(),
		},
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
		config:     cfg,
		node:       node,
		session:    node.GetNoOPSession(clusterID),
		operations: make([]wire.Operation, cfg.ProposalBatchSize),
	}

	// create stale read bundler
	coordinator.staleReads = newBundler(bundlerOptions{
		queueSize:   (cfg.ConcurrentReaders + 1) * cfg.LookupBatchSize,
		batchSize:   cfg.LookupBatchSize,
		concurrency: cfg.ConcurrentReaders,
		handler:     coordinator.performStaleLookup,
	})

	// create liner read bundler
	coordinator.linearReads = newBundler(bundlerOptions{
		queueSize:   (cfg.ConcurrentReaders + 1) * cfg.LookupBatchSize,
		batchSize:   cfg.LookupBatchSize,
		concurrency: cfg.ConcurrentReaders,
		handler:     coordinator.performLinearLookup,
	})

	// create write bundler
	coordinator.writes = newBundler(bundlerOptions{
		queueSize:   (cfg.ConcurrentProposers + 1) * cfg.ProposalBatchSize,
		batchSize:   cfg.ProposalBatchSize,
		concurrency: cfg.ConcurrentProposers,
		handler:     coordinator.performUpdates,
	})

	return coordinator, nil
}

var coordinatorUpdate = systemMetrics.WithLabelValues("coordinator.update")

func (c *coordinator) update(ins Instruction, fn func(error)) error {
	// observe
	timer := observe(coordinatorUpdate)
	defer timer.finish()

	// queue update
	err := c.writes.process(ins, fn)
	if err != nil {
		return err
	}

	return nil
}

var coordinatorPerformUpdates = systemMetrics.WithLabelValues("coordinator.performUpdates")

func (c *coordinator) performUpdates(list []Instruction) error {
	// observe
	timer := observe(coordinatorPerformUpdates)
	defer timer.finish()

	// TODO: Clarify the handling of proposal failures, retries and idempotency.

	// prepare command
	cmd := wire.Command{
		Operations: c.operations[:0],
	}

	// add operations
	for _, ins := range list {
		// encode instructions
		encodedInstruction, ref, err := ins.Encode()
		if err != nil {
			return err
		}

		// ensure release
		if ref != nil {
			defer ref.Release()
		}

		// add operation
		cmd.Operations = append(cmd.Operations, wire.Operation{
			Name: ins.Describe().Name,
			Code: encodedInstruction,
		})
	}

	// encode command
	encodedCommand, ref, err := cmd.Encode(true)
	if err != nil {
		return err
	}

	// release
	defer ref.Release()

	// prepare context
	ctx, cancel := context.WithTimeout(context.Background(), c.config.ProposalTimeout)
	defer cancel()

	// propose
	result, err := c.node.SyncPropose(ctx, c.session, encodedCommand)
	if err != nil {
		return err
	}

	// walk command and decode results
	err = wire.WalkCommand(result.Data, func(i int, op wire.Operation) (bool, error) {
		// decode result if available
		if len(op.Code) > 0 {
			return true, list[i].Decode(op.Code)
		}

		return true, nil
	})
	if err != nil {
		return err
	}

	return nil
}

var coordinatorLookup = systemMetrics.WithLabelValues("coordinator.lookup")

func (c *coordinator) lookup(ins Instruction, fn func(error), options Options) error {
	// observe
	timer := observe(coordinatorLookup)
	defer timer.finish()

	// queue read
	if options.StaleRead {
		return c.staleReads.process(ins, fn)
	}

	return c.linearReads.process(ins, fn)
}

var coordinatorPerformStaleLookup = systemMetrics.WithLabelValues("coordinator.performStaleLookup")

func (c *coordinator) performStaleLookup(list []Instruction) error {
	// observe
	timer := observe(coordinatorPerformStaleLookup)
	defer timer.finish()

	// perform stale read
	_, err := c.node.StaleRead(clusterID, list)
	if err != nil {
		return err
	}

	return nil
}

var coordinatorPerformLinearLookup = systemMetrics.WithLabelValues("coordinator.performLinearLookup")

func (c *coordinator) performLinearLookup(list []Instruction) error {
	// observe
	timer := observe(coordinatorPerformLinearLookup)
	defer timer.finish()

	// prepare context
	ctx, cancel := context.WithTimeout(context.Background(), c.config.LinearReadTimeout)
	defer cancel()

	// perform linear read
	_, err := c.node.SyncRead(ctx, clusterID, list)
	if err != nil {
		return err
	}

	return nil
}

var coordinatorStatus = systemMetrics.WithLabelValues("coordinator.status")

func (c *coordinator) status() Status {
	// observe
	timer := observe(coordinatorStatus)
	defer timer.finish()

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
			host, port, _ := net.SplitHostPort(addr)
			portNum, _ := strconv.Atoi(port)
			members = append(members, Member{
				ID:   id,
				Host: host,
				Port: portNum,
			})
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
