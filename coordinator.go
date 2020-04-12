package turing

import (
	"net"
	"strconv"
	"time"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/plugin/pebble"
	"github.com/lni/dragonboat/v3/statemachine"
)

const clusterID uint64 = 1

type coordinator struct {
	node       *dragonboat.NodeHost
	reads      *bundler
	writes     *bundler
	operations []Operation
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
		FS:             cfg.RaftFS(),
		WALDir:         cfg.RaftDir(),
		NodeHostDir:    cfg.RaftDir(),
		LogDBFactory:   pebble.NewBatchedLogDB,
		RTTMillisecond: rttMS,
		RaftAddress:    cfg.Local().Address(),
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
		node:       node,
		operations: make([]Operation, cfg.UpdateBatchSize),
	}

	// create read bundler
	coordinator.reads = newBundler(bundlerOptions{
		queueSize:   (cfg.ConcurrentReaders + 1) * cfg.LookupBatchSize,
		batchSize:   cfg.LookupBatchSize,
		concurrency: cfg.ConcurrentReaders,
		handler:     coordinator.performLookup,
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

func (c *coordinator) update(ins Instruction) error {
	// observe
	timer := observe(coordinatorUpdate)
	defer timer.finish()

	// queue update
	err := c.writes.process(ins)
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

	// get session
	session := c.node.GetNoOPSession(clusterID)

	// prepare command
	cmd := Command{
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
		defer ref.Release()

		// add operation
		cmd.Operations = append(cmd.Operations, Operation{
			Name: ins.Describe().Name,
			Data: encodedInstruction,
		})
	}

	// encode command
	encodedCommand, ref, err := cmd.Encode(true)
	if err != nil {
		return err
	}

	// release
	defer ref.Release()

	// propose change
	req, err := c.node.Propose(session, encodedCommand, 10*time.Second)
	if err != nil {
		return err
	}

	// ensure release
	defer req.Release()

	// await completion
	data, err := awaitRequest(req)
	if err != nil {
		return err
	}

	// walk command and decode results
	err = WalkCommand(data, func(i int, op Operation) error {
		// decode result if available
		if len(op.Data) > 0 {
			return list[i].Decode(op.Data)
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

var coordinatorLookup = systemMetrics.WithLabelValues("coordinator.lookup")

func (c *coordinator) lookup(ins Instruction, options Options) error {
	// observe
	timer := observe(coordinatorLookup)
	defer timer.finish()

	// immediately queue stale reads
	if options.StaleRead {
		return c.reads.process(ins)
	}

	// otherwise read index for linear reads

	// read index
	req, err := c.node.ReadIndex(clusterID, 10*time.Second)
	if err != nil {
		return err
	}

	// ensure release
	defer req.Release()

	// await completion
	_, err = awaitRequest(req)
	if err != nil {
		return err
	}

	// queue lookup
	err = c.reads.process(ins)
	if err != nil {
		return err
	}

	return nil
}

var coordinatorPerformLookup = systemMetrics.WithLabelValues("coordinator.performLookup")

func (c *coordinator) performLookup(list []Instruction) error {
	// observe
	timer := observe(coordinatorPerformLookup)
	defer timer.finish()

	// perform read as all instructions ar queued after their respective read
	// index request has completed
	_, err := c.node.StaleRead(clusterID, list)
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

func awaitRequest(rs *dragonboat.RequestState) ([]byte, error) {
	r := <-rs.CompletedC
	if r.Completed() {
		return r.GetResult().Data, nil
	} else if r.Rejected() {
		return nil, dragonboat.ErrRejected
	} else if r.Timeout() {
		return nil, dragonboat.ErrTimeout
	} else if r.Terminated() {
		return nil, dragonboat.ErrClusterClosed
	} else if r.Dropped() {
		return nil, dragonboat.ErrClusterNotReady
	}

	panic("unknown result")
}
