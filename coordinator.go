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
	node   *dragonboat.NodeHost
	reads  *bundler
	writes *bundler
}

func createCoordinator(cfg Config, registry *registry, manager *manager) (*coordinator, error) {
	// prepare members
	members := make(map[uint64]string)
	for _, member := range cfg.Members {
		members[member.ID] = member.raftAddr()
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
	}

	// create read bundler
	coordinator.reads = newBundler(bundlerOptions{
		queueSize:   (cfg.ConcurrentReaders + 1) * cfg.BatchSize,
		batchSize:   cfg.BatchSize,
		concurrency: cfg.ConcurrentReaders,
		handler:     coordinator.performLookup,
	})

	// create write bundler
	coordinator.writes = newBundler(bundlerOptions{
		queueSize:   (cfg.ConcurrentProposers + 1) * cfg.BatchSize,
		batchSize:   cfg.BatchSize,
		concurrency: cfg.ConcurrentProposers,
		handler:     coordinator.performUpdates,
	})

	return coordinator, nil
}

func (c *coordinator) update(ins Instruction) error {
	// observe
	timer := observe(operationMetrics, "coordinator.update")
	defer timer.finish()

	// queue update
	err := c.writes.process(ins)
	if err != nil {
		return err
	}

	return nil
}

func (c *coordinator) performUpdates(list []Instruction) error {
	// observe
	timer := observe(operationMetrics, "coordinator.performUpdates")
	defer timer.finish()

	// get session
	session := c.node.GetNoOPSession(clusterID)

	// prepare command
	cmd := Command{
		Operations: make([]Operation, 0, len(list)),
	}

	// add operations
	for _, ins := range list {
		// encode instructions
		encodedInstruction, err := ins.Encode()
		if err != nil {
			return err
		}

		// add operation
		cmd.Operations = append(cmd.Operations, Operation{
			Name: ins.Describe().Name,
			Data: encodedInstruction,
		})
	}

	// encode command
	encodedCommand, err := cmd.Encode()
	if err != nil {
		return err
	}

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

	// decode command (no need to clone as result is not reused)
	err = cmd.Decode(data, false)
	if err != nil {
		return err
	}

	// decode instructions
	for i, op := range cmd.Operations {
		err = list[i].Decode(op.Data)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *coordinator) lookup(ins Instruction, options Options) error {
	// observe
	timer := observe(operationMetrics, "coordinator.lookup")
	defer timer.finish()

	// immediately queue stale reads
	if options.StaleRead {
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

func (c *coordinator) performLookup(list []Instruction) error {
	// observe
	timer := observe(operationMetrics, "coordinator.performLookup")
	defer timer.finish()

	// perform read as all instructions ar queued after their respective read
	// index request has completed
	_, err := c.node.StaleRead(clusterID, list)
	if err != nil {
		return err
	}

	return nil
}

func (c *coordinator) status() Status {
	// observe
	timer := observe(operationMetrics, "coordinator.status")
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
