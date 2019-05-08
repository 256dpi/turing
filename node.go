package turing

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
	"github.com/kr/pretty"
)

type Node struct {
	opts Options

	db   *badger.DB
	fsm  *fsm
	serf *serf.Serf
	raft *raft.Raft
}

func CreateNode(opts Options) (*Node, error) {
	pretty.Println(opts)

	/* db */

	// open db
	db, err := openDB(opts.DBDir())
	if err != nil {
		return nil, err
	}

	/* fsm */

	// create instruction map
	instructions := make(map[string]Instruction)
	for _, i := range opts.Instructions {
		instructions[i.Name()] = i
	}

	// create fsm
	fsm := &fsm{
		db:           db,
		instructions: instructions,
	}

	/* serf */

	// prepare memberlist config
	memberlistConfig := memberlist.DefaultLANConfig()
	memberlistConfig.Name = opts.Name
	memberlistConfig.BindAddr = opts.Host
	memberlistConfig.BindPort = opts.SerfPort()
	memberlistConfig.LogOutput = os.Stdout

	// prepare events
	serfEvents := make(chan serf.Event, 16)

	// prepare serf config
	serfConfig := serf.DefaultConfig()
	serfConfig.NodeName = opts.Name
	serfConfig.EventCh = serfEvents
	serfConfig.MemberlistConfig = memberlistConfig
	serfConfig.LogOutput = os.Stdout

	// create serf
	srf, err := serf.Create(serfConfig)
	if err != nil {
		return nil, err
	}

	// prepare serf peers
	var serfPeers []string
	for _, peer := range opts.PeerRoutes() {
		serfPeers = append(serfPeers, peer.Addr())
	}

	pretty.Println(serfPeers)

	// join other serf peers if available
	if len(serfPeers) > 0 {
		_, err = srf.Join(serfPeers, false)
		if err != nil {
			//	return nil, err
		}
	}

	/* raft */

	// prepare raft config
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(opts.Name)
	config.SnapshotThreshold = 1024
	config.LogOutput = os.Stdout

	// resolve raft binding
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", opts.RaftPort()))
	if err != nil {
		return nil, err
	}

	// create raft transport
	transport, err := raft.NewTCPTransport(opts.RaftAddr(), addr, 3, 10*time.Second, os.Stdout)
	if err != nil {
		return nil, err
	}

	// create raft file snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(opts.RaftDir(), 2, os.Stdout)
	if err != nil {
		return nil, err
	}

	// create bolt db based raft store
	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(opts.RaftDir(), "raft.db"))
	if err != nil {
		return nil, err
	}

	// check if already bootstrapped
	bootstrapped, err := raft.HasExistingState(boltStore, boltStore, snapshotStore)
	if err != nil {
		return nil, err
	}

	// create raft instance
	rft, err := raft.NewRaft(config, fsm, boltStore, boltStore, snapshotStore, transport)
	if err != nil {
		return nil, err
	}

	// bootstrap cluster
	if !bootstrapped {
		// prepare servers
		servers := []raft.Server{
			{
				ID:      raft.ServerID(opts.Name),
				Address: transport.LocalAddr(),
			},
		}

		// add raft peers
		for _, peer := range opts.PeerRoutes() {
			// check if self
			if peer.Name == opts.Name {
				continue
			}

			// add peer
			peer.Port++
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(peer.Name),
				Address: raft.ServerAddress(peer.Addr()),
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

	// create node
	n := &Node{
		opts: opts,
		db:   db,
		fsm:  fsm,
		serf: srf,
		raft: rft,
	}

	// run serf handler
	go n.serfHandler(serfEvents)

	// run config printer
	go n.confPrinter()

	return n, nil
}

func (n *Node) Update(i Instruction) error {
	// // check state
	// if n.raft.State() != raft.Leader {
	// 	return ErrNotLeader
	// }

	// encode instruction
	data, err := i.Encode()
	if err != nil {
		return err
	}

	// prepare command
	cmd := &command{
		Name: i.Name(),
		Data: data,
	}

	// encode command
	bytes, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	// apply command
	err = n.raft.Apply(bytes, 10*time.Second).Error()
	if err != nil {
		return err
	}

	return nil
}

func (n *Node) View(i Instruction) (interface{}, error) {
	// TODO: Implement view command.

	return nil, nil
}

func (n *Node) Close() {
	// TODO: Implement close.
}

func (n *Node) confPrinter() {
	for {
		// wait some time
		time.Sleep(time.Second)

		// get configuration
		future := n.raft.GetConfiguration()
		err := future.Error()
		if err != nil {
			println(err.Error())
			continue
		}

		// collect peers
		var peers []string
		for _, server := range future.Configuration().Servers {
			peers = append(peers, fmt.Sprintf("%s@%s", server.ID, server.Address))
		}

		// print state
		fmt.Printf("State: %s | Peers: %s\n", n.raft.String(), peers)
	}
}

func (n *Node) serfHandler(events <-chan serf.Event) {
	for {
		// await event
		ev := <-events

		// prepare leader check
		err := n.raft.VerifyLeader().Error()
		if err != nil {
			println(err.Error())
			continue
		}

		// coerce event
		memberEvent, ok := ev.(serf.MemberEvent)
		if !ok {
			continue
		}

		// handle members
		for _, member := range memberEvent.Members {
			// raft port of peer
			peerName := raft.ServerID(member.Name)
			peerAddr := raft.ServerAddress(member.Addr.String() + ":" + strconv.Itoa(int(member.Port+1)))

			// handle event
			switch memberEvent.EventType() {
			case serf.EventMemberJoin:
				err = n.raft.AddVoter(peerName, peerAddr, 0, 0).Error()
				if err != nil {
					println(err.Error())
					continue
				}
			case serf.EventMemberLeave, serf.EventMemberFailed, serf.EventMemberReap:
				err = n.raft.RemoveServer(peerName, 0, 0).Error()
				if err != nil {
					println(err.Error())
					continue
				}
			}
		}
	}
}
