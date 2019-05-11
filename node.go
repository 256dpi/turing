package turing

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
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
	rsm  *rsm
	serf *serf.Serf
	raft *raft.Raft
}

func CreateNode(opts Options) (*Node, error) {
	pretty.Println(opts)

	/* db */

	// open db
	db, err := openDB(opts.dbDir())
	if err != nil {
		return nil, err
	}

	/* rsm */

	// create instruction map
	instructions := make(map[string]Instruction)
	for _, i := range opts.Instructions {
		instructions[i.Name()] = i
	}

	// create rsm
	fsm := &rsm{
		db:           db,
		instructions: instructions,
	}

	/* serf */

	// prepare memberlist config
	memberlistConfig := memberlist.DefaultLANConfig()
	memberlistConfig.Name = opts.Name
	memberlistConfig.BindAddr = opts.Host
	memberlistConfig.BindPort = opts.nodeRoute().serfPort()
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
	for _, peer := range opts.peerRoutes() {
		serfPeers = append(serfPeers, peer.serfAddr())
	}

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
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", opts.nodeRoute().raftPort()))
	if err != nil {
		return nil, err
	}

	// create raft transport
	transport, err := raft.NewTCPTransport(opts.nodeRoute().raftAddr(), addr, 3, 10*time.Second, os.Stdout)
	if err != nil {
		return nil, err
	}

	// create raft file snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(opts.raftDir(), 2, os.Stdout)
	if err != nil {
		return nil, err
	}

	// create bolt db based raft store
	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(opts.raftDir(), "raft.db"))
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
		for _, peer := range opts.peerRoutes() {
			// check if self
			if peer.name == opts.Name {
				continue
			}

			// add peer
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(peer.name),
				Address: raft.ServerAddress(peer.raftAddr()),
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

	/* node */

	// create node
	n := &Node{
		opts: opts,
		db:   db,
		rsm:  fsm,
		serf: srf,
		raft: rft,
	}

	// run rpc server
	go http.ListenAndServe(opts.nodeRoute().rpcAddr(), n.rpcEndpoint())

	// run serf handler
	go n.serfHandler(serfEvents)

	// run config printer
	go n.confPrinter()

	return n, nil
}

func (n *Node) Leader() bool {
	return n.raft.State() == raft.Leader
}

func (n *Node) Update(i Instruction) error {
	// check if leader
	if n.raft.State() != raft.Leader {
		return n.updateRemote(i)
	}

	// encode instruction
	id, err := i.Encode()
	if err != nil {
		return err
	}

	// prepare command
	cmd := &command{
		Name: i.Name(),
		Data: id,
	}

	// encode command
	cd, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	// apply command
	err = n.raft.Apply(cd, 10*time.Second).Error()
	if err != nil {
		return err
	}

	return nil
}

func (n *Node) updateRemote(i Instruction) error {
	// get leader
	leader := string(n.raft.Leader())
	if leader == "" {
		return fmt.Errorf("no leader")
	}

	// encode instruction
	id, err := i.Encode()
	if err != nil {
		return err
	}

	// prepare command
	cmd := command{
		Name: i.Name(),
		Data: id,
	}

	// encode command
	cd, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	// parse route
	route := parseRoute(string(n.raft.Leader()))
	route.port--

	// prepare url
	url := fmt.Sprintf("http://%s/update", route.rpcAddr())

	// create client
	client := http.Client{}

	// run request
	_, err = client.Post(url, "application/json", bytes.NewReader(cd))
	if err != nil {
		return err
	}

	return nil
}

func (n *Node) View(i Instruction, forward bool) error {
	// execute instruction locally if leader or not forwarded
	if !forward || n.raft.State() == raft.Leader {
		err := n.db.View(func(txn *badger.Txn) error {
			return i.Execute(&Transaction{txn: txn})
		})
		if err != nil {
			return err
		}

		return nil
	}

	// get leader
	leader := string(n.raft.Leader())
	if leader == "" {
		return fmt.Errorf("no leader")
	}

	// encode instruction
	id, err := i.Encode()
	if err != nil {
		return err
	}

	// prepare command
	cmd := command{
		Name: i.Name(),
		Data: id,
	}

	// encode command
	cd, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	// parse route
	route := parseRoute(string(n.raft.Leader()))
	route.port--

	// prepare url
	url := fmt.Sprintf("http://%s/view", route.rpcAddr())

	// create client
	client := http.Client{}

	// run request
	res, err := client.Post(url, "application/json", bytes.NewReader(cd))
	if err != nil {
		return err
	}

	// ensure closing
	defer res.Body.Close()

	// parse command
	var c command
	err = json.NewDecoder(res.Body).Decode(&c)
	if err != nil {
		return err
	}

	// decode instruction
	err = i.Decode(c.Data)
	if err != nil {
		return err
	}

	return nil
}

func (n *Node) Close() {
	// TODO: Implement close.
}

func (n *Node) rpcEndpoint() http.Handler {
	// create mux
	mux := http.NewServeMux()

	// add update handler
	mux.HandleFunc("/update", func(w http.ResponseWriter, r *http.Request) {
		// read command
		cmd, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// apply command
		err = n.raft.Apply(cmd, 0).Error()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})

	// add update handler
	mux.HandleFunc("/view", func(w http.ResponseWriter, r *http.Request) {
		// parse command
		var c command
		err := json.NewDecoder(r.Body).Decode(&c)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// get factory instruction
		factory, ok := n.rsm.instructions[c.Name]
		if !ok {
			http.Error(w, "missing instruction", http.StatusInternalServerError)
			return
		}

		// create new instruction
		instruction := factory.Build()

		// decode instruction
		err = instruction.Decode(c.Data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// execute instruction locally
		err = n.db.View(func(txn *badger.Txn) error {
			return instruction.Execute(&Transaction{txn: txn})
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// encode instruction
		id, err := instruction.Encode()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// prepare command
		cmd := &command{
			Name: instruction.Name(),
			Data: id,
		}

		// encode command
		cd, err := json.Marshal(cmd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// write result
		_, _ = w.Write(cd)
	})

	return mux
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
