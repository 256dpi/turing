package turing

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/dgraph-io/badger"
)

type Node struct {
	opts Config

	db   *badger.DB
	rsm  *rsm
	raft *raftNode
}

func CreateNode(config Config) (*Node, error) {
	/* db */

	// open db
	db, err := openDB(config.dbDir())
	if err != nil {
		return nil, err
	}

	/* rsm */

	// create instruction map
	instructions := make(map[string]Instruction)
	for _, i := range config.Instructions {
		instructions[i.Name()] = i
	}

	// create rsm
	fsm := &rsm{
		db:           db,
		instructions: instructions,
	}

	/* raft */

	// create raft node
	rn, err := newRaftNode(fsm, raftNodeConfig{
		Dir:    config.raftDir(),
		Server: config.nodeRoute(),
		Peers:  config.peerRoutes(),
	})
	if err != nil {
		return nil, err
	}

	/* node */

	// create node
	n := &Node{
		opts: config,
		db:   db,
		rsm:  fsm,
		raft: rn,
	}

	// run rpc server
	go http.ListenAndServe(config.nodeRoute().rpcAddr(), n.rpcEndpoint())

	// run config printer
	go n.confPrinter(config.nodeRoute(), config.peerRoutes())

	return n, nil
}

func (n *Node) Leader() bool {
	return n.raft.isLeader()
}

func (n *Node) Update(i Instruction) error {
	// update on remote if not leader
	if !n.raft.isLeader() {
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
	err = n.raft.apply(cd)
	if err != nil {
		return err
	}

	return nil
}

func (n *Node) updateRemote(i Instruction) error {
	// get leader route
	leader := n.raft.leaderRoute()
	if leader == nil {
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

	// prepare url
	url := fmt.Sprintf("http://%s/update", leader.rpcAddr())

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
	if !forward || n.raft.isLeader() {
		err := n.db.View(func(txn *badger.Txn) error {
			return i.Execute(&Transaction{txn: txn})
		})
		if err != nil {
			return err
		}

		return nil
	}

	// get leader
	leader := n.raft.leaderRoute()
	if leader == nil {
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

	// prepare url
	url := fmt.Sprintf("http://%s/view", leader.rpcAddr())

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
		err = n.raft.apply(cmd)
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

func (n *Node) confPrinter(local route, peers []route) {
	for {
		// wait some time
		time.Sleep(time.Second)

		// collect peers
		var list []string
		for _, peer := range peers {
			list = append(list, peer.name)
		}

		// get leader
		var leader string
		if n.raft.leaderRoute() != nil {
			leader = n.raft.leaderRoute().name
		}

		// print state
		fmt.Printf("Node: %s | State: %s | Leader: %s | Peers: %s\n", local.name, n.raft.state(), leader, strings.Join(list, ", "))
	}
}
