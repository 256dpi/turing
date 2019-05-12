package turing

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/dgraph-io/badger"
)

var ErrNoLeader = errors.New("no leader")

type Machine struct {
	database    *database
	replicator  *replicator
	coordinator *coordinator
}

func CreateMachine(config MachineConfig) (*Machine, error) {
	// check config
	err := config.check()
	if err != nil {
		return nil, err
	}

	// open database
	database, err := openDatabase(config.dbDir(), config.Logger)
	if err != nil {
		return nil, err
	}

	// create replicator
	replicator := newReplicator(database, config.Instructions)

	// create coordinator
	coordinator, err := createCoordinator(replicator, config.raftDir(), config.Server, config.Peers, config.Logger)
	if err != nil {
		return nil, err
	}

	// create machine
	n := &Machine{
		database:    database,
		replicator:  replicator,
		coordinator: coordinator,
	}

	// run rpc server
	go http.ListenAndServe(config.Server.rpcAddr(), n.rpcEndpoint())

	return n, nil
}

func (m *Machine) IsLeader() bool {
	return m.coordinator.isLeader()
}

func (m *Machine) Leader() *Route {
	return m.coordinator.leader()
}

func (m *Machine) State() string {
	return m.coordinator.state()
}

func (m *Machine) Update(i Instruction) error {
	// update on remote if not leader
	if !m.coordinator.isLeader() {
		return m.updateRemote(i)
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
	result, err := m.coordinator.apply(cd)
	if err != nil {
		return err
	}

	// decode result
	if result != nil {
		err = json.Unmarshal(result, i)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Machine) updateRemote(i Instruction) error {
	// get leader route
	leader := m.coordinator.leader()
	if leader == nil {
		return ErrNoLeader
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
	res, err := client.Post(url, "application/json", bytes.NewReader(cd))
	if err != nil {
		return err
	}

	// ensure closing
	defer res.Body.Close()

	// unmarshal instruction
	err = json.NewDecoder(res.Body).Decode(i)
	if err != nil {
		return err
	}

	return nil
}

func (m *Machine) View(i Instruction, forward bool) error {
	// execute instruction locally if leader or not forwarded
	if !forward || m.coordinator.isLeader() {
		err := m.database.View(func(txn *badger.Txn) error {
			return i.Execute(&Transaction{txn: txn})
		})
		if err != nil {
			return err
		}

		return nil
	}

	// get leader
	leader := m.coordinator.leader()
	if leader == nil {
		return ErrNoLeader
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

	// decode instruction
	err = json.NewDecoder(res.Body).Decode(i)
	if err != nil {
		return err
	}

	return nil
}

func (m *Machine) Close() {
	// TODO: Implement close.
}

func (m *Machine) rpcEndpoint() http.Handler {
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
		result, err := m.coordinator.apply(cmd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// write result
		_, err = w.Write(result)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})

	// add view handler
	mux.HandleFunc("/view", func(w http.ResponseWriter, r *http.Request) {
		// parse command
		var c command
		err := json.NewDecoder(r.Body).Decode(&c)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// get factory instruction
		factory, ok := m.replicator.instructions[c.Name]
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
		err = m.database.View(func(txn *badger.Txn) error {
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

		// write result
		_, err = w.Write(id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})

	return mux
}
