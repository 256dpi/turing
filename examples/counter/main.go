package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/256dpi/turing"
)

func main() {
	// prepare flags
	var serverFlag = flag.String("server", "n1@0.0.0.0:42010", "the server")
	var peersFlag = flag.String("peers", "", "the cluster peers")
	var dirFlag = flag.String("dir", "data", "the data directory")
	var cleanFlag = flag.Bool("clean", false, "remove existing data")

	// parse flags
	flag.Parse()

	// parse server route
	server, err := turing.ParseRoute(*serverFlag)
	if err != nil {
		panic(err)
	}

	// parse peer routes
	var peers []turing.Route
	for _, peer := range strings.Split(*peersFlag, ",") {
		// parse route
		route, err := turing.ParseRoute(peer)
		if err != nil {
			panic(err)
		}

		// add peer
		peers = append(peers, route)
	}

	// resolve directory
	directory, err := filepath.Abs(*dirFlag)
	if err != nil {
		panic(err)
	}

	// append server name
	directory = filepath.Join(directory, server.Name)

	// remove all previous data if requested
	if *cleanFlag {
		err = os.RemoveAll(directory)
		if err != nil {
			panic(err)
		}
	}

	// prepare config
	config := turing.MachineConfig{
		Server:    server,
		Peers:     peers,
		Directory: directory,
		Instructions: []turing.Instruction{
			&increment{}, &list{},
		},
	}

	// create machine
	machine, err := turing.CreateMachine(config)
	if err != nil {
		panic(err)
	}

	// ensure closing
	defer machine.Close()

	// run printer
	go printer(machine, config)

	// prepare exit
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

	for {
		// sleep
		select {
		case <-time.After(time.Second):
		case <-exit:
			return
		}

		// set value
		set := &increment{Key: server.Name}
		err = machine.Update(set)
		if err == turing.ErrNoLeader {
			time.Sleep(time.Second)
			continue
		} else if err != nil {
			panic(err)
		}

		// print instruction
		fmt.Printf("==> %+v\n", set)

		// list values
		list := &list{}
		err = machine.View(list, true)
		if err == turing.ErrNoLeader {
			time.Sleep(time.Second)
			continue
		} else if err != nil {
			panic(err)
		}

		// print instruction
		fmt.Printf("==> %+v\n", list)
	}
}

func printer(machine *turing.Machine, config turing.MachineConfig) {
	for {
		// wait some time
		time.Sleep(time.Second)

		// collect peers
		var list []string
		for _, peer := range config.Peers {
			list = append(list, peer.Name)
		}

		// get leader
		var leader string
		if machine.Leader() != nil {
			leader = machine.Leader().Name
		}

		// print info
		fmt.Printf("[%s] State: %s | Leader: %s | Peers: %s\n", config.Server.Name, machine.State(), leader, strings.Join(list, ", "))
	}
}
