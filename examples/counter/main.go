package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/256dpi/turing"
)

func main() {
	// prepare flags
	var serverFlag = flag.String("server", "1@0.0.0.0:42010", "the server")
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
	directory = filepath.Join(directory, strconv.FormatUint(server.ID, 10))

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
		set := &increment{Key: strconv.FormatUint(server.ID, 10)}
		err = machine.Execute(set)
		if err != nil {
			println(err.Error())
			time.Sleep(time.Second)
			continue
		}

		// print instruction
		fmt.Printf("==> %+v\n", set)

		// list values
		list := &list{}
		err = machine.Execute(list)
		if err != nil {
			println(err.Error())
			time.Sleep(time.Second)
			continue
		}

		// print instruction
		fmt.Printf("==> %+v\n", list)
	}
}

func printer(machine *turing.Machine, config turing.MachineConfig) {
	for {
		// wait some time
		time.Sleep(time.Second)

		// prepare ser er
		server := strconv.FormatUint(config.Server.ID, 10)

		// collect peers
		var peers []string
		for _, peer := range config.Peers {
			peers = append(peers, strconv.FormatUint(peer.ID, 10))
		}

		// get leader
		var leader string
		if machine.Leader() != nil {
			leader = strconv.FormatUint(machine.Leader().ID, 10)
		}

		// print info
		fmt.Printf("[%s] State: %s | Leader: %s | Peers: %s\n", server, machine.State(), leader, strings.Join(peers, ", "))
	}
}
