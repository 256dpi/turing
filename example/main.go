package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/256dpi/turing"
)

func main() {
	// prepare flags
	var serverFlag = flag.String("server", "n1@0.0.0.0:42000", "the server")
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

	// add server name
	directory = filepath.Join(directory, server.Name)

	// remove all previous data if requested
	if *cleanFlag {
		err = os.RemoveAll(directory)
		if err != nil {
			panic(err)
		}
	}

	// prepare config
	config := turing.NodeConfig{
		Server:    server,
		Peers:     peers,
		Directory: directory,
		Instructions: []turing.Instruction{
			&Increment{}, &List{},
		},
	}

	// create node
	node, err := turing.CreateNode(config)
	if err != nil {
		panic(err)
	}

	// ensure closing
	defer node.Close()

	// run printer
	go printer(node, config)

	for {
		// sleep
		time.Sleep(time.Second)

		// set value
		set := &Increment{Key: server.Name}
		err = node.Update(set)
		if err != nil {
			println(err.Error())
			continue
		}

		// print instruction
		fmt.Printf("==> %+v\n", set)

		// list values
		list := &List{}
		err = node.View(list, true)
		if err != nil {
			println(err.Error())
			continue
		}

		// print instruction
		fmt.Printf("==> %+v\n", list)
	}
}

func printer(node *turing.Node, config turing.NodeConfig) {
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
		if node.Leader() != nil {
			leader = node.Leader().Name
		}

		// print state
		fmt.Printf("Node: %s | State: %s | Leader: %s | peers: %s\n", config.Server.Name, node.State(), leader, strings.Join(list, ", "))
	}
}
