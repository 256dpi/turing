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
	var name = flag.String("name", "n1", "the node name")
	var host = flag.String("host", "0.0.0.0", "the node host")
	var port = flag.Int("port", 42000, "the node port")
	var peers = flag.String("peers", "", "the cluster peers")

	// parse flags
	flag.Parse()

	// prepare dir
	dir, err := filepath.Abs("./data/" + *name)
	if err != nil {
		panic(err)
	}

	// remove all previous data
	err = os.RemoveAll(dir)
	if err != nil {
		panic(err)
	}

	// parse peer routes
	var peerRoutes []turing.Route
	for _, peer := range strings.Split(*peers, ",") {
		route, err := turing.ParseRoute(peer)
		if err != nil {
			panic(err)
		}
		peerRoutes = append(peerRoutes, route)
	}

	// prepare config
	config := turing.NodeConfig{
		Server:    turing.NewRoute(*name, *host, *port),
		Directory: dir,
		Peers:     peerRoutes,
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
		set := &Increment{Key: *name}
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
