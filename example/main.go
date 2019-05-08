package main

import (
	"flag"
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

	// create node
	node, err := turing.CreateNode(turing.Options{
		Name:      *name,
		Host:      *host,
		Port:      *port,
		Directory: dir,
		Peers:     strings.Split(*peers, ","),
		Instructions: []turing.Instruction{
			&Set{}, &Get{}, &Del{},
		},
	})
	if err != nil {
		panic(err)
	}

	// ensure closing
	defer node.Close()

	for {
		// sleep
		time.Sleep(time.Second)

		// check leadership
		if !node.Leader() {
			continue
		}

		// set key
		err = node.Update(&Set{Key: "foo", Value: "bar"})
		if err != nil {
			println(err.Error())
		}

		// get key
		get := &Get{Key: "foo"}
		err = node.View(get)
		if err != nil {
			println(err.Error())
		}

		println(get.Value)

		// delete key
		err = node.Update(&Del{Key: "foo"})
		if err != nil {
			println(err.Error())
		}
	}
}
