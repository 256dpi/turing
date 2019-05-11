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

	// create node
	node, err := turing.CreateNode(turing.Config{
		Name:      *name,
		Host:      *host,
		Port:      *port,
		Directory: dir,
		Peers:     strings.Split(*peers, ","),
		Instructions: []turing.Instruction{
			&Increment{}, &List{},
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
