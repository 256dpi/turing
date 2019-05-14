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
	var membersFlag = flag.String("members", "", "the cluster members")
	var dirFlag = flag.String("dir", "data", "the data directory")
	var cleanFlag = flag.Bool("clean", false, "remove existing data")

	// parse flags
	flag.Parse()

	// parse server route
	server, err := turing.ParseRoute(*serverFlag)
	if err != nil {
		panic(err)
	}

	// parse member routes
	var members []turing.Route
	for _, member := range strings.Split(*membersFlag, ",") {
		// parse route
		route, err := turing.ParseRoute(member)
		if err != nil {
			panic(err)
		}

		// add member
		members = append(members, route)
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

	// create machine
	machine, err := turing.Create(turing.Config{
		Server:    server,
		Members:   members,
		Directory: directory,
		Instructions: []turing.Instruction{
			&increment{}, &list{},
		},
	})
	if err != nil {
		panic(err)
	}

	// ensure closing
	defer machine.Close()

	// run printer
	go printer(machine)

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

func printer(machine *turing.Machine) {
	for {
		// wait some time
		time.Sleep(time.Second)

		// print info
		fmt.Printf("%+v\n", machine.Status())
	}
}
