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
	var idFlag = flag.Uint64("id", 1, "the server id")
	var membersFlag = flag.String("members", "", "the cluster members")
	var dirFlag = flag.String("dir", "data", "the data directory")
	var cleanFlag = flag.Bool("clean", false, "remove existing data")

	// parse flags
	flag.Parse()

	// parse members
	var members []turing.Member
	for _, member := range strings.Split(*membersFlag, ",") {
		// parse member
		member, err := turing.ParseMember(member)
		if err != nil {
			panic(err)
		}

		// add member
		members = append(members, member)
	}

	// resolve directory
	directory, err := filepath.Abs(*dirFlag)
	if err != nil {
		panic(err)
	}

	// append server name
	directory = filepath.Join(directory, strconv.FormatUint(*idFlag, 10))

	// remove all previous data if requested
	if *cleanFlag {
		err = os.RemoveAll(directory)
		if err != nil {
			panic(err)
		}
	}

	// start machine
	machine, err := turing.Start(turing.Config{
		Member:    *idFlag,
		Members:   members,
		Directory: directory,
		Instructions: []turing.Instruction{
			&increment{}, &list{},
		},
	})
	if err != nil {
		panic(err)
	}

	// ensure stop
	defer machine.Stop()

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
		set := &increment{Key: strconv.FormatUint(*idFlag, 10)}
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
