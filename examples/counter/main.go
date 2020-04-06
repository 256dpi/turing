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
	"github.com/256dpi/turing/std/basic"
)

func main() {
	// prepare flags
	var idFlag = flag.Uint64("id", 1, "the server id")
	var membersFlag = flag.String("members", "", "the cluster members")
	var directoryFlag = flag.String("directory", "data", "the data directory")

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
	directory, err := filepath.Abs(*directoryFlag)
	if err != nil {
		panic(err)
	}

	// append server id
	directory = filepath.Join(directory, strconv.FormatUint(*idFlag, 10))

	// start machine
	machine, err := turing.Start(turing.Config{
		ID:        *idFlag,
		Members:   members,
		Directory: directory,
		Instructions: []turing.Instruction{
			&basic.Inc{}, &basic.Map{},
		},
	})
	if err != nil {
		panic(err)
	}

	// ensure stop
	defer machine.Stop()

	// subscribe observer
	machine.Subscribe(&observer{})

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

		// increment value
		err = machine.Execute(nil, &basic.Inc{
			Key:   strconv.AppendUint(nil, *idFlag, 10),
			Value: 1,
		}, false)
		if err != nil {
			println(err.Error())
			time.Sleep(time.Second)
			continue
		}

		// list values
		mp := &basic.Map{}
		err = machine.Execute(nil, mp, false)
		if err != nil {
			println(err.Error())
			time.Sleep(time.Second)
			continue
		}

		// print instruction
		fmt.Printf("KEYS: %+v\n", mp)
	}
}

type observer struct{}

func (*observer) Init() {
	fmt.Printf("==> Init\n")
}

func (*observer) Process(i turing.Instruction) bool {
	fmt.Printf("==> %+v\n", i)
	return true
}

func printer(machine *turing.Machine) {
	for {
		// wait some time
		time.Sleep(time.Second)

		// print info
		fmt.Printf("STATUS: %+v\n", machine.Status())
	}
}
