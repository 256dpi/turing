package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/256dpi/god"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/256dpi/turing"
	"github.com/256dpi/turing/stdset"
)

var id = flag.Uint64("id", 0, "the server id")
var members = flag.String("members", "", "the cluster members")
var directory = flag.String("directory", "data", "the data directory")

func main() {
	// parse flags
	flag.Parse()

	// enable debugging
	god.Init(god.Options{
		Port:           6060 + int(*id),
		MetricsHandler: promhttp.Handler().ServeHTTP,
	})

	// parse members
	memberList, err := turing.ParseMembers(*members)
	if err != nil {
		panic(err)
	}

	// resolve directory
	directory, err := filepath.Abs(*directory)
	if err != nil {
		panic(err)
	}

	// append server id
	directory = filepath.Join(directory, strconv.FormatUint(*id, 10))

	// start machine
	machine, err := turing.Start(turing.Config{
		ID:        *id,
		Members:   memberList,
		Directory: directory,
		Instructions: []turing.Instruction{
			&stdset.Inc{}, &stdset.Map{},
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
		err = machine.Execute(&stdset.Inc{
			Key:   strconv.AppendUint(nil, *id, 10),
			Value: 1,
		})
		if err != nil {
			println(err.Error())
			time.Sleep(time.Second)
			continue
		}

		// list values
		mp := &stdset.Map{}
		err = machine.Execute(mp)
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
