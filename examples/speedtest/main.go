package main

import (
	"flag"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/256dpi/god"

	"github.com/256dpi/turing"
)

const writers = 1000
const readers = 1000
const keySpace = 1000

var wg sync.WaitGroup

func main() {
	// enable debugging
	god.Debug()
	god.Metrics()

	// disable logging
	turing.SetLogger(nil)

	// prepare flags
	var serverFlag = flag.String("server", "1@0.0.0.0:42010", "the server")
	var peersFlag = flag.String("peers", "", "the cluster peers")
	var dirFlag = flag.String("dir", "data", "the data directory")

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

	// remove all previous data
	err = os.RemoveAll(directory)
	if err != nil {
		panic(err)
	}

	// prepare config
	config := turing.MachineConfig{
		Server:    server,
		Peers:     peers,
		Directory: directory,
		Instructions: []turing.Instruction{
			&increment{}, &retrieve{},
		},
	}

	// create machine
	machine, err := turing.CreateMachine(config)
	if err != nil {
		panic(err)
	}

	// ensure closing
	defer machine.Close()

	// create control channel
	done := make(chan struct{})

	// run writers
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go writer(machine, done)
	}

	// run readers
	wg.Add(readers)
	for i := 0; i < readers; i++ {
		go reader(machine, done)
	}

	// prepare exit
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)
	<-exit

	// close control channel
	close(done)
	wg.Wait()
}

var writeCounter = god.NewCounter("write")
var writeTimer = god.NewTimer("write")

func writer(machine *turing.Machine, done <-chan struct{}) {
	// signal return
	defer wg.Done()

	// write entries forever
	for {
		// check if done
		select {
		case <-done:
			return
		default:
		}

		// measure start
		start := time.Now()

		// prepare instruction
		increment := &increment{
			Key:   strconv.Itoa(rand.Intn(keySpace)),
			Value: 1,
		}

		// run update
		err := machine.Execute(increment)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		// increment
		writeCounter.Add(1)
		writeTimer.Add(time.Since(start))
	}
}

var readCounter = god.NewCounter("read")
var readTimer = god.NewTimer("read")

func reader(machine *turing.Machine, done <-chan struct{}) {
	// signal return
	defer wg.Done()

	// write entries forever
	for {
		// check if done
		select {
		case <-done:
			return
		default:
		}

		// measure start
		start := time.Now()

		// prepare instruction
		retrieve := &retrieve{
			Key: strconv.Itoa(rand.Intn(keySpace)),
		}

		// run update
		err := machine.Execute(retrieve)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		// retrieve
		readCounter.Add(1)
		readTimer.Add(time.Since(start))
	}
}
