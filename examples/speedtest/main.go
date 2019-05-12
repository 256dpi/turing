package main

import (
	"flag"
	"fmt"
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
	"github.com/montanaflynn/stats"

	"github.com/256dpi/turing"
)

var wg sync.WaitGroup

var send int64
var recv int64
var diffs []float64
var mutex sync.Mutex

func main() {
	// enable debugging
	god.Debug()

	// prepare flags
	var serverFlag = flag.String("server", "n1@0.0.0.0:42010", "the server")
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
	directory = filepath.Join(directory, server.Name)

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
			&increment{},
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
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go writer(machine, done)
	}

	// run printer
	wg.Add(1)
	go printer(machine, done)

	// prepare exit
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)
	<-exit

	// close control channel
	close(done)
	wg.Wait()
}

func writer(machine *turing.Machine, done <-chan struct{}) {
	// signal return
	defer wg.Done()

	// write entries forever
	for {
		// limit rate
		select {
		case <-time.After(5 * time.Microsecond):
		case <-done:
			return
		}

		// measure start
		start := time.Now()

		// prepare instruction
		increment := &increment{
			Key:   strconv.Itoa(rand.Intn(20)),
			Value: 1,
		}

		// run update
		err := machine.Update(increment)
		if err == turing.ErrNoLeader {
			time.Sleep(time.Second)
			continue
		} else if err != nil {
			panic(err)
		}

		// calculate diff
		diff := float64(time.Since(start)) / float64(time.Millisecond)

		// increment
		mutex.Lock()
		send += 1
		diffs = append(diffs, diff)
		mutex.Unlock()
	}
}

func printer(machine *turing.Machine, done <-chan struct{}) {
	// signal return
	defer wg.Done()

	// create ticker
	ticker := time.Tick(time.Second)

	for {
		// await signal
		select {
		case <-ticker:
		case <-done:
			return
		}

		// get data
		mutex.Lock()
		r := recv
		s := send
		d := diffs
		recv = 0
		send = 0
		diffs = nil
		mutex.Unlock()

		// get stats
		min, _ := stats.Min(d)
		max, _ := stats.Max(d)
		mean, _ := stats.Mean(d)
		p90, _ := stats.Percentile(d, 90)
		p95, _ := stats.Percentile(d, 95)
		p99, _ := stats.Percentile(d, 99)

		// print rate
		fmt.Printf("state: %s, ", machine.State())
		fmt.Printf("send: %d msg/s, ", s)
		fmt.Printf("recv %d msgs/s, ", r)
		fmt.Printf("min: %.2fms, ", min)
		fmt.Printf("mean: %.2fms, ", mean)
		fmt.Printf("p90: %.2fms, ", p90)
		fmt.Printf("p95: %.2fms, ", p95)
		fmt.Printf("p99: %.2fms, ", p99)
		fmt.Printf("max: %.2fms\n", max)
	}
}
