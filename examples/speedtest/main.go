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
	// prepare flags
	var idFlag = flag.Uint64("id", 1, "the server id")
	var membersFlag = flag.String("members", "", "the cluster members")
	var dirFlag = flag.String("dir", "data", "the data directory")
	var devFlag = flag.Bool("dev", false, "enable development mode")

	// parse flags
	flag.Parse()

	// enable debugging
	god.Debug(6060 + int(*idFlag))
	god.Metrics()

	// disable logging
	turing.SetLogger(nil)

	// register metrics
	turing.RegisterMetrics()

	// parse members
	var members []turing.Member
	if *membersFlag != "" {
		for _, member := range strings.Split(*membersFlag, ",") {
			// parse member
			member, err := turing.ParseMember(member)
			if err != nil {
				panic(err)
			}

			// add member
			members = append(members, member)
		}
	}

	// resolve directory
	directory, err := filepath.Abs(*dirFlag)
	if err != nil {
		panic(err)
	}

	// append server name
	directory = filepath.Join(directory, strconv.FormatUint(*idFlag, 10))

	// remove all previous data
	err = os.RemoveAll(directory)
	if err != nil {
		panic(err)
	}

	// start machine
	machine, err := turing.Start(turing.Config{
		ID:          *idFlag,
		Members:     members,
		Directory:   directory,
		Development: *devFlag,
		Instructions: []turing.Instruction{
			&increment{}, &retrieve{},
		},
	})
	if err != nil {
		panic(err)
	}

	// ensure stop
	defer machine.Stop()

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

var writeCounter = god.NewCounter("write", nil)
var readCounter = god.NewCounter("read", nil)
var errorCounter = god.NewCounter("error", nil)

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

		// increment value
		increment := &increment{Key: strconv.Itoa(rand.Intn(keySpace)), Value: 1}
		err := machine.Execute(nil, increment, false)
		if err != nil {
			errorCounter.Add(1)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// increment
		writeCounter.Add(1)
	}
}

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

		// retrieve value
		retrieve := &retrieve{Key: strconv.Itoa(rand.Intn(keySpace))}
		err := machine.Execute(nil, retrieve, false)
		if err != nil {
			errorCounter.Add(1)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// retrieve
		readCounter.Add(1)
	}
}
