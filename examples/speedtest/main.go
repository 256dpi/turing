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
	"github.com/lni/dragonboat/v3"

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
	var directoryFlag = flag.String("directory", "data", "the data directory")
	var standaloneFlag = flag.Bool("standalone", false, "enable standalone mode")
	var memoryFlag = flag.Bool("memory", false, "enable in-memory mode")

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
	directory, err := filepath.Abs(*directoryFlag)
	if err != nil {
		panic(err)
	}

	// append server id
	directory = filepath.Join(directory, strconv.FormatUint(*idFlag, 10))

	// check if in-memory is requested
	if *memoryFlag {
		directory = ""
	}

	// start machine
	machine, err := turing.Start(turing.Config{
		ID:         *idFlag,
		Members:    members,
		Directory:  directory,
		Standalone: *standaloneFlag,
		Instructions: []turing.Instruction{
			&inc{}, &get{},
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

		// inc value
		err := machine.Execute(nil, &inc{
			Key:   strconv.Itoa(rand.Intn(keySpace)),
			Value: 1,
		}, false)
		if err != nil {
			handle(err)
			continue
		}

		// increment
		writeCounter.Add(1)
	}
}

var readCounter = god.NewCounter("read", nil)

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

		// get value
		err := machine.Execute(nil, &get{
			Key: strconv.Itoa(rand.Intn(keySpace)),
		}, false)
		if err != nil {
			handle(err)
			continue
		}

		// increment
		readCounter.Add(1)
	}
}

var unreadyCounter = god.NewCounter("x-unready", nil)
var busyCounter = god.NewCounter("x-busy", nil)
var timeoutCounter = god.NewCounter("x-timeout", nil)
var errorCounter = god.NewCounter("x-error", nil)

func handle(err error) {
	if err == dragonboat.ErrClusterNotReady {
		unreadyCounter.Add(1)
	} else if err == dragonboat.ErrSystemBusy {
		busyCounter.Add(1)
	} else if err == dragonboat.ErrTimeout {
		timeoutCounter.Add(1)
	} else if err != nil {
		errorCounter.Add(1)
		println(err.Error())
	}

	time.Sleep(100 * time.Millisecond)
}
