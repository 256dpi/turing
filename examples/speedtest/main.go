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

// prepare flags
var id = flag.Uint64("id", 0, "the server id")
var members = flag.String("members", "", "the cluster members")
var directory = flag.String("directory", "data", "the data directory")
var standalone = flag.Bool("standalone", false, "enable standalone mode")
var memory = flag.Bool("memory", false, "enable in-memory mode")
var readers = flag.Int("readers", 1000, "the number of parallel readers")
var writers = flag.Int("writers", 1000, "the number of parallel writers")
var keySpace = flag.Int("keySpace", 1000, "the size of the key space")

var wg sync.WaitGroup

func main() {
	// parse flags
	flag.Parse()

	// enable debugging
	god.Debug(6060 + int(*id))
	god.Metrics()

	// disable logging
	turing.SetLogger(nil)

	// register metrics
	turing.EnableMetrics()

	// parse members
	var memberList []turing.Member
	if *members != "" {
		for _, member := range strings.Split(*members, ",") {
			// parse member
			member, err := turing.ParseMember(member)
			if err != nil {
				panic(err)
			}

			// add member
			memberList = append(memberList, member)
		}
	}

	// resolve directory
	directory, err := filepath.Abs(*directory)
	if err != nil {
		panic(err)
	}

	// append server id
	directory = filepath.Join(directory, strconv.FormatUint(*id, 10))

	// check if in-memory is requested
	if *memory {
		directory = ""
	}

	// start machine
	machine, err := turing.Start(turing.Config{
		ID:         *id,
		Members:    memberList,
		Directory:  directory,
		Standalone: *standalone,
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
	wg.Add(*writers)
	for i := 0; i < *writers; i++ {
		go writer(machine, done)
	}

	// run readers
	wg.Add(*readers)
	for i := 0; i < *readers; i++ {
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

		// determine merge
		merge := rand.Intn(4) > 0 // 75%

		// inc value
		err := machine.Execute(&inc{
			Key:   strconv.Itoa(rand.Intn(*keySpace)),
			Value: 1,
			Merge: merge,
		})
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

		// determine stale read
		staleRead := rand.Intn(4) > 0 // 75%

		// get value
		err := machine.Execute(&get{
			Key: strconv.Itoa(rand.Intn(*keySpace)),
		}, turing.Options{
			StaleRead: staleRead,
		})
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
