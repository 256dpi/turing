package main

import (
	"flag"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/256dpi/god"
	"github.com/lni/dragonboat/v3"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/256dpi/turing"
)

var id = flag.Uint64("id", 0, "the server id")
var members = flag.String("members", "", "the cluster members")
var directory = flag.String("directory", "data", "the data directory")
var standalone = flag.Bool("standalone", false, "enable standalone mode")
var memory = flag.Bool("memory", false, "enable in-memory mode")
var writeBatchSize = flag.Int("writeBatchSize", 500, "the write batch size")
var readBatchSize = flag.Int("readBatchSize", 500, "the read batch size")
var keySpace = flag.Int64("keySpace", 100000, "the maximum size of the key space")
var scanLength = flag.Int64("scanLength", 100, "the maximum length of the scan")

var wg sync.WaitGroup

func main() {
	// parse flags
	flag.Parse()

	// enable debugging
	god.Init(god.Options{
		Port:           6060 + int(*id),
		MetricsHandler: promhttp.Handler().ServeHTTP,
	})

	// disable logging
	turing.SetLogger(nil)

	// parse members
	var memberList []turing.Member
	var err error
	if *members != "" {
		memberList, err = turing.ParseMembers(*members)
		if err != nil {
			panic(err)
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
			&inc{}, &get{}, &sum{},
		},
		RoundTripTime:     time.Millisecond,
		UpdateBatchSize:   *writeBatchSize,
		LookupBatchSize:   *readBatchSize,
		ProposalBatchSize: *writeBatchSize,
		ProposalTimeout:   5 * time.Second,
		LinearReadTimeout: 5 * time.Second,
	})
	if err != nil {
		panic(err)
	}

	// track machine role
	god.Track("role", func() string {
		return machine.Status().Role.String()
	})

	// ensure stop
	defer machine.Stop()

	// create control channel
	done := make(chan struct{})

	// run writeBatchSize
	wg.Add(1)
	go writer(machine, done)

	// run readBatchSize
	wg.Add(1)
	go reader(machine, done)

	// run scanners
	wg.Add(1)
	go scanner(machine, done)

	// prepare exit
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)
	<-exit

	// close control channel
	close(done)
	wg.Wait()
}

func writer(machine *turing.Machine, done <-chan struct{}) {
	defer wg.Done()

	// create rng
	rng := rand.New(rand.NewSource(rand.Int63()))

	// prepare counter
	var iwg sync.WaitGroup

	// write entries forever
	for {
		// check if done
		select {
		case <-done:
			iwg.Wait()
			return
		default:
		}

		// prepare instruction
		ins := &inc{}
		ins.Key = uint64(rng.Int63n(*keySpace))
		ins.Value = uint64(rng.Int63n(10))
		ins.Merge = rng.Intn(4) > 0 // 75%

		// inc value
		iwg.Add(1)
		err := machine.ExecuteAsync(ins, func(err error) {
			iwg.Done()
			if err != nil {
				handle("writer", err, ins, turing.Options{})
			}
		})
		if err != nil {
			handle("writer", err, ins, turing.Options{})
		}
	}
}

func reader(machine *turing.Machine, done <-chan struct{}) {
	defer wg.Done()

	// create rng
	rng := rand.New(rand.NewSource(rand.Int63()))

	// prepare counter
	var iwg sync.WaitGroup

	// write entries forever
	for {
		// check if done
		select {
		case <-done:
			iwg.Wait()
			return
		default:
		}

		// prepare instruction
		ins := &get{}
		ins.Key = uint64(rng.Int63n(*keySpace))

		// prepare options
		opts := turing.Options{}
		opts.StaleRead = rng.Intn(4) > 0 // 75%

		// get value
		iwg.Add(1)
		err := machine.ExecuteAsync(ins, func(err error) {
			iwg.Done()
			if err != nil {
				handle("reader", err, ins, opts)
			}
		}, opts)
		if err != nil {
			handle("reader", err, ins, opts)
		}
	}
}

func scanner(machine *turing.Machine, done <-chan struct{}) {
	defer wg.Done()

	// create rng
	rng := rand.New(rand.NewSource(rand.Int63()))

	// prepare counter
	var iwg sync.WaitGroup

	// write entries forever
	for {
		// check if done
		select {
		case <-done:
			iwg.Wait()
			return
		default:
		}

		// prepare instruction
		ins := &sum{}
		ins.Count = uint64(rng.Int63n(*scanLength))
		ins.Start = uint64(rng.Int63n(*keySpace - int64(ins.Count)))

		// prepare options
		opts := turing.Options{}
		opts.StaleRead = rng.Intn(4) > 0 // 75%

		// get value
		iwg.Add(1)
		err := machine.ExecuteAsync(ins, func(err error) {
			iwg.Done()
			if err != nil {
				handle("scanner", err, ins, opts)
			}
		}, opts)
		if err != nil {
			handle("scanner", err, ins, opts)
		}
	}
}

var notInitCounter = god.NewCounter("x-not-init", nil)
var notReadyCounter = god.NewCounter("x-not-ready", nil)
var sysBusyCounter = god.NewCounter("x-sys-busy", nil)
var timeoutCounter = god.NewCounter("x-timeout", nil)
var errorCounter = god.NewCounter("x-error", nil)

func handle(com string, err error, ins turing.Instruction, opts turing.Options) {
	if err == dragonboat.ErrClusterNotInitialized {
		notInitCounter.Add(1)
	} else if err == dragonboat.ErrClusterNotReady {
		notReadyCounter.Add(1)
	} else if err == dragonboat.ErrSystemBusy {
		sysBusyCounter.Add(1)
	} else if err == dragonboat.ErrTimeout {
		timeoutCounter.Add(1)
	} else if err != nil {
		errorCounter.Add(1)
		println(err.Error())
	}

	time.Sleep(100 * time.Millisecond)
}
