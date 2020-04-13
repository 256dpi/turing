package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/256dpi/turing"
	"github.com/256dpi/turing/stdset"
)

var id = flag.Uint64("id", 0, "the server id")
var members = flag.String("members", "", "the cluster members")
var directory = flag.String("directory", "data", "the data directory")

func main() {
	// parse flags
	flag.Parse()

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

		// map values
		mp := &stdset.Map{}
		err = machine.Execute(mp)
		if err != nil {
			println(err.Error())
			time.Sleep(time.Second)
			continue
		}

		// collect keys
		var keys []string
		for key, value := range mp.Pairs {
			keys = append(keys, fmt.Sprintf("%s:%s", key, value))
		}

		// sort keys
		sort.Strings(keys)

		// print info
		fmt.Printf("Keys: %s, %s", strings.Join(keys, " "), machine.Status().String())
	}
}
