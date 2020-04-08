package turing

import (
	"fmt"
	"net"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	pfs "github.com/cockroachdb/pebble/vfs"
	dfs "github.com/lni/goutils/vfs"
)

// Config is used to configure a machine.
type Config struct {
	/* General Configuration */

	// The id of this member.
	ID uint64

	// All cluster members.
	Members []Member

	// The storage directory. If empty an in-memory filesystem is used.
	Directory string

	// The used instructions.
	Instructions []Instruction

	// Whether standalone mode should be enabled. In this mode the database
	// state is not replicated and
	Standalone bool

	/* Performance Tuning */

	// The average round trip time.
	//
	// Default: 1ms.
	RoundTripTime time.Duration

	// The number of concurrent database readers.
	//
	// Default: min(NumCPUs - 3, 2).
	ConcurrentReaders int

	// The number of concurrent raft proposers.
	//
	// Default: NumCPUs.
	ConcurrentProposers int

	// The maximum instruction batch size for updates and lookups.
	//
	// Default: 200.
	BatchSize int
}

// Local will return the local member.
func (c *Config) Local() *Member {
	// find member
	for _, member := range c.Members {
		if member.ID == c.ID {
			return &member
		}
	}

	return nil
}

func (c *Config) check() error {
	// check id
	if c.ID == 0 && !c.Standalone {
		return fmt.Errorf("turing: missing id")
	}

	// check local member
	if c.Local() == nil && !c.Standalone {
		return fmt.Errorf("turing: missing local member")
	}

	// check members
	for _, member := range c.Members {
		err := member.check()
		if err != nil {
			return err
		}
	}

	// check round trip time
	if c.RoundTripTime == 0 {
		c.RoundTripTime = time.Millisecond
	}

	// check concurrent readers
	if c.ConcurrentReaders == 0 {
		c.ConcurrentReaders = runtime.NumCPU() - 3
		if c.ConcurrentReaders < 2 {
			c.ConcurrentReaders = 2
		}
	}

	// check concurrent proposers
	if c.ConcurrentProposers == 0 {
		c.ConcurrentProposers = runtime.NumCPU()
	}

	// check batch size
	if c.BatchSize == 0 {
		c.BatchSize = 200
	}

	return nil
}

func (c Config) raftDir() string {
	return filepath.Join(c.Directory, "raft")
}

func (c Config) raftFS() dfs.FS {
	if c.Directory != "" {
		return dfs.Default
	}

	return dfs.NewMem()
}

func (c Config) dbDir() string {
	return filepath.Join(c.Directory, "db")
}

func (c Config) dbFS() pfs.FS {
	if c.Directory != "" {
		return pfs.Default
	}

	return pfs.NewMem()
}

// Member specifies a cluster member.
type Member struct {
	ID   uint64
	Host string
	Port int
}

// ParseMember will parse the provided string in the form of "7@0.0.0.0:1337"
// and return a member.
func ParseMember(str string) (Member, error) {
	// split name and addr
	s := strings.Split(str, "@")
	if len(s) == 1 {
		s = []string{"", s[0]}
	}

	// get id
	id, err := strconv.ParseUint(s[0], 10, 64)
	if err != nil {
		return Member{}, err
	}

	// split addr
	host, portString, err := net.SplitHostPort(s[1])
	if err != nil {
		return Member{}, err
	}

	// set default host
	if host == "" {
		host = "0.0.0.0"
	}

	// parse port
	port, err := strconv.Atoi(portString)
	if err != nil {
		return Member{}, err
	}

	// create member
	r := Member{
		ID:   id,
		Host: host,
		Port: port,
	}

	return r, nil
}

func (m Member) check() error {
	// check host
	if m.Host == "" {
		return fmt.Errorf("turing: missing host")
	}

	// check port
	if m.Port <= 0 {
		return fmt.Errorf("turing: invalid port")
	}

	return nil
}

func (m Member) raftPort() int {
	return m.Port
}

func (m Member) raftAddr() string {
	return net.JoinHostPort(m.Host, strconv.Itoa(m.raftPort()))
}

func (m Member) rpcPort() int {
	return m.Port + 1
}

func (m Member) rpcAddr() string {
	return net.JoinHostPort(m.Host, strconv.Itoa(m.rpcPort()))
}

func (m Member) string() string {
	return fmt.Sprintf("%d@%s", m.ID, m.raftAddr())
}
