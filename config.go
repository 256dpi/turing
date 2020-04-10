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

	// The maximum instruction batch sizes.
	//
	// Default: 600, 200, 200.
	UpdateBatchSize   int
	LookupBatchSize   int
	ProposalBatchSize int
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

// Validate will validate the configuration and ensure defaults.
func (c *Config) Validate() error {
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
		err := member.Validate()
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

	// check batch sizes
	if c.UpdateBatchSize == 0 {
		c.UpdateBatchSize = 600
	}
	if c.LookupBatchSize == 0 {
		c.LookupBatchSize = 200
	}
	if c.ProposalBatchSize == 0 {
		c.ProposalBatchSize = 200
	}

	return nil
}

// RaftDir returns the directory used for the raft files.
func (c Config) RaftDir() string {
	return filepath.Join(c.Directory, "raft")
}

// RaftFS returns the filesystem used for the raft files.
func (c Config) RaftFS() dfs.FS {
	// use in-memory if empty
	if c.Directory == "" {
		return dfs.NewMem()
	}

	return dfs.Default
}

// DatabaseDir returns the directory used for the database files.
func (c Config) DatabaseDir() string {
	return filepath.Join(c.Directory, "db")
}

// DatabaseFS returns the filesystem used for the database files.
func (c Config) DatabaseFS() pfs.FS {
	// use in-memory if empty
	if c.Directory != "" {
		return pfs.NewMem()
	}

	return pfs.Default
}

// Member specifies a cluster member.
type Member struct {
	ID   uint64
	Host string
	Port int
}

// ParseMember will parse the provided string and return a member. The string is
// expected to have the form "id@[host]:port".
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

// ParseMembers will parse the provided string and return a list of members. The
// string is expected to have the form "id@[host]:port,...".
func ParseMembers(str string) ([]Member, error) {
	// parse members
	var members []Member
	for _, member := range strings.Split(str, ",") {
		// parse member
		member, err := ParseMember(member)
		if err != nil {
			return nil, err
		}

		// add member
		members = append(members, member)
	}

	return members, nil
}

// Validate will validate the member.
func (m Member) Validate() error {
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

// Address will return the members full address in the form "host:port".
func (m Member) Address() string {
	return net.JoinHostPort(m.Host, strconv.Itoa(m.Port))
}

// String will return a formatted member string "id@host:port".
func (m Member) String() string {
	return fmt.Sprintf("%d@%s", m.ID, m.Address())
}
