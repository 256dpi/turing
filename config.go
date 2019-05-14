package turing

import (
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Config is used to configure a machine.
type Config struct {
	// The server id.
	ID uint64

	// The cluster members.
	Members []Member

	// The storage directory.
	Directory string

	// The used instructions.
	Instructions []Instruction

	// The average round trip time.
	RoundTripTime time.Duration
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
	// check local member
	if c.Local() == nil {
		return errors.New("turing: missing local member")
	}

	// check members
	for _, member := range c.Members {
		err := member.check()
		if err != nil {
			return err
		}
	}

	// check directory
	if c.Directory == "" {
		return errors.New("turing: missing directory")
	}

	// check round trip time
	if c.RoundTripTime == 0 {
		c.RoundTripTime = time.Millisecond
	}

	return nil
}

func (c Config) raftDir() string {
	return filepath.Join(c.Directory, "raft")
}

func (c Config) dbDir() string {
	return filepath.Join(c.Directory, "db")
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
		return errors.New("turing: missing host")
	}

	// check port
	if m.Port <= 0 {
		return errors.New("turing: invalid port")
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
