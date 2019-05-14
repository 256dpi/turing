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
	// The server route.
	Server Route

	// The cluster members.
	Members []Route

	// The storage directory.
	Directory string

	// The used instructions.
	Instructions []Instruction

	// The average round trip time.
	RoundTripTime time.Duration
}

func (c *Config) check() error {
	// check server route
	err := c.Server.check()
	if err != nil {
		return err
	}

	// check member route
	for _, member := range c.Members {
		err = member.check()
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

// Route is custom type to represent a cluster member.
type Route struct {
	ID   uint64
	Host string
	Port int
}

// ParseRoute will parse the provided string in the form of "7@0.0.0.0:1337" and
// return a route.
func ParseRoute(str string) (Route, error) {
	// split name and addr
	s := strings.Split(str, "@")
	if len(s) == 1 {
		s = []string{"", s[0]}
	}

	// get id
	id, err := strconv.ParseUint(s[0], 10, 64)
	if err != nil {
		return Route{}, err
	}

	// split addr
	host, portString, err := net.SplitHostPort(s[1])
	if err != nil {
		return Route{}, err
	}

	// set default host
	if host == "" {
		host = "0.0.0.0"
	}

	// parse port
	port, err := strconv.Atoi(portString)
	if err != nil {
		return Route{}, err
	}

	// create route
	r := Route{
		ID:   id,
		Host: host,
		Port: port,
	}

	return r, nil
}

func (r Route) check() error {
	// check host
	if r.Host == "" {
		return errors.New("turing: missing host")
	}

	// check port
	if r.Port <= 0 {
		return errors.New("turing: invalid port")
	}

	return nil
}

func (r Route) raftPort() int {
	return r.Port
}

func (r Route) raftAddr() string {
	return net.JoinHostPort(r.Host, strconv.Itoa(r.raftPort()))
}

func (r Route) rpcPort() int {
	return r.Port + 1
}

func (r Route) rpcAddr() string {
	return net.JoinHostPort(r.Host, strconv.Itoa(r.rpcPort()))
}

func (r Route) string() string {
	return fmt.Sprintf("%d@%s", r.ID, r.raftAddr())
}
