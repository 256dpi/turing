package turing

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"path/filepath"
	"strconv"
	"strings"
)

type NodeConfig struct {
	// The server route.
	Server Route

	// The cluster peers.
	Peers []Route

	// The storage directory.
	Directory string

	// The used instructions.
	Instructions []Instruction

	// The logger for internal logs (raft, badger).
	Logger io.Writer
}

func (c *NodeConfig) check() error {
	// TODO: Check route validity.

	// check directory
	if c.Directory == "" {
		return errors.New("turing: missing directory")
	}

	// set default logger
	if c.Logger == nil {
		c.Logger = ioutil.Discard
	}

	return nil
}

func (c NodeConfig) raftDir() string {
	return filepath.Join(c.Directory, "raft")
}

func (c NodeConfig) dbDir() string {
	return filepath.Join(c.Directory, "db")
}

type Route struct {
	Name string
	Host string
	Port int
}

func NewRoute(name, host string, port int) Route {
	return Route{
		Name: name,
		Host: host,
		Port: port,
	}
}

func ParseRoute(str string) (Route, error) {
	// split name and addr
	s := strings.Split(str, "@")
	if len(s) == 1 {
		s = []string{"", s[0]}
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
		Name: s[0],
		Host: host,
		Port: port,
	}

	return r, nil
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
	return fmt.Sprintf("%s@%s", r.Name, r.raftAddr())
}
