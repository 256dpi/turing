package turing

import (
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
)

type Route struct {
	Name string
	Host string
	Port int
}

func (r Route) Addr() string {
	return net.JoinHostPort(r.Host, strconv.Itoa(r.Port))
}

func (r Route) String() string {
	return fmt.Sprintf("%s@%s", r.Name, r.Addr())
}

type Options struct {
	// The node name.
	Name string

	// The host used for serf and raft.
	Host string

	// The port used for serf and raft (+1).
	Port int

	// The storage directory.
	Directory string

	// The cluster peers e.g. "ns1@0.0.0.0:1410, ns2@0.0.0.0:1420".
	Peers []string

	// The used instructions.
	Instructions []Instruction
}

func (o Options) NodeRoute() Route {
	return Route{
		Name: o.Name,
		Host: o.Host,
		Port: o.Port,
	}
}

func (o Options) SerfPort() int {
	return o.Port
}

func (o Options) SerfAddr() string {
	return net.JoinHostPort(o.Host, strconv.Itoa(o.SerfPort()))
}

func (o Options) RaftPort() int {
	return o.Port + 1
}

func (o Options) RaftAddr() string {
	return net.JoinHostPort(o.Host, strconv.Itoa(o.RaftPort()))
}

func (o Options) RaftDir() string {
	return filepath.Join(o.Directory, "raft")
}

func (o Options) DBDir() string {
	return filepath.Join(o.Directory, "db")
}

func (o Options) PeerRoutes() []Route {
	// prepare list
	var list []Route

	// parse peers
	for _, peer := range o.Peers {
		// split name and addr
		s := strings.Split(peer, "@")
		if len(s) != 2 {
			continue
		}

		// split addr
		host, portString, err := net.SplitHostPort(s[1])
		if err != nil {
			continue
		}

		// parse port
		port, err := strconv.Atoi(portString)
		if err != nil {
			continue
		}

		// add route
		list = append(list, Route{
			Name: s[0],
			Host: host,
			Port: port,
		})
	}

	return list
}
