package turing

import (
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
)

type route struct {
	Name string
	Host string
	Port int
}

func (r route) addr() string {
	return net.JoinHostPort(r.Host, strconv.Itoa(r.Port))
}

func (r route) string() string {
	return fmt.Sprintf("%s@%s", r.Name, r.addr())
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

func (o Options) nodeRoute() route {
	return route{
		Name: o.Name,
		Host: o.Host,
		Port: o.Port,
	}
}

func (o Options) serfPort() int {
	return o.Port
}

func (o Options) serfAddr() string {
	return net.JoinHostPort(o.Host, strconv.Itoa(o.serfPort()))
}

func (o Options) raftPort() int {
	return o.Port + 1
}

func (o Options) raftAddr() string {
	return net.JoinHostPort(o.Host, strconv.Itoa(o.raftPort()))
}

func (o Options) raftDir() string {
	return filepath.Join(o.Directory, "raft")
}

func (o Options) dbDir() string {
	return filepath.Join(o.Directory, "db")
}

func (o Options) peerRoutes() []route {
	// prepare list
	var list []route

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
		list = append(list, route{
			Name: s[0],
			Host: host,
			Port: port,
		})
	}

	return list
}
