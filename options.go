package turing

import (
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
)

type route struct {
	name string
	host string
	port int
}

func parseRoute(str string) route {
	// split name and addr
	s := strings.Split(str, "@")
	if len(s) == 1 {
		s = []string{"", s[0]}
	}

	// split addr
	host, portString, _ := net.SplitHostPort(s[1])

	// set default host
	if host == "" {
		host = "0.0.0.0"
	}

	// parse port
	port, _ := strconv.Atoi(portString)

	return route{
		name: s[0],
		host: host,
		port: port,
	}
}

func (r route) serfPort() int {
	return r.port
}

func (r route) serfAddr() string {
	return net.JoinHostPort(r.host, strconv.Itoa(r.serfPort()))
}

func (r route) raftPort() int {
	return r.port + 1
}

func (r route) raftAddr() string {
	return net.JoinHostPort(r.host, strconv.Itoa(r.raftPort()))
}

func (r route) rpcPort() int {
	return r.port + 2
}

func (r route) rpcAddr() string {
	return net.JoinHostPort(r.host, strconv.Itoa(r.rpcPort()))
}

func (r route) string() string {
	return fmt.Sprintf("%s@%s", r.name, r.serfAddr())
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
		name: o.Name,
		host: o.Host,
		port: o.Port,
	}
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
	for _, peer := range o.Peers {
		list = append(list, parseRoute(peer))
	}

	return list
}
