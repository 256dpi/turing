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

func (r route) raftPort() int {
	return r.port
}

func (r route) raftAddr() string {
	return net.JoinHostPort(r.host, strconv.Itoa(r.raftPort()))
}

func (r route) rpcPort() int {
	return r.port + 1
}

func (r route) rpcAddr() string {
	return net.JoinHostPort(r.host, strconv.Itoa(r.rpcPort()))
}

func (r route) string() string {
	return fmt.Sprintf("%s@%s", r.name, r.raftAddr())
}

type Config struct {
	// The node name.
	Name string

	// The host used for raft.
	Host string

	// The port used for raft.
	Port int

	// The storage directory.
	Directory string

	// The cluster peers e.g. "ns1@0.0.0.0:1410, ns2@0.0.0.0:1420".
	Peers []string

	// The used instructions.
	Instructions []Instruction
}

func (c Config) nodeRoute() route {
	return route{
		name: c.Name,
		host: c.Host,
		port: c.Port,
	}
}

func (c Config) raftDir() string {
	return filepath.Join(c.Directory, "raft")
}

func (c Config) dbDir() string {
	return filepath.Join(c.Directory, "db")
}

func (c Config) peerRoutes() []route {
	// prepare list
	var list []route
	for _, peer := range c.Peers {
		list = append(list, parseRoute(peer))
	}

	return list
}
