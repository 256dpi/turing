package turing

type Role int

const (
	_ Role = iota
	Leader
	Follower
	Observer
)

func (r Role) String() string {
	switch r {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	case Observer:
		return "Observer"
	default:
		return "Unknown"
	}
}

type Status struct {
	// The id of this server.
	ID uint64

	// The role of this server.
	Role Role

	// The cluster leader.
	Leader *Route

	// The cluster members.
	Members []Route
}
