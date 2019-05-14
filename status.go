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
	ID uint64

	Role Role

	Leader *Route

	Members []Route
}
