package turing

import "fmt"

// Status contains information about the cluster.
type Status struct {
	// The id of this member.
	ID uint64

	// The role of this member.
	Role Role

	// The cluster leader.
	Leader *Member

	// The cluster members.
	Members []Member
}

// String returns the status formatted as a string.
func (s Status) String() string {
	return fmt.Sprintf("ID: %d, Role: %s, Members: %d", s.ID, s.Role, len(s.Members))
}

// Role specifies the role of a cluster member.
type Role int

const (
	_ Role = iota

	// RoleLeader is the elected leader of a cluster.
	RoleLeader

	// RoleFollower is a electable cluster member.
	RoleFollower

	// RoleObserver is non-electable cluster member.
	RoleObserver
)

// String returns the name of the role.
func (r Role) String() string {
	switch r {
	case RoleLeader:
		return "Leader"
	case RoleFollower:
		return "Follower"
	case RoleObserver:
		return "Observer"
	default:
		return "Unknown"
	}
}
