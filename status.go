package turing

// Status contains information about the cluster.
type Status struct {
	// The id of this member.
	MemberID uint64

	// The role of this member.
	Role Role

	// The cluster leader.
	Leader *Member

	// The cluster members.
	Members []Member
}

// Role specifies the role of a cluster member.
type Role int

const (
	_ Role = iota

	// Leader is the elected leader of a cluster.
	Leader

	// Follower is a electable cluster member.
	Follower

	// Observer is non-electable cluster member.
	Observer
)

// String implements the name of the role.
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
