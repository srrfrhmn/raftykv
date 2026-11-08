// Package raft is the Raft consensus core (docs/DESIGN.md §5).
//
// The core is a pure state machine. It takes inputs through Tick, Step,
// Propose, and Campaign, and it returns its outputs through Ready. It does no
// I/O and reads no clock: time advances only through Tick. A driver
// (RKV-040) persists Ready.HardState and Ready.Entries, then sends
// Ready.Messages, then applies Ready.CommittedEntries, then calls Advance.
package raft

import "fmt"

// None is the zero node ID. It means "no node" (no leader, no vote).
const None uint64 = 0

// StateType is the role of a node.
type StateType uint8

// Roles.
const (
	StateFollower StateType = iota
	StatePreCandidate
	StateCandidate
	StateLeader
)

func (s StateType) String() string {
	switch s {
	case StateFollower:
		return "Follower"
	case StatePreCandidate:
		return "PreCandidate"
	case StateCandidate:
		return "Candidate"
	case StateLeader:
		return "Leader"
	}
	return fmt.Sprintf("StateType(%d)", s)
}

// EntryType is the kind of a log entry.
type EntryType uint8

// Entry kinds.
const (
	EntryNormal EntryType = iota
	EntryConfChange
)

// Entry is one Raft log entry.
type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

// MessageType is the kind of a Message. Values with the "local" comment never
// go on the network.
type MessageType uint8

// Message kinds.
const (
	MsgHup         MessageType = iota // local: start an election
	MsgBeat                           // local: leader sends heartbeats
	MsgCheckQuorum                    // local: leader checks quorum activity
	MsgProp                           // local: propose entries
	MsgVote
	MsgVoteResp
	MsgPreVote
	MsgPreVoteResp
	MsgApp
	MsgAppResp
	MsgHeartbeat
	MsgHeartbeatResp
)

var msgNames = [...]string{
	"MsgHup", "MsgBeat", "MsgCheckQuorum", "MsgProp",
	"MsgVote", "MsgVoteResp", "MsgPreVote", "MsgPreVoteResp",
	"MsgApp", "MsgAppResp", "MsgHeartbeat", "MsgHeartbeatResp",
}

func (t MessageType) String() string {
	if int(t) < len(msgNames) {
		return msgNames[t]
	}
	return fmt.Sprintf("MessageType(%d)", t)
}

// IsLocal reports whether t is generated and consumed inside one node.
func (t MessageType) IsLocal() bool {
	return t == MsgHup || t == MsgBeat || t == MsgCheckQuorum || t == MsgProp
}

// IsResponse reports whether t answers another message.
func (t MessageType) IsResponse() bool {
	return t == MsgVoteResp || t == MsgPreVoteResp || t == MsgAppResp || t == MsgHeartbeatResp
}

// Message is one Raft message.
type Message struct {
	Type MessageType
	From uint64
	To   uint64
	Term uint64
	// LogTerm and Index: for votes, the sender's last log position. For
	// MsgApp, the position before Entries.
	LogTerm uint64
	Index   uint64
	Entries []Entry
	Commit  uint64
	Reject  bool
}

func (m Message) String() string {
	return fmt.Sprintf("%s %d->%d term=%d logterm=%d index=%d commit=%d reject=%t entries=%d",
		m.Type, m.From, m.To, m.Term, m.LogTerm, m.Index, m.Commit, m.Reject, len(m.Entries))
}

// HardState is the state that MUST be durable before messages are sent.
type HardState struct {
	Term   uint64
	Vote   uint64
	Commit uint64
}

// IsEmpty reports whether h is the zero value.
func (h HardState) IsEmpty() bool { return h == HardState{} }

// Ready is the output of the core since the last Advance.
type Ready struct {
	// HardState is set when term, vote, or commit changed. Persist it
	// before sending Messages: a vote MUST NOT be lost in a crash.
	HardState *HardState
	// Entries to append to stable storage before sending Messages.
	Entries []Entry
	// Messages to send after HardState and Entries are durable.
	Messages []Message
	// CommittedEntries to apply to the state machine, in order.
	CommittedEntries []Entry
	// SoftState is set when the role or leader changed. It need not be durable.
	SoftState *SoftState
}

// SoftState is volatile state, useful for status and logs.
type SoftState struct {
	Lead  uint64
	State StateType
}

// Status is a snapshot of a node's state for tests and debug endpoints.
type Status struct {
	ID        uint64
	HardState HardState
	SoftState SoftState
	LastIndex uint64
	LastTerm  uint64
	Applied   uint64
}
