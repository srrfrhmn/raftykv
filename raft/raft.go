package raft

import (
	"errors"
	"fmt"
	"slices"
)

// Rand is the randomness the core needs. env.Rand satisfies it.
type Rand interface {
	Int64N(n int64) int64
}

// Config configures one Raft node.
type Config struct {
	// ID of this node. MUST NOT be None.
	ID uint64
	// Peers lists all voters, including ID.
	Peers []uint64
	// ElectionTick is the base election timeout in ticks. The randomized
	// timeout is in [ElectionTick, 2*ElectionTick). Default 10.
	ElectionTick int
	// HeartbeatTick is the leader heartbeat interval in ticks. Default 1.
	HeartbeatTick int
	// PreVote enables the PreVote phase (docs/DESIGN.md §5.2).
	PreVote bool
	// CheckQuorum makes a leader step down when it loses contact with a
	// majority, and makes followers ignore votes while a leader is active.
	CheckQuorum bool
	// Rand drives election timeout jitter. Required.
	Rand Rand
	// HardState and Entries restore durable state after a restart.
	// RKV-020 replaces Entries with a Storage interface.
	HardState HardState
	Entries   []Entry
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("raft: ID must not be zero")
	}
	if !slices.Contains(c.Peers, c.ID) {
		return fmt.Errorf("raft: ID %d not in Peers %v", c.ID, c.Peers)
	}
	if slices.Contains(c.Peers, None) {
		return errors.New("raft: Peers must not contain zero")
	}
	if c.ElectionTick == 0 {
		c.ElectionTick = 10
	}
	if c.HeartbeatTick == 0 {
		c.HeartbeatTick = 1
	}
	if c.HeartbeatTick < 0 || c.ElectionTick <= c.HeartbeatTick {
		return fmt.Errorf("raft: need 0 < HeartbeatTick (%d) < ElectionTick (%d)", c.HeartbeatTick, c.ElectionTick)
	}
	if c.Rand == nil {
		return errors.New("raft: Rand is required")
	}
	for i, e := range c.Entries {
		if e.Index != uint64(i+1) {
			return fmt.Errorf("raft: Entries[%d] has index %d, want %d", i, e.Index, i+1)
		}
	}
	return nil
}

// ErrProposalDropped is returned when a node cannot accept a proposal now
// (it is not the leader, or no leader is known).
var ErrProposalDropped = errors.New("raft: proposal dropped")

// Raft is the consensus state machine of one node. It is not safe for
// concurrent use: the driver owns it.
type Raft struct {
	id    uint64
	peers []uint64 // sorted, includes id

	term  uint64
	vote  uint64
	lead  uint64
	state StateType
	log   *raftLog

	votes        map[uint64]bool // election in progress: voter -> granted
	recentActive map[uint64]bool // leader: peers heard from since last quorum check

	msgs []Message

	electionElapsed           int
	heartbeatElapsed          int
	electionTimeout           int
	heartbeatTimeout          int
	randomizedElectionTimeout int

	preVote     bool
	checkQuorum bool
	rand        Rand

	prevHard HardState
	prevSoft SoftState
}

// New returns a node in the follower state.
func New(c Config) (*Raft, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	peers := slices.Clone(c.Peers)
	slices.Sort(peers)
	peers = slices.Compact(peers)

	r := &Raft{
		id:               c.ID,
		peers:            peers,
		term:             c.HardState.Term,
		vote:             c.HardState.Vote,
		log:              newRaftLog(c.Entries, c.HardState.Commit),
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		preVote:          c.PreVote,
		checkQuorum:      c.CheckQuorum,
		rand:             c.Rand,
	}
	r.becomeFollower(r.term, None)
	r.prevHard = r.hardState()
	r.prevSoft = r.softState()
	return r, nil
}

// ---- inputs ----

// Tick advances logical time by one tick.
func (r *Raft) Tick() {
	if r.state == StateLeader {
		r.tickHeartbeat()
	} else {
		r.tickElection()
	}
}

// Campaign starts an election now.
func (r *Raft) Campaign() error { return r.Step(Message{Type: MsgHup, From: r.id}) }

// Propose asks the leader to append data to the log.
// RKV-020 adds replication and commit; in S1 the entry stays local.
func (r *Raft) Propose(data []byte) error {
	return r.Step(Message{Type: MsgProp, From: r.id, Entries: []Entry{{Type: EntryNormal, Data: data}}})
}

// Step processes one message from the network or a local input.
func (r *Raft) Step(m Message) error {
	if !m.Type.IsLocal() {
		if handled := r.stepTerm(m); handled {
			return nil
		}
	}

	switch m.Type {
	case MsgHup:
		r.hup()
		return nil
	case MsgVote, MsgPreVote:
		r.handleVote(m)
		return nil
	}

	switch r.state {
	case StateLeader:
		return r.stepLeader(m)
	case StateCandidate, StatePreCandidate:
		return r.stepCandidate(m)
	default:
		return r.stepFollower(m)
	}
}

// stepTerm applies the term rules. It returns true if m needs no more work.
func (r *Raft) stepTerm(m Message) bool {
	switch {
	case m.Term > r.term:
		if m.Type == MsgVote || m.Type == MsgPreVote {
			// Leader stickiness: while a leader is active, ignore votes. This
			// keeps a rejoining node from disrupting a healthy cluster.
			if r.checkQuorum && r.lead != None && r.electionElapsed < r.electionTimeout {
				return true
			}
		}
		switch {
		case m.Type == MsgPreVote:
			// A pre-vote never changes our term.
		case m.Type == MsgPreVoteResp && !m.Reject:
			// The term bumps when the pre-candidate wins and campaigns.
		default:
			lead := None
			if m.Type == MsgApp || m.Type == MsgHeartbeat {
				lead = m.From
			}
			r.becomeFollower(m.Term, lead)
		}
	case m.Term < r.term:
		switch {
		case (r.checkQuorum || r.preVote) && m.Type == MsgHeartbeat:
			// Tell a stale leader about the newer term so it steps down.
			r.send(Message{To: m.From, Type: MsgHeartbeatResp})
		case (r.checkQuorum || r.preVote) && m.Type == MsgApp:
			r.send(Message{To: m.From, Type: MsgAppResp})
		case m.Type == MsgPreVote:
			r.send(Message{To: m.From, Type: MsgPreVoteResp, Term: r.term, Reject: true})
		}
		return true
	}
	return false
}

func (r *Raft) handleVote(m Message) {
	respType := MsgVoteResp
	if m.Type == MsgPreVote {
		respType = MsgPreVoteResp
	}
	canVote := r.vote == m.From ||
		(r.vote == None && r.lead == None) ||
		(m.Type == MsgPreVote && m.Term > r.term)
	if canVote && r.log.isUpToDate(m.Index, m.LogTerm) {
		// Echo the candidate's term: for a pre-vote it is our term + 1.
		r.send(Message{To: m.From, Type: respType, Term: m.Term})
		if m.Type == MsgVote {
			r.electionElapsed = 0
			r.vote = m.From
		}
		return
	}
	r.send(Message{To: m.From, Type: respType, Term: r.term, Reject: true})
}

func (r *Raft) stepLeader(m Message) error {
	switch m.Type {
	case MsgBeat:
		r.bcastHeartbeat()
	case MsgCheckQuorum:
		if !r.quorumActive() {
			r.becomeFollower(r.term, None)
			return nil
		}
		clear(r.recentActive)
	case MsgProp:
		for _, e := range m.Entries {
			e.Term = r.term
			e.Index = r.log.lastIndex() + 1
			r.log.append(e)
		}
	case MsgHeartbeatResp, MsgAppResp:
		r.recentActive[m.From] = true
	}
	return nil
}

func (r *Raft) stepCandidate(m Message) error {
	myResp := MsgVoteResp
	if r.state == StatePreCandidate {
		myResp = MsgPreVoteResp
	}
	switch m.Type {
	case MsgProp:
		return ErrProposalDropped
	case MsgHeartbeat, MsgApp:
		// A leader exists in this term (m.Term == r.term here).
		r.becomeFollower(m.Term, m.From)
		return r.stepFollower(m)
	case myResp:
		granted, rejected := r.poll(m.From, !m.Reject)
		switch r.voteResult(granted, rejected) {
		case voteWon:
			if r.state == StatePreCandidate {
				r.campaign(false)
			} else {
				r.becomeLeader()
			}
		case voteLost:
			r.becomeFollower(r.term, None)
		}
	}
	return nil
}

func (r *Raft) stepFollower(m Message) error {
	switch m.Type {
	case MsgProp:
		// Forwarding to the leader arrives with RKV-020.
		return ErrProposalDropped
	case MsgHeartbeat:
		r.electionElapsed = 0
		r.lead = m.From
		r.send(Message{To: m.From, Type: MsgHeartbeatResp})
	case MsgApp:
		r.electionElapsed = 0
		r.lead = m.From
		// Log replication arrives with RKV-020.
	}
	return nil
}

// ---- timers ----

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomizedElectionTimeout {
		r.electionElapsed = 0
		_ = r.Step(Message{Type: MsgHup, From: r.id})
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		if r.checkQuorum {
			_ = r.Step(Message{Type: MsgCheckQuorum, From: r.id})
		}
		if r.state != StateLeader {
			return
		}
	}
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		_ = r.Step(Message{Type: MsgBeat, From: r.id})
	}
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + int(r.rand.Int64N(int64(r.electionTimeout)))
}

// ---- role changes ----

func (r *Raft) reset(term uint64) {
	if r.term != term {
		r.term = term
		r.vote = None
	}
	r.lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
	r.votes = map[uint64]bool{}
	r.recentActive = map[uint64]bool{}
}

func (r *Raft) becomeFollower(term, lead uint64) {
	r.reset(term)
	r.lead = lead
	r.state = StateFollower
}

// becomePreCandidate changes the role only. It does not change term or vote.
func (r *Raft) becomePreCandidate() {
	if r.state == StateLeader {
		panic("raft: leader cannot become pre-candidate")
	}
	r.votes = map[uint64]bool{}
	r.lead = None
	r.resetRandomizedElectionTimeout()
	r.state = StatePreCandidate
}

func (r *Raft) becomeCandidate() {
	if r.state == StateLeader {
		panic("raft: leader cannot become candidate")
	}
	r.reset(r.term + 1)
	r.vote = r.id
	r.state = StateCandidate
}

func (r *Raft) becomeLeader() {
	if r.state == StateFollower {
		panic("raft: follower cannot become leader")
	}
	r.reset(r.term)
	r.lead = r.id
	r.state = StateLeader
	r.bcastHeartbeat()
}

// ---- elections ----

func (r *Raft) hup() {
	if r.state == StateLeader {
		return
	}
	r.campaign(r.preVote)
}

func (r *Raft) campaign(pre bool) {
	var (
		term uint64
		typ  MessageType
	)
	if pre {
		r.becomePreCandidate()
		term, typ = r.term+1, MsgPreVote
	} else {
		r.becomeCandidate()
		term, typ = r.term, MsgVote
	}
	if granted, rejected := r.poll(r.id, true); r.voteResult(granted, rejected) == voteWon {
		// Single-voter cluster.
		if pre {
			r.campaign(false)
		} else {
			r.becomeLeader()
		}
		return
	}
	for _, p := range r.peers {
		if p == r.id {
			continue
		}
		r.send(Message{To: p, Type: typ, Term: term, Index: r.log.lastIndex(), LogTerm: r.log.lastTerm()})
	}
}

type voteOutcome uint8

const (
	votePending voteOutcome = iota
	voteWon
	voteLost
)

func (r *Raft) poll(id uint64, granted bool) (g, rj int) {
	if _, seen := r.votes[id]; !seen {
		r.votes[id] = granted
	}
	for _, v := range r.votes {
		if v {
			g++
		} else {
			rj++
		}
	}
	return g, rj
}

func (r *Raft) quorum() int { return len(r.peers)/2 + 1 }

func (r *Raft) voteResult(granted, rejected int) voteOutcome {
	switch {
	case granted >= r.quorum():
		return voteWon
	case rejected > len(r.peers)-r.quorum():
		return voteLost
	}
	return votePending
}

func (r *Raft) quorumActive() bool {
	n := 1 // self
	for _, p := range r.peers {
		if p != r.id && r.recentActive[p] {
			n++
		}
	}
	return n >= r.quorum()
}

// ---- messages ----

func (r *Raft) bcastHeartbeat() {
	for _, p := range r.peers {
		if p != r.id {
			r.send(Message{To: p, Type: MsgHeartbeat})
		}
	}
}

// send queues m. Term defaults to the current term; pre-vote messages set
// their term explicitly.
func (r *Raft) send(m Message) {
	m.From = r.id
	if m.Term == 0 && m.Type != MsgPreVote && m.Type != MsgPreVoteResp {
		m.Term = r.term
	}
	r.msgs = append(r.msgs, m)
}

// ---- outputs ----

func (r *Raft) hardState() HardState {
	return HardState{Term: r.term, Vote: r.vote, Commit: r.log.committed}
}

func (r *Raft) softState() SoftState { return SoftState{Lead: r.lead, State: r.state} }

// HasReady reports whether Ready would return any work.
func (r *Raft) HasReady() bool {
	return r.hardState() != r.prevHard ||
		r.softState() != r.prevSoft ||
		len(r.msgs) > 0 ||
		r.log.stable < r.log.lastIndex() ||
		r.log.applied < r.log.committed
}

// Ready returns the pending output. Call Advance after the driver handles it.
func (r *Raft) Ready() Ready {
	rd := Ready{
		Entries:          r.log.unstableEntries(),
		Messages:         slices.Clone(r.msgs),
		CommittedEntries: r.log.nextCommitted(),
	}
	if hs := r.hardState(); hs != r.prevHard {
		rd.HardState = &hs
	}
	if ss := r.softState(); ss != r.prevSoft {
		rd.SoftState = &ss
	}
	return rd
}

// Advance records that the driver handled rd. Output produced after the
// Ready call stays pending.
func (r *Raft) Advance(rd Ready) {
	if rd.HardState != nil {
		r.prevHard = *rd.HardState
	}
	if rd.SoftState != nil {
		r.prevSoft = *rd.SoftState
	}
	if n := len(rd.Entries); n > 0 {
		r.log.stableTo(rd.Entries[n-1].Index)
	}
	if n := len(rd.CommittedEntries); n > 0 {
		r.log.appliedTo(rd.CommittedEntries[n-1].Index)
	}
	r.msgs = r.msgs[len(rd.Messages):]
}

// Status returns the node's current state.
func (r *Raft) Status() Status {
	return Status{
		ID:        r.id,
		HardState: r.hardState(),
		SoftState: r.softState(),
		LastIndex: r.log.lastIndex(),
		LastTerm:  r.log.lastTerm(),
		Applied:   r.log.applied,
	}
}
