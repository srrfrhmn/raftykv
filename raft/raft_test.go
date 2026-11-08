package raft_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/srrfrhmn/raftykv/env"
	"github.com/srrfrhmn/raftykv/raft"
	"github.com/srrfrhmn/raftykv/raft/rafttest"
)

func ids(n int) []uint64 {
	out := make([]uint64, n)
	for i := range out {
		out[i] = uint64(i + 1)
	}
	return out
}

func opts(preVote, checkQuorum bool) func(*raft.Config) {
	return func(c *raft.Config) {
		c.PreVote = preVote
		c.CheckQuorum = checkQuorum
	}
}

func noViolations(t *testing.T, n *rafttest.Network) {
	t.Helper()
	if v := n.Violations(); len(v) > 0 {
		t.Fatalf("safety violations:\n%v", v)
	}
}

func waitLeader(t *testing.T, n *rafttest.Network, maxTicks int) uint64 {
	t.Helper()
	if !n.TickUntil(func() bool { return len(n.Leaders()) == 1 }, maxTicks) {
		t.Fatalf("no single leader after %d ticks (leaders=%v)", maxTicks, n.Leaders())
	}
	l, _ := n.Leader()
	return l
}

func term(n *rafttest.Network, id uint64) uint64 { return n.Node(id).Status().HardState.Term }

func state(n *rafttest.Network, id uint64) raft.StateType { return n.Node(id).Status().SoftState.State }

// ---- config ----

func TestConfigValidation(t *testing.T) {
	good := raft.Config{ID: 1, Peers: []uint64{1, 2, 3}, Rand: env.NewRand(1)}
	cases := map[string]func(*raft.Config){
		"zero ID":           func(c *raft.Config) { c.ID = 0 },
		"ID not in peers":   func(c *raft.Config) { c.ID = 9 },
		"zero peer":         func(c *raft.Config) { c.Peers = []uint64{0, 1} },
		"election<=hb":      func(c *raft.Config) { c.ElectionTick, c.HeartbeatTick = 3, 3 },
		"nil rand":          func(c *raft.Config) { c.Rand = nil },
		"bad entry indexes": func(c *raft.Config) { c.Entries = []raft.Entry{{Index: 2, Term: 1}} },
	}
	if _, err := raft.New(good); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	for name, mutate := range cases {
		c := good
		mutate(&c)
		if _, err := raft.New(c); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}

// ---- election ----

func TestSingleNodeElectsItself(t *testing.T) {
	for _, pv := range []bool{false, true} {
		n := rafttest.New(1, ids(1), opts(pv, false))
		n.Campaign(1)
		if st := n.Node(1).Status(); st.SoftState.State != raft.StateLeader || st.HardState.Term != 1 {
			t.Fatalf("prevote=%t: status %+v, want leader at term 1", pv, st)
		}
	}
}

func TestLeaderElection(t *testing.T) {
	for _, size := range []int{3, 5} {
		for _, pv := range []bool{false, true} {
			t.Run(fmt.Sprintf("n=%d/prevote=%t", size, pv), func(t *testing.T) {
				n := rafttest.New(42, ids(size), opts(pv, pv))
				l := waitLeader(t, n, 100)
				n.TickN(3)
				lt := term(n, l)
				for _, id := range n.IDs() {
					st := n.Node(id).Status()
					if st.SoftState.Lead != l || st.HardState.Term != lt {
						t.Errorf("node %d: lead=%d term=%d, want lead=%d term=%d",
							id, st.SoftState.Lead, st.HardState.Term, l, lt)
					}
				}
				noViolations(t, n)
			})
		}
	}
}

func TestLeaderVotesForItself(t *testing.T) {
	n := rafttest.New(3, ids(3), nil)
	l := waitLeader(t, n, 100)
	if v := n.Durable(l).Vote; v != l {
		t.Fatalf("leader %d durable vote = %d", l, v)
	}
}

// Election Safety: at most one leader per term, under random drops, delays,
// partitions, crashes, and restarts. Network checks it on every step.
func TestElectionSafetyRandomized(t *testing.T) {
	seeds := 150
	if testing.Short() {
		seeds = 20
	}
	for _, pv := range []bool{false, true} {
		for seed := 1; seed <= seeds; seed++ {
			n := rafttest.New(uint64(seed), ids(5), opts(pv, pv))
			chaos := env.NewRand(uint64(seed) * 31)
			for round := 0; round < 20; round++ {
				n.Heal()
				switch chaos.Int64N(4) {
				case 0:
					n.SetDropAll(0.2)
					n.SetDelay(3)
				case 1:
					cut := chaos.Int64N(4) + 1
					n.Partition(ids(5)[:cut])
				case 2:
					victim := uint64(chaos.Int64N(5) + 1)
					n.Crash(victim)
					n.TickN(int(chaos.Int64N(30)))
					n.Restart(victim)
				}
				n.TickN(50)
			}
			if v := n.Violations(); len(v) > 0 {
				t.Fatalf("prevote=%t seed=%d: %v", pv, seed, v)
			}
		}
	}
}

func TestElectionLivenessAfterHeal(t *testing.T) {
	for seed := uint64(1); seed <= 30; seed++ {
		n := rafttest.New(seed, ids(5), opts(true, true))
		n.SetDropAll(0.3)
		n.SetDelay(5)
		n.TickN(300)
		n.Heal()
		n.TickN(10) // flush delayed messages
		waitLeader(t, n, 200)
		noViolations(t, n)
	}
}

// §5.4.1: a candidate whose log is behind cannot win.
func TestCandidateWithStaleLogLoses(t *testing.T) {
	n := rafttest.New(5, ids(3), nil)
	short := []raft.Entry{{Term: 1, Index: 1}}
	long := []raft.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}
	n.Load(1, raft.HardState{Term: 2}, short)
	n.Load(2, raft.HardState{Term: 2}, long)
	n.Load(3, raft.HardState{Term: 2}, long)

	n.Campaign(1)
	if state(n, 1) == raft.StateLeader {
		t.Fatal("node with stale log became leader")
	}
	if state(n, 1) != raft.StateFollower {
		t.Fatalf("stale candidate state = %v, want Follower after rejection", state(n, 1))
	}

	n.Campaign(2)
	if state(n, 2) != raft.StateLeader {
		t.Fatalf("up-to-date candidate state = %v, want Leader", state(n, 2))
	}
	noViolations(t, n)
}

// RKV-011: the vote is in Ready.HardState and survives a restart.
func TestVotePersistsAcrossRestart(t *testing.T) {
	cfg := raft.Config{ID: 3, Peers: []uint64{1, 2, 3}, Rand: env.NewRand(1)}
	r, _ := raft.New(cfg)
	_ = r.Step(raft.Message{Type: raft.MsgVote, From: 1, To: 3, Term: 1})
	rd := r.Ready()
	if rd.HardState == nil || rd.HardState.Vote != 1 || rd.HardState.Term != 1 {
		t.Fatalf("Ready.HardState = %+v, want term=1 vote=1", rd.HardState)
	}
	if len(rd.Messages) != 1 || rd.Messages[0].Type != raft.MsgVoteResp || rd.Messages[0].Reject {
		t.Fatalf("messages = %v, want one granted MsgVoteResp", rd.Messages)
	}

	// Crash after persisting, restart from the durable state.
	cfg.HardState = *rd.HardState
	r2, _ := raft.New(cfg)
	_ = r2.Step(raft.Message{Type: raft.MsgVote, From: 2, To: 3, Term: 1})
	rd2 := r2.Ready()
	if len(rd2.Messages) != 1 || !rd2.Messages[0].Reject {
		t.Fatalf("after restart: messages = %v, want rejection (already voted for 1)", rd2.Messages)
	}
	if rd2.HardState != nil {
		t.Fatalf("rejection changed HardState: %+v", rd2.HardState)
	}

	// The same candidate can ask again (lost response) and gets the vote.
	_ = r2.Step(raft.Message{Type: raft.MsgVote, From: 1, To: 3, Term: 1})
	if m := r2.Ready().Messages; m[len(m)-1].Reject {
		t.Fatal("repeat vote request from the voted-for candidate was rejected")
	}
}

func TestReadyAndAdvance(t *testing.T) {
	r, _ := raft.New(raft.Config{ID: 1, Peers: []uint64{1, 2, 3}, Rand: env.NewRand(1)})
	if r.HasReady() {
		t.Fatalf("fresh node has Ready: %+v", r.Ready())
	}
	r.Tick()
	if r.HasReady() {
		t.Fatal("a tick without a state change produced Ready")
	}
	_ = r.Campaign()
	rd := r.Ready()
	if rd.HardState == nil || rd.HardState.Term != 1 || rd.HardState.Vote != 1 {
		t.Fatalf("HardState = %+v", rd.HardState)
	}
	if rd.SoftState == nil || rd.SoftState.State != raft.StateCandidate {
		t.Fatalf("SoftState = %+v", rd.SoftState)
	}
	if len(rd.Messages) != 2 {
		t.Fatalf("got %d vote requests, want 2", len(rd.Messages))
	}
	// Output that arrives between Ready and Advance stays pending.
	_ = r.Step(raft.Message{Type: raft.MsgVoteResp, From: 2, To: 1, Term: 1})
	r.Advance(rd)
	if !r.HasReady() {
		t.Fatal("leader heartbeats produced after Ready were lost")
	}
	rd = r.Ready()
	if rd.SoftState == nil || rd.SoftState.State != raft.StateLeader || rd.HardState != nil {
		t.Fatalf("after win: soft=%+v hard=%+v", rd.SoftState, rd.HardState)
	}
	r.Advance(rd)
	if r.HasReady() {
		t.Fatal("HasReady after full Advance")
	}
}

// ---- PreVote (RKV-012) ----

func TestPreVoteIsolatedNodeKeepsTerm(t *testing.T) {
	n := rafttest.New(11, ids(3), opts(true, true))
	l := waitLeader(t, n, 100)
	t0 := term(n, l)
	f := uint64(1)
	if f == l {
		f = 2
	}

	n.Isolate(f)
	n.TickN(300)
	if got := term(n, f); got != t0 {
		t.Fatalf("isolated node term %d -> %d; PreVote must keep it", t0, got)
	}

	n.Heal()
	n.TickN(50)
	if nl, _ := n.Leader(); nl != l || term(n, l) != t0 {
		t.Fatalf("rejoin disrupted cluster: leader %d->%d term %d->%d", l, nl, t0, term(n, l))
	}
	if state(n, f) != raft.StateFollower || n.Node(f).Status().SoftState.Lead != l {
		t.Fatalf("rejoined node: %+v", n.Node(f).Status().SoftState)
	}
	noViolations(t, n)
}

// Contrast: without PreVote, the isolated node raises its term and forces
// the leader to step down when it rejoins.
func TestWithoutPreVoteRejoinDisrupts(t *testing.T) {
	n := rafttest.New(11, ids(3), opts(false, false))
	l := waitLeader(t, n, 100)
	t0 := term(n, l)
	f := uint64(1)
	if f == l {
		f = 2
	}
	n.Isolate(f)
	n.TickN(300)
	if term(n, f) <= t0+5 {
		t.Fatalf("isolated node term = %d, expected it to grow well past %d", term(n, f), t0)
	}
	n.Heal()
	n.TickN(100)
	nl := waitLeader(t, n, 200)
	if term(n, nl) <= t0 {
		t.Fatalf("cluster term %d did not move past %d", term(n, nl), t0)
	}
	noViolations(t, n)
}

func TestPreVoteDoesNotChangeVoterTerm(t *testing.T) {
	r, _ := raft.New(raft.Config{ID: 2, Peers: []uint64{1, 2, 3}, PreVote: true, Rand: env.NewRand(1)})
	_ = r.Step(raft.Message{Type: raft.MsgPreVote, From: 1, To: 2, Term: 1})
	rd := r.Ready()
	if rd.HardState != nil {
		t.Fatalf("pre-vote changed HardState: %+v", rd.HardState)
	}
	if len(rd.Messages) != 1 || rd.Messages[0].Type != raft.MsgPreVoteResp ||
		rd.Messages[0].Reject || rd.Messages[0].Term != 1 {
		t.Fatalf("messages = %v, want granted MsgPreVoteResp at term 1", rd.Messages)
	}
}

// ---- CheckQuorum (RKV-013) ----

func TestCheckQuorumLeaderStepsDown(t *testing.T) {
	n := rafttest.New(21, ids(5), opts(true, true))
	l := waitLeader(t, n, 100)
	t0 := term(n, l)
	other := uint64(1)
	if other == l {
		other = 2
	}
	n.Partition([]uint64{l, other})

	// Within two election timeouts the minority leader steps down.
	if !n.TickUntil(func() bool { return state(n, l) != raft.StateLeader }, 2*10+1) {
		t.Fatalf("minority leader %d still leads", l)
	}
	if !n.TickUntil(func() bool {
		nl, ok := n.Leader()
		return ok && nl != l && nl != other && term(n, nl) > t0
	}, 200) {
		t.Fatal("majority side did not elect a new leader")
	}
	noViolations(t, n)
}

func TestWithoutCheckQuorumLeaderStaysInMinority(t *testing.T) {
	n := rafttest.New(21, ids(5), opts(false, false))
	l := waitLeader(t, n, 100)
	other := uint64(1)
	if other == l {
		other = 2
	}
	n.Partition([]uint64{l, other})
	n.TickN(100)
	if state(n, l) != raft.StateLeader {
		t.Fatal("leader stepped down without CheckQuorum")
	}
}

func TestLeaderStickiness(t *testing.T) {
	n := rafttest.New(31, ids(3), opts(true, true))
	l := waitLeader(t, n, 100)
	n.TickN(2)
	f := uint64(1)
	if f == l {
		f = 2
	}
	t0 := term(n, f)
	r := n.Node(f)
	st := r.Status()
	_ = r.Step(raft.Message{Type: raft.MsgVote, From: 3, To: f, Term: t0 + 5, Index: st.LastIndex, LogTerm: st.LastTerm})
	if term(n, f) != t0 || r.Status().HardState.Vote == 3 {
		t.Fatalf("follower in leader lease accepted a disruptive vote: %+v", r.Status().HardState)
	}
}

// ---- proposals ----

func TestProposeOnlyOnLeader(t *testing.T) {
	n := rafttest.New(41, ids(3), nil)
	l := waitLeader(t, n, 100)
	f := uint64(1)
	if f == l {
		f = 2
	}
	if err := n.Node(f).Propose([]byte("x")); !errors.Is(err, raft.ErrProposalDropped) {
		t.Fatalf("follower Propose err = %v, want ErrProposalDropped", err)
	}
	r := n.Node(l)
	if err := r.Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	rd := r.Ready()
	if len(rd.Entries) != 1 || rd.Entries[0].Index != 1 || rd.Entries[0].Term != term(n, l) {
		t.Fatalf("leader Ready.Entries = %+v", rd.Entries)
	}
}

// ---- determinism ----

func TestNetworkIsDeterministic(t *testing.T) {
	run := func() string {
		n := rafttest.New(99, ids(5), opts(true, true))
		n.SetDropAll(0.2)
		n.SetDelay(4)
		n.TickN(200)
		n.Partition([]uint64{1, 2})
		n.TickN(200)
		out := fmt.Sprint(n.Delivered)
		for _, id := range n.IDs() {
			out += fmt.Sprintf("|%+v", n.Node(id).Status())
		}
		return out
	}
	if a, b := run(), run(); a != b {
		t.Fatalf("same seed, different runs:\n%s\n%s", a, b)
	}
}
