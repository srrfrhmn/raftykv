// Package rafttest is a deterministic in-memory network for Raft tests
// (RKV-014).
//
// Network drives a set of raft.Raft nodes in one goroutine. It persists each
// node's HardState and Entries the way a driver would, routes messages with
// configurable drop, delay, and partitions, and supports crash and restart.
// The same seed gives the same run. Network also checks Election Safety
// (at most one leader per term) on every step.
package rafttest

import (
	"fmt"
	"slices"

	"github.com/srrfrhmn/raftykv/env"
	"github.com/srrfrhmn/raftykv/raft"
)

type link struct{ from, to uint64 }

type pending struct {
	at  int
	seq int
	m   raft.Message
}

// durable is what a node's disk holds.
type durable struct {
	hs   raft.HardState
	ents []raft.Entry
}

// Network is a simulated cluster. It is not safe for concurrent use.
type Network struct {
	ids     []uint64
	nodes   map[uint64]*raft.Raft
	disk    map[uint64]*durable
	down    map[uint64]bool
	tmpl    raft.Config
	seed    uint64
	rand    env.Rand
	restart map[uint64]int // restart count, for per-incarnation seeds

	cut   map[link]bool
	drop  map[link]float64
	delay int // max extra delay in ticks, uniform in [0, delay]

	queue []pending
	seq   int
	now   int

	leaders    map[uint64]uint64 // term -> leader
	violations []string

	// OnDeliver, if set, sees every message just before delivery.
	OnDeliver func(raft.Message)
	// Delivered counts delivered messages by type.
	Delivered map[raft.MessageType]int
}

// New builds a cluster of ids. configure, if not nil, edits the config
// template (ElectionTick, PreVote, CheckQuorum, ...) before nodes start.
func New(seed uint64, ids []uint64, configure func(*raft.Config)) *Network {
	n := &Network{
		ids:       slices.Sorted(slices.Values(ids)),
		nodes:     map[uint64]*raft.Raft{},
		disk:      map[uint64]*durable{},
		down:      map[uint64]bool{},
		seed:      seed,
		rand:      env.NewRand(seed),
		restart:   map[uint64]int{},
		cut:       map[link]bool{},
		drop:      map[link]float64{},
		leaders:   map[uint64]uint64{},
		Delivered: map[raft.MessageType]int{},
	}
	n.tmpl = raft.Config{Peers: n.ids, ElectionTick: 10, HeartbeatTick: 1}
	if configure != nil {
		configure(&n.tmpl)
	}
	for _, id := range n.ids {
		n.disk[id] = &durable{}
		n.start(id)
	}
	return n
}

func (n *Network) start(id uint64) {
	c := n.tmpl
	c.ID = id
	c.Peers = slices.Clone(n.ids)
	c.Rand = env.NewRand(n.seed ^ (id * 0x9e3779b97f4a7c15) ^ uint64(n.restart[id])<<32)
	d := n.disk[id]
	c.HardState = d.hs
	c.Entries = slices.Clone(d.ents)
	r, err := raft.New(c)
	if err != nil {
		panic(err)
	}
	n.nodes[id] = r
}

// ---- accessors ----

// Node returns the Raft instance of id, or nil if id is down.
func (n *Network) Node(id uint64) *raft.Raft {
	if n.down[id] {
		return nil
	}
	return n.nodes[id]
}

// IDs returns all node IDs in sorted order.
func (n *Network) IDs() []uint64 { return slices.Clone(n.ids) }

// Now returns the current tick.
func (n *Network) Now() int { return n.now }

// Leaders returns the live nodes that think they are leader.
func (n *Network) Leaders() []uint64 {
	var out []uint64
	for _, id := range n.ids {
		if r := n.Node(id); r != nil && r.Status().SoftState.State == raft.StateLeader {
			out = append(out, id)
		}
	}
	return out
}

// Leader returns the live leader with the highest term.
func (n *Network) Leader() (uint64, bool) {
	var best, bestTerm uint64
	for _, id := range n.Leaders() {
		if t := n.nodes[id].Status().HardState.Term; best == raft.None || t > bestTerm {
			best, bestTerm = id, t
		}
	}
	return best, best != raft.None
}

// Violations returns safety violations seen so far.
func (n *Network) Violations() []string { return slices.Clone(n.violations) }

// Durable returns the persisted HardState of id.
func (n *Network) Durable(id uint64) raft.HardState { return n.disk[id].hs }

// ---- faults ----

// Isolate cuts all links to and from id.
func (n *Network) Isolate(id uint64) {
	for _, o := range n.ids {
		if o != id {
			n.cut[link{id, o}] = true
			n.cut[link{o, id}] = true
		}
	}
}

// Partition splits the cluster. Nodes in different groups cannot talk.
// Nodes not listed form one more group.
func (n *Network) Partition(groups ...[]uint64) {
	side := map[uint64]int{}
	for i, g := range groups {
		for _, id := range g {
			side[id] = i + 1
		}
	}
	for _, a := range n.ids {
		for _, b := range n.ids {
			if a != b && side[a] != side[b] {
				n.cut[link{a, b}] = true
			}
		}
	}
}

// Cut drops all messages from a to b.
func (n *Network) Cut(a, b uint64) { n.cut[link{a, b}] = true }

// SetDrop drops messages from a to b with probability p.
func (n *Network) SetDrop(a, b uint64, p float64) { n.drop[link{a, b}] = p }

// SetDropAll drops every message with probability p.
func (n *Network) SetDropAll(p float64) {
	for _, a := range n.ids {
		for _, b := range n.ids {
			if a != b {
				n.drop[link{a, b}] = p
			}
		}
	}
}

// SetDelay delays each message by a uniform random [0, maxTicks] ticks.
// Delay also reorders messages.
func (n *Network) SetDelay(maxTicks int) { n.delay = maxTicks }

// Heal removes all cuts, drops, and delays. Queued messages stay queued.
func (n *Network) Heal() {
	n.cut = map[link]bool{}
	n.drop = map[link]float64{}
	n.delay = 0
}

// Crash stops id. Volatile state is lost; durable state stays.
func (n *Network) Crash(id uint64) { n.down[id] = true }

// Load replaces the durable state of id and restarts it from that state.
// Tests use it to set up logs with specific terms.
func (n *Network) Load(id uint64, hs raft.HardState, ents []raft.Entry) {
	n.disk[id] = &durable{hs: hs, ents: slices.Clone(ents)}
	n.Restart(id)
}

// Restart starts id again from its durable state.
func (n *Network) Restart(id uint64) {
	n.restart[id]++
	n.down[id] = false
	n.start(id)
}

// ---- driving ----

// Tick advances time by one tick on every live node, then delivers due messages.
func (n *Network) Tick() {
	n.now++
	for _, id := range n.ids {
		if r := n.Node(id); r != nil {
			r.Tick()
			n.drain(id)
		}
	}
	n.Deliver()
}

// TickN calls Tick k times.
func (n *Network) TickN(k int) {
	for i := 0; i < k; i++ {
		n.Tick()
	}
}

// TickUntil ticks until cond is true or max ticks pass. It reports whether
// cond became true.
func (n *Network) TickUntil(cond func() bool, max int) bool {
	for i := 0; i < max; i++ {
		if cond() {
			return true
		}
		n.Tick()
	}
	return cond()
}

// Campaign makes id start an election now.
func (n *Network) Campaign(id uint64) {
	if r := n.Node(id); r != nil {
		_ = r.Campaign()
		n.drain(id)
		n.Deliver()
	}
}

// Deliver delivers all messages due at the current tick, including messages
// produced while delivering.
func (n *Network) Deliver() {
	for {
		i := n.nextDue()
		if i < 0 {
			return
		}
		p := n.queue[i]
		n.queue = slices.Delete(n.queue, i, i+1)
		r := n.Node(p.m.To)
		if r == nil {
			continue
		}
		if n.OnDeliver != nil {
			n.OnDeliver(p.m)
		}
		n.Delivered[p.m.Type]++
		_ = r.Step(p.m)
		n.drain(p.m.To)
	}
}

func (n *Network) nextDue() int {
	best := -1
	for i, p := range n.queue {
		if p.at <= n.now && (best < 0 || p.at < n.queue[best].at ||
			(p.at == n.queue[best].at && p.seq < n.queue[best].seq)) {
			best = i
		}
	}
	return best
}

// drain handles one node's Ready like a driver: persist, send, apply, advance.
func (n *Network) drain(id uint64) {
	r := n.nodes[id]
	for r.HasReady() {
		rd := r.Ready()
		d := n.disk[id]
		if rd.HardState != nil {
			d.hs = *rd.HardState
		}
		if len(rd.Entries) > 0 {
			first := rd.Entries[0].Index
			d.ents = append(d.ents[:first-1], rd.Entries...)
		}
		for _, m := range rd.Messages {
			n.send(m)
		}
		r.Advance(rd)
	}
	n.checkElectionSafety(id)
}

func (n *Network) send(m raft.Message) {
	l := link{m.From, m.To}
	if n.cut[l] {
		return
	}
	if p := n.drop[l]; p > 0 && n.rand.Float64() < p {
		return
	}
	at := n.now
	if n.delay > 0 {
		at += int(n.rand.Int64N(int64(n.delay) + 1))
	}
	n.seq++
	n.queue = append(n.queue, pending{at: at, seq: n.seq, m: m})
}

func (n *Network) checkElectionSafety(id uint64) {
	st := n.nodes[id].Status()
	if st.SoftState.State != raft.StateLeader {
		return
	}
	term := st.HardState.Term
	if prev, ok := n.leaders[term]; ok && prev != id {
		n.violations = append(n.violations,
			fmt.Sprintf("election safety: term %d has leaders %d and %d (tick %d)", term, prev, id, n.now))
		return
	}
	n.leaders[term] = id
}
