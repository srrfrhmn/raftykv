package raft

import (
	"testing"

	"github.com/srrfrhmn/raftykv/env"
)

func TestRandomizedElectionTimeoutRange(t *testing.T) {
	r, err := New(Config{ID: 1, Peers: []uint64{1, 2, 3}, ElectionTick: 10, Rand: env.NewRand(7)})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int]bool{}
	for i := 0; i < 1000; i++ {
		r.resetRandomizedElectionTimeout()
		v := r.randomizedElectionTimeout
		if v < 10 || v >= 20 {
			t.Fatalf("timeout %d outside [10, 20)", v)
		}
		seen[v] = true
	}
	if len(seen) != 10 {
		t.Fatalf("saw %d distinct timeouts, want all 10", len(seen))
	}
}

func TestVoteResult(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{1, 2, 3, 4, 5}, Rand: env.NewRand(1)})
	cases := []struct {
		granted, rejected int
		want              voteOutcome
	}{
		{1, 0, votePending},
		{3, 0, voteWon},
		{2, 2, votePending},
		{2, 3, voteLost},
		{3, 2, voteWon},
	}
	for _, c := range cases {
		if got := r.voteResult(c.granted, c.rejected); got != c.want {
			t.Errorf("voteResult(%d, %d) = %d, want %d", c.granted, c.rejected, got, c.want)
		}
	}
}

func TestPollCountsFirstVoteOnly(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{1, 2, 3}, Rand: env.NewRand(1)})
	r.poll(2, true)
	if g, rj := r.poll(2, false); g != 1 || rj != 0 {
		t.Fatalf("duplicate vote changed tally: granted=%d rejected=%d", g, rj)
	}
}
