package raft

// raftLog is the in-memory log used by the S1 core.
//
// It holds every entry since index 1, with stable/unstable tracking so Ready
// can hand new entries to the driver. RKV-020 replaces it with the Storage
// interface, conflict truncation, and compaction support.
type raftLog struct {
	entries   []Entry // entries[i].Index == i+1
	stable    uint64  // highest index handed to the driver and acknowledged
	committed uint64
	applied   uint64
}

func newRaftLog(initial []Entry, committed uint64) *raftLog {
	l := &raftLog{entries: append([]Entry(nil), initial...)}
	l.stable = l.lastIndex()
	l.committed = min(committed, l.lastIndex())
	l.applied = l.committed
	return l
}

func (l *raftLog) lastIndex() uint64 { return uint64(len(l.entries)) }

func (l *raftLog) lastTerm() uint64 { return l.term(l.lastIndex()) }

// term returns the term at index i, or 0 for i == 0 or i out of range.
func (l *raftLog) term(i uint64) uint64 {
	if i == 0 || i > l.lastIndex() {
		return 0
	}
	return l.entries[i-1].Term
}

// isUpToDate reports whether a log ending at (lastTerm, lastIndex) is at
// least as up-to-date as this log (Raft §5.4.1).
func (l *raftLog) isUpToDate(lastIndex, lastTerm uint64) bool {
	myTerm := l.lastTerm()
	return lastTerm > myTerm || (lastTerm == myTerm && lastIndex >= l.lastIndex())
}

// append adds entries at the end and returns the new last index.
func (l *raftLog) append(ents ...Entry) uint64 {
	l.entries = append(l.entries, ents...)
	return l.lastIndex()
}

func (l *raftLog) unstableEntries() []Entry {
	if l.stable >= l.lastIndex() {
		return nil
	}
	return append([]Entry(nil), l.entries[l.stable:]...)
}

func (l *raftLog) nextCommitted() []Entry {
	if l.applied >= l.committed {
		return nil
	}
	return append([]Entry(nil), l.entries[l.applied:l.committed]...)
}

func (l *raftLog) stableTo(i uint64)  { l.stable = max(l.stable, min(i, l.lastIndex())) }
func (l *raftLog) appliedTo(i uint64) { l.applied = max(l.applied, min(i, l.committed)) }
