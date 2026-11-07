// Package env defines the I/O boundary of raftykv.
//
// Deterministic packages (raft, kv, shardkv) MUST NOT import time, net, os,
// or math/rand. They get time, randomness, disk, and network access only
// through the interfaces in this package. Production code passes the real
// implementations from this package. The simulator (sim/) passes simulated
// implementations. See docs/DESIGN.md §9.1.
package env

import (
	"errors"
	"io"
	"time"
)

// Clock gives wall time and tickers.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// NewTicker returns a ticker that fires every d.
	NewTicker(d time.Duration) Ticker
}

// Ticker delivers ticks on a channel until Stop is called.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// Rand is a source of pseudo-random numbers.
// Implementations need not be safe for concurrent use.
type Rand interface {
	// Uint64 returns a pseudo-random 64-bit value.
	Uint64() uint64
	// Int64N returns a value in [0, n). It panics if n <= 0.
	Int64N(n int64) int64
	// Float64 returns a value in [0.0, 1.0).
	Float64() float64
}

// Disk is the file-system boundary. Paths use the host separator.
type Disk interface {
	// OpenFile opens a file. flag uses the os.O_* values.
	OpenFile(name string, flag int, perm uint32) (File, error)
	MkdirAll(path string, perm uint32) error
	Remove(name string) error
	Rename(oldpath, newpath string) error
	// ReadDir returns the sorted names of the entries in dir.
	ReadDir(dir string) ([]string, error)
	// SyncDir makes directory entries (create, rename, remove) durable.
	SyncDir(dir string) error
}

// File is an open file on a Disk.
type File interface {
	io.Reader
	io.ReaderAt
	io.Writer
	io.Seeker
	io.Closer
	// Sync makes all written data durable.
	Sync() error
	// Truncate changes the size of the file.
	Truncate(size int64) error
	// Size returns the current size of the file.
	Size() (int64, error)
}

// Envelope is one message on a Transport.
type Envelope struct {
	From, To uint64
	Payload  []byte
}

// Transport moves opaque messages between nodes.
//
// Delivery is best-effort: a message can be lost, delayed, or reordered.
// Raft tolerates all three. The gRPC implementation arrives with RKV-041;
// MemNetwork in this package is the in-process implementation.
type Transport interface {
	// Send queues payload for node to. It does not block on the network.
	Send(to uint64, payload []byte) error
	// Inbox returns the channel of received messages.
	Inbox() <-chan Envelope
	// Close stops the transport and closes Inbox.
	Close() error
}

// ErrClosed is returned by operations on a closed Transport.
var ErrClosed = errors.New("env: transport closed")

// ErrUnknownPeer is returned when Send targets a node that is not registered.
var ErrUnknownPeer = errors.New("env: unknown peer")
