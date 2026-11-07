package env

import "sync"

// MemNetwork is an in-process Transport hub for tests and local clusters.
// Each registered node gets a Transport. Messages go through buffered
// channels. A full inbox drops the message, like a congested network.
type MemNetwork struct {
	mu     sync.RWMutex
	nodes  map[uint64]*memTransport
	inbox  int
	broken map[[2]uint64]bool
}

// NewMemNetwork returns a hub. inboxSize is the per-node buffer length.
func NewMemNetwork(inboxSize int) *MemNetwork {
	if inboxSize <= 0 {
		inboxSize = 1024
	}
	return &MemNetwork{
		nodes:  map[uint64]*memTransport{},
		inbox:  inboxSize,
		broken: map[[2]uint64]bool{},
	}
}

// Join registers node id and returns its Transport.
func (n *MemNetwork) Join(id uint64) Transport {
	n.mu.Lock()
	defer n.mu.Unlock()
	t := &memTransport{id: id, net: n, in: make(chan Envelope, n.inbox)}
	n.nodes[id] = t
	return t
}

// Cut drops all messages from a to b (one direction).
func (n *MemNetwork) Cut(a, b uint64) {
	n.mu.Lock()
	n.broken[[2]uint64{a, b}] = true
	n.mu.Unlock()
}

// Heal restores all links.
func (n *MemNetwork) Heal() {
	n.mu.Lock()
	n.broken = map[[2]uint64]bool{}
	n.mu.Unlock()
}

type memTransport struct {
	id     uint64
	net    *MemNetwork
	in     chan Envelope
	mu     sync.Mutex
	closed bool
}

func (t *memTransport) Send(to uint64, payload []byte) error {
	t.mu.Lock()
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return ErrClosed
	}
	t.net.mu.RLock()
	dst, ok := t.net.nodes[to]
	cut := t.net.broken[[2]uint64{t.id, to}]
	t.net.mu.RUnlock()
	if !ok {
		return ErrUnknownPeer
	}
	if cut {
		return nil
	}
	buf := make([]byte, len(payload))
	copy(buf, payload)
	dst.deliver(Envelope{From: t.id, To: to, Payload: buf})
	return nil
}

func (t *memTransport) deliver(e Envelope) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	select {
	case t.in <- e:
	default: // inbox full: drop
	}
}

func (t *memTransport) Inbox() <-chan Envelope { return t.in }

func (t *memTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	close(t.in)
	t.net.mu.Lock()
	if t.net.nodes[t.id] == t {
		delete(t.net.nodes, t.id)
	}
	t.net.mu.Unlock()
	return nil
}
