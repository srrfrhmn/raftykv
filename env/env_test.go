package env

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRealClockAdvances(t *testing.T) {
	c := RealClock()
	a := c.Now()
	time.Sleep(2 * time.Millisecond)
	if !c.Now().After(a) {
		t.Fatal("clock did not advance")
	}
}

func TestRealTickerFires(t *testing.T) {
	tk := RealClock().NewTicker(time.Millisecond)
	defer tk.Stop()
	select {
	case <-tk.C():
	case <-time.After(time.Second):
		t.Fatal("ticker did not fire")
	}
}

func TestRandIsDeterministicPerSeed(t *testing.T) {
	a, b, c := NewRand(42), NewRand(42), NewRand(43)
	same := true
	for i := 0; i < 100; i++ {
		x, y, z := a.Uint64(), b.Uint64(), c.Uint64()
		if x != y {
			t.Fatalf("draw %d: same seed gave %d and %d", i, x, y)
		}
		if x != z {
			same = false
		}
	}
	if same {
		t.Fatal("different seeds gave the same sequence")
	}
}

func TestRandRanges(t *testing.T) {
	r := NewRand(1)
	for i := 0; i < 10000; i++ {
		if v := r.Int64N(10); v < 0 || v >= 10 {
			t.Fatalf("Int64N(10) = %d", v)
		}
		if f := r.Float64(); f < 0 || f >= 1 {
			t.Fatalf("Float64() = %f", f)
		}
	}
}

func TestRandomSeedVaries(t *testing.T) {
	if a, b := RandomSeed(), RandomSeed(); a == b {
		t.Fatal("two random seeds are equal")
	}
}

func TestRealDiskRoundTrip(t *testing.T) {
	d := RealDisk()
	dir := filepath.Join(t.TempDir(), "a", "b")
	if err := d.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, "f.dat")
	f, err := d.OpenFile(p, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("hello world")); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if n, _ := f.Size(); n != 11 {
		t.Fatalf("size = %d, want 11", n)
	}
	if err := f.Truncate(5); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := f.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, []byte("hello")) {
		t.Fatalf("read %q", buf)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	q := filepath.Join(dir, "g.dat")
	if err := d.Rename(p, q); err != nil {
		t.Fatal(err)
	}
	if err := d.SyncDir(dir); err != nil {
		t.Fatal(err)
	}
	names, err := d.ReadDir(dir)
	if err != nil || len(names) != 1 || names[0] != "g.dat" {
		t.Fatalf("ReadDir = %v, %v", names, err)
	}
	if err := d.Remove(q); err != nil {
		t.Fatal(err)
	}
	if err := d.SyncDir(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("SyncDir on missing dir: want error")
	}
}

func TestMemNetwork(t *testing.T) {
	n := NewMemNetwork(4)
	a, b := n.Join(1), n.Join(2)

	if err := a.Send(2, []byte("x")); err != nil {
		t.Fatal(err)
	}
	e := <-b.Inbox()
	if e.From != 1 || e.To != 2 || string(e.Payload) != "x" {
		t.Fatalf("got %+v", e)
	}

	if err := a.Send(9, nil); !errors.Is(err, ErrUnknownPeer) {
		t.Fatalf("send to unknown: %v", err)
	}

	n.Cut(1, 2)
	_ = a.Send(2, []byte("lost"))
	select {
	case e := <-b.Inbox():
		t.Fatalf("cut link delivered %+v", e)
	default:
	}
	n.Heal()

	// A full inbox drops; it does not block.
	for i := 0; i < 10; i++ {
		_ = a.Send(2, []byte{byte(i)})
	}
	if got := len(b.Inbox()); got != 4 {
		t.Fatalf("inbox len = %d, want 4", got)
	}

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Send(1, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("send after close: %v", err)
	}
	if err := a.Send(2, nil); !errors.Is(err, ErrUnknownPeer) {
		t.Fatalf("send to closed peer: %v", err)
	}
}
