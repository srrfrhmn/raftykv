package env

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"math/rand/v2"
	"os"
	"sort"
	"time"
)

// RealClock returns the system clock.
func RealClock() Clock { return realClock{} }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTicker(d time.Duration) Ticker { return realTicker{time.NewTicker(d)} }

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }

// NewRand returns a deterministic PCG source for seed.
// The same seed always gives the same sequence.
func NewRand(seed uint64) Rand {
	return &pcgRand{r: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}
}

// RandomSeed returns a seed from the OS entropy source.
// Production code uses it; tests and the simulator pass fixed seeds.
func RandomSeed() uint64 {
	var b [8]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		return uint64(time.Now().UnixNano())
	}
	return binary.LittleEndian.Uint64(b[:])
}

type pcgRand struct{ r *rand.Rand }

func (p *pcgRand) Uint64() uint64       { return p.r.Uint64() }
func (p *pcgRand) Int64N(n int64) int64 { return p.r.Int64N(n) }
func (p *pcgRand) Float64() float64     { return p.r.Float64() }

// RealDisk returns the host file system.
func RealDisk() Disk { return realDisk{} }

type realDisk struct{}

func (realDisk) OpenFile(name string, flag int, perm uint32) (File, error) {
	f, err := os.OpenFile(name, flag, os.FileMode(perm))
	if err != nil {
		return nil, err
	}
	return realFile{f}, nil
}

func (realDisk) MkdirAll(path string, perm uint32) error {
	return os.MkdirAll(path, os.FileMode(perm))
}

func (realDisk) Remove(name string) error             { return os.Remove(name) }
func (realDisk) Rename(oldpath, newpath string) error { return os.Rename(oldpath, newpath) }

func (realDisk) ReadDir(dir string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(ents))
	for _, e := range ents {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names, nil
}

func (realDisk) SyncDir(dir string) error { return syncDir(dir) }

type realFile struct{ *os.File }

func (f realFile) Size() (int64, error) {
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}
