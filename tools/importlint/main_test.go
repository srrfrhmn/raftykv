package main

import (
	"os"
	"path/filepath"
	"testing"
)

func write(t *testing.T, root, rel, src string) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestCheck(t *testing.T) {
	root := t.TempDir()
	write(t, root, "raft/ok.go", "package raft\nimport (\n\"errors\"\n\"sort\"\n)\nvar _ = errors.New\nvar _ = sort.Ints\n")
	write(t, root, "raft/bad.go", "package raft\nimport (\n\"time\"\n\"math/rand/v2\"\n)\nvar _ = time.Now\nvar _ = rand.Int\n")
	write(t, root, "raft/bad_test.go", "package raft\nimport \"os\"\nvar _ = os.Exit\n")
	write(t, root, "raft/wal/w.go", "package wal\nimport \"os\"\nvar _ = os.Open\n")
	write(t, root, "raft/transport/grpc.go", "package transport\nimport \"net\"\nvar _ = net.Dial\n")
	write(t, root, "kv/k.go", "package kv\nimport \"net/http\"\nvar _ = http.Get\n")
	write(t, root, "env/e.go", "package env\nimport \"time\"\nvar _ = time.Now\n")

	vs, err := Check(root)
	if err != nil {
		t.Fatal(err)
	}
	want := []Violation{
		{"kv/k.go", 2, "net/http"},
		{"raft/bad.go", 3, "time"},
		{"raft/bad.go", 4, "math/rand/v2"},
		{"raft/wal/w.go", 2, "os"},
	}
	if len(vs) != len(want) {
		t.Fatalf("got %v, want %v", vs, want)
	}
	for i := range want {
		if vs[i] != want[i] {
			t.Errorf("violation %d = %v, want %v", i, vs[i], want[i])
		}
	}
}

func TestIsForbiddenPrefixOnly(t *testing.T) {
	if isForbidden("timeutil") || isForbidden("osext") {
		t.Fatal("prefix without slash must not match")
	}
	if !isForbidden("os/exec") {
		t.Fatal("sub-package must match")
	}
}
