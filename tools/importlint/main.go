// Command importlint enforces the determinism boundary (docs/DESIGN.md §9.1).
//
// Packages under the deterministic roots MUST NOT import packages that read
// the wall clock, the network, the OS, or a global random source. They use
// the interfaces in package env instead. _test.go files are not checked.
//
// Usage: go run ./tools/importlint [repo-root]
package main

import (
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// deterministicRoots are checked recursively, relative to the repo root.
var deterministicRoots = []string{"raft", "kv", "shardkv", "shardctrl"}

// exempt directories hold real I/O adapters (e.g. the gRPC transport).
var exempt = []string{"raft/transport"}

// forbidden import paths. A path also matches its sub-packages ("net" bans "net/http").
var forbidden = []string{"time", "net", "os", "math/rand", "crypto/rand", "syscall"}

// Violation is one forbidden import.
type Violation struct {
	File   string
	Line   int
	Import string
}

func (v Violation) String() string {
	return fmt.Sprintf("%s:%d: forbidden import %q (use package env)", v.File, v.Line, v.Import)
}

func main() {
	root := "."
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	vs, err := Check(root)
	if err != nil {
		fmt.Fprintln(os.Stderr, "importlint:", err)
		os.Exit(2)
	}
	for _, v := range vs {
		fmt.Println(v)
	}
	if len(vs) > 0 {
		os.Exit(1)
	}
}

// Check scans the deterministic roots under root and returns all violations.
func Check(root string) ([]Violation, error) {
	var out []Violation
	fset := token.NewFileSet()
	for _, r := range deterministicRoots {
		dir := filepath.Join(root, r)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			continue
		}
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			if d.IsDir() {
				if isExempt(rel) || d.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			for _, imp := range f.Imports {
				p, _ := strconv.Unquote(imp.Path.Value)
				if isForbidden(p) {
					out = append(out, Violation{rel, fset.Position(imp.Pos()).Line, p})
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].File != out[j].File {
			return out[i].File < out[j].File
		}
		return out[i].Line < out[j].Line
	})
	return out, nil
}

func isForbidden(p string) bool {
	for _, f := range forbidden {
		if p == f || strings.HasPrefix(p, f+"/") {
			return true
		}
	}
	return false
}

func isExempt(rel string) bool {
	for _, e := range exempt {
		if rel == e || strings.HasPrefix(rel, e+"/") {
			return true
		}
	}
	return false
}
