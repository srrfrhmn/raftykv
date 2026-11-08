//go:build windows

package env

import "os"

// syncDir is a no-op on Windows: NTFS has no directory fsync. Metadata
// changes are journaled by the file system. The check below keeps the
// error behavior the same as on Unix for a missing directory.
func syncDir(dir string) error {
	_, err := os.Stat(dir)
	return err
}
