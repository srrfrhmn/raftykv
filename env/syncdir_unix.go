//go:build !windows

package env

import "os"

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := d.Sync()
	if err := d.Close(); syncErr == nil {
		syncErr = err
	}
	return syncErr
}
