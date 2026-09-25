package main

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// Every level -- mirror root, product, year, month -- comes out group-writable with
// setgid, even under a restrictive umask. The product level is the one that used to
// be missed (#35).
func TestMirrorDirsEveryLevel(t *testing.T) {
	old := syscall.Umask(0o022)
	defer syscall.Umask(old)
	root := filepath.Join(t.TempDir(), "dscovr")
	dst := filepath.Join(root, "f1m", "2027", "01", "x.nc.gz")
	if err := mirrorDirs(dst); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{root, filepath.Join(root, "f1m"), filepath.Join(root, "f1m", "2027"), filepath.Join(root, "f1m", "2027", "01")} {
		fi, err := os.Stat(d)
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm()&0o020 == 0 || fi.Mode()&os.ModeSetgid == 0 {
			t.Errorf("%s: mode %v, want group-writable with setgid", d, fi.Mode())
		}
	}
}
