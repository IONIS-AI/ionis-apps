package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeSrc(t *testing.T, dir string) string {
	t.Helper()
	src := filepath.Join(dir, "goes_xray_7day.json")
	if err := os.WriteFile(src, []byte(`[{"time_tag":"x"}]`), 0o644); err != nil {
		t.Fatal(err)
	}
	return src
}

// The archive directory is created group-writable, whatever the umask.
func TestDatedCopyCreatesGroupWritableDir(t *testing.T) {
	dir := t.TempDir()
	arch := filepath.Join(dir, "xray-archive")
	got, err := keepDatedCopy(writeSrc(t, dir), arch, time.Date(2026, 9, 24, 1, 17, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("keepDatedCopy: %v", err)
	}
	if filepath.Base(got) != "goes_xray_20260924.json" {
		t.Errorf("wrote %s", got)
	}
	fi, _ := os.Stat(arch)
	if fi.Mode().Perm()&0o020 == 0 || fi.Mode()&os.ModeSetgid == 0 {
		t.Errorf("archive dir mode %v, want group-writable with setgid", fi.Mode())
	}
}

// An archive directory the process cannot write is an ERROR, not a warning.
func TestDatedCopyUnwritableDirIsAnError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := t.TempDir()
	arch := filepath.Join(dir, "xray-archive")
	if err := os.Mkdir(arch, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(arch, 0o755) })
	if _, err := keepDatedCopy(writeSrc(t, dir), arch, time.Now()); err == nil {
		t.Error("copy into a read-only archive directory reported success")
	}
}

// A second run the same day replaces that day's copy.
func TestDatedCopySameDayReplaces(t *testing.T) {
	dir := t.TempDir()
	arch := filepath.Join(dir, "xray-archive")
	src := writeSrc(t, dir)
	now := time.Date(2026, 9, 24, 1, 0, 0, 0, time.UTC)
	if _, err := keepDatedCopy(src, arch, now); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, []byte(`[{"time_tag":"y"}]`), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := keepDatedCopy(src, arch, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(got); string(b) != `[{"time_tag":"y"}]` {
		t.Errorf("same-day copy not replaced: %s", b)
	}
}
