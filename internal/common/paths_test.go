package common

import (
	"strings"
	"testing"
)

func TestResolvePathPrefersFlag(t *testing.T) {
	t.Setenv("IONIS_TEST_DIR", "/from/env")
	got, err := ResolvePath("/from/flag", "IONIS_TEST_DIR", "test-dir", "test directory")
	if err != nil || got != "/from/flag" {
		t.Fatalf("flag should win: got %q, err %v", got, err)
	}
}

func TestResolvePathFallsBackToEnv(t *testing.T) {
	t.Setenv("IONIS_TEST_DIR", "/from/env")
	got, err := ResolvePath("", "IONIS_TEST_DIR", "test-dir", "test directory")
	if err != nil || got != "/from/env" {
		t.Fatalf("env should be used: got %q, err %v", got, err)
	}
}

// The point of the package: unconfigured must FAIL, never guess. A silently
// chosen path is what put live IONIS data on the AI-stack dataset.
func TestResolvePathFailsWhenUnset(t *testing.T) {
	t.Setenv("IONIS_TEST_DIR", "")
	_, err := ResolvePath("", "IONIS_TEST_DIR", "test-dir", "test directory")
	if err == nil {
		t.Fatal("unset must error, not default to anything")
	}
	// An error that does not say how to fix it is a second bug.
	for _, want := range []string{"IONIS_TEST_DIR", "--test-dir", "/etc/ionis/paths.conf", "test directory"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should name %q, got:\n%s", want, err)
		}
	}
}

// Whitespace-only is unset. A conf file line like `IONIS_X= ` must not resolve to
// a path of one space.
func TestResolvePathTreatsBlankAsUnset(t *testing.T) {
	t.Setenv("IONIS_TEST_DIR", "   ")
	if _, err := ResolvePath("  ", "IONIS_TEST_DIR", "test-dir", "test directory"); err == nil {
		t.Fatal("whitespace-only must be treated as unset")
	}
}

func TestResolveReportDirJoinsBase(t *testing.T) {
	t.Setenv("IONIS_REPORT_DIR", "/var/log/ionis")
	got, err := ResolveReportDir("", "reports-turbo")
	if err != nil || got != "/var/log/ionis/reports-turbo" {
		t.Fatalf("got %q, err %v", got, err)
	}
}

// An operator naming a path means that path, not a base to append to.
func TestResolveReportDirFlagIsFinal(t *testing.T) {
	t.Setenv("IONIS_REPORT_DIR", "/var/log/ionis")
	got, _ := ResolveReportDir("/tmp/mine", "reports-turbo")
	if got != "/tmp/mine" {
		t.Fatalf("explicit flag must be the final directory, got %q", got)
	}
}

func TestResolveReportDirUnsetFails(t *testing.T) {
	t.Setenv("IONIS_REPORT_DIR", "")
	if _, err := ResolveReportDir("", "reports-turbo"); err == nil {
		t.Fatal("unset base must error rather than pick somewhere")
	}
}
