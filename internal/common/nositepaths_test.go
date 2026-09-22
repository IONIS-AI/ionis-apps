package common

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// No site path may be compiled into any binary.
//
// THIS TEST EXISTS BECAUSE THE FIRST SWEEP MISSED FIVE OF THEM. That sweep
// enumerated by flag NAME -- source-dir, report-dir, dest -- and so walked
// straight past `-src` in rbn-ingest, contest-ingest, pskr-ingest and
// wspr-live-ingest, and `-outdir` in pskr-collector. Four of those five are
// ingesters that run on a timer, and contest-ingest's default was
// /mnt/contest-logs: the STALE tree, not _v2, which is the 40%-short corpus that
// already cost an afternoon elsewhere.
//
// So the check matches on the VALUE, which is the property that actually matters,
// rather than on a list of flag names someone has to remember to extend. A new
// tool with a new flag name is covered the day it is written.
func TestNoCompiledInSitePaths(t *testing.T) {
	root := "../../cmd"
	if _, err := os.Stat(root); err != nil {
		t.Skipf("no cmd/ directory here: %v", err)
	}

	// A default that begins with one of these is a statement about one machine,
	// compiled into a package published on COPR.
	sitePath := regexp.MustCompile(`flag\.String\([^)]*"(/mnt/|/scratch/|/var/(?:log|lib|cache)/)[^"]*"`)

	var found []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for i, line := range strings.Split(string(b), "\n") {
			if sitePath.MatchString(line) {
				found = append(found, filepath.Base(filepath.Dir(path))+
					" line "+itoa(i+1)+": "+strings.TrimSpace(line))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}

	if len(found) > 0 {
		t.Errorf("%d compiled-in site path default(s). Resolve through "+
			"common.ResolvePath from /etc/ionis-apps/paths.conf instead:\n  %s",
			len(found), strings.Join(found, "\n  "))
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
