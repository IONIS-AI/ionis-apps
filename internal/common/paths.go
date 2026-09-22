package common

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Site paths are NOT compiled in.
//
// These binaries ship in an RPM on COPR. A default like "/mnt/solar-data/raw"
// baked into the executable is a statement about one machine, asserted to every
// machine that installs it -- and the failure mode is not a clean error. The tool
// creates the directory it was told to write to and then works perfectly, so a
// third-party install silently populates a path that means nothing on their
// system, and nothing reports it. That is exactly how /mnt/ai-stack/solar-data
// came to hold live IONIS source data for months: not a decision anyone defended,
// a default nobody revisited.
//
// So resolution is: FLAG, then ENVIRONMENT, then FAIL -- and the failure names
// both, because an error that does not say how to fix it is a second bug.
//
// The lab's own values live in /etc/ionis-apps/paths.conf, shipped %config(noreplace)
// and read by the units through EnvironmentFile. Site configuration belongs in
// /etc where an operator can see and change it, which is the same pattern
// fleet-ops already uses for contest-reconcile.

// ResolvePath returns the site path for a setting, from the flag if the operator
// gave one, otherwise from env. It returns an error naming both rather than
// guessing, so that an unconfigured install fails at the first run with something
// actionable instead of writing somewhere arbitrary.
//
// flagName is the flag the operator would actually type, passed in rather than
// derived: the first version of this built it from the env var and told people to
// run --solar-data-dir when the flag is --source-dir. An instruction that does not
// work is worse than no instruction, because it costs the reader a round trip to
// find out it was wrong.
//
// what describes the setting in the error ("solar raw data directory").
func ResolvePath(flagValue, envVar, flagName, what string) (string, error) {
	if v := strings.TrimSpace(flagValue); v != "" {
		return v, nil
	}
	if v := strings.TrimSpace(os.Getenv(envVar)); v != "" {
		return v, nil
	}
	return "", fmt.Errorf(
		"no %s configured.\n"+
			"  Set it one of two ways:\n"+
			"    --%s <path>   for a one-off run\n"+
			"    %s=<path>     in /etc/ionis-apps/paths.conf, which the systemd units read\n"+
			"  This binary ships no default on purpose: a compiled-in path is a claim "+
			"about one machine, made to every machine that installs the package.",
		what, flagName, envVar)
}

// ResolveReportDir resolves a tool's run-report directory. The operator sets one
// base -- IONIS_REPORT_DIR -- and each tool places its reports in its own
// subdirectory of it.
//
// subdir is the tool's own name for its reports ("reports-turbo"), not a site
// path: it says nothing about any particular machine and is the same everywhere
// the package is installed. Compiling that in is fine. Compiling in where the
// base lives is not.
//
// An explicit --report-dir is the final directory, not a base, because an
// operator naming a path means that path.
func ResolveReportDir(flagValue, subdir string) (string, error) {
	if v := strings.TrimSpace(flagValue); v != "" {
		return v, nil
	}
	base, err := ResolvePath("", "IONIS_REPORT_DIR", "report-dir", "run-report directory")
	if err != nil {
		return "", err
	}
	return filepath.Join(base, subdir), nil
}
