package main

import "testing"

// The CONTEST: header is operator free text and produced 41 labels for 15
// contests. The directory is our own mirror structure, so it is the authority.
// This test is the map's contract: every series directory that exists under
// /mnt/contest-logs/_v2 must resolve, and anything else must fail rather than
// guess.
func TestContestFromSource(t *testing.T) {
	// Every series present in the mirror, with the season form it actually uses.
	// Four series split cw/ph into separate seasons; the rest carry the mode in
	// the series name and use a bare year.
	for _, c := range []struct{ src, want string }{
		{"cq-ww/2005cw", "CQ-WW-CW"},
		{"cq-ww/2025ph", "CQ-WW-SSB"},
		{"cq-wpx/2008cw", "CQ-WPX-CW"},
		{"cq-wpx/2026ph", "CQ-WPX-SSB"},
		{"cq-160/2022cw", "CQ-160-CW"},
		{"cq-160/2026ph", "CQ-160-SSB"},
		{"cq-ww-rtty/2009", "CQ-WW-RTTY"},
		{"cq-wpx-rtty/2012", "CQ-WPX-RTTY"},
		{"arrl-dx-cw/2018", "ARRL-DX-CW"},
		{"arrl-dx-ph/2026", "ARRL-DX-SSB"},
		{"arrl-rtty/2021", "ARRL-RTTY"},
		{"arrl-ss-cw/2018", "ARRL-SS-CW"},
		{"arrl-ss-ph/2025", "ARRL-SS-SSB"},
		{"arrl-10m/2018", "ARRL-10"},
		{"arrl-160m/2025", "ARRL-160"},
		{"arrl-digi/2022", "ARRL-DIGI"},
		{"iaru-hf/2018", "IARU-HF"},
		{"ww-digi/2019", "WW-DIGI"},
	} {
		got, ok := contestFromSource(c.src)
		if !ok {
			t.Errorf("%s: not resolved, but this series exists in the mirror", c.src)
			continue
		}
		if got != c.want {
			t.Errorf("%s = %q, want %q", c.src, got, c.want)
		}
	}
}

// An unknown series must fail loudly. Guessing is how the dimension got 41
// labels; a rejected file names itself and can be fixed, a mislabelled corpus
// cannot.
func TestContestFromSourceRejectsUnknown(t *testing.T) {
	for _, src := range []string{
		"cq-wwz/2020cw",   // typo in a series name
		"new-contest/2026", // a series added upstream that we have not classified
		"2005cw",           // no series component at all
		"",
	} {
		if got, ok := contestFromSource(src); ok {
			t.Errorf("%q resolved to %q, want rejection", src, got)
		}
	}
}

// The cw/ph split keys off the season suffix, not the year, and must not be
// fooled by a year that happens to end in those letters -- or by case.
func TestContestFromSourceSeasonSuffix(t *testing.T) {
	if got, _ := contestFromSource("CQ-WW/2015CW"); got != "CQ-WW-CW" {
		t.Errorf("uppercase series/season = %q, want CQ-WW-CW", got)
	}
	// A bare year under a split series is not phone; it falls to the cw side
	// rather than silently becoming SSB.
	if got, _ := contestFromSource("cq-ww/2015"); got != "CQ-WW-CW" {
		t.Errorf("bare year under split series = %q, want CQ-WW-CW", got)
	}
}
