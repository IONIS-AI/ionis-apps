package main

import (
	"testing"
	"time"
)

// Every guard here is tested against the states its author did not imagine first --
// empty, absent, boundary -- and only then against the happy path. Three rounds of
// review found the same defect shape each time: a check written for the one failure
// the author pictured, letting everything else through as success.

func TestSourceDeclaredYear(t *testing.T) {
	for _, c := range []struct {
		in   string
		year int
		ok   bool
		why  string
	}{
		{"cq-ww/2005cw", 2005, true, "normal"},
		{"cq-wpx-rtty/2018", 2018, true, "no mode suffix"},
		{"", 0, false, "EMPTY -- must not yield a year"},
		{"cq-ww", 0, false, "no digits at all"},
		{"cq-160/2023cw", 2023, true, "3-digit series name must not be read as the year"},
		{"foo/1899", 0, false, "below the plausible floor"},
		{"foo/2999", 0, false, "above the plausible ceiling"},
	} {
		y, ok := sourceDeclaredYear(c.in)
		if y != c.year || ok != c.ok {
			t.Errorf("%s: sourceDeclaredYear(%q) = (%d,%v), want (%d,%v)", c.why, c.in, y, ok, c.year, c.ok)
		}
	}
}

func TestInDeclaredYearBoundaries(t *testing.T) {
	d := func(s string) time.Time { v, _ := time.Parse(time.RFC3339, s); return v }
	for _, c := range []struct {
		ts   string
		year int
		want bool
		why  string
	}{
		{"2021-01-02T00:00:00Z", 2021, true, "ARRL RTTY Roundup -- 190k real QSOs live here"},
		{"2020-12-30T12:00:00Z", 2021, true, "2 days of slack before, a contest may straddle New Year"},
		{"2022-01-02T12:00:00Z", 2021, true, "2 days of slack after"},
		{"2020-12-29T12:00:00Z", 2021, false, "3 days before is outside the slack"},
		{"2022-01-03T12:00:00Z", 2021, false, "3 days after is outside the slack"},
		{"2017-02-11T00:00:00Z", 2018, false, "the cq-wpx-rtty/2018 duplication case"},
		{"2088-11-29T01:00:00Z", 2008, false, "corrupted year field"},
		{"1970-01-01T00:00:00Z", 2008, false, "epoch-zero parse failure"},
	} {
		if got := inDeclaredYear(d(c.ts), c.year); got != c.want {
			t.Errorf("%s: inDeclaredYear(%s, %d) = %v, want %v", c.why, c.ts, c.year, got, c.want)
		}
	}
}
