package main

import (
	"strings"
	"testing"
)

// Every guard here is tested against the states its author did not imagine first --
// empty, absent, boundary -- and only then against the happy path. Three rounds of
// review on this PR found the same defect shape each time: a check written for the
// one failure the author pictured, letting everything else through as success.

func TestARRLAdvertisedCount(t *testing.T) {
	for _, c := range []struct {
		in   string
		want int
		why  string
	}{
		{"Number of logs found for 2018 (excluding Checklogs): 3968\n", 3968, "real page"},
		{"Number of logs found for 2018: 1,234\n", 1234, "thousands separator"},
		{"", -1, "EMPTY page -- must NOT report a count"},
		{"<html>no count here</html>", -1, "page changed shape -- must NOT report a count"},
	} {
		if got := arrlAdvertisedCount([]byte(c.in)); got != c.want {
			t.Errorf("%s: arrlAdvertisedCount = %d, want %d", c.why, got, c.want)
		}
	}
}

func TestParseARRLIndexHandlesBothURLSchemes(t *testing.T) {
	// The scheme change from ?q=HASH to ?cn=&yr=&call= is what silently returned zero
	// logs across all 72 ARRL year indexes while reporting success.
	newStyle := `<a href="showpubliclog.php?cn=dxcw&yr=2018&call=2E0CVN" target="_new">2E0CVN</a>`
	if e := parseARRLIndex([]byte(newStyle)); len(e) != 1 || e[0].Callsign != "2e0cvn" || !strings.Contains(e[0].Hash, "call=2E0CVN") {
		t.Errorf("new scheme: got %+v", e)
	}
	oldStyle := `<a href="showpubliclog.php?q=BoxB148zknLDZr4" target="_new">K1ABC</a>`
	if e := parseARRLIndex([]byte(oldStyle)); len(e) != 1 || e[0].Callsign != "k1abc" {
		t.Errorf("old scheme: got %+v", e)
	}
	if e := parseARRLIndex([]byte("")); len(e) != 0 {
		t.Errorf("EMPTY page must parse to zero entries, got %d", len(e))
	}
	amp := `<a href="showpubliclog.php?cn=dxcw&amp;yr=2018&amp;call=K1ABC" target="_new">K1ABC</a>`
	if e := parseARRLIndex([]byte(amp)); len(e) != 1 || strings.Contains(e[0].Hash, "&amp;") {
		t.Errorf("HTML-escaped ampersands must be decoded, got %+v", e)
	}
}
