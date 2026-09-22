package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Some publishers ship a single file holding several operators' logs end to end --
// r0hq.log in the IARU mirror is the one that surfaced this. The parser stopped at
// the first END-OF-LOG, so every log after the first was dropped silently: the file
// parsed, reported no error, and contributed only its first station's QSOs.
//
// Bronze is supposed to hold what the archive holds. This is the test for that.

// oneLog renders a minimal but valid Cabrillo log.
func oneLog(call, grid string, qsoFreqs []string) string {
	s := "START-OF-LOG: 3.0\n" +
		"CALLSIGN: " + call + "\n" +
		"CONTEST: IARU-HF\n"
	if grid != "" {
		s += "GRID-LOCATOR: " + grid + "\n"
	}
	for _, f := range qsoFreqs {
		// freq mode date time mycall rst exch theircall rst exch
		s += "QSO: " + f + " CW 2024-07-13 1200 " + call + " 599 14 DL1ABC 599 28\n"
	}
	s += "END-OF-LOG:\n"
	return s
}

func writeTemp(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "multi.log")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return p
}

// Three logs concatenated. Every QSO must survive, and each must be attributed to
// the station whose log it came from -- not to the first one in the file.
func TestParseFileConcatenatedLogs(t *testing.T) {
	body := oneLog("R0HQ", "NO14", []string{"14025", "21025"}) +
		oneLog("DA0HQ", "JO50", []string{"7025"}) +
		oneLog("9A0HQ", "JN85", []string{"3525", "14030", "28025"})

	_, qsos, rejects, err := parseFile(writeTemp(t, body), "", "IARU-HF")
	if err != nil {
		t.Fatalf("parseFile: %v", err)
	}
	if len(rejects) != 0 {
		t.Errorf("rejected %d QSO lines, want 0: %+v", len(rejects), rejects)
	}
	if got, want := len(qsos), 6; got != want {
		t.Fatalf("parsed %d QSOs, want %d -- the parser is stopping at the first END-OF-LOG", got, want)
	}

	// Attribution: 2 from R0HQ, 1 from DA0HQ, 3 from 9A0HQ.
	counts := map[string]int{}
	for _, q := range qsos {
		counts[q.Call1]++
	}
	for call, want := range map[string]int{"R0HQ": 2, "DA0HQ": 1, "9A0HQ": 3} {
		if counts[call] != want {
			t.Errorf("Call1=%s: %d QSOs, want %d -- section headers are not being applied per log",
				call, counts[call], want)
		}
	}
}

// A single log must be unchanged by the fix.
func TestParseFileSingleLogUnchanged(t *testing.T) {
	_, qsos, rejects, err := parseFile(writeTemp(t, oneLog("K1ABC", "FN42", []string{"14025", "21025"})), "", "IARU-HF")
	if err != nil {
		t.Fatalf("parseFile: %v", err)
	}
	if len(rejects) != 0 {
		t.Errorf("rejected %d, want 0: %+v", len(rejects), rejects)
	}
	if len(qsos) != 2 {
		t.Fatalf("parsed %d QSOs, want 2", len(qsos))
	}
	for _, q := range qsos {
		if q.Call1 != "K1ABC" {
			t.Errorf("Call1 = %q, want K1ABC", q.Call1)
		}
	}
}

// Trailing junk after the last END-OF-LOG must not become a phantom section, and a
// log with no END-OF-LOG at all must still yield its QSOs.
func TestParseFileUnterminatedAndTrailingJunk(t *testing.T) {
	body := oneLog("K1ABC", "FN42", []string{"14025"}) +
		"\n; publisher footer, not a log\n\n" +
		"START-OF-LOG: 3.0\nCALLSIGN: W9XYZ\nCONTEST: IARU-HF\n" +
		"QSO: 14025 CW 2024-07-13 1300 W9XYZ 599 14 DL1ABC 599 28\n" // no END-OF-LOG

	_, qsos, _, err := parseFile(writeTemp(t, body), "", "IARU-HF")
	if err != nil {
		t.Fatalf("parseFile: %v", err)
	}
	if len(qsos) != 2 {
		t.Fatalf("parsed %d QSOs, want 2 (one per log, second log unterminated)", len(qsos))
	}
	if qsos[1].Call1 != "W9XYZ" {
		t.Errorf("second QSO Call1 = %q, want W9XYZ", qsos[1].Call1)
	}
}

// A skipped line must be RECORDED, not merely counted. Judge's rule: if the parser
// has an issue with a file or a line, skip it and log it for review.
//
// This is the control that was missing. Three real defects -- stopping at the first
// END-OF-LOG, mishandling compound callsigns, and refusing 7Q1 -- each dropped QSOs
// silently, because a skip incremented a counter and the line itself was discarded.
func TestSkippedLinesAreRecordedWithReason(t *testing.T) {
	body := "START-OF-LOG: 3.0\nCALLSIGN: K1ABC\nCONTEST: IARU-HF\n" +
		"QSO: 14025 CW 2024-07-13 1200 K1ABC 599 14 ON9TT 599 28\n" +
		"QSO: notafrequency CW 2024-07-13 1201 K1ABC 599 14 DL1ABC 599 28\n" +
		"QSO: 14025 CW nonsense-date 1202 K1ABC 599 14 DL2ABC 599 28\n" +
		"END-OF-LOG:\n"

	_, qsos, rejects, err := parseFile(writeTemp(t, body), "", "IARU-HF")
	if err != nil {
		t.Fatalf("parseFile: %v", err)
	}
	if len(qsos) != 1 {
		t.Errorf("parsed %d QSOs, want 1 (the two bad lines must not land)", len(qsos))
	}
	if len(rejects) != 2 {
		t.Fatalf("recorded %d rejects, want 2 -- a skipped line must be kept for review", len(rejects))
	}
	for _, r := range rejects {
		if r.LineNo == 0 {
			t.Errorf("reject has no line number: %+v", r)
		}
		if r.Reason == "" {
			t.Errorf("reject has no reason: %+v", r)
		}
		if r.RawLine == "" {
			t.Errorf("reject has no raw line: %+v", r)
		}
	}
	// Line numbers are 1-based and count every line, headers included.
	if rejects[0].LineNo != 5 || rejects[1].LineNo != 6 {
		t.Errorf("line numbers = %d, %d; want 5, 6", rejects[0].LineNo, rejects[1].LineNo)
	}
}

// A QSO line before any CALLSIGN: header cannot be attributed, and that reason must
// say so rather than arriving as a bare count.
func TestUnattributableLineSaysWhy(t *testing.T) {
	body := "START-OF-LOG: 3.0\nCONTEST: IARU-HF\n" +
		"QSO: 14025 CW 2024-07-13 1200 K1ABC 599 14 ON9TT 599 28\n" +
		"CALLSIGN: K1ABC\n" +
		"QSO: 14025 CW 2024-07-13 1201 K1ABC 599 14 DL1ABC 599 28\n"

	_, qsos, rejects, err := parseFile(writeTemp(t, body), "", "IARU-HF")
	if err != nil {
		t.Fatalf("parseFile: %v", err)
	}
	if len(qsos) != 1 {
		t.Errorf("parsed %d QSOs, want 1", len(qsos))
	}
	if len(rejects) != 1 {
		t.Fatalf("recorded %d rejects, want 1", len(rejects))
	}
	if !strings.Contains(rejects[0].Reason, "CALLSIGN") {
		t.Errorf("reason = %q, want it to name the missing CALLSIGN header", rejects[0].Reason)
	}
}

// reason must stay a BOUNDED set. It is a LowCardinality column, and the parser's
// raw error embeds the offending value -- bad freq "notafreq" -- so using it directly
// would mint a distinct string per bad value, which makes LowCardinality worse than
// String and makes grouping by reason useless.
func TestRejectReasonIsABoundedCategory(t *testing.T) {
	for _, c := range []struct{ err, want string }{
		{`bad freq "notafreq": strconv.ParseFloat: parsing "notafreq": invalid syntax`, "bad frequency"},
		{`bad freq "zzz": whatever`, "bad frequency"},
		{`bad timestamp "garbage" "1202": parsing time`, "bad timestamp"},
		{`too few fields: 4`, "too few fields"},
		{`too few fields after QSO: 3`, "too few fields"},
		{`no their_call found`, "no worked station"},
		{`something nobody has seen yet`, "other"},
	} {
		if got := rejectReason(errors.New(c.err)); got != c.want {
			t.Errorf("rejectReason(%q) = %q, want %q", c.err, got, c.want)
		}
	}
	if got := rejectReason(nil); got != "unknown" {
		t.Errorf("rejectReason(nil) = %q, want unknown", got)
	}

	// Two different bad values must collapse to ONE reason -- that is the whole point.
	a := rejectReason(errors.New(`bad freq "aaa": x`))
	b := rejectReason(errors.New(`bad freq "bbb": y`))
	if a != b {
		t.Errorf("distinct bad values produced distinct reasons (%q vs %q); LowCardinality would blow up", a, b)
	}
}
