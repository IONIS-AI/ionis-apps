package main

import (
	"os"
	"path/filepath"
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

	_, qsos, skipped, err := parseFile(writeTemp(t, body), "")
	if err != nil {
		t.Fatalf("parseFile: %v", err)
	}
	if skipped != 0 {
		t.Errorf("skipped %d QSO lines, want 0", skipped)
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
	_, qsos, skipped, err := parseFile(writeTemp(t, oneLog("K1ABC", "FN42", []string{"14025", "21025"})), "")
	if err != nil {
		t.Fatalf("parseFile: %v", err)
	}
	if skipped != 0 {
		t.Errorf("skipped %d, want 0", skipped)
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

	_, qsos, _, err := parseFile(writeTemp(t, body), "")
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
