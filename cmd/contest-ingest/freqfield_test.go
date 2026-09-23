package main

import "testing"

// A Cabrillo band designator is a legal frequency field above 1 GHz. The real line
// from iaru-hf/2024 that used to be skipped as a bad frequency.
func TestBandDesignatorIsAFrequency(t *testing.T) {
	f := []string{"QSO:", "2.3G", "CW", "2024-07-14", "0352", "HA5BA", "599", "28", "VE6BBP", "599", "02"}
	q, err := parseQSOLine(f, "HA5BA", "IARU-HF")
	if err != nil {
		t.Fatalf("parseQSOLine: %v", err)
	}
	if q.Frequency != 2_300_000 || q.Mode != "CW" || q.Call2 != "VE6BBP" {
		t.Errorf("got freq %d mode %q call2 %q, want 2300000 CW VE6BBP", q.Frequency, q.Mode, q.Call2)
	}
}

// A frequency with its mode glued on splits into the two fields it should have been.
func TestGluedModeSplits(t *testing.T) {
	f := []string{"QSO:", "21170CW", "2005-11-26", "1200", "KI7MT", "599", "03", "ON9TT", "599", "14"}
	q, err := parseQSOLine(f, "KI7MT", "CQ-WW-CW")
	if err != nil {
		t.Fatalf("parseQSOLine: %v", err)
	}
	if q.Frequency != 21170 || q.Mode != "CW" || q.Call2 != "ON9TT" {
		t.Errorf("got freq %d mode %q call2 %q, want 21170 CW ON9TT", q.Frequency, q.Mode, q.Call2)
	}
}

// A trailing letter that is not a Cabrillo mode is a typo, not a glued field.
func TestNonModeSuffixStillFails(t *testing.T) {
	f := []string{"QSO:", "3500P", "PH", "2012-10-28", "1228", "GM6NX", "59", "14", "MM3PDM/P", "59", "14"}
	if _, err := parseQSOLine(f, "GM6NX", "CQ-WW-SSB"); err == nil {
		t.Error("3500P parsed; it should fail as a bad frequency")
	}
}

// Every QSO line carries its provenance, and a normalisation is named on the row
// rather than applied silently -- bronze is packaging, the patches are recorded.
func TestParsedLinesCarryProvenanceAndPatches(t *testing.T) {
	body := "START-OF-LOG: 3.0\nCALLSIGN: K1ABC\n" +
		"QSO:14025 CW 2024-07-13 1200 K1ABC 599 14 ON9TT 599 28\n" +
		"QSO: 2.3G CW 2024-07-13 1201 K1ABC 599 14 DL1ABC 599 28\n" +
		"QSO: 14025 CW 2024-07-13 1202 K1ABC 599 14 DL2ABC 599 28\n" +
		"END-OF-LOG:\n"
	_, qsos, rejects, err := parseFile(writeTemp(t, body), "", "IARU-HF")
	if err != nil || len(qsos) != 3 || len(rejects) != 0 {
		t.Fatalf("got %d QSOs, %d rejects, err %v; want 3, 0, nil", len(qsos), len(rejects), err)
	}
	want := [][]string{{"glued-qso-tag"}, {"band-designator"}, nil}
	for i, q := range qsos {
		if q.LineNo != uint32(i+3) {
			t.Errorf("QSO %d: line %d, want %d", i, q.LineNo, i+3)
		}
		if q.RawLine == "" {
			t.Errorf("QSO %d: no raw line", i)
		}
		if len(q.Patches) != len(want[i]) || (len(want[i]) > 0 && q.Patches[0] != want[i][0]) {
			t.Errorf("QSO %d: patches %v, want %v", i, q.Patches, want[i])
		}
	}
}

// The raw line is the line as the file holds it -- not trimmed, not upper-cased.
func TestRawLineIsVerbatim(t *testing.T) {
	line := "  qso: 14025 cw 2024-07-13 1200 k1abc 599 14 on9tt 599 28  "
	_, qsos, _, err := parseFile(writeTemp(t, "CALLSIGN: K1ABC\n"+line+"\n"), "", "IARU-HF")
	if err != nil || len(qsos) != 1 {
		t.Fatalf("got %d QSOs, err %v", len(qsos), err)
	}
	if qsos[0].RawLine != line {
		t.Errorf("raw line %q, want %q", qsos[0].RawLine, line)
	}
}
