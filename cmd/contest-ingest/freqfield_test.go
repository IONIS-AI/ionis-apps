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
