package main

import (
	"strings"
	"testing"
)

// The first line of sidc_ssn_daily.csv (-1 value, -1 error, 0 stations), a recent
// observed day, and an unreadable line.
const sample = `1818;01;01;1818.001;  -1; -1.0;   0;1
2026;08;31;2026.664;  58;  8.1;  33;0
nonsense
`

func TestEveryLineKept(t *testing.T) {
	rows, err := parseFile(strings.NewReader(sample))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("%d rows, want 3", len(rows))
	}
	if rows[0].ssn != nil || rows[0].stddev != nil || rows[0].nobs == nil || *rows[0].nobs != 0 {
		t.Errorf("unobserved day: ssn %v stddev %v nobs %v, want NULL, NULL, 0", rows[0].ssn, rows[0].stddev, rows[0].nobs)
	}
	if rows[0].decimalYear != 1818.001 {
		t.Errorf("decimal year %v, want 1818.001", rows[0].decimalYear)
	}
	if *rows[1].ssn != 58 || *rows[1].nobs != 33 || *rows[1].definitive != 0 {
		t.Errorf("observed day parsed wrong: %+v", rows[1])
	}
	if rows[2].parseErr == "" || rows[2].lineNo != 3 {
		t.Errorf("unreadable line not kept: %+v", rows[2])
	}
}
