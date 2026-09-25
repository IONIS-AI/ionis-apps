package main

import (
	"strings"
	"testing"
)

// A real line from the head of gfz_kp_ap_since_1932.txt, a -1 line (GFZ's "no value"),
// and an unreadable line. The old ingester skipped the -1 line entirely.
const sample = `# The parameters in each line are:
#YYY MM DD hh.h hh._m        days      days_m     Kp   ap D
1932 01 01 00.0 01.50     0.00000     0.06250  3.333   18 1
2026 09 25 03.0 04.50 34601.12500 34601.18750 -1.000   -1 0
1932 01 01 garbage
`

func TestEveryLineKept(t *testing.T) {
	rows, err := parseFile(strings.NewReader(sample))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("%d rows, want 3", len(rows))
	}
	r := rows[0]
	if r.lineNo != 3 || *r.kp != 3.333 || *r.ap != 18 || *r.definitive != 1 || r.daysMid != 0.0625 || r.hourMid != 1.5 {
		t.Errorf("first line parsed wrong: %+v", r)
	}
	if rows[1].kp != nil || rows[1].ap != nil || rows[1].parseErr != "" {
		t.Errorf("-1 line: kp %v ap %v err %q, want NULL/NULL/none -- kept, not skipped", rows[1].kp, rows[1].ap, rows[1].parseErr)
	}
	if rows[2].parseErr == "" || !strings.Contains(rows[2].raw, "garbage") {
		t.Errorf("unreadable line not kept with an error: %+v", rows[2])
	}
}
