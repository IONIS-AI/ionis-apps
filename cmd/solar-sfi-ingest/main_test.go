package main

import (
	"strings"
	"testing"
)

// Lines 14558-14561 of penticton_fluxtable.txt (2026-09-25): two pairs that share a
// rounded fluxtime but are separate observations -- their fluxjulian differs. The old
// table kept one of each pair (#46). Every one must survive as its own row.
const sample = `fluxdate    fluxtime    fluxjulian    fluxcarrington  fluxobsflux  fluxadjflux  fluxursi  
----------  ----------  ------------  --------------  -----------  -----------  ----------
20180224    180000      02458174.239  002201.048      000067.5     000066.1     000059.5  
20180224    180000      02458174.2515 002201.0461     000069.3     000067.9     000061.1  
20180224    200000      02458174.322  002201.051      000068.2     000066.8     000060.1  
20180224    200000      02458174.3349 002201.0492     000070.0     000068.6     000061.7  
20180225    garbage
`

func TestEveryLineIsARow(t *testing.T) {
	rows, err := parseFile(strings.NewReader(sample))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 5 {
		t.Fatalf("%d rows, want 5 (4 observations + 1 unreadable line kept)", len(rows))
	}
	if rows[0].julian == rows[1].julian || rows[0].t != rows[1].t {
		t.Errorf("pair not kept apart: julian %v/%v time %v/%v", rows[0].julian, rows[1].julian, rows[0].t, rows[1].t)
	}
	if rows[1].obs != 69.3 || rows[1].julian != 2458174.2515 {
		t.Errorf("second of pair = obs %v julian %v, want 69.3 2458174.2515", rows[1].obs, rows[1].julian)
	}
	if rows[2].lineNo != 5 || rows[3].lineNo != 6 {
		t.Errorf("line numbers %d,%d, want 5,6 (headers count)", rows[2].lineNo, rows[3].lineNo)
	}
	if rows[4].parseErr == "" || !strings.Contains(rows[4].raw, "garbage") {
		t.Errorf("unreadable line: parseErr %q raw %q, want it kept with an error", rows[4].parseErr, rows[4].raw)
	}
}

func TestEmptyFileRefused(t *testing.T) {
	if _, err := parseFile(strings.NewReader("fluxdate x\n---\n")); err == nil {
		t.Error("a file with no data lines must not replace the table")
	}
}
