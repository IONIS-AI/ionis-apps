package main

import (
	"fmt"
	"strings"
	"testing"
)

// cdl builds ncdump -p 9,17 style output for n records with the expected variables.
// fillAt marks one record as "_" in every one-per-record variable.
func cdl(n, fillAt int, decl []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "netcdf x {\ndimensions:\n\ttime = UNLIMITED ; // (%d currently)\n\tquad_diode = 4 ;\nvariables:\n", n)
	for _, d := range decl {
		fmt.Fprintf(&b, "\t%s ;\n\t\tsomething:long_name = \"x\" ;\n", d)
	}
	b.WriteString("\n// global attributes:\n\t\t:title = \"t\" ;\ndata:\n\n")
	for _, v := range vars {
		var xs []string
		per := 1
		if v.kind == f32x4 {
			per = 4
		}
		for r := 0; r < n; r++ {
			for j := 0; j < per; j++ {
				switch {
				case r == fillAt && per == 1:
					xs = append(xs, "_")
				case v.name == "time":
					xs = append(xs, fmt.Sprintf("%d", 843393600+60*r)) // 2026-09-23 00:00:00: the first value of NOAA's g19 d20260923 file
				default:
					xs = append(xs, fmt.Sprintf("%d", r+j))
				}
			}
		}
		fmt.Fprintf(&b, " %s = %s ;\n\n", v.name, strings.Join(xs, ", \n    "))
	}
	b.WriteString("}\n")
	return b.String()
}

func expectedDecl() []string {
	out := make([]string, len(vars))
	for i, v := range vars {
		out[i] = v.ncdl
	}
	return out
}

func TestParseAllVariablesAndFill(t *testing.T) {
	n, vals, err := parse([]byte(cdl(3, 1, expectedDecl())))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if n != 3 {
		t.Fatalf("n = %d, want 3", n)
	}
	if got := vals["xrsa_flux"]; got[1] != nil || got[0] == nil || *got[2] != 2 {
		t.Errorf("xrsa_flux = %v, want value/nil/2", got)
	}
	if got := vals["corrected_current_xrsb2"]; len(got) != 12 {
		t.Errorf("corrected_current_xrsb2 has %d values, want 12 (4 per record)", len(got))
	}
}

// The epoch is the file's own: 2000-01-01 12:00:00 UTC.
func TestTimeDecodes(t *testing.T) {
	_, vals, err := parse([]byte(cdl(2, -1, expectedDecl())))
	if err != nil {
		t.Fatal(err)
	}
	got := epoch.Add(secs(*vals["time"][1]))
	if want := "2026-09-23T00:01:00Z"; got.Format("2006-01-02T15:04:05Z") != want {
		t.Errorf("time[1] = %s, want %s", got, want)
	}
}

// A variable added, removed or retyped refuses the file.
func TestDriftRefused(t *testing.T) {
	d := expectedDecl()
	d = append(d, "float new_measurement(time)")
	if _, _, err := parse([]byte(cdl(2, -1, d))); err == nil || !strings.Contains(err.Error(), "variables differ") {
		t.Fatalf("added variable: err = %v, want refusal", err)
	}
	d = expectedDecl()
	d[0] = "double xrsa_flux(time)"
	if _, _, err := parse([]byte(cdl(2, -1, d))); err == nil {
		t.Fatal("retyped variable accepted")
	}
}

// A value list shorter than the record count is an error, not a short row.
func TestShortVariableRefused(t *testing.T) {
	s := cdl(3, -1, expectedDecl())
	s = strings.Replace(s, " roll_angle = 0, \n    1, \n    2 ;", " roll_angle = 0, \n    1 ;", 1)
	if _, _, err := parse([]byte(s)); err == nil || !strings.Contains(err.Error(), "roll_angle") {
		t.Fatalf("err = %v, want roll_angle count error", err)
	}
}
