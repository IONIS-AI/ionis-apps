package main

import (
	"strings"
	"testing"

	"github.com/IONIS-AI/ionis-apps/internal/netcdf3"
)

func parse(t *testing.T, name string) *netcdf3.File {
	t.Helper()
	b, _, err := readGz("../../internal/netcdf3/testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	f, err := netcdf3.Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// Every record goes in; a missing_value measurement becomes NULL, never 0.
func TestPlasmaFileLoadsEveryRecordWithNulls(t *testing.T) {
	b := newBatch(products["f1m"])
	if err := b.addFile(parse(t, "f1m_20160726.nc.gz"), "dscovr/f1m/2016/07/x.nc.gz"); err != nil {
		t.Fatalf("addFile: %v", err)
	}
	if b.rows != 1440 || b.obs.Rows() != 1440 || b.rec.Rows() != 1440 {
		t.Fatalf("rows %d / obs %d / rec %d, want 1440", b.rows, b.obs.Rows(), b.rec.Rows())
	}
	idx := -1
	for i, n := range b.p.floats {
		if n == "proton_density" {
			idx = i
		}
	}
	col := b.floats[idx]
	nulls := 0
	for i := 0; i < col.Rows(); i++ {
		if !col.Row(i).Set {
			nulls++
		} else if col.Row(i).Value == -99999 {
			t.Fatalf("row %d stored the missing marker as a value", i)
		}
	}
	if nulls != 3 {
		t.Errorf("proton_density NULLs = %d, want 3 (scipy counts 3 missing_value)", nulls)
	}
	if got := b.flags.Row(0); len(got) == 0 || !strings.HasSuffix(firstKey(got), "_flag") {
		t.Errorf("flags map row 0 = %v, want *_flag keys", got)
	}
}

func TestMagnetometerFileLoads(t *testing.T) {
	b := newBatch(products["m1m"])
	if err := b.addFile(parse(t, "m1m_20260629.nc.gz"), "dscovr/m1m/2026/06/x.nc.gz"); err != nil {
		t.Fatalf("addFile: %v", err)
	}
	if b.rows != 1440 {
		t.Fatalf("rows %d, want 1440", b.rows)
	}
}

// A file whose variables do not match the product is refused WHOLE: nothing appended.
func TestMismatchedFileIsRefusedWhole(t *testing.T) {
	b := newBatch(products["m1m"])
	err := b.addFile(parse(t, "f1m_20160726.nc.gz"), "dscovr/m1m/2016/07/x.nc.gz")
	if err == nil || !strings.Contains(err.Error(), "unknown variable") {
		t.Fatalf("err = %v, want an unknown-variable refusal", err)
	}
	if b.rows != 0 || b.obs.Rows() != 0 {
		t.Errorf("refused file left %d rows in the batch", b.obs.Rows())
	}
}

func firstKey(m map[string]int8) string {
	for k := range m {
		return k
	}
	return ""
}
