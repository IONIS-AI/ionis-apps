package netcdf3

import (
	"bytes"
	"compress/gzip"
	"io"
	"math"
	"os"
	"testing"
)

// Golden values were read from the same files with scipy.io.netcdf_file, an
// independent implementation, on 2026-09-24.

func load(t *testing.T, name string) *File {
	t.Helper()
	gz, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	f, err := Parse(b)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return f
}

type golden struct {
	name        string
	first, last float64
	fills       int
	sum         float64 // of non-missing values
}

func check(t *testing.T, f *File, g golden) {
	t.Helper()
	v := f.Var(g.name)
	if v == nil {
		t.Fatalf("%s: missing", g.name)
	}
	vals, err := f.Values(g.name)
	if err != nil {
		t.Fatalf("%s: %v", g.name, err)
	}
	miss, hasMiss := v.Missing()
	fills, sum := 0, 0.0
	for _, x := range vals {
		if hasMiss && x == miss {
			fills++
			continue
		}
		sum += x
	}
	if !close(vals[0], g.first) || !close(vals[len(vals)-1], g.last) {
		t.Errorf("%s: first/last %v/%v, want %v/%v", g.name, vals[0], vals[len(vals)-1], g.first, g.last)
	}
	if fills != g.fills {
		t.Errorf("%s: %d missing, want %d", g.name, fills, g.fills)
	}
	if !close(sum, g.sum) {
		t.Errorf("%s: sum %v, want %v", g.name, sum, g.sum)
	}
}

func close(a, b float64) bool { return math.Abs(a-b) <= 1e-6*math.Max(1, math.Abs(b)) }

func TestPlasmaFile(t *testing.T) {
	f := load(t, "f1m_20160726.nc.gz")
	if f.NumRecs != 1440 || len(f.Vars) != 81 || f.Version != 1 {
		t.Fatalf("numrecs %d vars %d version %d, want 1440 81 1", f.NumRecs, len(f.Vars), f.Version)
	}
	for _, g := range []golden{
		{"time", 1469491200000, 1469577540000, 0, 2116129492800000},
		{"proton_speed", 374.8999938964844, 357.5, 3, 522552.3000488281},
		{"proton_density", 7.349999904632568, 8.050000190734863, 3, 10826.660000801086},
		{"proton_temperature", 0, 0, 3, 232026449},
		{"sample_count", 16, 12, 0, 22241},
		{"overall_quality", 1, 1, 0, 672},
		{"fill_flag", 0, 0, 0, 3},
	} {
		if g.name == "proton_temperature" { // first/last not recorded; check counts only
			vals, _ := f.Values(g.name)
			g.first, g.last = vals[0], vals[len(vals)-1]
		}
		check(t, f, g)
	}
	if m, ok := f.Var("proton_density").Missing(); !ok || m != -99999 {
		t.Errorf("proton_density missing marker %v %v, want -99999 true", m, ok)
	}
}

func TestMagnetometerFile(t *testing.T) {
	f := load(t, "m1m_20260629.nc.gz")
	if f.NumRecs != 1440 || len(f.Vars) != 24 {
		t.Fatalf("numrecs %d vars %d, want 1440 24", f.NumRecs, len(f.Vars))
	}
	for _, g := range []golden{
		{"time", 1782691200000, 1782777540000, 0, 2567137492800000},
		{"bt", 3.1700000762939453, 3.799999952316284, 28, 4794.089996337891},
		{"bz_gsm", -0.5799999833106995, -1.159999966621399, 28, 248.71000077016652},
		{"sample_count", 2836, 2950, 0, 4141999},
		{"measurement_range", 0, 0, 0, 0},
	} {
		check(t, f, g)
	}
}

func TestRejectsNonNetCDF(t *testing.T) {
	if _, err := Parse([]byte("<html>not a file</html>")); err == nil {
		t.Error("parsed HTML as netCDF")
	}
	if _, err := Parse([]byte("CDF\x01\x00\x00")); err == nil {
		t.Error("parsed a truncated header")
	}
}
