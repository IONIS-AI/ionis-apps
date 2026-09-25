// goes-xrs-ingest loads the local GOES X-ray (XRS) 1-minute mirror into bronze.
//
// It reads $IONIS_SOLAR_DATA_DIR/goes-xrs/<satellite>/YYYY/MM/*.nc, written by
// goes-xrs-download, and never touches the network. One row per record (one per
// minute), every variable, into solar.goes_xrs_1m_bronze (ionis-core
// 49-solar_goes_xrs_bronze.sql).
//
// THE FILES ARE netCDF-4 (HDF5). They are read with Unidata's ncdump (EPEL package
// netcdf), the reference implementation, and nothing is linked: ncdump -p 9,17 prints
// every value as text at round-trip precision and this program parses the text. A
// missing value prints as "_" -- ncdump's own marker for a value equal to the
// variable's _FillValue -- and is stored as NULL, never as a number.
//
// A FILE WHOSE VARIABLES DIFFER FROM THE EXPECTED SET IS REFUSED: not loaded, not
// watermarked, the run exits non-zero. Every file of every satellite checked on
// 2026-09-25 carries the same 43 variables (product version v2-2-1).
//
// Watermarked per file in solar.ingest_log, only after its rows are in bronze.
package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/IONIS-AI/ionis-apps/internal/common"
	"github.com/IONIS-AI/ionis-apps/internal/watermark"
)

var Version = "dev"

const ncdumpBin = "/usr/bin/ncdump"

// Destination; flags so a test run can target a scratch database and touch nothing real.
var (
	table = "solar.goes_xrs_1m_bronze"
	wmDB  = "solar"
)

// kinds of variable, by netCDF type and shape.
const (
	f32   = iota // float(time)            -> Nullable(Float32)
	f64          // double(time)           -> Nullable(Float64)
	u8           // ubyte(time)            -> Nullable(UInt8)
	u16          // ushort(time)           -> Nullable(UInt16)
	f32x4        // float(time,quad_diode) -> Array(Nullable(Float32)), 4 per record
)

type variable struct {
	name string
	kind int
	ncdl string // the declaration as ncdump -h prints it, for the drift guard
}

// vars is every variable of product xrsf-l2-avg1m_science v2-2-1, in file order.
var vars = func() []variable {
	decl := []struct {
		t, n string
	}{
		{"float", "xrsa_flux(time)"}, {"float", "xrsa_flux_observed(time)"}, {"float", "xrsa_flux_electrons(time)"},
		{"float", "xrsb_flux(time)"}, {"float", "xrsb_flux_observed(time)"}, {"float", "xrsb_flux_electrons(time)"},
		{"ubyte", "xrsa_flag(time)"}, {"ubyte", "xrsb_flag(time)"}, {"ubyte", "xrsa_num(time)"}, {"ubyte", "xrsb_num(time)"},
		{"double", "time(time)"},
		{"ushort", "xrsa_flag_excluded(time)"}, {"ushort", "xrsb_flag_excluded(time)"},
		{"float", "au_factor(time)"}, {"float", "corrected_current_xrsb2(time, quad_diode)"}, {"float", "roll_angle(time)"},
		{"float", "xrsa1_flux(time)"}, {"float", "xrsa1_flux_observed(time)"}, {"float", "xrsa1_flux_electrons(time)"},
		{"float", "xrsa2_flux(time)"}, {"float", "xrsa2_flux_observed(time)"}, {"float", "xrsa2_flux_electrons(time)"},
		{"float", "xrsb1_flux(time)"}, {"float", "xrsb1_flux_observed(time)"}, {"float", "xrsb1_flux_electrons(time)"},
		{"float", "xrsb2_flux(time)"}, {"float", "xrsb2_flux_observed(time)"}, {"float", "xrsb2_flux_electrons(time)"},
		{"ubyte", "xrs_primary_chan(time)"},
		{"ushort", "xrsa1_flag(time)"}, {"ushort", "xrsa2_flag(time)"}, {"ushort", "xrsb1_flag(time)"}, {"ushort", "xrsb2_flag(time)"},
		{"ubyte", "xrsa1_num(time)"}, {"ubyte", "xrsa2_num(time)"}, {"ubyte", "xrsb1_num(time)"}, {"ubyte", "xrsb2_num(time)"},
		{"ushort", "xrsa1_flag_excluded(time)"}, {"ushort", "xrsa2_flag_excluded(time)"},
		{"ushort", "xrsb1_flag_excluded(time)"}, {"ushort", "xrsb2_flag_excluded(time)"},
		{"ubyte", "yaw_flip_flag(time)"}, {"ushort", "electron_correction_flag(time)"},
	}
	var out []variable
	for _, d := range decl {
		name := d.n[:strings.Index(d.n, "(")]
		k := map[string]int{"float": f32, "double": f64, "ubyte": u8, "ushort": u16}[d.t]
		if strings.Contains(d.n, "quad_diode") {
			k = f32x4
		}
		out = append(out, variable{name, k, d.t + " " + d.n})
	}
	return out
}()

// epoch is the file's time origin: "seconds since 2000-01-01 12:00:00 UTC", leap
// seconds neglected, exactly as the file's own time:comments attribute says.
var epoch = time.Date(2000, 1, 1, 12, 0, 0, 0, time.UTC)

// secs converts the file's float seconds to a Duration.
func secs(x float64) time.Duration { return time.Duration(x * float64(time.Second)) }

var satRe = regexp.MustCompile(`_g(\d+)_d\d{8}_`)

// parsed is one file's values, one slice per variable, "_" already turned into nil.
type parsed struct {
	rel, sat string
	size     int64
	n        int
	vals     map[string][]*float64
	elapsed  time.Duration
}

// declRe matches a variable declaration line in ncdump's header.
var declRe = regexp.MustCompile(`^\s+(\w+) (\w+\([^)]*\)) ;$`)

// parse turns ncdump -p 9,17 output into values. It checks the declared variables
// against the expected set before reading a single value.
func parse(out []byte) (int, map[string][]*float64, error) {
	head, data, ok := bytes.Cut(out, []byte("\ndata:\n"))
	if !ok {
		return 0, nil, fmt.Errorf("ncdump output has no data section")
	}
	var got []string
	n := -1
	sc := bufio.NewScanner(bytes.NewReader(head))
	for sc.Scan() {
		line := sc.Text()
		if m := declRe.FindStringSubmatch(line); m != nil {
			got = append(got, m[1]+" "+m[2])
		}
		if strings.Contains(line, "time = UNLIMITED ;") {
			i := strings.Index(line, "// (")
			if i < 0 {
				return 0, nil, fmt.Errorf("record count missing from %q", line)
			}
			v, err := strconv.Atoi(strings.Fields(line[i+4:])[0])
			if err != nil {
				return 0, nil, fmt.Errorf("record count in %q: %w", line, err)
			}
			n = v
		}
	}
	if n < 0 {
		return 0, nil, fmt.Errorf("no UNLIMITED time dimension")
	}
	want := make([]string, len(vars))
	for i, v := range vars {
		want[i] = v.ncdl
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		return 0, nil, fmt.Errorf("variables differ from xrsf-l2-avg1m_science v2-2-1 (%d declared, %d expected): NOAA changed the product; update the table and this ingester before loading", len(got), len(want))
	}

	// Data section: "name = v, v, v ;" possibly across many lines.
	vals := map[string][]*float64{}
	for _, stmt := range strings.Split(string(data), ";") {
		name, list, ok := strings.Cut(stmt, "=")
		if !ok {
			continue
		}
		name = strings.TrimSpace(name)
		var xs []*float64
		for _, tok := range strings.Split(list, ",") {
			tok = strings.TrimSpace(tok)
			if tok == "" {
				continue
			}
			if tok == "_" {
				xs = append(xs, nil)
				continue
			}
			tok = strings.TrimRight(tok, "fUBSL") // ncdump type suffixes, if any
			x, err := strconv.ParseFloat(tok, 64)
			if err != nil {
				return 0, nil, fmt.Errorf("%s: value %q: %w", name, tok, err)
			}
			xs = append(xs, &x)
		}
		vals[name] = xs
	}
	for _, v := range vars {
		want := n
		if v.kind == f32x4 {
			want = 4 * n
		}
		if len(vals[v.name]) != want {
			return 0, nil, fmt.Errorf("%s: %d values, want %d", v.name, len(vals[v.name]), want)
		}
	}
	return n, vals, nil
}

func dump(ctx context.Context, path string) ([]byte, error) {
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, ncdumpBin, "-p", "9,17", path)
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("ncdump: %v: %s", err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

// batch is the columnar insert, spanning files.
type batch struct {
	sat     *proto.ColLowCardinality[string]
	obs     *proto.ColNullable[time.Time]
	cols    []proto.ColInput
	path    proto.ColStr
	rec     proto.ColUInt32
	entries []watermark.LogEntry
	rows    int
}

func newBatch() *batch {
	b := &batch{
		sat: new(proto.ColStr).LowCardinality(),
		obs: proto.NewColNullable[time.Time](new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli).WithLocation(time.UTC)),
	}
	for _, v := range vars {
		switch v.kind {
		case f32:
			b.cols = append(b.cols, proto.NewColNullable[float32](new(proto.ColFloat32)))
		case f64:
			b.cols = append(b.cols, proto.NewColNullable[float64](new(proto.ColFloat64)))
		case u8:
			b.cols = append(b.cols, proto.NewColNullable[uint8](new(proto.ColUInt8)))
		case u16:
			b.cols = append(b.cols, proto.NewColNullable[uint16](new(proto.ColUInt16)))
		case f32x4:
			b.cols = append(b.cols, proto.NewArray[proto.Nullable[float32]](proto.NewColNullable[float32](new(proto.ColFloat32))))
		}
	}
	return b
}

func (b *batch) add(p *parsed) {
	tv := p.vals["time"]
	for r := 0; r < p.n; r++ {
		b.sat.Append(p.sat)
		if tv[r] == nil {
			b.obs.Append(proto.Null[time.Time]())
		} else {
			b.obs.Append(proto.NewNullable(epoch.Add(secs(*tv[r])).Round(time.Millisecond)))
		}
		for i, v := range vars {
			xs := p.vals[v.name]
			switch c := b.cols[i].(type) {
			case *proto.ColNullable[float32]:
				if x := xs[r]; x == nil {
					c.Append(proto.Null[float32]())
				} else {
					c.Append(proto.NewNullable(float32(*x)))
				}
			case *proto.ColNullable[float64]:
				if x := xs[r]; x == nil {
					c.Append(proto.Null[float64]())
				} else {
					c.Append(proto.NewNullable(*x))
				}
			case *proto.ColNullable[uint8]:
				if x := xs[r]; x == nil {
					c.Append(proto.Null[uint8]())
				} else {
					c.Append(proto.NewNullable(uint8(*x)))
				}
			case *proto.ColNullable[uint16]:
				if x := xs[r]; x == nil {
					c.Append(proto.Null[uint16]())
				} else {
					c.Append(proto.NewNullable(uint16(*x)))
				}
			case *proto.ColArr[proto.Nullable[float32]]:
				row := make([]proto.Nullable[float32], 4)
				for j := 0; j < 4; j++ {
					if x := xs[4*r+j]; x != nil {
						row[j] = proto.NewNullable(float32(*x))
					}
				}
				c.Append(row)
			}
		}
		b.path.Append(p.rel)
		b.rec.Append(uint32(r))
	}
	b.rows += p.n
	b.entries = append(b.entries, watermark.LogEntry{FilePath: p.rel, FileSize: uint64(p.size),
		RowCount: uint64(p.n), ElapsedMs: uint32(p.elapsed.Milliseconds())})
}

func (b *batch) input() proto.Input {
	in := proto.Input{{Name: "satellite", Data: b.sat}, {Name: "observed_at", Data: b.obs}}
	for i, v := range vars {
		in = append(in, proto.InputColumn{Name: v.name, Data: b.cols[i]})
	}
	return append(in, proto.InputColumn{Name: "file_path", Data: &b.path}, proto.InputColumn{Name: "record_no", Data: &b.rec})
}

func (b *batch) commit(ctx context.Context, conn *ch.Client) error {
	if len(b.entries) == 0 {
		return nil
	}
	in := b.input()
	names := make([]string, len(in))
	for i, c := range in {
		names[i] = c.Name
	}
	q := fmt.Sprintf("INSERT INTO %s (%s) VALUES", table, strings.Join(names, ", "))
	if err := conn.Do(ctx, ch.Query{Body: q, Input: in}); err != nil {
		return fmt.Errorf("insert %d rows: %w (none of these %d files watermarked)", b.rows, err, len(b.entries))
	}
	if err := watermark.InsertLogEntries(ctx, conn, wmDB, b.entries); err != nil {
		return fmt.Errorf("watermark %d files: %w", len(b.entries), err)
	}
	return nil
}

func main() {
	var (
		src      = flag.String("src", "", "Solar data dir holding goes-xrs/ (default: $IONIS_SOLAR_DATA_DIR)")
		host     = flag.String("host", "", "ClickHouse host:port (default: $IONIS_CH_HOST)")
		batchSz  = flag.Int("batch", 200000, "Rows per INSERT")
		workers  = flag.Int("workers", 8, "Concurrent ncdump + parse")
		fullMode = flag.Bool("full", false, "Ignore the watermark and load every file (truncate the table first)")
		tbl      = flag.String("table", table, "Destination table")
		wdb      = flag.String("watermark-db", wmDB, "Database holding ingest_log")
	)
	flag.Parse()
	table, wmDB = *tbl, *wdb
	dir, err := common.ResolvePath(*src, "IONIS_SOLAR_DATA_DIR", "src", "solar raw data directory")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	chHost, err := common.ResolvePath(*host, "IONIS_CH_HOST", "host", "ClickHouse endpoint")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if _, err := os.Stat(ncdumpBin); err != nil {
		fmt.Fprintf(os.Stderr, "%s not found: install the netcdf package (EPEL)\n", ncdumpBin)
		os.Exit(1)
	}
	fmt.Printf("goes-xrs-ingest v%s\n", Version)

	ctx := context.Background()
	wm := map[string]watermark.Entry{}
	if !*fullMode {
		if wm, err = watermark.LoadWatermark(ctx, chHost, wmDB); err != nil {
			fmt.Fprintf(os.Stderr, "load watermark: %v\n", err)
			os.Exit(1)
		}
	}
	conn, err := ch.Dial(ctx, ch.Options{Address: chHost})
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial %s: %v\n", chHost, err)
		os.Exit(1)
	}
	defer conn.Close()

	var todo []string
	onDisk, done := 0, 0
	_ = filepath.WalkDir(filepath.Join(dir, "goes-xrs"), func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ".nc") {
			onDisk++
			rel, _ := filepath.Rel(dir, path)
			if _, ok := wm[rel]; ok {
				done++
			} else {
				todo = append(todo, path)
			}
		}
		return nil
	})
	sort.Strings(todo)

	type result struct {
		p   *parsed
		err error
		rel string
	}
	jobs := make(chan string)
	results := make(chan result)
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range jobs {
				rel, _ := filepath.Rel(dir, path)
				start := time.Now()
				out, err := dump(ctx, path)
				if err != nil {
					results <- result{rel: rel, err: err}
					continue
				}
				n, vals, err := parse(out)
				if err != nil {
					results <- result{rel: rel, err: err}
					continue
				}
				m := satRe.FindStringSubmatch(filepath.Base(path))
				if m == nil {
					results <- result{rel: rel, err: fmt.Errorf("no satellite in file name")}
					continue
				}
				fi, _ := os.Stat(path)
				results <- result{rel: rel, p: &parsed{rel: rel, sat: "g" + m[1], size: fi.Size(), n: n, vals: vals, elapsed: time.Since(start)}}
			}
		}()
	}
	go func() {
		for _, p := range todo {
			jobs <- p
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	b := newBatch()
	loaded, rows, failed := 0, 0, 0
	for r := range results {
		if r.err != nil {
			fmt.Fprintf(os.Stderr, "  FAIL %s: %v\n", r.rel, r.err)
			failed++
			continue
		}
		b.add(r.p)
		loaded++
		rows += r.p.n
		if b.rows >= *batchSz {
			if err := b.commit(ctx, conn); err != nil {
				fmt.Fprintln(os.Stderr, "  "+err.Error())
				os.Exit(1)
			}
			b = newBatch()
		}
	}
	if err := b.commit(ctx, conn); err != nil {
		fmt.Fprintln(os.Stderr, "  "+err.Error())
		os.Exit(1)
	}
	fmt.Printf("  %d files on disk, %d already loaded, %d loaded now (%d rows) -> %s\n", onDisk, done, loaded, rows, table)
	if failed > 0 {
		fmt.Fprintf(os.Stderr, "%d file(s) refused -- not loaded, not watermarked\n", failed)
		os.Exit(1)
	}
}
