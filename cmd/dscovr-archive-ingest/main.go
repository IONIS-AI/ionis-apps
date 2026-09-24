// dscovr-archive-ingest loads the local DSCOVR archive mirror into bronze.
//
// It reads $IONIS_SOLAR_DATA_DIR/dscovr/<product>/YYYY/MM/*.nc.gz, written by
// dscovr-archive-download, and never touches the network. One row per record, every
// variable, into solar.dscovr_<product>_bronze (ionis-core 48-solar_dscovr_archive.sql).
//
// BRONZE GETS EVERYTHING. Nothing is filtered on quality; overall_quality and every
// *_flag variable are carried as sent. A measurement equal to the variable's
// missing_value (-99999.0) is stored as NULL -- never 0.
//
// A NEW NON-FLAG VARIABLE FAILS THE FILE. NOAA has only ever added flags, which the
// flags map absorbs. A new measurement would otherwise be dropped without a word, so
// the file is refused, left unwatermarked, and the run exits non-zero.
//
// Watermarked per file in solar.ingest_log. A file is recorded only after its rows are
// in bronze; the batch spans files (one INSERT per -batch rows, not per file).
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/IONIS-AI/ionis-apps/internal/common"
	"github.com/IONIS-AI/ionis-apps/internal/netcdf3"
	"github.com/IONIS-AI/ionis-apps/internal/watermark"
)

var Version = "dev"

// product is one archive product and the columns its bronze table carries besides
// observed_at, sample_count, overall_quality, flags, file_path and record_no.
type product struct {
	name   string
	table  string
	floats []string // Nullable(Float32); missing_value -> NULL
	int8s  []string // Int8, no missing marker
}

var products = map[string]product{
	"f1m": {name: "f1m", table: "solar.dscovr_f1m_bronze", floats: []string{
		"proton_vx_gse", "proton_vy_gse", "proton_vz_gse", "proton_vx_gsm", "proton_vy_gsm", "proton_vz_gsm",
		"proton_speed", "proton_density", "proton_temperature",
		"alpha_vx_gse", "alpha_vy_gse", "alpha_vz_gse", "alpha_vx_gsm", "alpha_vy_gsm", "alpha_vz_gsm",
		"alpha_speed", "alpha_density", "alpha_temperature"}},
	"m1m": {name: "m1m", table: "solar.dscovr_m1m_bronze", floats: []string{
		"bt", "bx_gse", "by_gse", "bz_gse", "theta_gse", "phi_gse",
		"bx_gsm", "by_gsm", "bz_gsm", "theta_gsm", "phi_gsm"},
		int8s: []string{"measurement_mode", "measurement_range"}},
}

// flagsByName are flag variables whose names do not end in _flag. Stored in the flags
// map under the name the file uses, like every other flag.
//
//	large_flow_angles  0/1, "flow angles exceed expected range", in plasma files from
//	                   2016-12-14 to 2017-04-12; renamed large_flow_angle_flag after.
//	                   Found by the unknown-variable guard on the first full load: 134
//	                   files refused until it was named here.
var flagsByName = map[string]bool{"large_flow_angles": true}

// batch is the columnar insert for one product, spanning files.
type batch struct {
	p       product
	obs     *proto.ColDateTime64
	samples proto.ColInt16
	floats  []*proto.ColNullable[float32]
	int8s   []*proto.ColInt8
	quality proto.ColInt8
	flags   *proto.ColMap[string, int8]
	path    proto.ColStr
	rec     proto.ColUInt32
	entries []watermark.LogEntry
	rows    int
}

func newBatch(p product) *batch {
	b := &batch{p: p}
	b.reset()
	return b
}

func (b *batch) reset() {
	b.obs = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli).WithLocation(time.UTC)
	b.samples = proto.ColInt16{}
	b.floats = make([]*proto.ColNullable[float32], len(b.p.floats))
	for i := range b.floats {
		b.floats[i] = proto.NewColNullable[float32](new(proto.ColFloat32))
	}
	b.int8s = make([]*proto.ColInt8, len(b.p.int8s))
	for i := range b.int8s {
		b.int8s[i] = new(proto.ColInt8)
	}
	b.quality = proto.ColInt8{}
	b.flags = proto.NewMap[string, int8](new(proto.ColStr).LowCardinality(), new(proto.ColInt8))
	b.path = proto.ColStr{}
	b.rec = proto.ColUInt32{}
	b.entries = b.entries[:0]
	b.rows = 0
}

func (b *batch) input() proto.Input {
	in := proto.Input{{Name: "observed_at", Data: b.obs}, {Name: "sample_count", Data: &b.samples}}
	for i, n := range b.p.int8s {
		in = append(in, proto.InputColumn{Name: n, Data: b.int8s[i]})
	}
	for i, n := range b.p.floats {
		in = append(in, proto.InputColumn{Name: n, Data: b.floats[i]})
	}
	return append(in,
		proto.InputColumn{Name: "overall_quality", Data: &b.quality},
		proto.InputColumn{Name: "flags", Data: b.flags},
		proto.InputColumn{Name: "file_path", Data: &b.path},
		proto.InputColumn{Name: "record_no", Data: &b.rec},
	)
}

func (b *batch) insertQuery() string {
	cols := make([]string, 0, 32)
	for _, c := range b.input() {
		cols = append(cols, c.Name)
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES", b.p.table, strings.Join(cols, ", "))
}

// addFile appends every record of one parsed file. It checks everything first and
// appends nothing if the file does not match the product, so a batch never holds
// half a file.
func (b *batch) addFile(f *netcdf3.File, rel string) error {
	known := map[string]bool{"time": true, "sample_count": true, "overall_quality": true}
	for _, n := range b.p.floats {
		known[n] = true
	}
	for _, n := range b.p.int8s {
		known[n] = true
	}
	var flagNames []string
	for _, v := range f.Vars {
		switch {
		case known[v.Name]:
		case strings.HasSuffix(v.Name, "_flag") || flagsByName[v.Name]:
			flagNames = append(flagNames, v.Name)
		default:
			return fmt.Errorf("unknown variable %q: NOAA changed the product; update the table and this ingester before loading", v.Name)
		}
	}
	for n := range known {
		if f.Var(n) == nil {
			return fmt.Errorf("expected variable %q is absent", n)
		}
	}

	read := func(n string) ([]float64, error) { return f.Values(n) }
	tms, err := read("time")
	if err != nil {
		return err
	}
	samples, err := read("sample_count")
	if err != nil {
		return err
	}
	quality, err := read("overall_quality")
	if err != nil {
		return err
	}
	floats := make([][]float64, len(b.p.floats))
	misses := make([]float64, len(b.p.floats))
	hasMiss := make([]bool, len(b.p.floats))
	for i, n := range b.p.floats {
		if floats[i], err = read(n); err != nil {
			return err
		}
		misses[i], hasMiss[i] = f.Var(n).Missing()
	}
	int8s := make([][]float64, len(b.p.int8s))
	for i, n := range b.p.int8s {
		if int8s[i], err = read(n); err != nil {
			return err
		}
	}
	flags := make([][]float64, len(flagNames))
	for i, n := range flagNames {
		if flags[i], err = read(n); err != nil {
			return err
		}
	}

	for r := 0; r < f.NumRecs; r++ {
		b.obs.Append(time.UnixMilli(int64(tms[r])).UTC())
		b.samples.Append(int16(samples[r]))
		for i := range b.p.int8s {
			b.int8s[i].Append(int8(int8s[i][r]))
		}
		for i := range b.p.floats {
			x := floats[i][r]
			if hasMiss[i] && x == misses[i] {
				b.floats[i].Append(proto.Null[float32]())
			} else {
				b.floats[i].Append(proto.NewNullable(float32(x)))
			}
		}
		b.quality.Append(int8(quality[r]))
		m := make(map[string]int8, len(flagNames))
		for i, n := range flagNames {
			m[n] = int8(flags[i][r])
		}
		b.flags.Append(m)
		b.path.Append(rel)
		b.rec.Append(uint32(r))
	}
	b.rows += f.NumRecs
	return nil
}

// commit writes the batch, then the watermark for every file in it.
func (b *batch) commit(ctx context.Context, conn *ch.Client) error {
	defer b.reset()
	if len(b.entries) == 0 {
		return nil
	}
	if err := conn.Do(ctx, ch.Query{Body: b.insertQuery(), Input: b.input()}); err != nil {
		return fmt.Errorf("insert %d rows into %s: %w (none of these %d files watermarked)", b.rows, b.p.table, err, len(b.entries))
	}
	if err := watermark.InsertLogEntries(ctx, conn, "solar", b.entries); err != nil {
		return fmt.Errorf("watermark %d files: %w", len(b.entries), err)
	}
	return nil
}

func readGz(path string) ([]byte, int64, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, 0, err
	}
	b, err := io.ReadAll(zr)
	return b, int64(len(raw)), err
}

func main() {
	var (
		src      = flag.String("src", "", "Solar data dir holding dscovr/ (default: $IONIS_SOLAR_DATA_DIR)")
		host     = flag.String("host", "", "ClickHouse host:port (default: $IONIS_CH_HOST)")
		prods    = flag.String("products", "f1m,m1m", "Comma-separated products")
		batchSz  = flag.Int("batch", 200000, "Rows per INSERT")
		fullMode = flag.Bool("full", false, "Ignore the watermark and load every file (truncate the tables first)")
	)
	flag.Parse()

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
	fmt.Printf("dscovr-archive-ingest v%s\n", Version)

	ctx := context.Background()
	wm := map[string]watermark.Entry{}
	if !*fullMode {
		if wm, err = watermark.LoadWatermark(ctx, chHost, "solar"); err != nil {
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

	failed := 0
	for _, name := range strings.Split(*prods, ",") {
		p, ok := products[strings.TrimSpace(name)]
		if !ok {
			fmt.Fprintf(os.Stderr, "unknown product %q\n", name)
			os.Exit(1)
		}
		root := filepath.Join(dir, "dscovr", p.name)
		var files []string
		_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err == nil && !d.IsDir() && strings.HasSuffix(path, ".nc.gz") {
				files = append(files, path)
			}
			return nil
		})
		sort.Strings(files)

		b := newBatch(p)
		loaded, rows, skippedWM := 0, 0, 0
		for _, path := range files {
			rel, _ := filepath.Rel(dir, path)
			if _, done := wm[rel]; done {
				skippedWM++
				continue
			}
			start := time.Now()
			data, size, err := readGz(path)
			var f *netcdf3.File
			if err == nil {
				f, err = netcdf3.Parse(data)
			}
			if err == nil {
				err = b.addFile(f, rel)
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "  FAIL %s: %v\n", rel, err)
				failed++
				continue
			}
			b.entries = append(b.entries, watermark.LogEntry{FilePath: rel, FileSize: uint64(size),
				RowCount: uint64(f.NumRecs), ElapsedMs: uint32(time.Since(start).Milliseconds())})
			loaded++
			rows += f.NumRecs
			if b.rows >= *batchSz {
				if err := b.commit(ctx, conn); err != nil {
					fmt.Fprintln(os.Stderr, "  "+err.Error())
					os.Exit(1)
				}
			}
		}
		if err := b.commit(ctx, conn); err != nil {
			fmt.Fprintln(os.Stderr, "  "+err.Error())
			os.Exit(1)
		}
		fmt.Printf("  %s: %d files on disk, %d already loaded, %d loaded now (%d rows) -> %s\n",
			p.name, len(files), skippedWM, loaded, rows, p.table)
	}
	if failed > 0 {
		fmt.Fprintf(os.Stderr, "%d file(s) refused -- not loaded, not watermarked\n", failed)
		os.Exit(1)
	}
}
