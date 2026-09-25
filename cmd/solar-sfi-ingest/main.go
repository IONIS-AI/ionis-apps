// solar-sfi-ingest loads the Penticton 10.7cm flux table into solar.sfi_bronze.
//
// ONE SOURCE, ONE TABLE -- see solar-kp-ingest for why the merged solar.bronze lost
// five months of 2026.
//
// SOURCE FORMAT, fixed columns with a two-line header:
//
//	fluxdate    fluxtime  fluxjulian  fluxcarrington  fluxobsflux  fluxadjflux  fluxursi
//	20260922    170000    2461306.2   2301.9          148.1        146.9        133.4
//
// fluxdate is YYYYMMDD and fluxtime is HHMMSS, both UTC. Penticton observes at
// roughly 17, 20 and 23 UTC, so a complete day is three rows -- not one, which is
// what NOAA's rollup gives.
//
// EVERY LINE IS A ROW (#46). The table was a ReplacingMergeTree keyed on observed_at,
// and fluxtime is ROUNDED: 49 pairs of lines share a fluxtime but are separate
// observations -- their fluxjulian differs (e.g. .322 and .3349, ~19:44 and ~20:02 UTC)
// and so do their fluxes. The merge kept one of each pair, chosen by merge order, and
// fluxjulian, the column that tells them apart, was not stored at all. Now: plain
// MergeTree, one row per data line with its line number and raw text, every column
// including fluxjulian; an unreadable line is kept with parse_error set.
//
// EXACT COPY, SWAPPED ATOMICALLY. Penticton republishes the whole history in one file,
// so each run loads it into solar.sfi_bronze_staging, counts what LANDED there, and
// only if that equals the file's data lines does EXCHANGE TABLES make it the live
// table. A failed run leaves the previous table untouched, and a rerun never
// duplicates. The success line reports the count read back from the table -- the
// ingester used to report what it sent, which is how 49 losses read as "inserted".
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/IONIS-AI/ionis-apps/internal/common"
)

var Version = "dev"

func main() {
	var (
		src   = flag.String("src", "", "Directory holding penticton_fluxtable.txt (default: $IONIS_SOLAR_DATA_DIR)")
		file  = flag.String("file", "penticton_fluxtable.txt", "Archive filename")
		host  = flag.String("host", "", "ClickHouse host:port (default: $IONIS_CH_HOST)")
		table = flag.String("table", "solar.sfi_bronze", "Destination table (a <table>_staging is used for the load)")
		dry   = flag.Bool("dry-run", false, "Parse and report, insert nothing")
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

	path := filepath.Join(dir, *file)
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open %s: %v\n  run solar-sfi-download first\n", path, err)
		os.Exit(1)
	}
	defer f.Close()
	sourceFile := filepath.Base(path)
	fmt.Printf("solar-sfi-ingest v%s\n  %s -> %s\n", Version, path, *table)

	rows, err := parseFile(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	failed := 0
	for _, r := range rows {
		if r.parseErr != "" {
			failed++
		}
	}
	fmt.Printf("  %d data lines read, %d unreadable (kept with parse_error)\n", len(rows), failed)
	if *dry {
		fmt.Println("  DRY RUN - nothing written")
		return
	}

	ctx := context.Background()
	conn, err := ch.Dial(ctx, ch.Options{Address: chHost})
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial %s: %v\n", chHost, err)
		os.Exit(1)
	}
	defer conn.Close()

	staging := *table + "_staging"
	for _, q := range []string{
		fmt.Sprintf("DROP TABLE IF EXISTS %s", staging),
		fmt.Sprintf("CREATE TABLE %s AS %s", staging, *table),
	} {
		if err := conn.Do(ctx, ch.Query{Body: q}); err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", q, err)
			os.Exit(1)
		}
	}
	if err := insert(ctx, conn, staging, sourceFile, rows); err != nil {
		fmt.Fprintf(os.Stderr, "insert into %s: %v (live table untouched)\n", staging, err)
		os.Exit(1)
	}
	landed, err := count(ctx, conn, staging)
	if err != nil {
		fmt.Fprintf(os.Stderr, "count %s: %v (live table untouched)\n", staging, err)
		os.Exit(1)
	}
	if landed != len(rows) {
		fmt.Fprintf(os.Stderr, "%s holds %d rows, the file has %d data lines -- not swapping (live table untouched)\n", staging, landed, len(rows))
		os.Exit(1)
	}
	if err := conn.Do(ctx, ch.Query{Body: fmt.Sprintf("EXCHANGE TABLES %s AND %s", *table, staging)}); err != nil {
		fmt.Fprintf(os.Stderr, "exchange: %v\n", err)
		os.Exit(1)
	}
	_ = conn.Do(ctx, ch.Query{Body: fmt.Sprintf("DROP TABLE IF EXISTS %s", staging)})
	live, err := count(ctx, conn, *table)
	if err != nil {
		fmt.Fprintf(os.Stderr, "count %s: %v\n", *table, err)
		os.Exit(1)
	}
	fmt.Printf("  %s now holds %d rows = %d data lines in %s\n", *table, live, len(rows), sourceFile)
}

// row is one data line of the file, as read.
type row struct {
	lineNo         uint32
	raw            string
	t              time.Time
	julian, car    float64
	obs, adj, ursi float32
	parseErr       string
}

// parseFile reads every data line; the two header lines are the only lines skipped.
func parseFile(r io.Reader) ([]row, error) {
	var rows []row
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	var n uint32
	for sc.Scan() {
		n++
		raw := sc.Text()
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "fluxdate") || strings.HasPrefix(line, "---") {
			continue
		}
		rows = append(rows, parseLine(n, raw))
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("no data lines -- refusing to replace the table with nothing")
	}
	return rows, nil
}

func parseLine(n uint32, raw string) row {
	r := row{lineNo: n, raw: raw}
	fs := strings.Fields(raw)
	if len(fs) < 7 {
		r.parseErr = fmt.Sprintf("%d fields, want 7", len(fs))
		return r
	}
	t, err := time.Parse("20060102150405", fs[0]+fmt.Sprintf("%06s", fs[1]))
	if err != nil {
		r.parseErr = "date/time: " + err.Error()
		return r
	}
	jul, e0 := strconv.ParseFloat(fs[2], 64)
	car, e1 := strconv.ParseFloat(fs[3], 64)
	obs, e2 := strconv.ParseFloat(fs[4], 32)
	adj, e3 := strconv.ParseFloat(fs[5], 32)
	ursi, e4 := strconv.ParseFloat(fs[6], 32)
	for _, e := range []error{e0, e1, e2, e3, e4} {
		if e != nil {
			r.parseErr = "number: " + e.Error()
			return r
		}
	}
	r.t, r.julian, r.car = t, jul, car
	r.obs, r.adj, r.ursi = float32(obs), float32(adj), float32(ursi)
	return r
}

func insert(ctx context.Context, conn *ch.Client, table, sourceFile string, rows []row) error {
	cT := proto.NewColNullable[time.Time](new(proto.ColDateTime))
	cJ := proto.NewColNullable[float64](new(proto.ColFloat64))
	cC := proto.NewColNullable[float64](new(proto.ColFloat64))
	cO := proto.NewColNullable[float32](new(proto.ColFloat32))
	cA := proto.NewColNullable[float32](new(proto.ColFloat32))
	cU := proto.NewColNullable[float32](new(proto.ColFloat32))
	var cL proto.ColUInt32
	var cR, cE proto.ColStr
	cS := new(proto.ColStr).LowCardinality()
	for _, r := range rows {
		if r.parseErr != "" {
			cT.Append(proto.Null[time.Time]())
			cJ.Append(proto.Null[float64]())
			cC.Append(proto.Null[float64]())
			cO.Append(proto.Null[float32]())
			cA.Append(proto.Null[float32]())
			cU.Append(proto.Null[float32]())
		} else {
			cT.Append(proto.NewNullable(r.t))
			cJ.Append(proto.NewNullable(r.julian))
			cC.Append(proto.NewNullable(r.car))
			cO.Append(proto.NewNullable(r.obs))
			cA.Append(proto.NewNullable(r.adj))
			cU.Append(proto.NewNullable(r.ursi))
		}
		cL.Append(r.lineNo)
		cR.Append(r.raw)
		cE.Append(r.parseErr)
		cS.Append(sourceFile)
	}
	return conn.Do(ctx, ch.Query{
		Body: fmt.Sprintf("INSERT INTO %s (observed_at, julian, carrington, observed_flux, adjusted_flux, ursi_flux, line_no, raw_line, parse_error, source_file) VALUES", table),
		Input: proto.Input{
			{Name: "observed_at", Data: cT},
			{Name: "julian", Data: cJ},
			{Name: "carrington", Data: cC},
			{Name: "observed_flux", Data: cO},
			{Name: "adjusted_flux", Data: cA},
			{Name: "ursi_flux", Data: cU},
			{Name: "line_no", Data: &cL},
			{Name: "raw_line", Data: &cR},
			{Name: "parse_error", Data: &cE},
			{Name: "source_file", Data: cS},
		},
	})
}

func count(ctx context.Context, conn *ch.Client, table string) (int, error) {
	var c proto.ColUInt64
	err := conn.Do(ctx, ch.Query{Body: "SELECT count() FROM " + table,
		Result: proto.Results{{Name: "count()", Data: &c}}})
	if err != nil || c.Rows() == 0 {
		return 0, err
	}
	return int(c.Row(0)), nil
}
