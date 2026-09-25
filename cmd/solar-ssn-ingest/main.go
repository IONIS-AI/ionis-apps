// solar-ssn-ingest loads the SIDC daily sunspot series into solar.ssn_bronze.
//
// SOURCE FORMAT, semicolon-separated, no header:
//
//	Year;Month;Day;DecimalDate;SNvalue;SNerror;Nobs;Definitive
//	2026;08;31;2026.664;  58;  8.1;  33;0
//
// -1 MEANS NO OBSERVATION, not a count of minus one. Cloud, war, instrument
// failure. It is stored as NULL rather than -1 because -1 is exactly the kind of
// sentinel that survives into a mean and quietly drags it down -- the same class of
// defect as the zero-filled Kp this rebuild exists to remove. A day with -1 is still
// a row: a missing day and an unobserved day are different facts.
//
// EVERY LINE IS A ROW, EVERY COLUMN KEPT (2026-09-25, the #46 pattern). The table was
// a ReplacingMergeTree keyed on the date, and DecimalDate was not stored. Now one row
// per line with its line number and raw text; an unreadable line is kept with
// parse_error set. Nobs 0 is a real count (no stations) and is stored as 0; only a
// negative Nobs is NULL.
//
// SIDC republishes the whole series in one file, so each run is a full exact copy,
// loaded through internal/stagedload: staging table, count checked, EXCHANGE TABLES.
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
	"github.com/IONIS-AI/ionis-apps/internal/stagedload"
)

var Version = "dev"

type row struct {
	lineNo      uint32
	raw         string
	day         time.Time
	decimalYear float64
	ssn         *int16
	stddev      *float32
	nobs        *uint16
	definitive  *uint8
	parseErr    string
}

func parseFile(r io.Reader) ([]row, error) {
	var rows []row
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	var n uint32
	for sc.Scan() {
		n++
		raw := sc.Text()
		if strings.TrimSpace(raw) == "" {
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
	fs := strings.Split(strings.TrimSpace(raw), ";")
	if len(fs) != 8 {
		r.parseErr = fmt.Sprintf("%d fields, want 8", len(fs))
		return r
	}
	for i := range fs {
		fs[i] = strings.TrimSpace(fs[i])
	}
	y, e1 := strconv.Atoi(fs[0])
	mo, e2 := strconv.Atoi(fs[1])
	d, e3 := strconv.Atoi(fs[2])
	dec, e4 := strconv.ParseFloat(fs[3], 64)
	v, e5 := strconv.Atoi(fs[4])
	er, e6 := strconv.ParseFloat(fs[5], 32)
	no, e7 := strconv.Atoi(fs[6])
	df, e8 := strconv.Atoi(fs[7])
	for _, e := range []error{e1, e2, e3, e4, e5, e6, e7, e8} {
		if e != nil {
			r.parseErr = "number: " + e.Error()
			return r
		}
	}
	r.day = time.Date(y, time.Month(mo), d, 0, 0, 0, 0, time.UTC)
	r.decimalYear = dec
	if v >= 0 {
		s := int16(v)
		r.ssn = &s
	}
	if er >= 0 {
		s := float32(er)
		r.stddev = &s
	}
	if no >= 0 {
		s := uint16(no)
		r.nobs = &s
	}
	if df >= 0 {
		s := uint8(df)
		r.definitive = &s
	}
	return r
}

func opt[T any](p *T) proto.Nullable[T] {
	if p == nil {
		return proto.Null[T]()
	}
	return proto.NewNullable(*p)
}

func insert(ctx context.Context, conn *ch.Client, table, source string, rows []row) error {
	cD := proto.NewColNullable[time.Time](new(proto.ColDate32))
	cY := proto.NewColNullable[float64](new(proto.ColFloat64))
	cN := proto.NewColNullable[int16](new(proto.ColInt16))
	cSD := proto.NewColNullable[float32](new(proto.ColFloat32))
	cO := proto.NewColNullable[uint16](new(proto.ColUInt16))
	cF := proto.NewColNullable[uint8](new(proto.ColUInt8))
	var cL proto.ColUInt32
	var cR, cE proto.ColStr
	cS := new(proto.ColStr).LowCardinality()
	for _, r := range rows {
		if r.parseErr != "" {
			cD.Append(proto.Null[time.Time]())
			cY.Append(proto.Null[float64]())
		} else {
			cD.Append(proto.NewNullable(r.day))
			cY.Append(proto.NewNullable(r.decimalYear))
		}
		cN.Append(opt(r.ssn))
		cSD.Append(opt(r.stddev))
		cO.Append(opt(r.nobs))
		cF.Append(opt(r.definitive))
		cL.Append(r.lineNo)
		cR.Append(r.raw)
		cE.Append(r.parseErr)
		cS.Append(source)
	}
	return conn.Do(ctx, ch.Query{
		Body: fmt.Sprintf("INSERT INTO %s (observed_on, decimal_year, ssn, ssn_stddev, observations, definitive, line_no, raw_line, parse_error, source_file) VALUES", table),
		Input: proto.Input{
			{Name: "observed_on", Data: cD}, {Name: "decimal_year", Data: cY},
			{Name: "ssn", Data: cN}, {Name: "ssn_stddev", Data: cSD},
			{Name: "observations", Data: cO}, {Name: "definitive", Data: cF},
			{Name: "line_no", Data: &cL}, {Name: "raw_line", Data: &cR},
			{Name: "parse_error", Data: &cE}, {Name: "source_file", Data: cS},
		},
	})
}

func main() {
	var (
		src   = flag.String("src", "", "Directory holding sidc_ssn_daily.csv (default: $IONIS_SOLAR_DATA_DIR)")
		file  = flag.String("file", "sidc_ssn_daily.csv", "Archive filename")
		host  = flag.String("host", "", "ClickHouse host:port (default: $IONIS_CH_HOST)")
		table = flag.String("table", "solar.ssn_bronze", "Destination table (a <table>_staging is used for the load)")
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
		fmt.Fprintf(os.Stderr, "open %s: %v\n  run solar-ssn-download first\n", path, err)
		os.Exit(1)
	}
	defer f.Close()
	fmt.Printf("solar-ssn-ingest v%s\n  %s -> %s\n", Version, path, *table)
	rows, err := parseFile(f)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	unread, noObs := 0, 0
	for _, r := range rows {
		if r.parseErr != "" {
			unread++
		} else if r.ssn == nil {
			noObs++
		}
	}
	fmt.Printf("  %d lines read, %d unobserved days (-1, kept as NULL), %d unreadable (kept with parse_error)\n", len(rows), noObs, unread)
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
	live, err := stagedload.Replace(ctx, conn, *table, len(rows), func(staging string) error {
		return insert(ctx, conn, staging, filepath.Base(path), rows)
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "  "+err.Error())
		os.Exit(1)
	}
	fmt.Printf("  %s now holds %d rows = %d lines in %s\n", *table, live, len(rows), filepath.Base(path))
}
