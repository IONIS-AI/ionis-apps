// solar-kp-ingest loads the GFZ planetary Kp/ap archive into solar.kp_bronze.
//
// ONE SOURCE, ONE TABLE. The table this replaces merged three NOAA streams into a
// row per (date, time) with max() over whatever had staged, so a stream that failed
// to download became 0 and the INSERT still succeeded. kp_index was a non-nullable
// Float32, which made a missing Kp and a genuinely quiet Kp=0 the same value. There
// is no merge here, so there is nothing to zero-fill.
//
// GFZ FORMAT, whitespace-separated, # comments:
//
//	YYY MM DD hh.h hh._m  days  days_m  Kp  ap  D
//	2026 06 15 00.0 01.50 34499.00000 34499.06250 1.333 5 1
//
// Kp is the REAL value (0.000, 0.333, 0.667, ...), not the 0-9 integer. ap is its
// linear equivalent. D is 1 for definitive and 0 for provisional.
//
// EVERY DATA LINE IS A ROW, EVERY COLUMN KEPT (2026-09-25, the #46 pattern). The
// table was a ReplacingMergeTree keyed on observed_at that stored four of the ten
// columns, and this ingester SKIPPED any line where GFZ publishes -1 ("no value") --
// a gap that would have disappeared from bronze instead of being recorded. Now every
// data line is a row with its line number and raw text; -1 is NULL, never a number
// and never a skipped line; an unreadable line is kept with parse_error set.
//
// GFZ republishes the whole series in one file, so each run is a full exact copy,
// loaded through internal/stagedload: staging table, count checked against the file,
// EXCHANGE TABLES. Provisional values that GFZ later revises are simply the next copy.
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

// row is one data line of the file.
type row struct {
	lineNo        uint32
	raw           string
	t             time.Time
	hourMid       float32
	days, daysMid float64
	kp            *float32
	ap            *uint16
	definitive    *uint8
	parseErr      string
}

func parseFile(r io.Reader) ([]row, error) {
	var rows []row
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	var n uint32
	for sc.Scan() {
		n++
		raw := sc.Text()
		t := strings.TrimSpace(raw)
		if t == "" || strings.HasPrefix(t, "#") {
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
	if len(fs) != 10 {
		r.parseErr = fmt.Sprintf("%d fields, want 10", len(fs))
		return r
	}
	var ints [3]int
	for i := 0; i < 3; i++ {
		v, err := strconv.Atoi(fs[i])
		if err != nil {
			r.parseErr = "date: " + err.Error()
			return r
		}
		ints[i] = v
	}
	var fl [5]float64 // hh.h hh._m days days_m Kp
	for i, s := range []string{fs[3], fs[4], fs[5], fs[6], fs[7]} {
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			r.parseErr = "number: " + err.Error()
			return r
		}
		fl[i] = v
	}
	ap, e1 := strconv.Atoi(fs[8])
	df, e2 := strconv.Atoi(fs[9])
	if e1 != nil || e2 != nil {
		r.parseErr = fmt.Sprintf("ap/D: %v %v", e1, e2)
		return r
	}
	r.t = time.Date(ints[0], time.Month(ints[1]), ints[2], int(fl[0]), 0, 0, 0, time.UTC)
	r.hourMid, r.days, r.daysMid = float32(fl[1]), fl[2], fl[3]
	if fl[4] >= 0 { // -1 is GFZ's "no value"
		k := float32(fl[4])
		r.kp = &k
	}
	if ap >= 0 {
		a := uint16(ap)
		r.ap = &a
	}
	if df >= 0 {
		d := uint8(df)
		r.definitive = &d
	}
	return r
}

func insert(ctx context.Context, conn *ch.Client, table, source string, rows []row) error {
	cT := proto.NewColNullable[time.Time](new(proto.ColDateTime64).WithPrecision(proto.PrecisionSecond).WithLocation(time.UTC))
	cH := proto.NewColNullable[float32](new(proto.ColFloat32))
	cD := proto.NewColNullable[float64](new(proto.ColFloat64))
	cDM := proto.NewColNullable[float64](new(proto.ColFloat64))
	cK := proto.NewColNullable[float32](new(proto.ColFloat32))
	cA := proto.NewColNullable[uint16](new(proto.ColUInt16))
	cF := proto.NewColNullable[uint8](new(proto.ColUInt8))
	var cL proto.ColUInt32
	var cR, cE proto.ColStr
	cS := new(proto.ColStr).LowCardinality()
	for _, r := range rows {
		if r.parseErr != "" {
			cT.Append(proto.Null[time.Time]())
			cH.Append(proto.Null[float32]())
			cD.Append(proto.Null[float64]())
			cDM.Append(proto.Null[float64]())
		} else {
			cT.Append(proto.NewNullable(r.t))
			cH.Append(proto.NewNullable(r.hourMid))
			cD.Append(proto.NewNullable(r.days))
			cDM.Append(proto.NewNullable(r.daysMid))
		}
		cK.Append(opt(r.kp))
		cA.Append(opt(r.ap))
		cF.Append(opt(r.definitive))
		cL.Append(r.lineNo)
		cR.Append(r.raw)
		cE.Append(r.parseErr)
		cS.Append(source)
	}
	return conn.Do(ctx, ch.Query{
		Body: fmt.Sprintf("INSERT INTO %s (observed_at, hour_mid, days, days_mid, kp, ap, definitive, line_no, raw_line, parse_error, source_file) VALUES", table),
		Input: proto.Input{
			{Name: "observed_at", Data: cT}, {Name: "hour_mid", Data: cH},
			{Name: "days", Data: cD}, {Name: "days_mid", Data: cDM},
			{Name: "kp", Data: cK}, {Name: "ap", Data: cA}, {Name: "definitive", Data: cF},
			{Name: "line_no", Data: &cL}, {Name: "raw_line", Data: &cR},
			{Name: "parse_error", Data: &cE}, {Name: "source_file", Data: cS},
		},
	})
}

func opt[T any](p *T) proto.Nullable[T] {
	if p == nil {
		return proto.Null[T]()
	}
	return proto.NewNullable(*p)
}

func main() {
	var (
		src   = flag.String("src", "", "Directory holding gfz_kp_ap_since_1932.txt (default: $IONIS_SOLAR_DATA_DIR)")
		file  = flag.String("file", "gfz_kp_ap_since_1932.txt", "Archive filename")
		host  = flag.String("host", "", "ClickHouse host:port (default: $IONIS_CH_HOST)")
		table = flag.String("table", "solar.kp_bronze", "Destination table (a <table>_staging is used for the load)")
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
		fmt.Fprintf(os.Stderr, "open %s: %v\n  run solar-kp-download first\n", path, err)
		os.Exit(1)
	}
	defer f.Close()
	fmt.Printf("solar-kp-ingest v%s\n  %s -> %s\n", Version, path, *table)

	rows, err := parseFile(f)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	unread, noKp := 0, 0
	for _, r := range rows {
		if r.parseErr != "" {
			unread++
		} else if r.kp == nil {
			noKp++
		}
	}
	fmt.Printf("  %d data lines read, %d with no Kp value (-1, kept as NULL), %d unreadable (kept with parse_error)\n", len(rows), noKp, unread)
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
	fmt.Printf("  %s now holds %d rows = %d data lines in %s\n", *table, live, len(rows), filepath.Base(path))
}
