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
// defect as the zero-filled Kp this rebuild exists to remove.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
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
		src   = flag.String("src", "", "Directory holding sidc_ssn_daily.csv (default: $IONIS_SOLAR_DATA_DIR)")
		file  = flag.String("file", "sidc_ssn_daily.csv", "Archive filename")
		host  = flag.String("host", "", "ClickHouse host:port (default: $IONIS_CH_HOST)")
		table = flag.String("table", "solar.ssn_bronze", "Destination table")
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
	sourceFile := filepath.Base(path)
	fmt.Printf("solar-ssn-ingest v%s\n  %s -> %s\n", Version, path, *table)

	var (
		dates      []time.Time
		ssn        []int16
		ssnNull    []bool
		sd         []float32
		sdNull     []bool
		nobs       []uint16
		nobsNull   []bool
		defs       []uint8
		bad, noObs int
	)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		fs := strings.Split(strings.TrimSpace(sc.Text()), ";")
		if len(fs) < 8 {
			bad++
			continue
		}
		y, e1 := strconv.Atoi(strings.TrimSpace(fs[0]))
		mo, e2 := strconv.Atoi(strings.TrimSpace(fs[1]))
		d, e3 := strconv.Atoi(strings.TrimSpace(fs[2]))
		v, e4 := strconv.Atoi(strings.TrimSpace(fs[4]))
		er, e5 := strconv.ParseFloat(strings.TrimSpace(fs[5]), 32)
		no, e6 := strconv.Atoi(strings.TrimSpace(fs[6]))
		df, e7 := strconv.Atoi(strings.TrimSpace(fs[7]))
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil || e6 != nil || e7 != nil {
			bad++
			continue
		}
		dates = append(dates, time.Date(y, time.Month(mo), d, 0, 0, 0, 0, time.UTC))
		// -1 is "no observation". Kept as a row with NULLs so the day still exists in
		// the series -- a missing day and an unobserved day are different facts.
		if v < 0 {
			ssn = append(ssn, 0)
			ssnNull = append(ssnNull, true)
			noObs++
		} else {
			ssn = append(ssn, int16(v))
			ssnNull = append(ssnNull, false)
		}
		if er < 0 {
			sd = append(sd, 0)
			sdNull = append(sdNull, true)
		} else {
			sd = append(sd, float32(er))
			sdNull = append(sdNull, false)
		}
		if no <= 0 {
			nobs = append(nobs, 0)
			nobsNull = append(nobsNull, true)
		} else {
			nobs = append(nobs, uint16(no))
			nobsNull = append(nobsNull, false)
		}
		defs = append(defs, uint8(df))
	}
	if err := sc.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "scan: %v\n", err)
		os.Exit(1)
	}
	if len(dates) == 0 {
		fmt.Fprintf(os.Stderr, "parsed 0 usable rows (%d unparseable) - refusing to continue\n", bad)
		os.Exit(1)
	}
	fmt.Printf("  parsed %d rows (%s .. %s), %d unobserved (-1 -> NULL), %d skipped\n",
		len(dates), dates[0].Format("2006-01-02"), dates[len(dates)-1].Format("2006-01-02"), noObs, bad)
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

	var cD proto.ColDate32
	cS := proto.NewColNullable[int16](new(proto.ColInt16))
	cE := proto.NewColNullable[float32](new(proto.ColFloat32))
	cN := proto.NewColNullable[uint16](new(proto.ColUInt16))
	var cF proto.ColUInt8
	cSrc := new(proto.ColStr).LowCardinality()
	var cI proto.ColDateTime
	now := time.Now()
	for i := range dates {
		cD.Append(dates[i])
		if ssnNull[i] {
			cS.Append(proto.Null[int16]())
		} else {
			cS.Append(proto.NewNullable(ssn[i]))
		}
		if sdNull[i] {
			cE.Append(proto.Null[float32]())
		} else {
			cE.Append(proto.NewNullable(sd[i]))
		}
		if nobsNull[i] {
			cN.Append(proto.Null[uint16]())
		} else {
			cN.Append(proto.NewNullable(nobs[i]))
		}
		cF.Append(defs[i])
		cSrc.Append(sourceFile)
		cI.Append(now)
	}
	if err := conn.Do(ctx, ch.Query{
		Body: fmt.Sprintf("INSERT INTO %s (observed_on, ssn, ssn_stddev, observations, definitive, source_file, ingested_at) VALUES", *table),
		Input: proto.Input{
			{Name: "observed_on", Data: &cD},
			{Name: "ssn", Data: cS},
			{Name: "ssn_stddev", Data: cE},
			{Name: "observations", Data: cN},
			{Name: "definitive", Data: &cF},
			{Name: "source_file", Data: cSrc},
			{Name: "ingested_at", Data: &cI},
		},
	}); err != nil {
		fmt.Fprintf(os.Stderr, "insert: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("  inserted %d rows\n", len(dates))
}
