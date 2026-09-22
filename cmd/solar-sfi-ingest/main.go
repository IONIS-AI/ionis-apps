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
		src     = flag.String("src", "", "Directory holding penticton_fluxtable.txt (default: $IONIS_SOLAR_DATA_DIR)")
		file    = flag.String("file", "penticton_fluxtable.txt", "Archive filename")
		host    = flag.String("host", "", "ClickHouse host:port (default: $IONIS_CH_HOST)")
		table   = flag.String("table", "solar.sfi_bronze", "Destination table")
		dry     = flag.Bool("dry-run", false, "Parse and report, insert nothing")
		batchSz = flag.Int("batch", 100000, "Rows per INSERT")
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

	type row struct {
		t                   time.Time
		obs, adj, ursi, car float32
	}
	var rows []row
	var bad int

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "fluxdate") || strings.HasPrefix(line, "---") {
			continue
		}
		fs := strings.Fields(line)
		if len(fs) < 7 {
			bad++
			continue
		}
		// fluxdate YYYYMMDD, fluxtime HHMMSS
		t, err := time.Parse("20060102150405", fs[0]+fmt.Sprintf("%06s", fs[1]))
		if err != nil {
			bad++
			continue
		}
		car, e1 := strconv.ParseFloat(fs[3], 32)
		obs, e2 := strconv.ParseFloat(fs[4], 32)
		adj, e3 := strconv.ParseFloat(fs[5], 32)
		ursi, e4 := strconv.ParseFloat(fs[6], 32)
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
			bad++
			continue
		}
		rows = append(rows, row{t, float32(obs), float32(adj), float32(ursi), float32(car)})
	}
	if err := sc.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "scan: %v\n", err)
		os.Exit(1)
	}
	if len(rows) == 0 {
		fmt.Fprintf(os.Stderr, "parsed 0 usable rows (%d unparseable) - refusing to continue\n", bad)
		os.Exit(1)
	}
	fmt.Printf("  parsed %d rows (%s .. %s), %d skipped\n",
		len(rows), rows[0].t.Format("2006-01-02"), rows[len(rows)-1].t.Format("2006-01-02"), bad)
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

	ins := 0
	for start := 0; start < len(rows); start += *batchSz {
		end := start + *batchSz
		if end > len(rows) {
			end = len(rows)
		}
		var cT, cI proto.ColDateTime
		var cO, cA, cU, cC proto.ColFloat32
		cS := new(proto.ColStr).LowCardinality()
		now := time.Now()
		for i := start; i < end; i++ {
			cT.Append(rows[i].t)
			cO.Append(rows[i].obs)
			cA.Append(rows[i].adj)
			cU.Append(rows[i].ursi)
			cC.Append(rows[i].car)
			cS.Append(sourceFile)
			cI.Append(now)
		}
		if err := conn.Do(ctx, ch.Query{
			Body: fmt.Sprintf("INSERT INTO %s (observed_at, observed_flux, adjusted_flux, ursi_flux, carrington, source_file, ingested_at) VALUES", *table),
			Input: proto.Input{
				{Name: "observed_at", Data: &cT},
				{Name: "observed_flux", Data: &cO},
				{Name: "adjusted_flux", Data: &cA},
				{Name: "ursi_flux", Data: &cU},
				{Name: "carrington", Data: &cC},
				{Name: "source_file", Data: cS},
				{Name: "ingested_at", Data: &cI},
			},
		}); err != nil {
			fmt.Fprintf(os.Stderr, "insert: %v\n", err)
			os.Exit(1)
		}
		ins += end - start
	}
	fmt.Printf("  inserted %d rows\n", ins)
}
