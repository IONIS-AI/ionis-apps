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
// linear equivalent. D is 1 for definitive and 0 for provisional -- provisional rows
// get revised, which is why the table is a ReplacingMergeTree keyed on observed_at.
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
		src     = flag.String("src", "", "Directory holding gfz_kp_ap_since_1932.txt (default: $IONIS_SOLAR_DATA_DIR)")
		file    = flag.String("file", "gfz_kp_ap_since_1932.txt", "Archive filename")
		host    = flag.String("host", "", "ClickHouse host:port (default: $IONIS_CH_HOST)")
		table   = flag.String("table", "solar.kp_bronze", "Destination table")
		since   = flag.String("since", "", "Only load rows on or after this date, YYYY-MM-DD (default: all)")
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

	var cutoff time.Time
	if *since != "" {
		if cutoff, err = time.Parse("2006-01-02", *since); err != nil {
			fmt.Fprintf(os.Stderr, "bad -since: %v\n", err)
			os.Exit(1)
		}
	}

	path := filepath.Join(dir, *file)
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open %s: %v\n  run solar-kp-download first\n", path, err)
		os.Exit(1)
	}
	defer f.Close()
	sourceFile := filepath.Base(path)

	fmt.Printf("solar-kp-ingest v%s\n  %s -> %s\n", Version, path, *table)

	var (
		obs   []time.Time
		kps   []float32
		aps   []uint16
		defs  []uint8
		total int
		bad   int
	)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		// YYY MM DD hh.h hh._m days days_m Kp ap D
		if len(fields) < 10 {
			bad++
			continue
		}
		y, e1 := strconv.Atoi(fields[0])
		mo, e2 := strconv.Atoi(fields[1])
		d, e3 := strconv.Atoi(fields[2])
		hh, e4 := strconv.ParseFloat(fields[3], 64)
		kp, e5 := strconv.ParseFloat(fields[7], 32)
		ap, e6 := strconv.Atoi(fields[8])
		df, e7 := strconv.Atoi(fields[9])
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil || e6 != nil || e7 != nil {
			bad++
			continue
		}
		// GFZ uses -1 for "no value" on both Kp and ap. Skipping rather than storing
		// it: a sentinel that reaches a mean is exactly the class of defect this
		// rebuild exists to remove.
		if kp < 0 || ap < 0 {
			bad++
			continue
		}
		t := time.Date(y, time.Month(mo), d, int(hh), 0, 0, 0, time.UTC)
		if !cutoff.IsZero() && t.Before(cutoff) {
			continue
		}
		obs = append(obs, t)
		kps = append(kps, float32(kp))
		aps = append(aps, uint16(ap))
		defs = append(defs, uint8(df))
		total++
	}
	if err := sc.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "scan: %v\n", err)
		os.Exit(1)
	}

	if total == 0 {
		fmt.Fprintf(os.Stderr, "parsed 0 usable rows from %s (%d unparseable) - refusing to continue\n", path, bad)
		os.Exit(1)
	}
	fmt.Printf("  parsed %d rows (%s .. %s), %d skipped\n",
		total, obs[0].Format("2006-01-02"), obs[len(obs)-1].Format("2006-01-02"), bad)

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

	inserted := 0
	for start := 0; start < total; start += *batchSz {
		end := start + *batchSz
		if end > total {
			end = total
		}
		cT := new(proto.ColDateTime64).WithPrecision(proto.PrecisionSecond)
		var cK proto.ColFloat32
		var cA proto.ColUInt16
		var cD proto.ColUInt8
		cS := new(proto.ColStr).LowCardinality()
		var cI proto.ColDateTime
		now := time.Now()
		for i := start; i < end; i++ {
			cT.Append(obs[i])
			cK.Append(kps[i])
			cA.Append(aps[i])
			cD.Append(defs[i])
			cS.Append(sourceFile)
			cI.Append(now)
		}
		if err := conn.Do(ctx, ch.Query{
			Body: fmt.Sprintf("INSERT INTO %s (observed_at, kp, ap, definitive, source_file, ingested_at) VALUES", *table),
			Input: proto.Input{
				{Name: "observed_at", Data: cT},
				{Name: "kp", Data: &cK},
				{Name: "ap", Data: &cA},
				{Name: "definitive", Data: &cD},
				{Name: "source_file", Data: cS},
				{Name: "ingested_at", Data: &cI},
			},
		}); err != nil {
			fmt.Fprintf(os.Stderr, "insert: %v\n", err)
			os.Exit(1)
		}
		inserted += end - start
	}
	fmt.Printf("  inserted %d rows\n", inserted)
}
