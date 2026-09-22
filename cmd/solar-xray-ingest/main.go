// solar-xray-ingest aggregates GOES X-ray samples into 3-hour buckets.
//
// STORED AT 3-HOUR GRAIN, AND THAT IS A DECISION, not a limitation.
//
// NOAA publishes 1-minute samples. Every consumer we have joins solar at 3-hour
// buckets to match the Kp cadence -- all eight signature builds do
// intDiv(toHour(timestamp), 3). Over our era, 1-minute is ~11.6M rows to serve ~64k
// joins: 180x more rows than anything asks for.
//
// What is kept per bucket is what flare work needs:
//
//	max   an X-class flare is defined by its PEAK; a three-hour mean erases it
//	mean  the background level
//	count how many samples the bucket was built from, so a partial bucket is
//	      visible instead of silently equal to a full one
//
// The raw files are retained under xray-archive/, so the 1-minute series is
// recoverable if this grain ever turns out to be wrong. A consumer needing
// sub-3-hour X-ray gets its own table from those files rather than this one growing
// a second grain.
//
// TWO ENERGY BANDS PER TIMESTAMP. The feed interleaves 0.05-0.4nm (short) and
// 0.1-0.8nm (long) as separate records sharing a time_tag. Long is the flare-class
// band -- an M5 is 5e-5 W/m2 on long.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/IONIS-AI/ionis-apps/internal/common"
)

var Version = "dev"

type xraySample struct {
	TimeTag   string  `json:"time_tag"`
	Satellite int     `json:"satellite"`
	Flux      float64 `json:"flux"`
	EnergyStr string  `json:"energy"`
}

type bucket struct {
	shortMax, shortSum float64
	longMax, longSum   float64
	shortN, longN      int
	sat                string
}

func main() {
	var (
		src   = flag.String("src", "", "Directory holding goes_xray_7day.json (default: $IONIS_SOLAR_DATA_DIR)")
		glob  = flag.String("glob", "goes_xray_7day.json", "File or glob to ingest, relative to -src")
		host  = flag.String("host", "", "ClickHouse host:port (default: $IONIS_CH_HOST)")
		table = flag.String("table", "solar.xray_bronze", "Destination table")
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

	files, err := filepath.Glob(filepath.Join(dir, *glob))
	if err != nil || len(files) == 0 {
		fmt.Fprintf(os.Stderr, "no files matching %s in %s\n  run solar-xray-download first\n", *glob, dir)
		os.Exit(1)
	}
	fmt.Printf("solar-xray-ingest v%s\n  %d file(s) -> %s\n", Version, len(files), *table)

	buckets := map[time.Time]*bucket{}
	totalSamples := 0
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  WARN %s: %v\n", filepath.Base(path), err)
			continue
		}
		var samples []xraySample
		if err := json.Unmarshal(data, &samples); err != nil {
			fmt.Fprintf(os.Stderr, "  WARN %s: %v\n", filepath.Base(path), err)
			continue
		}
		for _, s := range samples {
			t, err := time.Parse("2006-01-02T15:04:05Z", s.TimeTag)
			if err != nil {
				if t, err = time.Parse("2006-01-02T15:04:05", s.TimeTag); err != nil {
					continue
				}
			}
			// Floor to the 3-hour grid Kp uses, so the join is exact rather than approximate.
			b := time.Date(t.Year(), t.Month(), t.Day(), (t.Hour()/3)*3, 0, 0, 0, time.UTC)
			e := buckets[b]
			if e == nil {
				e = &bucket{sat: fmt.Sprintf("G%d", s.Satellite)}
				buckets[b] = e
			}
			switch s.EnergyStr {
			case "0.05-0.4nm":
				if s.Flux > e.shortMax {
					e.shortMax = s.Flux
				}
				e.shortSum += s.Flux
				e.shortN++
			case "0.1-0.8nm":
				if s.Flux > e.longMax {
					e.longMax = s.Flux
				}
				e.longSum += s.Flux
				e.longN++
			}
			totalSamples++
		}
	}
	if len(buckets) == 0 {
		fmt.Fprintln(os.Stderr, "no usable samples parsed - refusing to continue")
		os.Exit(1)
	}

	keys := make([]time.Time, 0, len(buckets))
	for k := range buckets {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].Before(keys[j]) })
	fmt.Printf("  %d samples -> %d three-hour buckets (%s .. %s)\n",
		totalSamples, len(keys), keys[0].Format("2006-01-02 15:04"), keys[len(keys)-1].Format("2006-01-02 15:04"))
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

	var cT, cI proto.ColDateTime
	var cSM, cSA, cLM, cLA proto.ColFloat64
	var cN proto.ColUInt16
	cSat := new(proto.ColStr).LowCardinality()
	cSrc := new(proto.ColStr).LowCardinality()
	now := time.Now()
	for _, k := range keys {
		b := buckets[k]
		cT.Append(k)
		cSM.Append(b.shortMax)
		cSA.Append(safeMean(b.shortSum, b.shortN))
		cLM.Append(b.longMax)
		cLA.Append(safeMean(b.longSum, b.longN))
		n := b.longN
		if b.shortN > n {
			n = b.shortN
		}
		cN.Append(uint16(n))
		cSat.Append(b.sat)
		cSrc.Append(filepath.Base(*glob))
		cI.Append(now)
	}
	if err := conn.Do(ctx, ch.Query{
		Body: fmt.Sprintf("INSERT INTO %s (observed_at, short_max, short_mean, long_max, long_mean, sample_count, satellite, source_file, ingested_at) VALUES", *table),
		Input: proto.Input{
			{Name: "observed_at", Data: &cT},
			{Name: "short_max", Data: &cSM},
			{Name: "short_mean", Data: &cSA},
			{Name: "long_max", Data: &cLM},
			{Name: "long_mean", Data: &cLA},
			{Name: "sample_count", Data: &cN},
			{Name: "satellite", Data: cSat},
			{Name: "source_file", Data: cSrc},
			{Name: "ingested_at", Data: &cI},
		},
	}); err != nil {
		fmt.Fprintf(os.Stderr, "insert: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("  inserted %d buckets\n", len(keys))
}

func safeMean(sum float64, n int) float64 {
	if n == 0 {
		return 0
	}
	return sum / float64(n)
}
