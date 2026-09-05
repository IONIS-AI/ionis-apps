// dscovr-ingest - L1 real-time solar wind ingestion into ClickHouse
//
// Downloads rolling magnetometer (Bz, Bt, Bx, By) and plasma (speed,
// density, temperature) JSON from NOAA SWPC and inserts into ClickHouse
// solar.dscovr. ReplacingMergeTree handles deduplication across
// overlapping fetch windows.
//
// Source: https://services.swpc.noaa.gov/json/rtsw/
// Format: JSON array of objects, typed values, newest-first
//
// ENDPOINT MIGRATION (v4.0.4, 2026-09-05). NOAA retired the entire
// /products/solar-wind/ path — mag-7-day.json and plasma-7-day.json both 404,
// as does the directory index. Ingest had been failing every 15 minutes since
// 2026-06-30, leaving a two-month hole in solar.dscovr that nobody saw because
// the fleet health check was itself dead. Three things changed, and only the
// first is a URL swap:
//
//  1. Shape: was a 2D array with a header row and every value a STRING. Now a
//     flat array of objects with real JSON types and null for missing values.
//
//  2. Provenance: the feed is no longer DSCOVR-only. It carries SOLAR1, ACE and
//     IMAP concurrently — ~2.5 rows per minute-slot — with `active` marking the
//     authoritative spacecraft. We MUST filter on active, or ReplacingMergeTree
//     (ORDER BY date,time) silently collapses three spacecraft into whichever
//     row merged last. Filtering active yields exactly one row per minute.
//     The originating spacecraft is now recorded per-row in source_file, so the
//     table no longer implies DSCOVR for data that may be ACE or IMAP.
//
//  3. Window: was 7 days per fetch, now ~24 hours. The 15-minute timer gives
//     ample overlap, but an outage longer than 24h is now UNRECOVERABLE from
//     this endpoint — it has no history. Backfill requires NASA OMNIWeb
//     (omni_hro_1min), which is a different, time-shifted product.
//
// Build: CGO_ENABLED=0 go build -ldflags="-s -w" -o build/dscovr-ingest ./cmd/dscovr-ingest

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

var Version = "dev"

const (
	magURL    = "https://services.swpc.noaa.gov/json/rtsw/rtsw_mag_1m.json"
	plasmaURL = "https://services.swpc.noaa.gov/json/rtsw/rtsw_wind_1m.json"
	// Fallback tag for rows whose spacecraft the feed does not name.
	sourceTag = "rtsw-1m"
)

// DscovrRecord holds merged magnetometer + plasma data for one timestamp.
type DscovrRecord struct {
	Time time.Time
	// Source is the originating spacecraft (SOLAR1, ACE, IMAP) as reported by
	// the feed. Stored per-row so a table named "dscovr" cannot silently imply
	// DSCOVR for data that came from somewhere else.
	Source      string
	BzGSM       float32
	Bt          float32
	BxGSM       float32
	ByGSM       float32
	Speed       float32
	Density     float32
	Temperature float32
}

// DscovrBatch holds columnar data for native ClickHouse insert.
type DscovrBatch struct {
	Date        *proto.ColDate32
	Time        *proto.ColDateTime
	BzGSM       *proto.ColFloat32
	Bt          *proto.ColFloat32
	BxGSM       *proto.ColFloat32
	ByGSM       *proto.ColFloat32
	Speed       *proto.ColFloat32
	Density     *proto.ColFloat32
	Temperature *proto.ColFloat32
	SourceFile  *proto.ColStr
}

func NewDscovrBatch() *DscovrBatch {
	return &DscovrBatch{
		Date:        new(proto.ColDate32),
		Time:        new(proto.ColDateTime),
		BzGSM:       new(proto.ColFloat32),
		Bt:          new(proto.ColFloat32),
		BxGSM:       new(proto.ColFloat32),
		ByGSM:       new(proto.ColFloat32),
		Speed:       new(proto.ColFloat32),
		Density:     new(proto.ColFloat32),
		Temperature: new(proto.ColFloat32),
		SourceFile:  new(proto.ColStr),
	}
}

func (b *DscovrBatch) Len() int {
	return b.Date.Rows()
}

func (b *DscovrBatch) Input() proto.Input {
	return proto.Input{
		{Name: "date", Data: b.Date},
		{Name: "time", Data: b.Time},
		{Name: "bz_gsm", Data: b.BzGSM},
		{Name: "bt", Data: b.Bt},
		{Name: "bx_gsm", Data: b.BxGSM},
		{Name: "by_gsm", Data: b.ByGSM},
		{Name: "speed", Data: b.Speed},
		{Name: "density", Data: b.Density},
		{Name: "temperature", Data: b.Temperature},
		{Name: "source_file", Data: b.SourceFile},
	}
}

func (b *DscovrBatch) AddRow(rec *DscovrRecord) {
	date := rec.Time.Truncate(24 * time.Hour)
	b.Date.Append(date)
	b.Time.Append(rec.Time)
	b.BzGSM.Append(rec.BzGSM)
	b.Bt.Append(rec.Bt)
	b.BxGSM.Append(rec.BxGSM)
	b.ByGSM.Append(rec.ByGSM)
	b.Speed.Append(rec.Speed)
	b.Density.Append(rec.Density)
	b.Temperature.Append(rec.Temperature)
	src := rec.Source
	if src == "" {
		src = sourceTag
	} else {
		src = sourceTag + "/" + src
	}
	b.SourceFile.Append(src)
}

func parseTimestamp(s string) (time.Time, error) {
	// RTSW emits "2026-09-05T18:18:00" (T-separated, no zone, UTC implied). The
	// space-separated forms are the retired /products/ layout, kept so a
	// re-pointed or archived feed still parses.
	for _, layout := range []string{
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02 15:04:05.000",
		"2006-01-02 15:04:05",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognised timestamp %q", s)
}

// fetchJSON downloads a URL and returns the body bytes.
func fetchJSON(url string, timeout time.Duration) ([]byte, error) {
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("HTTP GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading body from %s: %w", url, err)
	}
	return data, nil
}

// parseMag parses magnetometer JSON into the record map.
// Format: [["time_tag","bx_gsm","by_gsm","bz_gsm","lon_gsm","lat_gsm","bt"], ...]
// rtswMag is one record of rtsw_mag_1m.json. Numeric fields are pointers because
// the feed emits null for a missing sample; a plain float32 would silently read 0,
// which for Bz is a real physical value (northward/neutral IMF) and would look like
// valid quiet-field data rather than a gap.
type rtswMag struct {
	TimeTag string   `json:"time_tag"`
	Active  bool     `json:"active"`
	Source  string   `json:"source"`
	Bt      *float32 `json:"bt"`
	BxGSM   *float32 `json:"bx_gsm"`
	ByGSM   *float32 `json:"by_gsm"`
	BzGSM   *float32 `json:"bz_gsm"`
}

// rtswWind is one record of rtsw_wind_1m.json. Same null-vs-zero reasoning: a
// proton density of 0 is not physical, so a null must not become one.
type rtswWind struct {
	TimeTag     string   `json:"time_tag"`
	Active      bool     `json:"active"`
	Source      string   `json:"source"`
	Speed       *float32 `json:"proton_speed"`
	Density     *float32 `json:"proton_density"`
	Temperature *float32 `json:"proton_temperature"`
}

// deref returns the pointed-to value, or 0 when the feed sent null. The column is
// Float32 DEFAULT 0 and every prior row used the same convention, so this keeps
// the existing contract rather than changing the table's meaning in a bugfix.
func deref(f *float32) float32 {
	if f == nil {
		return 0
	}
	return *f
}

func parseMag(data []byte, records map[time.Time]*DscovrRecord) (int, error) {
	var rows []rtswMag
	if err := json.Unmarshal(data, &rows); err != nil {
		return 0, fmt.Errorf("mag JSON parse: %w", err)
	}
	if len(rows) == 0 {
		return 0, fmt.Errorf("mag JSON: no data rows")
	}

	count := 0
	for i := range rows {
		row := &rows[i]

		// Only the active spacecraft is authoritative. Without this the feed
		// delivers SOLAR1, ACE and IMAP for the same minute and the last one
		// merged would win, non-deterministically.
		if !row.Active {
			continue
		}

		t, err := parseTimestamp(row.TimeTag)
		if err != nil {
			continue
		}

		rec, ok := records[t]
		if !ok {
			rec = &DscovrRecord{Time: t}
			records[t] = rec
		}
		if rec.Source == "" {
			rec.Source = row.Source
		}

		rec.Bt = deref(row.Bt)
		rec.BxGSM = deref(row.BxGSM)
		rec.ByGSM = deref(row.ByGSM)
		rec.BzGSM = deref(row.BzGSM)
		count++
	}

	if count == 0 {
		return 0, fmt.Errorf("mag JSON: %d rows but none active — feed shape may have changed", len(rows))
	}
	return count, nil
}

func parsePlasma(data []byte, records map[time.Time]*DscovrRecord) (int, error) {
	var rows []rtswWind
	if err := json.Unmarshal(data, &rows); err != nil {
		return 0, fmt.Errorf("plasma JSON parse: %w", err)
	}
	if len(rows) == 0 {
		return 0, fmt.Errorf("plasma JSON: no data rows")
	}

	count := 0
	for i := range rows {
		row := &rows[i]
		if !row.Active {
			continue
		}

		t, err := parseTimestamp(row.TimeTag)
		if err != nil {
			continue
		}

		rec, ok := records[t]
		if !ok {
			rec = &DscovrRecord{Time: t}
			records[t] = rec
		}
		if rec.Source == "" {
			rec.Source = row.Source
		}

		rec.Density = deref(row.Density)
		rec.Speed = deref(row.Speed)
		rec.Temperature = deref(row.Temperature)
		count++
	}

	if count == 0 {
		return 0, fmt.Errorf("plasma JSON: %d rows but none active — feed shape may have changed", len(rows))
	}
	return count, nil
}

func flushBatch(ctx context.Context, conn *ch.Client, batch *DscovrBatch) error {
	if batch.Len() == 0 {
		return nil
	}
	return conn.Do(ctx, ch.Query{
		Body:  "INSERT INTO solar.dscovr (date, time, bz_gsm, bt, bx_gsm, by_gsm, speed, density, temperature, source_file) VALUES",
		Input: batch.Input(),
	})
}

func main() {
	chHost := flag.String("ch-host", "192.168.1.90:9000", "ClickHouse native protocol address")
	dryRun := flag.Bool("dry-run", false, "Download and parse only, skip insert")
	httpTimeout := flag.Int("timeout", 60, "HTTP timeout in seconds")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "dscovr-ingest v%s — DSCOVR L1 Solar Wind Ingester\n\n", Version)
		fmt.Fprintf(os.Stderr, "Downloads rolling ~24h magnetometer + plasma JSON from NOAA SWPC\n")
		fmt.Fprintf(os.Stderr, "and inserts into ClickHouse solar.dscovr.\n\n")
		fmt.Fprintf(os.Stderr, "Sources:\n")
		fmt.Fprintf(os.Stderr, "  %s\n", magURL)
		fmt.Fprintf(os.Stderr, "  %s\n\n", plasmaURL)
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS]\n\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s -ch-host 192.168.1.90:9000\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -dry-run\n", os.Args[0])
	}
	flag.Parse()

	log.Println("=========================================================")
	log.Printf("dscovr-ingest v%s — DSCOVR L1 Solar Wind Ingester", Version)
	log.Println("=========================================================")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("\nShutdown requested...")
		cancel()
	}()

	timeout := time.Duration(*httpTimeout) * time.Second

	// Download both JSON files
	log.Printf("Downloading magnetometer data...")
	log.Printf("  URL: %s", magURL)
	magData, err := fetchJSON(magURL, timeout)
	if err != nil {
		log.Fatalf("Magnetometer download failed: %v", err)
	}
	log.Printf("  Received %d bytes", len(magData))

	log.Printf("Downloading plasma data...")
	log.Printf("  URL: %s", plasmaURL)
	plasmaData, err := fetchJSON(plasmaURL, timeout)
	if err != nil {
		log.Fatalf("Plasma download failed: %v", err)
	}
	log.Printf("  Received %d bytes", len(plasmaData))

	// Parse into merged map
	records := make(map[time.Time]*DscovrRecord)

	magCount, err := parseMag(magData, records)
	if err != nil {
		log.Fatalf("Magnetometer parse failed: %v", err)
	}
	log.Printf("Parsed %d magnetometer rows", magCount)

	plasmaCount, err := parsePlasma(plasmaData, records)
	if err != nil {
		log.Fatalf("Plasma parse failed: %v", err)
	}
	log.Printf("Parsed %d plasma rows", plasmaCount)
	log.Printf("Merged into %d unique timestamps", len(records))

	if len(records) == 0 {
		log.Fatal("No data parsed")
	}

	// Sort by timestamp
	timestamps := make([]time.Time, 0, len(records))
	for t := range records {
		timestamps = append(timestamps, t)
	}
	sort.Slice(timestamps, func(i, j int) bool { return timestamps[i].Before(timestamps[j]) })

	// Compute summary stats
	var sumBz, sumSpeed, sumDensity float64
	var minBz, maxBz float32 = 999, -999
	var minSpeed, maxSpeed float32 = 99999, 0
	for _, t := range timestamps {
		r := records[t]
		sumBz += float64(r.BzGSM)
		sumSpeed += float64(r.Speed)
		sumDensity += float64(r.Density)
		if r.BzGSM < minBz {
			minBz = r.BzGSM
		}
		if r.BzGSM > maxBz {
			maxBz = r.BzGSM
		}
		if r.Speed < minSpeed {
			minSpeed = r.Speed
		}
		if r.Speed > maxSpeed {
			maxSpeed = r.Speed
		}
	}

	n := float64(len(timestamps))
	log.Printf("Date range: %s to %s",
		timestamps[0].Format("2006-01-02 15:04"),
		timestamps[len(timestamps)-1].Format("2006-01-02 15:04"))
	log.Printf("Bz:      avg %.2f nT, range [%.2f, %.2f] nT", sumBz/n, minBz, maxBz)
	log.Printf("Speed:   avg %.0f km/s, range [%.0f, %.0f] km/s", sumSpeed/n, minSpeed, maxSpeed)
	log.Printf("Density: avg %.2f p/cm³", sumDensity/n)

	if *dryRun {
		log.Printf("Dry run — %d rows parsed, skipping ClickHouse insert", len(timestamps))
		return
	}

	// Connect to ClickHouse
	log.Printf("Connecting to ClickHouse at %s...", *chHost)
	conn, err := ch.Dial(ctx, ch.Options{
		Address:     *chHost,
		Database:    "solar",
		Compression: ch.CompressionLZ4,
	})
	if err != nil {
		log.Fatalf("ClickHouse connection failed: %v", err)
	}
	defer conn.Close()

	// Build batch and insert
	t0 := time.Now()
	batch := NewDscovrBatch()
	for _, t := range timestamps {
		batch.AddRow(records[t])
	}

	if err := flushBatch(ctx, conn, batch); err != nil {
		log.Fatalf("Insert failed: %v", err)
	}

	elapsed := time.Since(t0)

	log.Println()
	log.Println("=========================================================")
	log.Println("Ingestion Complete")
	log.Println("=========================================================")
	log.Printf("Rows:     %d", batch.Len())
	log.Printf("Range:    %s to %s",
		timestamps[0].Format("2006-01-02 15:04"),
		timestamps[len(timestamps)-1].Format("2006-01-02 15:04"))
	log.Printf("Elapsed:  %v", elapsed.Round(time.Millisecond))
	log.Printf("Source:   %s", sourceTag)
	log.Println("=========================================================")
}
