// dscovr-ingest - DSCOVR L1 solar wind data ingestion into ClickHouse
//
// Downloads rolling 7-day magnetometer (Bz, Bt, Bx, By) and plasma
// (speed, density, temperature) JSON from NOAA SWPC and inserts into
// ClickHouse solar.dscovr. ReplacingMergeTree handles deduplication
// across overlapping 7-day windows.
//
// Source: https://services.swpc.noaa.gov/products/solar-wind/
// Format: 2D JSON arrays with header row + string data rows
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
	"strconv"
	"syscall"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

var Version = "dev"

const (
	magURL    = "https://services.swpc.noaa.gov/products/solar-wind/mag-7-day.json"
	plasmaURL = "https://services.swpc.noaa.gov/products/solar-wind/plasma-7-day.json"
	sourceTag = "dscovr-7day"
)

// DscovrRecord holds merged magnetometer + plasma data for one timestamp.
type DscovrRecord struct {
	Time        time.Time
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
	b.SourceFile.Append(sourceTag)
}

// parseFloat32 parses a string to float32, returning 0 on error or null.
func parseFloat32(s string) float32 {
	if s == "" || s == "null" {
		return 0
	}
	v, err := strconv.ParseFloat(s, 32)
	if err != nil {
		return 0
	}
	return float32(v)
}

// parseTimestamp parses NOAA SWPC time format: "2026-02-21 22:22:00.000"
func parseTimestamp(s string) (time.Time, error) {
	t, err := time.Parse("2006-01-02 15:04:05.000", s)
	if err != nil {
		// Try without milliseconds
		t, err = time.Parse("2006-01-02 15:04:05", s)
	}
	return t, err
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
func parseMag(data []byte, records map[time.Time]*DscovrRecord) (int, error) {
	var rows [][]string
	if err := json.Unmarshal(data, &rows); err != nil {
		return 0, fmt.Errorf("mag JSON parse: %w", err)
	}
	if len(rows) < 2 {
		return 0, fmt.Errorf("mag JSON: no data rows")
	}

	count := 0
	for i := 1; i < len(rows); i++ {
		row := rows[i]
		if len(row) < 7 {
			continue
		}

		t, err := parseTimestamp(row[0])
		if err != nil {
			continue
		}

		rec, ok := records[t]
		if !ok {
			rec = &DscovrRecord{Time: t}
			records[t] = rec
		}

		rec.BxGSM = parseFloat32(row[1])
		rec.ByGSM = parseFloat32(row[2])
		rec.BzGSM = parseFloat32(row[3])
		// row[4] = lon_gsm, row[5] = lat_gsm — not stored
		rec.Bt = parseFloat32(row[6])
		count++
	}

	return count, nil
}

// parsePlasma parses plasma JSON into the record map.
// Format: [["time_tag","density","speed","temperature"], ...]
func parsePlasma(data []byte, records map[time.Time]*DscovrRecord) (int, error) {
	var rows [][]string
	if err := json.Unmarshal(data, &rows); err != nil {
		return 0, fmt.Errorf("plasma JSON parse: %w", err)
	}
	if len(rows) < 2 {
		return 0, fmt.Errorf("plasma JSON: no data rows")
	}

	count := 0
	for i := 1; i < len(rows); i++ {
		row := rows[i]
		if len(row) < 4 {
			continue
		}

		t, err := parseTimestamp(row[0])
		if err != nil {
			continue
		}

		rec, ok := records[t]
		if !ok {
			rec = &DscovrRecord{Time: t}
			records[t] = rec
		}

		rec.Density = parseFloat32(row[1])
		rec.Speed = parseFloat32(row[2])
		rec.Temperature = parseFloat32(row[3])
		count++
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
		fmt.Fprintf(os.Stderr, "Downloads 7-day magnetometer + plasma JSON from NOAA SWPC\n")
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
