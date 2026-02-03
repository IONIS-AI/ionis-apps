// solar-ingest - NOAA/SIDC solar flux data ingestion into ClickHouse
//
// Supports multiple solar data formats:
//   - SIDC CSV (sidc_YYYY.csv): Daily sunspot numbers from SILSO
//   - SFI JSON (sfi_daily_flux.txt): NOAA solar flux indices
//   - Penticton flux files
//
// Build: CGO_ENABLED=0 go build -ldflags="-s -w" -o build/solar-ingest ./cmd/solar-ingest

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

// Version can be overridden at build time via -ldflags
var Version = "2.0.8"

// SFIRecord represents a record from NOAA SFI JSON
type SFIRecord struct {
	TimeTag      string  `json:"time-tag"`
	SSN          float64 `json:"ssn"`
	SmoothedSSN  float64 `json:"smoothed_ssn"`
	ObservedSWPC float64 `json:"observed_swpc_ssn"`
	SmoothedSWPC float64 `json:"smoothed_swpc_ssn"`
	F107         float64 `json:"f10.7"`
	SmoothedF107 float64 `json:"smoothed_f10.7"`
}

// XrayRecord represents a record from GOES X-ray flux JSON
type XrayRecord struct {
	TimeTag              string  `json:"time_tag"`
	Satellite            int     `json:"satellite"`
	Flux                 float64 `json:"flux"`
	ObservedFlux         float64 `json:"observed_flux"`
	ElectronCorrection   float64 `json:"electron_correction"`
	ElectronContaminated bool    `json:"electron_contaminaton"` // Note: NOAA typo in API
	Energy               string  `json:"energy"`
}

// SolarBatch holds column data for native insert
type SolarBatch struct {
	Date         *proto.ColDate32
	Time         *proto.ColDateTime
	ObservedFlux *proto.ColFloat32
	AdjustedFlux *proto.ColFloat32
	SSN          *proto.ColFloat32
	KpIndex      *proto.ColFloat32
	ApIndex      *proto.ColFloat32
	XrayShort    *proto.ColFloat32
	XrayLong     *proto.ColFloat32
	SourceFile   *proto.ColStr
}

func NewSolarBatch() *SolarBatch {
	return &SolarBatch{
		Date:         new(proto.ColDate32),
		Time:         new(proto.ColDateTime),
		ObservedFlux: new(proto.ColFloat32),
		AdjustedFlux: new(proto.ColFloat32),
		SSN:          new(proto.ColFloat32),
		KpIndex:      new(proto.ColFloat32),
		ApIndex:      new(proto.ColFloat32),
		XrayShort:    new(proto.ColFloat32),
		XrayLong:     new(proto.ColFloat32),
		SourceFile:   new(proto.ColStr),
	}
}

func (b *SolarBatch) Reset() {
	b.Date.Reset()
	b.Time.Reset()
	b.ObservedFlux.Reset()
	b.AdjustedFlux.Reset()
	b.SSN.Reset()
	b.KpIndex.Reset()
	b.ApIndex.Reset()
	b.XrayShort.Reset()
	b.XrayLong.Reset()
	b.SourceFile.Reset()
}

func (b *SolarBatch) Len() int {
	return b.Date.Rows()
}

func (b *SolarBatch) Input() proto.Input {
	return proto.Input{
		{Name: "date", Data: b.Date},
		{Name: "time", Data: b.Time},
		{Name: "observed_flux", Data: b.ObservedFlux},
		{Name: "adjusted_flux", Data: b.AdjustedFlux},
		{Name: "ssn", Data: b.SSN},
		{Name: "kp_index", Data: b.KpIndex},
		{Name: "ap_index", Data: b.ApIndex},
		{Name: "xray_short", Data: b.XrayShort},
		{Name: "xray_long", Data: b.XrayLong},
		{Name: "source_file", Data: b.SourceFile},
	}
}

func (b *SolarBatch) AddRecord(date time.Time, observedFlux, adjustedFlux, ssn, kp, ap, xrayShort, xrayLong float32, sourceFile string) {
	b.Date.Append(date)
	b.Time.Append(date)
	b.ObservedFlux.Append(observedFlux)
	b.AdjustedFlux.Append(adjustedFlux)
	b.SSN.Append(ssn)
	b.KpIndex.Append(kp)
	b.ApIndex.Append(ap)
	b.XrayShort.Append(xrayShort)
	b.XrayLong.Append(xrayLong)
	b.SourceFile.Append(sourceFile)
}

func flushBatch(ctx context.Context, conn *ch.Client, tableFQN string, batch *SolarBatch) error {
	if batch.Len() == 0 {
		return nil
	}

	query := fmt.Sprintf("INSERT INTO %s (date, time, observed_flux, adjusted_flux, ssn, kp_index, ap_index, xray_short, xray_long, source_file) VALUES", tableFQN)
	return conn.Do(ctx, ch.Query{
		Body:  query,
		Input: batch.Input(),
	})
}

// parseSIDC parses SIDC format: YYYY;MM;DD;decimal_year;SSN;std_dev;observations;flag
func parseSIDC(filePath string, batch *SolarBatch) (int, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	sourceFile := filepath.Base(filePath)
	scanner := bufio.NewScanner(f)
	count := 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		fields := strings.Split(line, ";")
		if len(fields) < 5 {
			continue
		}

		year, _ := strconv.Atoi(strings.TrimSpace(fields[0]))
		month, _ := strconv.Atoi(strings.TrimSpace(fields[1]))
		day, _ := strconv.Atoi(strings.TrimSpace(fields[2]))
		ssn, _ := strconv.ParseFloat(strings.TrimSpace(fields[4]), 32)

		if year < 1900 || year > 2100 || month < 1 || month > 12 || day < 1 || day > 31 {
			continue
		}

		date := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
		batch.AddRecord(date, 0, 0, float32(ssn), 0, 0, 0, 0, sourceFile)
		count++
	}

	return count, scanner.Err()
}

// parseSFIJSON parses NOAA SFI JSON format
func parseSFIJSON(filePath string, batch *SolarBatch) (int, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return 0, err
	}

	var records []SFIRecord
	if err := json.Unmarshal(data, &records); err != nil {
		return 0, err
	}

	sourceFile := filepath.Base(filePath)
	count := 0

	for _, rec := range records {
		// Parse time-tag "YYYY-MM" to date
		parts := strings.Split(rec.TimeTag, "-")
		if len(parts) != 2 {
			continue
		}

		year, _ := strconv.Atoi(parts[0])
		month, _ := strconv.Atoi(parts[1])

		if year < 1900 || year > 2100 || month < 1 || month > 12 {
			continue
		}

		date := time.Date(year, time.Month(month), 15, 0, 0, 0, 0, time.UTC) // Middle of month

		// Use F10.7 as observed flux (SFI)
		observedFlux := float32(rec.F107)
		if observedFlux < 0 {
			observedFlux = 0
		}

		ssn := float32(rec.SSN)
		if ssn < 0 {
			ssn = 0
		}

		batch.AddRecord(date, observedFlux, float32(rec.SmoothedF107), ssn, 0, 0, 0, 0, sourceFile)
		count++
	}

	return count, nil
}

// parseKpIndexJSON parses NOAA planetary K-index JSON format
// Format: 2D array with header row ["time_tag", "Kp", "a_running", "station_count"]
func parseKpIndexJSON(filePath string, batch *SolarBatch) (int, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return 0, err
	}

	// Parse as 2D string array
	var rows [][]string
	if err := json.Unmarshal(data, &rows); err != nil {
		return 0, fmt.Errorf("JSON parse error: %w", err)
	}

	if len(rows) < 2 {
		return 0, fmt.Errorf("no data rows found")
	}

	sourceFile := filepath.Base(filePath)
	count := 0

	// Skip header row (index 0)
	for i := 1; i < len(rows); i++ {
		row := rows[i]
		if len(row) < 3 {
			continue
		}

		// Parse time_tag: "2026-01-27 00:00:00.000"
		timeStr := row[0]
		t, err := time.Parse("2006-01-02 15:04:05.000", timeStr)
		if err != nil {
			// Try without milliseconds
			t, err = time.Parse("2006-01-02 15:04:05", timeStr)
			if err != nil {
				continue
			}
		}

		// Parse Kp value
		kp, err := strconv.ParseFloat(row[1], 32)
		if err != nil {
			continue
		}

		// Parse a_running (Ap equivalent)
		ap, err := strconv.ParseFloat(row[2], 32)
		if err != nil {
			ap = 0
		}

		// Add record with Kp/Ap values, all other fields zero
		batch.AddRecord(t, 0, 0, 0, float32(kp), float32(ap), 0, 0, sourceFile)
		count++
	}

	return count, nil
}

// parseXrayFluxJSON parses GOES X-ray flux JSON format
// Two energy bands per timestamp: "0.05-0.4nm" (short) and "0.1-0.8nm" (long)
func parseXrayFluxJSON(filePath string, batch *SolarBatch) (int, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return 0, err
	}

	var records []XrayRecord
	if err := json.Unmarshal(data, &records); err != nil {
		return 0, fmt.Errorf("JSON parse error: %w", err)
	}

	sourceFile := filepath.Base(filePath)

	// Group records by timestamp (two per timestamp: short and long)
	type xrayPair struct {
		time      time.Time
		xrayShort float32
		xrayLong  float32
	}
	pairs := make(map[string]*xrayPair)

	for _, rec := range records {
		// Parse time_tag: "2026-02-02T18:14:00Z"
		t, err := time.Parse(time.RFC3339, rec.TimeTag)
		if err != nil {
			continue
		}

		key := rec.TimeTag
		if pairs[key] == nil {
			pairs[key] = &xrayPair{time: t}
		}

		// Assign flux based on energy band
		switch rec.Energy {
		case "0.05-0.4nm":
			pairs[key].xrayShort = float32(rec.Flux)
		case "0.1-0.8nm":
			pairs[key].xrayLong = float32(rec.Flux)
		}
	}

	count := 0
	for _, p := range pairs {
		// Add record with X-ray values, all other fields zero
		batch.AddRecord(p.time, 0, 0, 0, 0, 0, p.xrayShort, p.xrayLong, sourceFile)
		count++
	}

	return count, nil
}

// detectFormat determines the file format based on extension and content
func detectFormat(filePath string) string {
	ext := strings.ToLower(filepath.Ext(filePath))
	base := strings.ToLower(filepath.Base(filePath))

	// SIDC CSV format
	if strings.HasPrefix(base, "sidc_") && ext == ".csv" {
		return "sidc"
	}

	// NOAA K-index JSON
	if strings.Contains(base, "kp_index") && ext == ".json" {
		return "kp_json"
	}

	// GOES X-ray flux JSON
	if strings.Contains(base, "xray") && ext == ".json" {
		return "xray_json"
	}

	// SFI JSON (legacy naming)
	if strings.Contains(base, "flux") && ext == ".txt" {
		// Check if JSON
		data, err := os.ReadFile(filePath)
		if err == nil && len(data) > 0 && data[0] == '[' {
			return "sfi_json"
		}
	}

	return "unknown"
}

func main() {
	chHost := flag.String("ch-host", "127.0.0.1:9000", "ClickHouse address")
	chDB := flag.String("ch-db", "solar", "ClickHouse database")
	chTable := flag.String("ch-table", "indices_raw", "ClickHouse table")
	sourceDir := flag.String("source-dir", "/mnt/ai-stack/solar-data/raw", "Solar data source directory")
	truncate := flag.Bool("truncate", false, "Truncate table before insert")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "solar-ingest v%s - Solar Flux Data Ingester\n\n", Version)
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] [files...]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Ingests solar flux data from NOAA/SIDC/GOES sources into ClickHouse.\n\n")
		fmt.Fprintf(os.Stderr, "Supported formats:\n")
		fmt.Fprintf(os.Stderr, "  - SIDC CSV (sidc_*.csv): Daily sunspot numbers\n")
		fmt.Fprintf(os.Stderr, "  - SFI JSON (sfi_daily_flux.txt): NOAA solar flux indices\n")
		fmt.Fprintf(os.Stderr, "  - Kp JSON (noaa_kp_index.json): Planetary K-index (3-hourly)\n")
		fmt.Fprintf(os.Stderr, "  - X-ray JSON (goes_xray_flux.json): GOES X-ray flux (6-hour)\n\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	log.Println("=========================================================")
	log.Printf("Solar Ingest v%s", Version)
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

	// Connect to ClickHouse
	log.Printf("Connecting to ClickHouse at %s...", *chHost)
	conn, err := ch.Dial(ctx, ch.Options{
		Address:     *chHost,
		Database:    *chDB,
		Compression: ch.CompressionLZ4,
	})
	if err != nil {
		log.Fatalf("ClickHouse connection failed: %v", err)
	}
	defer conn.Close()

	tableFQN := fmt.Sprintf("%s.%s", *chDB, *chTable)
	log.Printf("Table: %s", tableFQN)

	// Truncate if requested
	if *truncate {
		log.Printf("Truncating table %s...", tableFQN)
		if err := conn.Do(ctx, ch.Query{Body: fmt.Sprintf("TRUNCATE TABLE %s", tableFQN)}); err != nil {
			log.Printf("Truncate warning: %v", err)
		}
	}

	// Discover files
	var files []string
	if len(flag.Args()) > 0 {
		files = flag.Args()
	} else {
		entries, err := os.ReadDir(*sourceDir)
		if err != nil {
			log.Fatalf("Cannot read source directory: %v", err)
		}
		for _, e := range entries {
			if !e.IsDir() {
				files = append(files, filepath.Join(*sourceDir, e.Name()))
			}
		}
	}

	if len(files) == 0 {
		log.Fatal("No files to process")
	}

	log.Printf("Found %d file(s)", len(files))

	startTime := time.Now()
	totalRecords := 0
	batch := NewSolarBatch()

	for _, filePath := range files {
		select {
		case <-ctx.Done():
			break
		default:
		}

		format := detectFormat(filePath)
		if format == "unknown" {
			log.Printf("[%s] Skipping (unknown format)", filepath.Base(filePath))
			continue
		}

		var count int
		var err error

		switch format {
		case "sidc":
			count, err = parseSIDC(filePath, batch)
		case "sfi_json":
			count, err = parseSFIJSON(filePath, batch)
		case "kp_json":
			count, err = parseKpIndexJSON(filePath, batch)
		case "xray_json":
			count, err = parseXrayFluxJSON(filePath, batch)
		}

		if err != nil {
			log.Printf("[%s] Parse error: %v", filepath.Base(filePath), err)
			continue
		}

		log.Printf("[%s] Parsed %d records (%s format)", filepath.Base(filePath), count, format)
		totalRecords += count
	}

	// Flush batch
	if batch.Len() > 0 {
		if err := flushBatch(ctx, conn, tableFQN, batch); err != nil {
			log.Fatalf("Insert error: %v", err)
		}
		log.Printf("Inserted %d records", batch.Len())
	}

	elapsed := time.Since(startTime)

	log.Println()
	log.Println("=========================================================")
	log.Println("Final Statistics")
	log.Println("=========================================================")
	log.Printf("Total Records: %d", totalRecords)
	log.Printf("Elapsed:       %v", elapsed.Round(time.Millisecond))
	log.Printf("Rate:          %.0f records/sec", float64(totalRecords)/elapsed.Seconds())
	log.Println("=========================================================")
}
