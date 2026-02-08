// solar-backfill - Historical solar index backfill from GFZ Potsdam
//
// Downloads the definitive Kp/ap/Ap/SN/F10.7 dataset from GFZ Potsdam
// and inserts into ClickHouse solar.bronze with 3-hour bucketing.
//
// Source: https://kp.gfz-potsdam.de (Helmholtz Centre Potsdam, GFZ)
// Format: Daily SSN + F10.7 (SFI) + 8x 3-hourly Kp/ap values per day
//
// Each day produces 8 rows (one per 3-hour bucket: 00, 03, 06, ..., 21 UTC).
// SSN and SFI are replicated across all 8 buckets; Kp/ap are bucket-specific.
// ReplacingMergeTree(updated_at) on (date, time) handles deduplication.
//
// Build: CGO_ENABLED=0 go build -ldflags="-s -w" -o build/solar-backfill ./cmd/solar-backfill

package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

var Version = "1.0.0"

const (
	gfzURL     = "https://kp.gfz-potsdam.de/app/files/Kp_ap_Ap_SN_F107_since_1932.txt"
	sourceTag  = "gfz-kp-backfill"
	batchLimit = 50000 // flush every 50k rows (~6250 days)
)

// 3-hour bucket start hours (UTC)
var bucketHours = [8]int{0, 3, 6, 9, 12, 15, 18, 21}

// SolarBatch holds columnar data for native ClickHouse insert.
// Matches schema: solar.bronze (date, time, observed_flux, adjusted_flux,
// ssn, kp_index, ap_index, xray_short, xray_long, source_file)
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

func (b *SolarBatch) AddRow(date time.Time, ts time.Time, sfi, sfiAdj, ssn, kp, ap float32) {
	b.Date.Append(date)
	b.Time.Append(ts)
	b.ObservedFlux.Append(sfi)
	b.AdjustedFlux.Append(sfiAdj)
	b.SSN.Append(ssn)
	b.KpIndex.Append(kp)
	b.ApIndex.Append(ap)
	b.XrayShort.Append(0)
	b.XrayLong.Append(0)
	b.SourceFile.Append(sourceTag)
}

// GFZDay holds one parsed day from the GFZ Kp file.
type GFZDay struct {
	Date   time.Time
	Kp     [8]float32 // 3-hourly Kp (0-9 scale)
	Ap     [8]float32 // 3-hourly ap
	DayAp  float32
	SSN    float32
	SFIObs float32 // observed F10.7
	SFIAdj float32 // adjusted F10.7
}

// parseGFZLine parses one data line from the GFZ Kp file.
// Format (whitespace-delimited):
//
//	Col  0: Year
//	Col  1: Month
//	Col  2: Day
//	Col  3: Days (day of year)
//	Col  4: Days_m (modified Julian)
//	Col  5: Bsr (Bartels rotation)
//	Col  6: dB (day within rotation)
//	Col  7-14: Kp1..Kp8 (3-hourly, decimal 0.000-9.000)
//	Col 15-22: ap1..ap8 (3-hourly)
//	Col 23: Ap (daily)
//	Col 24: SN (sunspot number)
//	Col 25: F10.7obs
//	Col 26: F10.7adj
//
// Missing values are -1.000 or -1.
func parseGFZLine(line string) (GFZDay, bool) {
	fields := strings.Fields(line)
	if len(fields) < 27 {
		return GFZDay{}, false
	}

	year, err := strconv.Atoi(fields[0])
	if err != nil || year < 1900 || year > 2100 {
		return GFZDay{}, false
	}
	month, _ := strconv.Atoi(fields[1])
	day, _ := strconv.Atoi(fields[2])
	if month < 1 || month > 12 || day < 1 || day > 31 {
		return GFZDay{}, false
	}

	d := GFZDay{
		Date: time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC),
	}

	// Kp values: columns 7-14
	for i := 0; i < 8; i++ {
		v, err := strconv.ParseFloat(fields[7+i], 32)
		if err != nil || v < 0 {
			d.Kp[i] = 0
		} else {
			d.Kp[i] = float32(v)
		}
	}

	// ap values: columns 15-22
	for i := 0; i < 8; i++ {
		v, err := strconv.ParseFloat(fields[15+i], 32)
		if err != nil || v < 0 {
			d.Ap[i] = 0
		} else {
			d.Ap[i] = float32(v)
		}
	}

	// Daily Ap: column 23
	if v, err := strconv.ParseFloat(fields[23], 32); err == nil && v >= 0 {
		d.DayAp = float32(v)
	}

	// SSN: column 24
	if v, err := strconv.ParseFloat(fields[24], 32); err == nil && v >= 0 {
		d.SSN = float32(v)
	}

	// F10.7 observed: column 25
	if v, err := strconv.ParseFloat(fields[25], 32); err == nil && v >= 0 {
		d.SFIObs = float32(v)
	}

	// F10.7 adjusted: column 26
	if v, err := strconv.ParseFloat(fields[26], 32); err == nil && v >= 0 {
		d.SFIAdj = float32(v)
	}

	return d, true
}

// parseGFZ reads the GFZ file and returns days within the date range.
func parseGFZ(reader io.Reader, startDate, endDate time.Time) ([]GFZDay, error) {
	var days []GFZDay
	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		d, ok := parseGFZLine(line)
		if !ok {
			continue
		}

		if d.Date.Before(startDate) || d.Date.After(endDate) {
			continue
		}

		days = append(days, d)
	}

	return days, scanner.Err()
}

func flushBatch(ctx context.Context, conn *ch.Client, table string, batch *SolarBatch) error {
	if batch.Len() == 0 {
		return nil
	}
	query := fmt.Sprintf("INSERT INTO %s (date, time, observed_flux, adjusted_flux, ssn, kp_index, ap_index, xray_short, xray_long, source_file) VALUES", table)
	return conn.Do(ctx, ch.Query{
		Body:  query,
		Input: batch.Input(),
	})
}

func main() {
	chHost := flag.String("ch-host", "127.0.0.1:9000", "ClickHouse native protocol address")
	chDB := flag.String("ch-db", "solar", "ClickHouse database")
	chTable := flag.String("ch-table", "bronze", "ClickHouse table")
	startStr := flag.String("start", "2020-01-01", "Start date (YYYY-MM-DD)")
	endStr := flag.String("end", "", "End date (default: today)")
	localFile := flag.String("file", "", "Local GFZ file (skip download)")
	dryRun := flag.Bool("dry-run", false, "Parse only, no ClickHouse insert")
	httpTimeout := flag.Int("timeout", 120, "HTTP download timeout (seconds)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "solar-backfill v%s — Historical Solar Index Backfill (GFZ Potsdam)\n\n", Version)
		fmt.Fprintf(os.Stderr, "Downloads SSN, SFI (F10.7), and 3-hourly Kp/ap from GFZ Potsdam\n")
		fmt.Fprintf(os.Stderr, "and inserts into ClickHouse solar.bronze.\n\n")
		fmt.Fprintf(os.Stderr, "Source: %s\n\n", gfzURL)
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS]\n\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s -ch-host 192.168.1.90:9000 -start 2020-01-01\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -file /tmp/Kp_ap_Ap_SN_F107_since_1932.txt -dry-run\n", os.Args[0])
	}
	flag.Parse()

	log.Println("=========================================================")
	log.Printf("solar-backfill v%s — GFZ Potsdam Solar Index Backfill", Version)
	log.Println("=========================================================")

	// Parse date range
	startDate, err := time.Parse("2006-01-02", *startStr)
	if err != nil {
		log.Fatalf("Invalid start date: %v", err)
	}
	endDate := time.Now().UTC().Truncate(24 * time.Hour)
	if *endStr != "" {
		endDate, err = time.Parse("2006-01-02", *endStr)
		if err != nil {
			log.Fatalf("Invalid end date: %v", err)
		}
	}
	log.Printf("Date range: %s to %s", startDate.Format("2006-01-02"), endDate.Format("2006-01-02"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("\nShutdown requested...")
		cancel()
	}()

	// Fetch or read GFZ data
	var reader io.Reader
	if *localFile != "" {
		log.Printf("Reading local file: %s", *localFile)
		f, err := os.Open(*localFile)
		if err != nil {
			log.Fatalf("Cannot open file: %v", err)
		}
		defer f.Close()
		reader = f
	} else {
		log.Printf("Downloading from GFZ Potsdam...")
		log.Printf("  URL: %s", gfzURL)
		client := &http.Client{Timeout: time.Duration(*httpTimeout) * time.Second}
		resp, err := client.Get(gfzURL)
		if err != nil {
			log.Fatalf("Download failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			log.Fatalf("HTTP %d from GFZ", resp.StatusCode)
		}
		log.Printf("  HTTP 200 OK, Content-Length: %s", resp.Header.Get("Content-Length"))
		reader = resp.Body
	}

	// Parse
	log.Printf("Parsing GFZ data...")
	t0 := time.Now()
	days, err := parseGFZ(reader, startDate, endDate)
	if err != nil {
		log.Fatalf("Parse error: %v", err)
	}
	log.Printf("Parsed %d days in %v", len(days), time.Since(t0).Round(time.Millisecond))

	if len(days) == 0 {
		log.Fatal("No data found in date range")
	}

	// Stats
	var sfiCount, kpCount, ssnCount int
	var sfiMin, sfiMax float32 = 999, 0
	var kpMin, kpMax float32 = 99, 0
	var ssnMin, ssnMax float32 = 9999, 0

	for _, d := range days {
		if d.SFIObs > 0 {
			sfiCount++
			if d.SFIObs < sfiMin {
				sfiMin = d.SFIObs
			}
			if d.SFIObs > sfiMax {
				sfiMax = d.SFIObs
			}
		}
		if d.SSN > 0 {
			ssnCount++
			if d.SSN < ssnMin {
				ssnMin = d.SSN
			}
			if d.SSN > ssnMax {
				ssnMax = d.SSN
			}
		}
		for _, kp := range d.Kp {
			if kp > 0 {
				kpCount++
				if kp < kpMin {
					kpMin = kp
				}
				if kp > kpMax {
					kpMax = kp
				}
			}
		}
	}

	log.Printf("Coverage (%s to %s):", days[0].Date.Format("2006-01-02"), days[len(days)-1].Date.Format("2006-01-02"))
	if ssnCount > 0 {
		log.Printf("  SSN: %d days with data (%.0f - %.0f)", ssnCount, ssnMin, ssnMax)
	} else {
		log.Printf("  SSN: no data")
	}
	if sfiCount > 0 {
		log.Printf("  SFI: %d days with data (%.1f - %.1f SFU)", sfiCount, sfiMin, sfiMax)
	} else {
		log.Printf("  SFI: no data")
	}
	if kpCount > 0 {
		log.Printf("  Kp:  %d 3-hour values with data (%.1f - %.1f)", kpCount, kpMin, kpMax)
	} else {
		log.Printf("  Kp:  no data")
	}

	totalRows := len(days) * 8
	log.Printf("Will insert: %d rows (%d days x 8 buckets)", totalRows, len(days))

	if *dryRun {
		log.Println("Dry run — skipping ClickHouse insert")
		return
	}

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

	// Insert in batches
	t0 = time.Now()
	batch := NewSolarBatch()
	inserted := 0

	for _, d := range days {
		select {
		case <-ctx.Done():
			log.Printf("Interrupted after %d rows", inserted)
			return
		default:
		}

		for i, hour := range bucketHours {
			ts := time.Date(d.Date.Year(), d.Date.Month(), d.Date.Day(),
				hour, 0, 0, 0, time.UTC)
			batch.AddRow(d.Date, ts, d.SFIObs, d.SFIAdj, d.SSN, d.Kp[i], d.Ap[i])
		}

		if batch.Len() >= batchLimit {
			if err := flushBatch(ctx, conn, tableFQN, batch); err != nil {
				log.Fatalf("Insert error at row %d: %v", inserted, err)
			}
			inserted += batch.Len()
			elapsed := time.Since(t0)
			rps := float64(inserted) / elapsed.Seconds()
			log.Printf("  Inserted %d / %d rows (%.0f rows/sec)", inserted, totalRows, rps)
			batch.Reset()
		}
	}

	// Final flush
	if batch.Len() > 0 {
		if err := flushBatch(ctx, conn, tableFQN, batch); err != nil {
			log.Fatalf("Final insert error: %v", err)
		}
		inserted += batch.Len()
	}

	elapsed := time.Since(t0)
	rps := float64(inserted) / elapsed.Seconds()

	log.Println()
	log.Println("=========================================================")
	log.Println("Backfill Complete")
	log.Println("=========================================================")
	log.Printf("Days:    %d (%s to %s)", len(days), days[0].Date.Format("2006-01-02"), days[len(days)-1].Date.Format("2006-01-02"))
	log.Printf("Rows:    %d (8 per day)", inserted)
	log.Printf("Elapsed: %v", elapsed.Round(time.Millisecond))
	log.Printf("Rate:    %.0f rows/sec", rps)
	log.Printf("Source:  %s", sourceTag)
	log.Println("=========================================================")
	log.Println()
	log.Println("Run OPTIMIZE TABLE solar.bronze FINAL to merge duplicates.")
}
