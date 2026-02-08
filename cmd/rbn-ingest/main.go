// rbn-ingest - Stream RBN daily ZIP archives into ClickHouse via native protocol
//
// Reads daily ZIP files from /mnt/rbn-data/{year}/*.zip, parses the CSV inside
// each ZIP, normalizes band via bands.GetBand(), and inserts into rbn.bronze
// using ch-go native protocol with LZ4 compression.
//
// Handles three RBN CSV format eras:
//   - 2009-02-21 to 2010-06-14: no header, 11 columns
//   - 2010-06-15 to 2010-12-31: header row, 11 columns
//   - 2011-01-01 to present:    header row, 13 columns (speed + tx_mode)
//
// Build: CGO_ENABLED=0 go build -ldflags="-s -w" -o build/rbn-ingest ./cmd/rbn-ingest

package main

import (
	"archive/zip"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/KI7MT/ki7mt-ai-lab-apps/internal/bands"
)

var Version = "2.2.0"

const (
	DefaultBatchSize = 100_000
	DefaultWorkers   = 8
)

// Stats tracks ingestion metrics with atomic operations.
type Stats struct {
	TotalRows     atomic.Uint64
	TotalFiles    atomic.Uint64
	SkippedRows   atomic.Uint64
	FailedFiles   atomic.Uint64
	StartTime     time.Time
}

// RBNBatch holds columnar data for a batch INSERT into rbn.bronze.
type RBNBatch struct {
	Timestamp *proto.ColDateTime
	DeCall    *proto.ColStr
	DePfx     *proto.ColStr
	DeCont    *proto.ColStr
	Frequency *proto.ColUInt32
	Band      *proto.ColInt32
	DxCall    *proto.ColStr
	DxPfx     *proto.ColStr
	DxCont    *proto.ColStr
	SpotType  *proto.ColLowCardinality[string]
	SNR       *proto.ColInt16
	Speed     *proto.ColUInt16
	TxMode    *proto.ColLowCardinality[string]
}

func NewRBNBatch() *RBNBatch {
	return &RBNBatch{
		Timestamp: new(proto.ColDateTime),
		DeCall:    new(proto.ColStr),
		DePfx:     new(proto.ColStr),
		DeCont:    new(proto.ColStr),
		Frequency: new(proto.ColUInt32),
		Band:      new(proto.ColInt32),
		DxCall:    new(proto.ColStr),
		DxPfx:     new(proto.ColStr),
		DxCont:    new(proto.ColStr),
		SpotType:  new(proto.ColStr).LowCardinality(),
		SNR:       new(proto.ColInt16),
		Speed:     new(proto.ColUInt16),
		TxMode:    new(proto.ColStr).LowCardinality(),
	}
}

func (b *RBNBatch) Reset() {
	b.Timestamp.Reset()
	b.DeCall.Reset()
	b.DePfx.Reset()
	b.DeCont.Reset()
	b.Frequency.Reset()
	b.Band.Reset()
	b.DxCall.Reset()
	b.DxPfx.Reset()
	b.DxCont.Reset()
	b.SpotType.Reset()
	b.SNR.Reset()
	b.Speed.Reset()
	b.TxMode.Reset()
}

func (b *RBNBatch) Len() int {
	return b.Timestamp.Rows()
}

func (b *RBNBatch) Input() proto.Input {
	return proto.Input{
		{Name: "timestamp", Data: b.Timestamp},
		{Name: "de_call", Data: b.DeCall},
		{Name: "de_pfx", Data: b.DePfx},
		{Name: "de_cont", Data: b.DeCont},
		{Name: "frequency", Data: b.Frequency},
		{Name: "band", Data: b.Band},
		{Name: "dx_call", Data: b.DxCall},
		{Name: "dx_pfx", Data: b.DxPfx},
		{Name: "dx_cont", Data: b.DxCont},
		{Name: "spot_type", Data: b.SpotType},
		{Name: "snr", Data: b.SNR},
		{Name: "speed", Data: b.Speed},
		{Name: "tx_mode", Data: b.TxMode},
	}
}

var batchPool = sync.Pool{
	New: func() interface{} { return NewRBNBatch() },
}

// parseTimestamp handles both "YYYY-MM-DD HH:MM:SS" and "YYYY-MM-DD HH:MM:SS+00".
func parseTimestamp(s string) (time.Time, error) {
	// Strip timezone suffix if present (early 2009 files)
	s = strings.TrimSuffix(s, "+00")
	return time.Parse("2006-01-02 15:04:05", s)
}

// parseRow parses a single CSV record into the batch.
// numFields distinguishes 11-column (2009-2010) from 13-column (2011+) format.
func parseRow(fields []string, numFields int, batch *RBNBatch) error {
	if len(fields) < 11 {
		return fmt.Errorf("too few fields: %d", len(fields))
	}

	// Column layout (both formats):
	//  0: callsign (de_call - skimmer)
	//  1: de_pfx
	//  2: de_cont
	//  3: freq (kHz, float)
	//  4: band (string label, we ignore — normalize from freq)
	//  5: dx (dx_call - spotted station)
	//  6: dx_pfx
	//  7: dx_cont
	//  8: mode (spot type: CQ, BEACON, DX, NCDXF B)
	//  9: db (SNR)
	// 10: date (timestamp)
	// 13-col only:
	// 11: speed (WPM/baud)
	// 12: tx_mode (CW, RTTY, PSK31)

	// Timestamp
	ts, err := parseTimestamp(fields[10])
	if err != nil {
		return fmt.Errorf("bad timestamp %q: %w", fields[10], err)
	}
	batch.Timestamp.Append(ts)

	// Skimmer (receiver)
	batch.DeCall.Append(fields[0])
	batch.DePfx.Append(fields[1])

	cont := fields[2]
	if len(cont) > 2 {
		cont = cont[:2]
	}
	batch.DeCont.Append(cont)

	// Frequency: kHz as float in CSV → UInt32 kHz (truncate decimal)
	freqKHz, err := strconv.ParseFloat(fields[3], 64)
	if err != nil {
		return fmt.Errorf("bad freq %q: %w", fields[3], err)
	}
	batch.Frequency.Append(uint32(freqKHz))

	// Band: normalize from frequency using shared bands package
	// RBN freq is kHz, GetBand expects MHz
	bandID, _ := bands.GetBand(freqKHz / 1000.0)
	batch.Band.Append(bandID)

	// Spotted station (DX)
	batch.DxCall.Append(fields[5])
	batch.DxPfx.Append(fields[6])

	dxCont := fields[7]
	if len(dxCont) > 2 {
		dxCont = dxCont[:2]
	}
	batch.DxCont.Append(dxCont)

	// Spot type (CSV "mode" column — CQ, BEACON, DX, NCDXF B)
	batch.SpotType.Append(fields[8])

	// SNR
	snr, err := strconv.ParseInt(fields[9], 10, 16)
	if err != nil {
		return fmt.Errorf("bad snr %q: %w", fields[9], err)
	}
	batch.SNR.Append(int16(snr))

	// Speed and tx_mode (13-column format only)
	if numFields >= 13 && len(fields) >= 13 {
		spd, _ := strconv.ParseUint(fields[11], 10, 16)
		batch.Speed.Append(uint16(spd))
		batch.TxMode.Append(fields[12])
	} else {
		batch.Speed.Append(0)
		batch.TxMode.Append("")
	}

	return nil
}

// flushBatch sends the accumulated batch to ClickHouse.
func flushBatch(ctx context.Context, conn *ch.Client, tableFQN string, batch *RBNBatch) error {
	query := fmt.Sprintf(
		"INSERT INTO %s (timestamp, de_call, de_pfx, de_cont, frequency, band, "+
			"dx_call, dx_pfx, dx_cont, spot_type, snr, speed, tx_mode) VALUES",
		tableFQN,
	)
	return conn.Do(ctx, ch.Query{
		Body:  query,
		Input: batch.Input(),
	})
}

// processZIP opens a ZIP, reads the CSV inside, parses rows, and inserts batches.
func processZIP(ctx context.Context, zipPath, host, db, table string, batchSize int, stats *Stats) {
	fileName := filepath.Base(zipPath)

	// Open ZIP
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		log.Printf("[%s] open error: %v", fileName, err)
		stats.FailedFiles.Add(1)
		return
	}
	defer zr.Close()

	if len(zr.File) == 0 {
		log.Printf("[%s] empty ZIP", fileName)
		stats.FailedFiles.Add(1)
		return
	}

	// Open the first (only) CSV file inside the ZIP
	csvFile := zr.File[0]
	rc, err := csvFile.Open()
	if err != nil {
		log.Printf("[%s] csv open error: %v", fileName, err)
		stats.FailedFiles.Add(1)
		return
	}
	defer rc.Close()

	// Connect to ClickHouse
	conn, err := ch.Dial(ctx, ch.Options{
		Address:     host,
		Database:    db,
		Compression: ch.CompressionLZ4,
	})
	if err != nil {
		log.Printf("[%s] ClickHouse connect error: %v", fileName, err)
		stats.FailedFiles.Add(1)
		return
	}
	defer conn.Close()

	tableFQN := fmt.Sprintf("%s.%s", db, table)

	// CSV reader
	csvReader := csv.NewReader(rc)
	csvReader.ReuseRecord = true
	csvReader.FieldsPerRecord = -1 // variable columns
	csvReader.LazyQuotes = true

	batch := batchPool.Get().(*RBNBatch)
	defer batchPool.Put(batch)
	batch.Reset()

	var rowCount uint64
	var skipCount uint64
	numFields := 0 // detected on first data row
	startTime := time.Now()

	for {
		if ctx.Err() != nil {
			return
		}

		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			skipCount++
			continue
		}

		// Skip header rows (start with "callsign") and empty-marker rows
		if len(record) > 0 && (strings.HasPrefix(record[0], "callsign") || strings.HasPrefix(record[0], "(")) {
			continue
		}

		// Detect format on first data row
		if numFields == 0 {
			numFields = len(record)
		}

		if err := parseRow(record, numFields, batch); err != nil {
			skipCount++
			continue
		}
		rowCount++

		if batch.Len() >= batchSize {
			if err := flushBatch(ctx, conn, tableFQN, batch); err != nil {
				log.Printf("[%s] flush error: %v", fileName, err)
			}
			batch.Reset()
		}
	}

	// Flush remainder
	if batch.Len() > 0 {
		if err := flushBatch(ctx, conn, tableFQN, batch); err != nil {
			log.Printf("[%s] final flush error: %v", fileName, err)
		}
		batch.Reset()
	}

	elapsed := time.Since(startTime)
	mrps := float64(rowCount) / elapsed.Seconds() / 1_000_000

	stats.TotalRows.Add(rowCount)
	stats.SkippedRows.Add(skipCount)
	stats.TotalFiles.Add(1)

	log.Printf("[%s] %d rows in %.1fs (%.2f Mrps), %d skipped",
		fileName, rowCount, elapsed.Seconds(), mrps, skipCount)
}

// discoverFiles walks srcDir for ZIP files, optionally filtered by year.
func discoverFiles(srcDir string, year int) ([]string, error) {
	var files []string

	if year > 0 {
		// Single year directory
		yearDir := filepath.Join(srcDir, strconv.Itoa(year))
		entries, err := os.ReadDir(yearDir)
		if err != nil {
			return nil, fmt.Errorf("cannot read %s: %w", yearDir, err)
		}
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".zip") {
				files = append(files, filepath.Join(yearDir, e.Name()))
			}
		}
	} else {
		// All year directories
		err := filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if !info.IsDir() && strings.HasSuffix(path, ".zip") {
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	sort.Strings(files)
	return files, nil
}

func main() {
	src := flag.String("src", "/mnt/rbn-data", "Source directory with {year}/*.zip")
	host := flag.String("host", "192.168.1.90:9000", "ClickHouse host:port")
	db := flag.String("db", "rbn", "ClickHouse database")
	table := flag.String("table", "bronze", "ClickHouse table")
	workers := flag.Int("workers", DefaultWorkers, "Parallel ZIP workers")
	batchSize := flag.Int("batch", DefaultBatchSize, "Rows per INSERT batch")
	year := flag.Int("year", 0, "Process only this year (0 = all)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "rbn-ingest v%s — Stream RBN ZIP archives into ClickHouse\n\n", Version)
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Reads daily ZIP files from --src/{year}/*.zip, parses CSV,\n")
		fmt.Fprintf(os.Stderr, "normalizes band via ADIF lookup, and inserts into ClickHouse\n")
		fmt.Fprintf(os.Stderr, "using ch-go native protocol with LZ4 compression.\n\n")
		fmt.Fprintf(os.Stderr, "Handles all three RBN CSV format eras:\n")
		fmt.Fprintf(os.Stderr, "  2009-2010: 11 columns (no speed/tx_mode)\n")
		fmt.Fprintf(os.Stderr, "  2011+:     13 columns (speed + tx_mode)\n\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  rbn-ingest --year 2024 --workers 4\n")
		fmt.Fprintf(os.Stderr, "  rbn-ingest --workers 8\n")
		fmt.Fprintf(os.Stderr, "  rbn-ingest --src /mnt/rbn-data --host 10.60.1.1:9000\n")
	}

	flag.Parse()

	log.Println("=========================================================")
	log.Printf("RBN Ingest v%s", Version)
	log.Println("=========================================================")
	log.Printf("Source:   %s", *src)
	log.Printf("Target:   %s.%s @ %s", *db, *table, *host)
	log.Printf("Workers:  %d | Batch: %d", *workers, *batchSize)
	log.Printf("CPUs:     %d", runtime.NumCPU())
	if *year > 0 {
		log.Printf("Year:     %d", *year)
	} else {
		log.Printf("Year:     all")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("\nShutdown requested...")
		cancel()
	}()

	// Test ClickHouse connection
	log.Printf("Connecting to ClickHouse at %s...", *host)
	testConn, err := ch.Dial(ctx, ch.Options{
		Address:  *host,
		Database: *db,
	})
	if err != nil {
		log.Fatalf("ClickHouse connection failed: %v", err)
	}
	testConn.Close()
	log.Println("Connection OK")

	// Discover ZIP files
	files, err := discoverFiles(*src, *year)
	if err != nil {
		log.Fatalf("File discovery failed: %v", err)
	}
	if len(files) == 0 {
		log.Fatal("No ZIP files found")
	}
	log.Printf("Found %d ZIP file(s)", len(files))
	log.Println("=========================================================")

	stats := &Stats{StartTime: time.Now()}

	// Worker pool with semaphore
	sem := make(chan struct{}, *workers)
	var wg sync.WaitGroup

	for _, zipPath := range files {
		if ctx.Err() != nil {
			break
		}

		sem <- struct{}{}
		wg.Add(1)

		go func(zp string) {
			defer func() { <-sem }()
			defer wg.Done()
			processZIP(ctx, zp, *host, *db, *table, *batchSize, stats)
		}(zipPath)
	}

	wg.Wait()

	elapsed := time.Since(stats.StartTime)
	totalRows := stats.TotalRows.Load()
	totalFiles := stats.TotalFiles.Load()
	skippedRows := stats.SkippedRows.Load()
	failedFiles := stats.FailedFiles.Load()
	mrps := float64(totalRows) / elapsed.Seconds() / 1_000_000

	log.Println()
	log.Println("=========================================================")
	log.Println("Final Statistics")
	log.Println("=========================================================")
	log.Printf("Files OK:     %d", totalFiles)
	log.Printf("Files Failed: %d", failedFiles)
	log.Printf("Total Rows:   %d", totalRows)
	log.Printf("Skipped Rows: %d", skippedRows)
	log.Printf("Elapsed:      %v", elapsed.Round(time.Second))
	log.Printf("Throughput:   %.2f Mrps", mrps)
	log.Println("=========================================================")
}

// Ensure net import is used (for ch-go).
var _ = net.Dial
