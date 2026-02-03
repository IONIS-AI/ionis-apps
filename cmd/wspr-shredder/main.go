// wspr-shredder - Maximum throughput WSPR ingester using ClickHouse Native protocol
//
// Optimizations:
//   - ch-go native protocol (fastest ClickHouse client)
//   - 1MB read buffers via bufio.NewReaderSize
//   - csv.Reader with ReuseRecord: true (zero-allocation parsing)
//   - Per-file worker pool to saturate PCIe 5.0 lanes
//   - Designed for uncompressed CSV files
//
// Build: CGO_ENABLED=0 go build -ldflags="-s -w" -o build/wspr-shredder ./cmd/wspr-shredder

package main

import (
	"bufio"
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

// Version can be overridden at build time via -ldflags
var Version = "2.1.0"

const (
	ReadBufferSize = 1024 * 1024 // 1MB read buffer
	BatchSize      = 100_000     // Rows per batch for native insert
	NumWorkers     = 16          // Workers = physical cores on 9950X3D
)

// Stats tracks ingestion metrics with atomic operations
type Stats struct {
	TotalRows     atomic.Uint64
	TotalBytes    atomic.Uint64
	FilesComplete atomic.Uint64
	StartTime     time.Time
}

func NewStats() *Stats {
	return &Stats{StartTime: time.Now()}
}

// SpotBatch holds a batch of parsed WSPR spots for native insert
type SpotBatch struct {
	// Column arrays for native protocol
	ID           *proto.ColUInt64
	Timestamp    *proto.ColDateTime
	Reporter     *proto.ColStr
	ReporterGrid *proto.ColStr
	SNR          *proto.ColInt8
	Frequency    *proto.ColUInt64
	Callsign     *proto.ColStr
	Grid         *proto.ColStr
	Power        *proto.ColInt8
	Drift        *proto.ColInt8
	Distance     *proto.ColUInt32
	Azimuth      *proto.ColUInt16
	Band         *proto.ColInt32
	Mode         *proto.ColStr
	Version      *proto.ColStr
	Code         *proto.ColUInt8
	ColumnCount  *proto.ColUInt8
}

func NewSpotBatch() *SpotBatch {
	return &SpotBatch{
		ID:           new(proto.ColUInt64),
		Timestamp:    new(proto.ColDateTime),
		Reporter:     new(proto.ColStr),
		ReporterGrid: new(proto.ColStr),
		SNR:          new(proto.ColInt8),
		Frequency:    new(proto.ColUInt64),
		Callsign:     new(proto.ColStr),
		Grid:         new(proto.ColStr),
		Power:        new(proto.ColInt8),
		Drift:        new(proto.ColInt8),
		Distance:     new(proto.ColUInt32),
		Azimuth:      new(proto.ColUInt16),
		Band:         new(proto.ColInt32),
		Mode:         new(proto.ColStr),
		Version:      new(proto.ColStr),
		Code:         new(proto.ColUInt8),
		ColumnCount:  new(proto.ColUInt8),
	}
}

func (b *SpotBatch) Reset() {
	b.ID.Reset()
	b.Timestamp.Reset()
	b.Reporter.Reset()
	b.ReporterGrid.Reset()
	b.SNR.Reset()
	b.Frequency.Reset()
	b.Callsign.Reset()
	b.Grid.Reset()
	b.Power.Reset()
	b.Drift.Reset()
	b.Distance.Reset()
	b.Azimuth.Reset()
	b.Band.Reset()
	b.Mode.Reset()
	b.Version.Reset()
	b.Code.Reset()
	b.ColumnCount.Reset()
}

func (b *SpotBatch) Len() int {
	return b.ID.Rows()
}

func (b *SpotBatch) Input() proto.Input {
	return proto.Input{
		{Name: "id", Data: b.ID},
		{Name: "timestamp", Data: b.Timestamp},
		{Name: "reporter", Data: b.Reporter},
		{Name: "reporter_grid", Data: b.ReporterGrid},
		{Name: "snr", Data: b.SNR},
		{Name: "frequency", Data: b.Frequency},
		{Name: "callsign", Data: b.Callsign},
		{Name: "grid", Data: b.Grid},
		{Name: "power", Data: b.Power},
		{Name: "drift", Data: b.Drift},
		{Name: "distance", Data: b.Distance},
		{Name: "azimuth", Data: b.Azimuth},
		{Name: "band", Data: b.Band},
		{Name: "mode", Data: b.Mode},
		{Name: "version", Data: b.Version},
		{Name: "code", Data: b.Code},
		{Name: "column_count", Data: b.ColumnCount},
	}
}

var batchPool = sync.Pool{
	New: func() interface{} {
		return NewSpotBatch()
	},
}

// sanitizeString removes quotes and brackets
func sanitizeString(s string) string {
	return strings.Trim(s, "\"'`<>[]{}()")
}

func truncateString(s string, maxLen int) string {
	if len(s) > maxLen {
		return s[:maxLen]
	}
	return s
}

// parseRow parses a CSV record into the batch columns
func parseRow(record []string, batch *SpotBatch) error {
	if len(record) < 15 {
		return fmt.Errorf("insufficient columns: %d", len(record))
	}

	// ID
	id, err := strconv.ParseUint(record[0], 10, 64)
	if err != nil {
		return err
	}
	batch.ID.Append(id)

	// Timestamp (Unix seconds -> DateTime)
	ts, err := strconv.ParseInt(record[1], 10, 64)
	if err != nil {
		return err
	}
	batch.Timestamp.Append(time.Unix(ts, 0).UTC())

	// Reporter (FixedString(16))
	batch.Reporter.Append(truncateString(sanitizeString(record[2]), 16))

	// Reporter Grid (FixedString(8))
	batch.ReporterGrid.Append(truncateString(strings.ToUpper(record[3]), 8))

	// SNR
	snr, _ := strconv.ParseInt(record[4], 10, 8)
	batch.SNR.Append(int8(snr))

	// Frequency (MHz -> Hz)
	freq, _ := strconv.ParseFloat(record[5], 64)
	batch.Frequency.Append(uint64(freq * 1_000_000))

	// Callsign (FixedString(16))
	batch.Callsign.Append(truncateString(sanitizeString(record[6]), 16))

	// Grid (FixedString(8))
	batch.Grid.Append(truncateString(strings.ToUpper(record[7]), 8))

	// Power
	power, _ := strconv.ParseInt(record[8], 10, 8)
	batch.Power.Append(int8(power))

	// Drift
	drift, _ := strconv.ParseInt(record[9], 10, 8)
	batch.Drift.Append(int8(drift))

	// Distance
	dist, _ := strconv.ParseUint(record[10], 10, 32)
	batch.Distance.Append(uint32(dist))

	// Azimuth
	az, _ := strconv.ParseUint(record[11], 10, 16)
	batch.Azimuth.Append(uint16(az))

	// Band — normalize from frequency (ignore CSV band column)
	normalizedBand, _ := bands.GetBand(freq)
	batch.Band.Append(normalizedBand)

	// Mode
	batch.Mode.Append("WSPR")

	// Version (truncate to 8 chars for FixedString(8))
	version := ""
	if len(record) > 13 {
		version = record[13]
		if len(version) > 8 {
			version = version[:8]
		}
	}
	batch.Version.Append(version)

	// Code
	code := uint8(0)
	if len(record) > 14 {
		c, _ := strconv.ParseUint(record[14], 10, 8)
		code = uint8(c)
	}
	batch.Code.Append(code)

	// Column count
	batch.ColumnCount.Append(uint8(len(record)))

	return nil
}

// Worker processes a single file
func processFile(ctx context.Context, filePath string, chHost, chDB, chTable string, stats *Stats, wg *sync.WaitGroup) {
	defer wg.Done()

	fileName := filepath.Base(filePath)

	// Open file with 1MB buffer
	f, err := os.Open(filePath)
	if err != nil {
		log.Printf("[%s] Open error: %v", fileName, err)
		return
	}
	defer f.Close()

	// Get file size for stats
	info, _ := f.Stat()
	fileSize := uint64(info.Size())

	reader := bufio.NewReaderSize(f, ReadBufferSize)

	// Create CSV reader with ReuseRecord for zero-allocation
	csvReader := csv.NewReader(reader)
	csvReader.ReuseRecord = true
	csvReader.FieldsPerRecord = -1 // Variable fields

	// Connect to ClickHouse using native protocol
	conn, err := ch.Dial(ctx, ch.Options{
		Address:     chHost,
		Database:    chDB,
		Compression: ch.CompressionLZ4,
	})
	if err != nil {
		log.Printf("[%s] ClickHouse connect error: %v", fileName, err)
		return
	}
	defer conn.Close()

	// Truncate partition for this file
	if err := truncatePartition(ctx, conn, chTable, filePath); err != nil {
		log.Printf("[%s] Truncate warning: %v", fileName, err)
	}

	tableFQN := fmt.Sprintf("%s.%s", chDB, chTable)
	batch := batchPool.Get().(*SpotBatch)
	defer batchPool.Put(batch)

	rowCount := 0
	flushCount := 0
	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		if err := parseRow(record, batch); err != nil {
			continue
		}

		rowCount++

		if batch.Len() >= BatchSize {
			if err := flushBatch(ctx, conn, tableFQN, batch); err != nil {
				log.Printf("[%s] Flush error: %v", fileName, err)
			}
			flushCount += batch.Len()
			batch.Reset()
		}
	}

	// Flush remaining
	if batch.Len() > 0 {
		if err := flushBatch(ctx, conn, tableFQN, batch); err != nil {
			log.Printf("[%s] Final flush error: %v", fileName, err)
		}
		flushCount += batch.Len()
		batch.Reset()
	}

	elapsed := time.Since(startTime)
	mrps := float64(rowCount) / elapsed.Seconds() / 1_000_000

	stats.TotalRows.Add(uint64(rowCount))
	stats.TotalBytes.Add(fileSize)
	stats.FilesComplete.Add(1)

	log.Printf("[%s] %d rows in %.1fs (%.2f Mrps)", fileName, rowCount, elapsed.Seconds(), mrps)
}

func flushBatch(ctx context.Context, conn *ch.Client, tableFQN string, batch *SpotBatch) error {
	query := fmt.Sprintf("INSERT INTO %s (id, timestamp, reporter, reporter_grid, snr, frequency, callsign, grid, power, drift, distance, azimuth, band, mode, version, code, column_count) VALUES", tableFQN)
	return conn.Do(ctx, ch.Query{
		Body:  query,
		Input: batch.Input(),
	})
}

func truncatePartition(ctx context.Context, conn *ch.Client, table, filePath string) error {
	fileName := filepath.Base(filePath)

	if !strings.HasPrefix(fileName, "wsprspots-") || len(fileName) < 17 {
		return nil
	}

	yearMonth := fileName[10:17]
	parts := strings.Split(yearMonth, "-")
	if len(parts) != 2 {
		return nil
	}

	partition := parts[0] + parts[1]
	query := fmt.Sprintf("ALTER TABLE %s DROP PARTITION '%s'", table, partition)

	if err := conn.Do(ctx, ch.Query{Body: query}); err != nil {
		if !strings.Contains(err.Error(), "not found") && !strings.Contains(err.Error(), "NO_SUCH_DATA_PART") {
			return err
		}
	}

	return nil
}

func main() {
	chHost := flag.String("ch-host", "127.0.0.1:9000", "ClickHouse address")
	chDB := flag.String("ch-db", "wspr", "ClickHouse database")
	chTable := flag.String("ch-table", "spots", "ClickHouse table")
	workers := flag.Int("workers", NumWorkers, "Number of parallel file workers")
	sourceDir := flag.String("source-dir", "/scratch/ai-stack/wspr-data/csv", "Default CSV source directory")
	reportDir := flag.String("report-dir", "/mnt/ai-stack/wspr-data/reports-shredder", "Report output directory")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "wspr-shredder v%s - Maximum Throughput WSPR Ingester\n\n", Version)
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] [path|files...]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "If no paths specified, uses -source-dir default.\n\n")
		fmt.Fprintf(os.Stderr, "Optimizations:\n")
		fmt.Fprintf(os.Stderr, "  - ch-go native protocol (fastest ClickHouse client)\n")
		fmt.Fprintf(os.Stderr, "  - 1MB read buffers (bufio.NewReaderSize)\n")
		fmt.Fprintf(os.Stderr, "  - csv.Reader with ReuseRecord (zero-allocation)\n")
		fmt.Fprintf(os.Stderr, "  - Per-file workers to saturate PCIe 5.0 lanes\n\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	// Use default source directory if no paths provided
	var inputPaths []string
	if len(flag.Args()) < 1 {
		inputPaths = []string{*sourceDir}
	} else {
		inputPaths = flag.Args()
	}

	// Create report directory
	if err := os.MkdirAll(*reportDir, 0755); err != nil {
		log.Fatalf("Cannot create report directory: %v", err)
	}

	log.Println("=========================================================")
	log.Printf("WSPR Shredder v%s", Version)
	log.Println("=========================================================")
	log.Printf("Input: %d path(s)", len(inputPaths))
	log.Printf("Workers: %d | Buffer: %dMB | Batch: %d", *workers, ReadBufferSize/1024/1024, BatchSize)
	log.Printf("Protocol: ch-go Native with LZ4")
	log.Printf("CPUs: %d", runtime.NumCPU())

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
	log.Printf("Connecting to ClickHouse at %s...", *chHost)
	testConn, err := ch.Dial(ctx, ch.Options{
		Address:  *chHost,
		Database: *chDB,
	})
	if err != nil {
		log.Fatalf("ClickHouse connection failed: %v", err)
	}
	testConn.Close()
	log.Printf("Table: %s.%s", *chDB, *chTable)

	// Discover files
	var files []string
	for _, inputPath := range inputPaths {
		info, err := os.Stat(inputPath)
		if err != nil {
			log.Printf("Warning: cannot access %s: %v", inputPath, err)
			continue
		}

		if info.IsDir() {
			filepath.Walk(inputPath, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return nil
				}
				if !info.IsDir() && strings.HasSuffix(path, ".csv") {
					files = append(files, path)
				}
				return nil
			})
		} else if strings.HasSuffix(inputPath, ".csv") {
			files = append(files, inputPath)
		}
	}

	if len(files) == 0 {
		log.Fatal("No CSV files found")
	}

	sort.Strings(files)
	log.Printf("Found %d CSV file(s)", len(files))

	stats := NewStats()

	// Create worker pool with semaphore
	sem := make(chan struct{}, *workers)
	var wg sync.WaitGroup

	for _, filePath := range files {
		select {
		case <-ctx.Done():
			break
		default:
		}

		sem <- struct{}{} // Acquire
		wg.Add(1)

		go func(fp string) {
			defer func() { <-sem }() // Release
			processFile(ctx, fp, *chHost, *chDB, *chTable, stats, &wg)
		}(filePath)
	}

	wg.Wait()

	elapsed := time.Since(stats.StartTime)
	totalRows := stats.TotalRows.Load()
	totalBytes := stats.TotalBytes.Load()
	mrps := float64(totalRows) / elapsed.Seconds() / 1_000_000
	mbps := float64(totalBytes) / elapsed.Seconds() / 1024 / 1024

	log.Println()
	log.Println("=========================================================")
	log.Println("Final Statistics")
	log.Println("=========================================================")
	log.Printf("Total Rows:   %d", totalRows)
	log.Printf("Total Size:   %.2f GB", float64(totalBytes)/1024/1024/1024)
	log.Printf("Elapsed:      %v", elapsed.Round(time.Second))
	log.Printf("Throughput:   %.2f Mrps", mrps)
	log.Printf("I/O Rate:     %.2f MB/s", mbps)
	log.Println("=========================================================")

	// Write report
	reportFile := filepath.Join(*reportDir, fmt.Sprintf("shredder_%s.log", time.Now().Format("20060102_150405")))
	if f, err := os.Create(reportFile); err == nil {
		fmt.Fprintf(f, "WSPR Shredder v%s Report\n", Version)
		fmt.Fprintf(f, "========================\n")
		fmt.Fprintf(f, "Files:      %d\n", stats.FilesComplete.Load())
		fmt.Fprintf(f, "Total Rows: %d\n", totalRows)
		fmt.Fprintf(f, "Total Size: %.2f GB\n", float64(totalBytes)/1024/1024/1024)
		fmt.Fprintf(f, "Elapsed:    %v\n", elapsed.Round(time.Second))
		fmt.Fprintf(f, "Throughput: %.2f Mrps\n", mrps)
		fmt.Fprintf(f, "I/O Rate:   %.2f MB/s\n", mbps)
		f.Close()
		log.Printf("Report: %s", reportFile)
	}
}

// Ensure net import is used (for ch-go)
var _ = net.Dial
