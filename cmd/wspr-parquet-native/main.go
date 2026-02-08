// wspr-parquet-native - Native Go Parquet reader with ch-go insert
//
// Reads Parquet files directly in Go, inserts via ch-go native protocol
// No ClickHouse file() function - bypasses all path restrictions
//
// Build: CGO_ENABLED=0 go build -ldflags="-s -w" -o build/wspr-parquet-native ./cmd/wspr-parquet-native

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/parquet-go/parquet-go"
)

// Version can be overridden at build time via -ldflags
var Version = "dev"

const (
	NumWorkers = 8
	BatchSize  = 100_000
)

// WsprSpot matches the Parquet schema
type WsprSpot struct {
	ID           uint64  `parquet:"id"`
	Timestamp    int64   `parquet:"timestamp"`
	Reporter     string  `parquet:"reporter"`
	ReporterGrid string  `parquet:"reporter_grid"`
	SNR          int16   `parquet:"snr"`
	Frequency    float64 `parquet:"frequency"`
	Callsign     string  `parquet:"callsign"`
	Grid         string  `parquet:"grid"`
	Power        int16   `parquet:"power"`
	Drift        int16   `parquet:"drift"`
	Distance     uint32  `parquet:"distance"`
	Azimuth      uint16  `parquet:"azimuth"`
	Band         int32   `parquet:"band"`
	Version      string  `parquet:"version"`
	Code         uint8   `parquet:"code"`
}

type Stats struct {
	TotalRows     atomic.Uint64
	TotalBytes    atomic.Uint64
	FilesComplete atomic.Uint64
	StartTime     time.Time
}

func NewStats() *Stats {
	return &Stats{StartTime: time.Now()}
}

// Batch holds column data for native insert
type Batch struct {
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

func NewBatch() *Batch {
	return &Batch{
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

func (b *Batch) Reset() {
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

func (b *Batch) Len() int {
	return b.ID.Rows()
}

func (b *Batch) Input() proto.Input {
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

func truncate(s string, maxLen int) string {
	if len(s) > maxLen {
		return s[:maxLen]
	}
	return s
}

func (b *Batch) AddSpot(spot WsprSpot) {
	b.ID.Append(spot.ID)
	b.Timestamp.Append(time.Unix(spot.Timestamp, 0).UTC())
	b.Reporter.Append(truncate(spot.Reporter, 16))
	b.ReporterGrid.Append(truncate(spot.ReporterGrid, 8))
	b.SNR.Append(int8(spot.SNR))
	b.Frequency.Append(uint64(spot.Frequency))
	b.Callsign.Append(truncate(spot.Callsign, 16))
	b.Grid.Append(truncate(spot.Grid, 8))
	b.Power.Append(int8(spot.Power))
	b.Drift.Append(int8(spot.Drift))
	b.Distance.Append(spot.Distance)
	b.Azimuth.Append(spot.Azimuth)
	b.Band.Append(spot.Band)
	b.Mode.Append("WSPR")
	b.Version.Append(truncate(spot.Version, 8))
	b.Code.Append(spot.Code)
	b.ColumnCount.Append(15)
}

func truncatePartition(ctx context.Context, conn *ch.Client, table, filePath string) error {
	fileName := filepath.Base(filePath)

	if !strings.HasPrefix(fileName, "wsprspots-") || len(fileName) < 21 {
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

func flushBatch(ctx context.Context, conn *ch.Client, tableFQN string, batch *Batch) error {
	if batch.Len() == 0 {
		return nil
	}

	query := fmt.Sprintf("INSERT INTO %s (id, timestamp, reporter, reporter_grid, snr, frequency, callsign, grid, power, drift, distance, azimuth, band, mode, version, code, column_count) VALUES", tableFQN)
	return conn.Do(ctx, ch.Query{
		Body:  query,
		Input: batch.Input(),
	})
}

func processFile(ctx context.Context, filePath string, chHost, chDB, chTable string, stats *Stats, wg *sync.WaitGroup) {
	defer wg.Done()

	fileName := filepath.Base(filePath)

	// Open file for reading
	f, err := os.Open(filePath)
	if err != nil {
		log.Printf("[%s] Open error: %v", fileName, err)
		return
	}
	defer f.Close()

	// Get file size
	info, err := f.Stat()
	if err != nil {
		log.Printf("[%s] Stat error: %v", fileName, err)
		return
	}
	fileSize := uint64(info.Size())

	// Open as Parquet file
	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		log.Printf("[%s] Parquet open error: %v", fileName, err)
		return
	}

	// Connect to ClickHouse
	conn, err := ch.Dial(ctx, ch.Options{
		Address:     chHost,
		Database:    chDB,
		Compression: ch.CompressionLZ4,
	})
	if err != nil {
		log.Printf("[%s] Connect error: %v", fileName, err)
		return
	}
	defer conn.Close()

	tableFQN := fmt.Sprintf("%s.%s", chDB, chTable)

	// Truncate partition
	if err := truncatePartition(ctx, conn, tableFQN, filePath); err != nil {
		log.Printf("[%s] Truncate warning: %v", fileName, err)
	}

	startTime := time.Now()
	batch := NewBatch()
	rowCount := 0

	// Read Parquet rows
	reader := parquet.NewGenericReader[WsprSpot](pf)
	spots := make([]WsprSpot, 1000)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		n, err := reader.Read(spots)
		if n == 0 {
			break
		}

		for i := 0; i < n; i++ {
			batch.AddSpot(spots[i])
			rowCount++

			if batch.Len() >= BatchSize {
				if err := flushBatch(ctx, conn, tableFQN, batch); err != nil {
					log.Printf("[%s] Flush error: %v", fileName, err)
				}
				batch.Reset()
			}
		}

		if err != nil {
			break
		}
	}

	// Flush remaining
	if batch.Len() > 0 {
		if err := flushBatch(ctx, conn, tableFQN, batch); err != nil {
			log.Printf("[%s] Final flush error: %v", fileName, err)
		}
	}

	elapsed := time.Since(startTime)
	mrps := float64(rowCount) / elapsed.Seconds() / 1_000_000

	stats.TotalRows.Add(uint64(rowCount))
	stats.TotalBytes.Add(fileSize)
	stats.FilesComplete.Add(1)

	log.Printf("[%s] %d rows in %.1fs (%.2f Mrps)", fileName, rowCount, elapsed.Seconds(), mrps)
}

func main() {
	chHost := flag.String("ch-host", "127.0.0.1:9000", "ClickHouse address")
	chDB := flag.String("ch-db", "wspr", "ClickHouse database")
	chTable := flag.String("ch-table", "bronze", "ClickHouse table")
	workers := flag.Int("workers", NumWorkers, "Number of parallel file workers")
	sourceDir := flag.String("source-dir", "/scratch/ai-stack/wspr-data/parquet", "Default Parquet source directory")
	reportDir := flag.String("report-dir", "/mnt/ai-stack/wspr-data/reports-parquet-native", "Report output directory")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "wspr-parquet-native v%s - Native Go Parquet Ingester\n\n", Version)
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] [path|files...]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "If no paths specified, uses -source-dir default.\n\n")
		fmt.Fprintf(os.Stderr, "Features:\n")
		fmt.Fprintf(os.Stderr, "  - Native Go Parquet reading (parquet-go)\n")
		fmt.Fprintf(os.Stderr, "  - ch-go native protocol with LZ4\n")
		fmt.Fprintf(os.Stderr, "  - No ClickHouse file() restrictions\n")
		fmt.Fprintf(os.Stderr, "  - Parallel file processing\n\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	var inputPaths []string
	if len(flag.Args()) < 1 {
		inputPaths = []string{*sourceDir}
	} else {
		inputPaths = flag.Args()
	}

	if err := os.MkdirAll(*reportDir, 0755); err != nil {
		log.Fatalf("Cannot create report directory: %v", err)
	}

	log.Println("=========================================================")
	log.Printf("WSPR Parquet Native Ingester v%s", Version)
	log.Println("=========================================================")
	log.Printf("Input: %d path(s)", len(inputPaths))
	log.Printf("Workers: %d | Batch: %d", *workers, BatchSize)
	log.Printf("Protocol: parquet-go + ch-go Native with LZ4")
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
				if !info.IsDir() && strings.HasSuffix(path, ".parquet") {
					files = append(files, path)
				}
				return nil
			})
		} else if strings.HasSuffix(inputPath, ".parquet") {
			files = append(files, inputPath)
		}
	}

	if len(files) == 0 {
		log.Fatal("No Parquet files found")
	}

	sort.Strings(files)
	log.Printf("Found %d Parquet file(s)", len(files))

	stats := NewStats()

	sem := make(chan struct{}, *workers)
	var wg sync.WaitGroup

	for _, filePath := range files {
		select {
		case <-ctx.Done():
			break
		default:
		}

		sem <- struct{}{}
		wg.Add(1)

		go func(fp string) {
			defer func() { <-sem }()
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
	log.Printf("Total Size:   %.2f GB (Parquet)", float64(totalBytes)/1024/1024/1024)
	log.Printf("Elapsed:      %v", elapsed.Round(time.Second))
	log.Printf("Throughput:   %.2f Mrps", mrps)
	log.Printf("I/O Rate:     %.2f MB/s", mbps)
	log.Println("=========================================================")

	reportFile := filepath.Join(*reportDir, fmt.Sprintf("parquet_native_%s.log", time.Now().Format("20060102_150405")))
	if f, err := os.Create(reportFile); err == nil {
		fmt.Fprintf(f, "WSPR Parquet Native Ingester v%s Report\n", Version)
		fmt.Fprintf(f, "=======================================\n")
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
