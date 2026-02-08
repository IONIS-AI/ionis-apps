// wspr-parquet-ingest - Maximum throughput Parquet to ClickHouse ingester
//
// Uses ClickHouse's native file() function for direct Parquet reading
// No Go-side parsing - ClickHouse handles everything natively
//
// Build: CGO_ENABLED=0 go build -ldflags="-s -w" -o build/wspr-parquet-ingest ./cmd/wspr-parquet-ingest

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
)

const (
	Version    = "1.0.0"
	NumWorkers = 16
)

type Stats struct {
	TotalRows     atomic.Uint64
	TotalBytes    atomic.Uint64
	FilesComplete atomic.Uint64
	StartTime     time.Time
}

func NewStats() *Stats {
	return &Stats{StartTime: time.Now()}
}

func truncatePartition(ctx context.Context, conn *ch.Client, table, filePath string) error {
	fileName := filepath.Base(filePath)

	// Extract YYYYMM from wsprspots-YYYY-MM.parquet
	if !strings.HasPrefix(fileName, "wsprspots-") || len(fileName) < 21 {
		return nil
	}

	yearMonth := fileName[10:17] // YYYY-MM
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

func processFile(ctx context.Context, filePath string, chHost, chDB, chTable string, stats *Stats, wg *sync.WaitGroup) {
	defer wg.Done()

	fileName := filepath.Base(filePath)

	// Get file size
	info, err := os.Stat(filePath)
	if err != nil {
		log.Printf("[%s] Stat error: %v", fileName, err)
		return
	}
	fileSize := uint64(info.Size())

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

	// Use ClickHouse's native file() function for Parquet reading
	// This is the fastest path - no Go parsing, ClickHouse reads Parquet directly
	query := fmt.Sprintf(`
		INSERT INTO %s
		SELECT
			id,
			toDateTime(timestamp) as timestamp,
			reporter,
			reporter_grid,
			snr,
			frequency,
			callsign,
			grid,
			power,
			drift,
			distance,
			azimuth,
			band,
			'WSPR' as mode,
			version,
			code,
			15 as column_count
		FROM file('%s', Parquet)
	`, tableFQN, filePath)

	var rowCount uint64
	if err := conn.Do(ctx, ch.Query{
		Body: query,
		OnProgress: func(ctx context.Context, p proto.Progress) error {
			rowCount = p.Rows
			return nil
		},
	}); err != nil {
		log.Printf("[%s] Insert error: %v", fileName, err)
		return
	}

	// Get actual row count if progress didn't report
	if rowCount == 0 {
		var result proto.ColUInt64
		if err := conn.Do(ctx, ch.Query{
			Body: fmt.Sprintf("SELECT count() FROM file('%s', Parquet)", filePath),
			Result: proto.Results{
				{Name: "count()", Data: &result},
			},
		}); err == nil && result.Rows() > 0 {
			rowCount = result.Row(0)
		}
	}

	elapsed := time.Since(startTime)
	mrps := float64(rowCount) / elapsed.Seconds() / 1_000_000

	stats.TotalRows.Add(rowCount)
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
	reportDir := flag.String("report-dir", "/mnt/ai-stack/wspr-data/reports-parquet", "Report output directory")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "wspr-parquet-ingest v%s - Maximum Throughput Parquet Ingester\n\n", Version)
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] [path|files...]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "If no paths specified, uses -source-dir default.\n\n")
		fmt.Fprintf(os.Stderr, "Features:\n")
		fmt.Fprintf(os.Stderr, "  - ClickHouse native file() function (zero-copy Parquet read)\n")
		fmt.Fprintf(os.Stderr, "  - ch-go native protocol with LZ4\n")
		fmt.Fprintf(os.Stderr, "  - Parallel file processing\n\n")
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
	log.Printf("WSPR Parquet Ingester v%s", Version)
	log.Println("=========================================================")
	log.Printf("Input: %d path(s)", len(inputPaths))
	log.Printf("Workers: %d", *workers)
	log.Printf("Protocol: ch-go Native + ClickHouse file() function")
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

	// Discover Parquet files
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
	log.Printf("Total Size:   %.2f GB (Parquet)", float64(totalBytes)/1024/1024/1024)
	log.Printf("Elapsed:      %v", elapsed.Round(time.Second))
	log.Printf("Throughput:   %.2f Mrps", mrps)
	log.Printf("I/O Rate:     %.2f MB/s", mbps)
	log.Println("=========================================================")

	// Write report
	reportFile := filepath.Join(*reportDir, fmt.Sprintf("parquet_%s.log", time.Now().Format("20060102_150405")))
	if f, err := os.Create(reportFile); err == nil {
		fmt.Fprintf(f, "WSPR Parquet Ingester v%s Report\n", Version)
		fmt.Fprintf(f, "================================\n")
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
