// pskr-ingest — Load PSK Reporter JSONL files into ClickHouse via native protocol
//
// Reads hourly-rotated gzip JSONL files from /mnt/pskr-data/YYYY/MM/DD/,
// parses spots written by pskr-collector, and inserts into pskr.bronze
// using ch-go native protocol with LZ4 compression.
//
// Uses a watermark table (pskr.ingest_log) to track which files have been
// loaded, so only new files are processed on each run. This enables safe
// cron-based incremental loading.
//
// Build: CGO_ENABLED=0 go build -ldflags="-s -w" -o build/pskr-ingest ./cmd/pskr-ingest

package main

import (
	"bufio"
	"context"
	"encoding/json"
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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/klauspost/compress/gzip"
)

var Version = "dev"

const (
	DefaultBatchSize = 100_000
	DefaultWorkers   = 4
	DefaultMinAge    = 5 * time.Minute
)

// Stats tracks ingestion metrics with atomic operations.
type Stats struct {
	TotalRows   atomic.Uint64
	TotalFiles  atomic.Uint64
	SkippedRows atomic.Uint64
	FailedFiles atomic.Uint64
	StartTime   time.Time
}

// PSKRSpot matches the JSONL output format from pskr-collector.
type PSKRSpot struct {
	Timestamp    string `json:"ts"`
	SenderCall   string `json:"sc"`
	SenderGrid   string `json:"sg"`
	ReceiverCall string `json:"rc"`
	ReceiverGrid string `json:"rg"`
	FreqHz       uint64 `json:"f"`
	Band         int32  `json:"band"`
	Mode         string `json:"mode"`
	SNR          int    `json:"snr"`
}

// PSKRBatch holds columnar data for a batch INSERT into pskr.bronze.
type PSKRBatch struct {
	Timestamp    *proto.ColDateTime
	SenderCall   *proto.ColStr
	SenderGrid   *proto.ColStr
	ReceiverCall *proto.ColStr
	ReceiverGrid *proto.ColStr
	Frequency    *proto.ColUInt64
	Band         *proto.ColInt32
	Mode         *proto.ColLowCardinality[string]
	SNR          *proto.ColInt16
}

func NewPSKRBatch() *PSKRBatch {
	return &PSKRBatch{
		Timestamp:    new(proto.ColDateTime),
		SenderCall:   new(proto.ColStr),
		SenderGrid:   new(proto.ColStr),
		ReceiverCall: new(proto.ColStr),
		ReceiverGrid: new(proto.ColStr),
		Frequency:    new(proto.ColUInt64),
		Band:         new(proto.ColInt32),
		Mode:         new(proto.ColStr).LowCardinality(),
		SNR:          new(proto.ColInt16),
	}
}

func (b *PSKRBatch) Reset() {
	b.Timestamp.Reset()
	b.SenderCall.Reset()
	b.SenderGrid.Reset()
	b.ReceiverCall.Reset()
	b.ReceiverGrid.Reset()
	b.Frequency.Reset()
	b.Band.Reset()
	b.Mode.Reset()
	b.SNR.Reset()
}

func (b *PSKRBatch) Len() int {
	return b.Timestamp.Rows()
}

func (b *PSKRBatch) Input() proto.Input {
	return proto.Input{
		{Name: "timestamp", Data: b.Timestamp},
		{Name: "sender_call", Data: b.SenderCall},
		{Name: "sender_grid", Data: b.SenderGrid},
		{Name: "receiver_call", Data: b.ReceiverCall},
		{Name: "receiver_grid", Data: b.ReceiverGrid},
		{Name: "frequency", Data: b.Frequency},
		{Name: "band", Data: b.Band},
		{Name: "mode", Data: b.Mode},
		{Name: "snr", Data: b.SNR},
	}
}

var batchPool = sync.Pool{
	New: func() interface{} { return NewPSKRBatch() },
}

// flushBatch sends the accumulated batch to ClickHouse.
func flushBatch(ctx context.Context, conn *ch.Client, tableFQN string, batch *PSKRBatch) error {
	query := fmt.Sprintf(
		"INSERT INTO %s (timestamp, sender_call, sender_grid, receiver_call, "+
			"receiver_grid, frequency, band, mode, snr) VALUES",
		tableFQN,
	)
	return conn.Do(ctx, ch.Query{
		Body:  query,
		Input: batch.Input(),
	})
}

// logColumns holds columnar data for an INSERT into pskr.ingest_log.
type logColumns struct {
	FilePath  *proto.ColStr
	FileSize  *proto.ColUInt64
	RowCount  *proto.ColUInt64
	ElapsedMs *proto.ColUInt32
	Hostname  *proto.ColLowCardinality[string]
}

func newLogColumns() *logColumns {
	return &logColumns{
		FilePath:  new(proto.ColStr),
		FileSize:  new(proto.ColUInt64),
		RowCount:  new(proto.ColUInt64),
		ElapsedMs: new(proto.ColUInt32),
		Hostname:  new(proto.ColStr).LowCardinality(),
	}
}

// insertLogEntry records a processed file in pskr.ingest_log.
func insertLogEntry(ctx context.Context, conn *ch.Client, relPath string, fileSize, rowCount uint64, elapsedMs uint32, hostname string) error {
	cols := newLogColumns()
	cols.FilePath.Append(relPath)
	cols.FileSize.Append(fileSize)
	cols.RowCount.Append(rowCount)
	cols.ElapsedMs.Append(elapsedMs)
	cols.Hostname.Append(hostname)

	return conn.Do(ctx, ch.Query{
		Body: "INSERT INTO pskr.ingest_log (file_path, file_size, row_count, elapsed_ms, hostname) VALUES",
		Input: proto.Input{
			{Name: "file_path", Data: cols.FilePath},
			{Name: "file_size", Data: cols.FileSize},
			{Name: "row_count", Data: cols.RowCount},
			{Name: "elapsed_ms", Data: cols.ElapsedMs},
			{Name: "hostname", Data: cols.Hostname},
		},
	})
}

// loadWatermark reads all previously loaded file paths from pskr.ingest_log.
func loadWatermark(ctx context.Context, host, db string) (map[string]bool, error) {
	conn, err := ch.Dial(ctx, ch.Options{
		Address:  host,
		Database: db,
	})
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	watermark := make(map[string]bool)
	col := new(proto.ColStr)

	err = conn.Do(ctx, ch.Query{
		Body: "SELECT file_path FROM pskr.ingest_log FINAL",
		Result: proto.Results{
			{Name: "file_path", Data: col},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < col.Rows(); i++ {
				watermark[col.Row(i)] = true
			}
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	return watermark, nil
}

// discoverFiles walks srcDir for *.jsonl.gz files, sorted by path (chronological).
func discoverFiles(srcDir string) ([]string, error) {
	var files []string
	err := filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() && strings.HasSuffix(path, ".jsonl.gz") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

// relPath returns the path relative to srcDir (for watermark keys).
func relPath(srcDir, fullPath string) string {
	rel, err := filepath.Rel(srcDir, fullPath)
	if err != nil {
		return fullPath
	}
	return rel
}

// processFile opens a gzip JSONL file, parses spots, and inserts batches.
func processFile(ctx context.Context, filePath, srcDir, host, db, table string, batchSize int, stats *Stats) {
	fileName := relPath(srcDir, filePath)
	startTime := time.Now()

	// Get file size
	fi, err := os.Stat(filePath)
	if err != nil {
		log.Printf("[%s] stat error: %v", fileName, err)
		stats.FailedFiles.Add(1)
		return
	}
	fileSize := uint64(fi.Size())

	// Open file
	f, err := os.Open(filePath)
	if err != nil {
		log.Printf("[%s] open error: %v", fileName, err)
		stats.FailedFiles.Add(1)
		return
	}
	defer f.Close()

	// Gzip reader (klauspost)
	gz, err := gzip.NewReader(f)
	if err != nil {
		log.Printf("[%s] gzip error: %v", fileName, err)
		stats.FailedFiles.Add(1)
		return
	}
	defer gz.Close()

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

	batch := batchPool.Get().(*PSKRBatch)
	defer batchPool.Put(batch)
	batch.Reset()

	scanner := bufio.NewScanner(gz)
	// Increase scanner buffer for potentially long lines
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var rowCount uint64
	var skipCount uint64

	for scanner.Scan() {
		if ctx.Err() != nil {
			return
		}

		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var spot PSKRSpot
		if err := json.Unmarshal(line, &spot); err != nil {
			skipCount++
			continue
		}

		// Parse timestamp (RFC3339: "2026-02-10T01:17:43Z")
		ts, err := time.Parse(time.RFC3339, spot.Timestamp)
		if err != nil {
			skipCount++
			continue
		}

		// Validate required fields
		if spot.SenderCall == "" || spot.ReceiverCall == "" || spot.FreqHz == 0 {
			skipCount++
			continue
		}

		batch.Timestamp.Append(ts)
		batch.SenderCall.Append(spot.SenderCall)
		batch.SenderGrid.Append(spot.SenderGrid)
		batch.ReceiverCall.Append(spot.ReceiverCall)
		batch.ReceiverGrid.Append(spot.ReceiverGrid)
		batch.Frequency.Append(spot.FreqHz)
		batch.Band.Append(spot.Band)
		batch.Mode.Append(spot.Mode)
		batch.SNR.Append(int16(spot.SNR))
		rowCount++

		if batch.Len() >= batchSize {
			if err := flushBatch(ctx, conn, tableFQN, batch); err != nil {
				log.Printf("[%s] flush error: %v", fileName, err)
				stats.FailedFiles.Add(1)
				return
			}
			batch.Reset()
		}
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		log.Printf("[%s] scanner error: %v", fileName, err)
	}

	// Flush remainder
	if batch.Len() > 0 {
		if err := flushBatch(ctx, conn, tableFQN, batch); err != nil {
			log.Printf("[%s] final flush error: %v", fileName, err)
			stats.FailedFiles.Add(1)
			return
		}
		batch.Reset()
	}

	elapsed := time.Since(startTime)
	elapsedMs := uint32(elapsed.Milliseconds())

	// Record in watermark
	hostname, _ := os.Hostname()
	if err := insertLogEntry(ctx, conn, fileName, fileSize, rowCount, elapsedMs, hostname); err != nil {
		log.Printf("[%s] watermark insert error: %v", fileName, err)
	}

	stats.TotalRows.Add(rowCount)
	stats.SkippedRows.Add(skipCount)
	stats.TotalFiles.Add(1)

	mrps := float64(rowCount) / elapsed.Seconds() / 1_000_000
	log.Printf("[%s] %d rows in %.1fs (%.2f Mrps), %d skipped",
		fileName, rowCount, elapsed.Seconds(), mrps, skipCount)
}

// primeWatermark marks all existing files in the watermark without loading data.
func primeWatermark(ctx context.Context, srcDir, host, db string, minAge time.Duration) error {
	files, err := discoverFiles(srcDir)
	if err != nil {
		return fmt.Errorf("discover: %w", err)
	}

	// Load existing watermark to avoid duplicates
	watermark, err := loadWatermark(ctx, host, db)
	if err != nil {
		return fmt.Errorf("load watermark: %w", err)
	}

	conn, err := ch.Dial(ctx, ch.Options{
		Address:  host,
		Database: db,
	})
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	hostname, _ := os.Hostname()
	now := time.Now()
	var primed int

	for _, fp := range files {
		rel := relPath(srcDir, fp)
		if watermark[rel] {
			continue
		}

		// Skip active files
		fi, err := os.Stat(fp)
		if err != nil {
			continue
		}
		if now.Sub(fi.ModTime()) < minAge {
			log.Printf("[prime] skipping active file: %s", rel)
			continue
		}

		if err := insertLogEntry(ctx, conn, rel, uint64(fi.Size()), 0, 0, hostname); err != nil {
			log.Printf("[prime] error for %s: %v", rel, err)
			continue
		}
		primed++
	}

	log.Printf("Primed %d file(s) in watermark (row_count=0)", primed)
	return nil
}

func main() {
	src := flag.String("src", "/mnt/pskr-data", "Source directory (YYYY/MM/DD/spots-*.jsonl.gz)")
	host := flag.String("host", "192.168.1.90:9000", "ClickHouse host:port")
	db := flag.String("db", "pskr", "ClickHouse database")
	table := flag.String("table", "bronze", "ClickHouse table")
	workers := flag.Int("workers", DefaultWorkers, "Parallel file workers")
	batchSize := flag.Int("batch", DefaultBatchSize, "Rows per INSERT batch")
	minAge := flag.Duration("min-age", DefaultMinAge, "Skip files modified within this duration")
	dryRun := flag.Bool("dry-run", false, "List new files without loading")
	prime := flag.Bool("prime", false, "Mark all existing files as loaded (bootstrap)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "pskr-ingest v%s — Load PSK Reporter JSONL files into ClickHouse\n\n", Version)
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Reads gzip JSONL files written by pskr-collector, parses spots,\n")
		fmt.Fprintf(os.Stderr, "and inserts into ClickHouse using ch-go native protocol with\n")
		fmt.Fprintf(os.Stderr, "LZ4 compression. Uses a watermark table (pskr.ingest_log) to\n")
		fmt.Fprintf(os.Stderr, "track loaded files for incremental processing.\n\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  pskr-ingest --prime                  # Bootstrap watermark\n")
		fmt.Fprintf(os.Stderr, "  pskr-ingest --dry-run                # Show new files\n")
		fmt.Fprintf(os.Stderr, "  pskr-ingest                          # Load new files\n")
		fmt.Fprintf(os.Stderr, "  pskr-ingest --host 10.60.1.1:9000    # Use DAC link\n")
		fmt.Fprintf(os.Stderr, "  pskr-ingest --workers 8 --batch 200000\n")
	}

	flag.Parse()

	log.Println("=========================================================")
	log.Printf("PSKR Ingest v%s", Version)
	log.Println("=========================================================")
	log.Printf("Source:   %s", *src)
	log.Printf("Target:   %s.%s @ %s", *db, *table, *host)
	log.Printf("Workers:  %d | Batch: %d | MinAge: %s", *workers, *batchSize, *minAge)
	log.Printf("CPUs:     %d", runtime.NumCPU())
	if *dryRun {
		log.Printf("Mode:     DRY-RUN (no data will be loaded)")
	} else if *prime {
		log.Printf("Mode:     PRIME (bootstrap watermark only)")
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

	// Prime mode: mark existing files and exit
	if *prime {
		log.Println("=========================================================")
		log.Println("Priming watermark...")
		if err := primeWatermark(ctx, *src, *host, *db, *minAge); err != nil {
			log.Fatalf("Prime failed: %v", err)
		}
		log.Println("=========================================================")
		return
	}

	// Load watermark
	watermark, err := loadWatermark(ctx, *host, *db)
	if err != nil {
		log.Fatalf("Load watermark failed: %v", err)
	}
	log.Printf("Watermark: %d file(s) already loaded", len(watermark))

	// Discover files
	allFiles, err := discoverFiles(*src)
	if err != nil {
		log.Fatalf("File discovery failed: %v", err)
	}
	log.Printf("Found %d total JSONL file(s) on disk", len(allFiles))

	// Filter: skip watermarked and active files
	now := time.Now()
	var newFiles []string
	for _, fp := range allFiles {
		rel := relPath(*src, fp)
		if watermark[rel] {
			continue
		}
		fi, err := os.Stat(fp)
		if err != nil {
			continue
		}
		if now.Sub(fi.ModTime()) < *minAge {
			log.Printf("Skipping active file: %s (age: %s)", rel, now.Sub(fi.ModTime()).Round(time.Second))
			continue
		}
		newFiles = append(newFiles, fp)
	}

	if len(newFiles) == 0 {
		log.Println("0 new files to process")
		log.Println("=========================================================")
		return
	}

	log.Printf("%d new file(s) to process", len(newFiles))
	log.Println("=========================================================")

	// Dry-run mode: list files and exit
	if *dryRun {
		for _, fp := range newFiles {
			rel := relPath(*src, fp)
			fi, _ := os.Stat(fp)
			fmt.Printf("  %s (%d bytes)\n", rel, fi.Size())
		}
		log.Printf("\nDry-run complete: %d file(s) would be loaded", len(newFiles))
		return
	}

	stats := &Stats{StartTime: time.Now()}

	// Worker pool with semaphore
	sem := make(chan struct{}, *workers)
	var wg sync.WaitGroup

	for _, fp := range newFiles {
		if ctx.Err() != nil {
			break
		}

		sem <- struct{}{}
		wg.Add(1)

		go func(filePath string) {
			defer func() { <-sem }()
			defer wg.Done()
			processFile(ctx, filePath, *src, *host, *db, *table, *batchSize, stats)
		}(fp)
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
