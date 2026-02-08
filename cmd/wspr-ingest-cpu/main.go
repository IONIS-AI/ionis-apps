// Package main provides a CPU-only WSPR CSV ingester.
// Pure Go implementation - no CUDA/CGO dependencies.
//
// Architecture:
//   io.Reader -> csv.Reader -> Worker Pool (16) -> ClickHouse Batch Insert
//
// Optimizations:
//   - sync.Pool for struct reuse (reduces GC pressure)
//   - Buffered channels for pipeline flow control
//   - Async ClickHouse inserts with LZ4 compression
package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/KI7MT/ki7mt-ai-lab-apps/internal/bands"
)

// Version is set at build time
var Version = "cpu-1.1.0"

// =============================================================================
// Configuration
// =============================================================================

const (
	NumWorkers          = 16        // Match 9950X3D physical cores
	BatchSize           = 1_000_000 // 1M rows per batch
	ClickHouseBatchSize = 5_000_000 // 5M rows per ClickHouse flush
	ChannelBuffer       = 32        // Buffered channel size
)

// Command-line flags
var (
	chHost    = flag.String("ch-host", "127.0.0.1:9000", "ClickHouse address")
	chDB      = flag.String("ch-db", "wspr", "ClickHouse database")
	chTable   = flag.String("ch-table", "bronze", "ClickHouse table")
	limit     = flag.Int("limit", 0, "Row limit (0 = unlimited)")
	silent    = flag.Bool("silent", false, "Suppress progress output")
	batchSize = flag.Int("batch-size", BatchSize, "Rows per batch")
)

// =============================================================================
// Spot - Parsed WSPR record (aligned for efficiency)
// =============================================================================

type Spot struct {
	ID           uint64
	Timestamp    uint32
	Reporter     [16]byte
	ReporterGrid [8]byte
	SNR          int8
	Frequency    uint64
	Callsign     [16]byte
	Grid         [8]byte
	Power        int8
	Drift        int8
	Distance     uint32
	Azimuth      uint16
	Band         int32
	Mode         [8]byte
	Version      [8]byte
	Code         uint8
	ColumnCount  uint8
}

// SpotBatch holds a batch of spots
type SpotBatch struct {
	Spots []Spot
	Count int
}

// =============================================================================
// sync.Pool for Spot and Batch reuse
// =============================================================================

var spotPool = sync.Pool{
	New: func() interface{} {
		return &Spot{}
	},
}

var batchPool = sync.Pool{
	New: func() interface{} {
		return &SpotBatch{
			Spots: make([]Spot, BatchSize),
			Count: 0,
		}
	},
}

func getSpot() *Spot {
	return spotPool.Get().(*Spot)
}

func putSpot(s *Spot) {
	*s = Spot{} // Zero out
	spotPool.Put(s)
}

func getBatch() *SpotBatch {
	b := batchPool.Get().(*SpotBatch)
	b.Count = 0
	return b
}

func putBatch(b *SpotBatch) {
	b.Count = 0
	batchPool.Put(b)
}

// =============================================================================
// Telemetry
// =============================================================================

type Stats struct {
	rowsProcessed uint64
	bytesRead     uint64
	startTime     time.Time
	lastReport    time.Time
	mu            sync.Mutex
}

func NewStats() *Stats {
	return &Stats{
		startTime:  time.Now(),
		lastReport: time.Now(),
	}
}

func (s *Stats) AddRows(n uint64) {
	atomic.AddUint64(&s.rowsProcessed, n)
}

func (s *Stats) AddBytes(n uint64) {
	atomic.AddUint64(&s.bytesRead, n)
}

func (s *Stats) GetRows() uint64 {
	return atomic.LoadUint64(&s.rowsProcessed)
}

func (s *Stats) Report() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(s.startTime).Seconds()
	rows := atomic.LoadUint64(&s.rowsProcessed)

	if elapsed > 0 {
		mrps := float64(rows) / elapsed / 1_000_000
		fmt.Printf("\r[Progress] %.2f Mrps | Total: %d rows | Elapsed: %.1fs",
			mrps, rows, elapsed)
	}
}

// =============================================================================
// CSV Parsing (Pure Go, no CGO)
// =============================================================================

// parseRow parses a CSV row into a Spot
func parseRow(fields []string, spot *Spot, rowNum int64) error {
	colCount := len(fields)
	if colCount < 14 {
		return fmt.Errorf("row %d: insufficient columns (%d)", rowNum, colCount)
	}

	// Parse SpotID
	id, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return fmt.Errorf("row %d: invalid id: %v", rowNum, err)
	}
	spot.ID = id

	// Parse Timestamp (Unix seconds)
	ts, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return fmt.Errorf("row %d: invalid timestamp: %v", rowNum, err)
	}
	spot.Timestamp = uint32(ts)

	// Reporter (sanitize quotes)
	copyFixedString(spot.Reporter[:], sanitizeString(fields[2]))

	// Reporter Grid
	copyFixedString(spot.ReporterGrid[:], strings.ToUpper(fields[3]))

	// SNR
	snr, err := strconv.ParseInt(fields[4], 10, 8)
	if err != nil {
		return fmt.Errorf("row %d: invalid snr: %v", rowNum, err)
	}
	spot.SNR = int8(snr)

	// Frequency (MHz to Hz as uint64)
	freqFloat, err := strconv.ParseFloat(fields[5], 64)
	if err != nil {
		return fmt.Errorf("row %d: invalid frequency: %v", rowNum, err)
	}
	spot.Frequency = uint64(freqFloat * 1_000_000)

	// Callsign (sanitize quotes)
	copyFixedString(spot.Callsign[:], sanitizeString(fields[6]))

	// Grid
	copyFixedString(spot.Grid[:], strings.ToUpper(fields[7]))

	// Power
	power, err := strconv.ParseInt(fields[8], 10, 8)
	if err != nil {
		return fmt.Errorf("row %d: invalid power: %v", rowNum, err)
	}
	spot.Power = int8(power)

	// Drift
	drift, err := strconv.ParseInt(fields[9], 10, 8)
	if err != nil {
		return fmt.Errorf("row %d: invalid drift: %v", rowNum, err)
	}
	spot.Drift = int8(drift)

	// Distance
	dist, err := strconv.ParseUint(fields[10], 10, 32)
	if err != nil {
		return fmt.Errorf("row %d: invalid distance: %v", rowNum, err)
	}
	spot.Distance = uint32(dist)

	// Azimuth
	az, err := strconv.ParseUint(fields[11], 10, 16)
	if err != nil {
		return fmt.Errorf("row %d: invalid azimuth: %v", rowNum, err)
	}
	spot.Azimuth = uint16(az)

	// Band — normalize from frequency (ignore CSV band column)
	spot.Band, _ = bands.GetBand(freqFloat)

	// Version (field 13)
	copyFixedString(spot.Version[:], fields[13])

	// Code (field 14, optional)
	if colCount > 14 {
		code, _ := strconv.ParseUint(fields[14], 10, 8)
		spot.Code = uint8(code)
	}

	// Mode (derived from band if not present)
	copyFixedString(spot.Mode[:], "WSPR")

	spot.ColumnCount = uint8(colCount)
	return nil
}

// sanitizeString removes quotes and brackets
func sanitizeString(s string) string {
	s = strings.Trim(s, "\"'`<>[]{}()")
	return s
}

// copyFixedString copies a string into a fixed-size byte array
func copyFixedString(dst []byte, src string) {
	for i := range dst {
		dst[i] = 0
	}
	copy(dst, src)
}

// =============================================================================
// File Processing
// =============================================================================

// ProcessFile reads a WSPR CSV file and sends batches to the channel
func ProcessFile(ctx context.Context, filePath string, batchChan chan<- *SpotBatch, stats *Stats, rowLimit int64) error {
	info, err := os.Stat(filePath)
	if err != nil {
		return err
	}
	stats.AddBytes(uint64(info.Size()))

	// Open file
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Setup reader (handle gzip)
	var reader io.Reader
	if strings.HasSuffix(filePath, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return err
		}
		defer gz.Close()
		reader = gz
	} else {
		reader = bufio.NewReaderSize(f, 64*1024) // 64KB buffer
	}

	// CSV reader
	csvReader := csv.NewReader(reader)
	csvReader.FieldsPerRecord = -1 // Variable fields
	csvReader.ReuseRecord = true   // Reuse record slice

	batch := getBatch()
	var rowNum int64
	var totalRows int64

	for {
		select {
		case <-ctx.Done():
			if batch.Count > 0 {
				putBatch(batch)
			}
			return ctx.Err()
		default:
		}

		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue // Skip malformed rows
		}

		rowNum++

		// Check row limit
		if rowLimit > 0 && totalRows >= rowLimit {
			break
		}

		// Parse into batch
		if batch.Count < len(batch.Spots) {
			if err := parseRow(record, &batch.Spots[batch.Count], rowNum); err != nil {
				continue // Skip parse errors
			}
			batch.Count++
			totalRows++
		}

		// Send full batch
		if batch.Count >= *batchSize {
			select {
			case batchChan <- batch:
				batch = getBatch()
			case <-ctx.Done():
				putBatch(batch)
				return ctx.Err()
			}
		}
	}

	// Send remaining batch
	if batch.Count > 0 {
		select {
		case batchChan <- batch:
		case <-ctx.Done():
			putBatch(batch)
			return ctx.Err()
		}
	}

	return nil
}

// =============================================================================
// ClickHouse Writer
// =============================================================================

func ClickHouseWriter(ctx context.Context, conn driver.Conn, batchChan <-chan *SpotBatch, stats *Stats, tableFQN string) error {
	var pendingBatch driver.Batch
	var pendingCount int
	var batchStartTime time.Time

	flush := func() error {
		if pendingBatch == nil || pendingCount == 0 {
			return nil
		}

		if err := pendingBatch.Send(); err != nil {
			return err
		}

		elapsed := time.Since(batchStartTime)
		mrps := float64(pendingCount) / elapsed.Seconds() / 1_000_000
		log.Printf("Flushed %d rows in %v (%.2f Mrps)", pendingCount, elapsed, mrps)

		stats.AddRows(uint64(pendingCount))
		pendingBatch = nil
		pendingCount = 0
		return nil
	}

	defer func() {
		if err := flush(); err != nil {
			log.Printf("Final flush error: %v", err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case batch, ok := <-batchChan:
			if !ok {
				return nil
			}

			// Create ClickHouse batch if needed
			if pendingBatch == nil {
				var err error
				pendingBatch, err = conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", tableFQN))
				if err != nil {
					putBatch(batch)
					return err
				}
				batchStartTime = time.Now()
			}

			// Append rows
			for i := 0; i < batch.Count; i++ {
				spot := &batch.Spots[i]
				err := pendingBatch.Append(
					spot.ID,
					time.Unix(int64(spot.Timestamp), 0).UTC(),
					string(spot.Reporter[:]),
					string(spot.ReporterGrid[:]),
					spot.SNR,
					spot.Frequency,
					string(spot.Callsign[:]),
					string(spot.Grid[:]),
					spot.Power,
					spot.Drift,
					spot.Distance,
					spot.Azimuth,
					spot.Band,
					string(spot.Mode[:]),
					string(spot.Version[:]),
					spot.Code,
					spot.ColumnCount,
				)
				if err != nil {
					log.Printf("Append error: %v", err)
					continue
				}
				pendingCount++
			}

			// Return batch to pool
			putBatch(batch)

			// Flush if large enough
			if pendingCount >= ClickHouseBatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

// =============================================================================
// Partition Management
// =============================================================================

func truncatePartition(ctx context.Context, conn driver.Conn, tableFQN, filePath string) error {
	fileName := filepath.Base(filePath)

	// Extract YYYYMM from wsprspots-YYYY-MM.csv.gz
	var year, month string
	if len(fileName) > 15 && fileName[0:10] == "wsprspots-" {
		yearMonth := fileName[10:17]
		parts := strings.Split(yearMonth, "-")
		if len(parts) == 2 {
			year = parts[0]
			month = parts[1]
		}
	}

	if year == "" || month == "" {
		return fmt.Errorf("could not extract YYYY-MM from: %s", fileName)
	}

	partition := year + month
	query := fmt.Sprintf("ALTER TABLE %s DROP PARTITION '%s'", tableFQN, partition)
	log.Printf("Truncating partition %s", partition)

	if err := conn.Exec(ctx, query); err != nil {
		if !strings.Contains(err.Error(), "not found") && !strings.Contains(err.Error(), "NO_SUCH_DATA_PART") {
			return err
		}
	}

	return nil
}

// =============================================================================
// Main Pipeline
// =============================================================================

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "wspr-ingest-cpu v%s - CPU-Only WSPR Ingester\n\n", Version)
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] <path>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Pure Go implementation - no CUDA/CGO dependencies.\n")
		fmt.Fprintf(os.Stderr, "Uses %d workers (matching 9950X3D cores) with sync.Pool.\n\n", NumWorkers)
		flag.PrintDefaults()
	}

	flag.Parse()

	if len(flag.Args()) < 1 {
		fmt.Fprintf(os.Stderr, "Error: missing <path> argument\n")
		flag.Usage()
		os.Exit(1)
	}

	inputPath := flag.Args()[0]

	log.Println("=================================================")
	log.Printf("WSPR CPU-Only Ingester v%s", Version)
	log.Println("=================================================")
	log.Printf("Input: %s", inputPath)
	log.Printf("Workers: %d (sync.Pool enabled)", NumWorkers)
	log.Printf("Batch Size: %d rows", *batchSize)

	// Context with signal handling
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
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{*chHost},
		Auth: clickhouse.Auth{
			Database: *chDB,
			Username: "default",
			Password: "",
		},
		Settings: clickhouse.Settings{
			"max_execution_time":           60,
			"async_insert":                 1,
			"wait_for_async_insert":        0,
			"async_insert_max_data_size":   100000000,
			"async_insert_busy_timeout_ms": 1000,
			"max_insert_block_size":        1048576,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		MaxOpenConns:    2,
		MaxIdleConns:    1,
		ConnMaxLifetime: time.Hour,
	})
	if err != nil {
		log.Fatalf("ClickHouse connection failed: %v", err)
	}
	defer conn.Close()

	if err := conn.Ping(ctx); err != nil {
		log.Fatalf("ClickHouse ping failed: %v", err)
	}

	tableFQN := fmt.Sprintf("%s.%s", *chDB, *chTable)
	log.Printf("ClickHouse table: %s", tableFQN)

	// Discover files
	var files []string
	info, err := os.Stat(inputPath)
	if err != nil {
		log.Fatalf("Cannot access path: %v", err)
	}

	if info.IsDir() {
		filepath.Walk(inputPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			ext := filepath.Ext(path)
			if !info.IsDir() && (ext == ".gz" || ext == ".csv") {
				files = append(files, path)
			}
			return nil
		})
	} else {
		files = append(files, inputPath)
	}

	if len(files) == 0 {
		log.Fatal("No CSV files found")
	}

	log.Printf("Found %d files to process", len(files))

	// Setup pipeline
	batchChan := make(chan *SpotBatch, ChannelBuffer)
	stats := NewStats()

	// Start progress reporter
	if !*silent {
		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					stats.Report()
				}
			}
		}()
	}

	// Start ClickHouse writer
	var writerWg sync.WaitGroup
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		if err := ClickHouseWriter(ctx, conn, batchChan, stats, tableFQN); err != nil && err != context.Canceled {
			log.Printf("Writer error: %v", err)
		}
	}()

	// Process files with worker pool
	fileChan := make(chan string, len(files))
	var workerWg sync.WaitGroup

	for i := 0; i < NumWorkers; i++ {
		workerWg.Add(1)
		go func(id int) {
			defer workerWg.Done()
			for filePath := range fileChan {
				select {
				case <-ctx.Done():
					return
				default:
				}

				// Truncate partition
				if err := truncatePartition(ctx, conn, tableFQN, filePath); err != nil {
					log.Printf("Worker[%d] truncate error: %v", id, err)
				}

				// Process file
				log.Printf("Worker[%d] processing %s", id, filepath.Base(filePath))
				if err := ProcessFile(ctx, filePath, batchChan, stats, int64(*limit)); err != nil {
					if err != context.Canceled {
						log.Printf("Worker[%d] error: %v", id, err)
					}
				}
			}
		}(i)
	}

	// Queue files
	for _, f := range files {
		fileChan <- f
	}
	close(fileChan)

	// Wait for workers
	workerWg.Wait()
	close(batchChan)

	// Wait for writer
	writerWg.Wait()

	// Final stats
	elapsed := time.Since(stats.startTime)
	rows := stats.GetRows()
	mrps := float64(rows) / elapsed.Seconds() / 1_000_000

	fmt.Printf("\n\n=== Final Statistics ===\n")
	fmt.Printf("Total Rows: %d\n", rows)
	fmt.Printf("Elapsed: %v\n", elapsed.Round(time.Second))
	fmt.Printf("Throughput: %.2f Mrps\n", mrps)
	fmt.Printf("========================\n")

	log.Println("Ingestion complete!")
}
