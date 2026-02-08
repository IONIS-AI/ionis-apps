// Package main provides a high-performance WSPR CSV ingester.
//
// Optimizations:
//   - Parallel gzip decompression (pgzip) - uses all CPU cores
//   - Zero-allocation CSV parser - no string allocations
//   - Pre-allocated batch buffers - reduces GC pressure
//   - Optimized ClickHouse inserts - batched with LZ4
//   - Lock-free statistics with atomics
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
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

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/KI7MT/ki7mt-ai-lab-apps/internal/bands"
	"github.com/klauspost/pgzip"
)

var Version = "dev"

// =============================================================================
// Configuration - Tuned for high-performance multi-core systems
// =============================================================================

const (
	NumWriters          = 2         // Parallel ClickHouse writers
	BatchSize           = 500_000   // Rows per batch
	ClickHouseBatchSize = 2_000_000 // Flush every 2M rows
	ChannelBuffer       = 64        // Batch channel buffer
	ReadBufferSize      = 4 * 1024 * 1024 // 4MB read buffer
)

// Command-line flags
var (
	chHost    = flag.String("ch-host", "127.0.0.1:9000", "ClickHouse address")
	chDB      = flag.String("ch-db", "wspr", "ClickHouse database")
	chTable   = flag.String("ch-table", "bronze", "ClickHouse table")
	silent    = flag.Bool("silent", false, "Suppress progress output")
)

// =============================================================================
// Spot - Optimized struct layout (cache-line aligned)
// =============================================================================

type Spot struct {
	ID           uint64
	Timestamp    uint32
	Frequency    uint64
	Distance     uint32
	Band         int32
	Azimuth      uint16
	SNR          int8
	Power        int8
	Drift        int8
	Code         uint8
	ColumnCount  uint8
	_pad         [1]byte
	Reporter     [16]byte
	ReporterGrid [8]byte
	Callsign     [16]byte
	Grid         [8]byte
	Mode         [8]byte
	Version      [8]byte
}

// SpotBatch - Pre-allocated batch
type SpotBatch struct {
	Spots []Spot
	Count int
}

// =============================================================================
// Object Pools - Reduce GC pressure
// =============================================================================

var batchPool = sync.Pool{
	New: func() interface{} {
		return &SpotBatch{
			Spots: make([]Spot, BatchSize),
			Count: 0,
		}
	},
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
// Lock-free Statistics
// =============================================================================

type Stats struct {
	rowsProcessed uint64
	bytesRead     uint64
	batchesSent   uint64
	startTime     time.Time
}

func NewStats() *Stats {
	return &Stats{startTime: time.Now()}
}

func (s *Stats) AddRows(n uint64)   { atomic.AddUint64(&s.rowsProcessed, n) }
func (s *Stats) AddBytes(n uint64)  { atomic.AddUint64(&s.bytesRead, n) }
func (s *Stats) AddBatch()          { atomic.AddUint64(&s.batchesSent, 1) }
func (s *Stats) GetRows() uint64    { return atomic.LoadUint64(&s.rowsProcessed) }

func (s *Stats) Report() string {
	elapsed := time.Since(s.startTime).Seconds()
	rows := atomic.LoadUint64(&s.rowsProcessed)
	if elapsed > 0 {
		mrps := float64(rows) / elapsed / 1_000_000
		return fmt.Sprintf("%.2f Mrps | %d rows | %.1fs", mrps, rows, elapsed)
	}
	return "starting..."
}

// =============================================================================
// Zero-Allocation CSV Parser
// =============================================================================

func parseField(line []byte, start int) ([]byte, int) {
	end := start
	for end < len(line) && line[end] != ',' && line[end] != '\n' && line[end] != '\r' {
		end++
	}
	return line[start:end], end + 1
}

func parseUint64Fast(b []byte) uint64 {
	var n uint64
	for _, c := range b {
		if c >= '0' && c <= '9' {
			n = n*10 + uint64(c-'0')
		}
	}
	return n
}

func parseInt64Fast(b []byte) int64 {
	if len(b) == 0 {
		return 0
	}
	neg := b[0] == '-'
	if neg || b[0] == '+' {
		b = b[1:]
	}
	var n int64
	for _, c := range b {
		if c >= '0' && c <= '9' {
			n = n*10 + int64(c-'0')
		}
	}
	if neg {
		return -n
	}
	return n
}

func parseFloatFast(b []byte) uint64 {
	if len(b) == 0 {
		return 0
	}

	var intPart, fracPart uint64
	var fracDigits int
	inFrac := false

	for _, c := range b {
		if c == '.' {
			inFrac = true
			continue
		}
		if c >= '0' && c <= '9' {
			if inFrac {
				fracPart = fracPart*10 + uint64(c-'0')
				fracDigits++
			} else {
				intPart = intPart*10 + uint64(c-'0')
			}
		}
	}

	hz := intPart * 1_000_000
	if fracDigits > 0 {
		scale := uint64(1)
		for i := 0; i < 6-fracDigits && i < 6; i++ {
			scale *= 10
		}
		for i := 0; i < fracDigits-6 && fracDigits > 6; i++ {
			fracPart /= 10
		}
		hz += fracPart * scale
	}
	return hz
}

func copyToFixed(dst []byte, src []byte) {
	for i := range dst {
		dst[i] = 0
	}
	j := 0
	for _, c := range src {
		if j >= len(dst) {
			break
		}
		if c == '"' || c == '\'' || c == '`' || c == '<' || c == '>' ||
			c == '[' || c == ']' || c == '{' || c == '}' || c == '(' || c == ')' {
			continue
		}
		dst[j] = c
		j++
	}
}

func copyToFixedUpper(dst []byte, src []byte) {
	for i := range dst {
		dst[i] = 0
	}
	j := 0
	for _, c := range src {
		if j >= len(dst) {
			break
		}
		if c >= 'a' && c <= 'z' {
			c -= 32
		}
		dst[j] = c
		j++
	}
}

func parseLine(line []byte, spot *Spot) bool {
	if len(line) < 20 {
		return false
	}

	var field []byte
	pos := 0
	fieldNum := 0

	for pos < len(line) && fieldNum < 15 {
		field, pos = parseField(line, pos)

		switch fieldNum {
		case 0:
			spot.ID = parseUint64Fast(field)
		case 1:
			spot.Timestamp = uint32(parseUint64Fast(field))
		case 2:
			copyToFixed(spot.Reporter[:], field)
		case 3:
			copyToFixedUpper(spot.ReporterGrid[:], field)
		case 4:
			spot.SNR = int8(parseInt64Fast(field))
		case 5:
			spot.Frequency = parseFloatFast(field)
		case 6:
			copyToFixed(spot.Callsign[:], field)
		case 7:
			copyToFixedUpper(spot.Grid[:], field)
		case 8:
			spot.Power = int8(parseInt64Fast(field))
		case 9:
			spot.Drift = int8(parseInt64Fast(field))
		case 10:
			spot.Distance = uint32(parseUint64Fast(field))
		case 11:
			spot.Azimuth = uint16(parseUint64Fast(field))
		case 12:
			spot.Band = int32(parseInt64Fast(field))
		case 13:
			copyToFixed(spot.Version[:], field)
		case 14:
			spot.Code = uint8(parseUint64Fast(field))
		}
		fieldNum++
	}

	copy(spot.Mode[:], "WSPR")
	spot.ColumnCount = uint8(fieldNum)
	return fieldNum >= 13
}

// =============================================================================
// Line Reader with Parallel Gzip
// =============================================================================

type LineReader struct {
	reader    *bufio.Reader
	batchChan chan *SpotBatch
	stats     *Stats
}

func NewLineReader(r io.Reader, batchChan chan *SpotBatch, stats *Stats) *LineReader {
	return &LineReader{
		reader:    bufio.NewReaderSize(r, ReadBufferSize),
		batchChan: batchChan,
		stats:     stats,
	}
}

func (lr *LineReader) Process(ctx context.Context) error {
	batch := getBatch()

	for {
		select {
		case <-ctx.Done():
			if batch.Count > 0 {
				putBatch(batch)
			}
			return ctx.Err()
		default:
		}

		line, err := lr.reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			continue
		}

		if batch.Count < len(batch.Spots) {
			if parseLine(line, &batch.Spots[batch.Count]) {
				// Normalize band from frequency (single source of truth: bands.go)
				spot := &batch.Spots[batch.Count]
				freqMHz := float64(spot.Frequency) / 1_000_000.0
				spot.Band, _ = bands.GetBand(freqMHz)
				batch.Count++
			}
		}

		if batch.Count >= BatchSize {
			select {
			case lr.batchChan <- batch:
				batch = getBatch()
			case <-ctx.Done():
				putBatch(batch)
				return ctx.Err()
			}
		}
	}

	if batch.Count > 0 {
		select {
		case lr.batchChan <- batch:
		case <-ctx.Done():
			putBatch(batch)
		}
	}

	return nil
}

// =============================================================================
// File Processing
// =============================================================================

func processFile(ctx context.Context, filePath string, batchChan chan *SpotBatch, stats *Stats) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	info, _ := f.Stat()
	stats.AddBytes(uint64(info.Size()))

	var reader io.Reader

	if strings.HasSuffix(filePath, ".gz") {
		gz, err := pgzip.NewReaderN(f, 256*1024, runtime.NumCPU())
		if err != nil {
			return err
		}
		defer gz.Close()
		reader = gz
	} else {
		reader = f
	}

	lr := NewLineReader(reader, batchChan, stats)
	return lr.Process(ctx)
}

// =============================================================================
// ClickHouse Writer
// =============================================================================

func clickhouseWriter(ctx context.Context, id int, conn driver.Conn, batchChan <-chan *SpotBatch, stats *Stats, tableFQN string, wg *sync.WaitGroup) {
	defer wg.Done()

	var pendingBatch driver.Batch
	var pendingCount int
	var batchStart time.Time

	flush := func() error {
		if pendingBatch == nil || pendingCount == 0 {
			return nil
		}

		if err := pendingBatch.Send(); err != nil {
			return err
		}

		elapsed := time.Since(batchStart)
		mrps := float64(pendingCount) / elapsed.Seconds() / 1_000_000
		log.Printf("Writer[%d]: flushed %d rows (%.2f Mrps)", id, pendingCount, mrps)

		stats.AddRows(uint64(pendingCount))
		stats.AddBatch()
		pendingBatch = nil
		pendingCount = 0
		return nil
	}

	defer func() {
		if err := flush(); err != nil {
			log.Printf("Writer[%d]: final flush error: %v", id, err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return

		case batch, ok := <-batchChan:
			if !ok {
				return
			}

			if pendingBatch == nil {
				var err error
				pendingBatch, err = conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", tableFQN))
				if err != nil {
					putBatch(batch)
					log.Printf("Writer[%d]: prepare error: %v", id, err)
					continue
				}
				batchStart = time.Now()
			}

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
					continue
				}
				pendingCount++
			}

			putBatch(batch)

			if pendingCount >= ClickHouseBatchSize {
				if err := flush(); err != nil {
					log.Printf("Writer[%d]: flush error: %v", id, err)
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

	if !strings.HasPrefix(fileName, "wsprspots-") || len(fileName) < 17 {
		return nil
	}

	yearMonth := fileName[10:17]
	parts := strings.Split(yearMonth, "-")
	if len(parts) != 2 {
		return nil
	}

	partition := parts[0] + parts[1]
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
// Main
// =============================================================================

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "wspr-ingest v%s - High-Performance WSPR CSV Ingester\n\n", Version)
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] <path>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Features:\n")
		fmt.Fprintf(os.Stderr, "  - Parallel gzip decompression using %d cores\n", runtime.NumCPU())
		fmt.Fprintf(os.Stderr, "  - Zero-allocation CSV parser\n")
		fmt.Fprintf(os.Stderr, "  - Pre-allocated batch buffers with sync.Pool\n")
		fmt.Fprintf(os.Stderr, "  - %d parallel ClickHouse writers with LZ4\n\n", NumWriters)
		flag.PrintDefaults()
	}

	flag.Parse()

	if len(flag.Args()) < 1 {
		fmt.Fprintf(os.Stderr, "Error: missing <path> argument\n")
		flag.Usage()
		os.Exit(1)
	}

	inputPaths := flag.Args()

	log.Println("=========================================================")
	log.Printf("WSPR Ingester v%s", Version)
	log.Println("=========================================================")
	log.Printf("Input: %d path(s)", len(inputPaths))
	log.Printf("CPUs: %d | Batch: %d | Writers: %d", runtime.NumCPU(), BatchSize, NumWriters)

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
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{*chHost},
		Auth: clickhouse.Auth{
			Database: *chDB,
			Username: "default",
			Password: "",
		},
		Settings: clickhouse.Settings{
			"max_execution_time":           300,
			"max_insert_threads":           16, // Match 9950X3D physical cores
			"input_format_parallel_parsing": 0, // Disable - Go code does parallel work
			"async_insert":                 1,
			"wait_for_async_insert":        0,
			"async_insert_max_data_size":   500000000,
			"async_insert_busy_timeout_ms": 500,
			"max_insert_block_size":        1048576,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		MaxOpenConns:    NumWriters + 2,
		MaxIdleConns:    NumWriters,
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
	log.Printf("Table: %s", tableFQN)

	var files []string
	for _, inputPath := range inputPaths {
		info, err := os.Stat(inputPath)
		if err != nil {
			log.Printf("Warning: cannot access path %s: %v", inputPath, err)
			continue
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
			ext := filepath.Ext(inputPath)
			if ext == ".gz" || ext == ".csv" {
				files = append(files, inputPath)
			}
		}
	}

	if len(files) == 0 {
		log.Fatal("No CSV files found")
	}

	sort.Strings(files)
	log.Printf("Found %d file(s)", len(files))

	batchChan := make(chan *SpotBatch, ChannelBuffer)
	stats := NewStats()

	if !*silent {
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					fmt.Printf("\r[Progress] %s", stats.Report())
				}
			}
		}()
	}

	var writerWg sync.WaitGroup
	for i := 0; i < NumWriters; i++ {
		writerWg.Add(1)
		go clickhouseWriter(ctx, i, conn, batchChan, stats, tableFQN, &writerWg)
	}

	for _, filePath := range files {
		select {
		case <-ctx.Done():
			break
		default:
		}

		if err := truncatePartition(ctx, conn, tableFQN, filePath); err != nil {
			log.Printf("Truncate warning: %v", err)
		}

		log.Printf("Processing: %s", filepath.Base(filePath))
		startFile := time.Now()

		if err := processFile(ctx, filePath, batchChan, stats); err != nil {
			if err != context.Canceled {
				log.Printf("Error: %v", err)
			}
		}

		log.Printf("File complete: %v", time.Since(startFile).Round(time.Second))
	}

	close(batchChan)
	writerWg.Wait()

	elapsed := time.Since(stats.startTime)
	rows := stats.GetRows()
	mrps := float64(rows) / elapsed.Seconds() / 1_000_000

	fmt.Printf("\n\n")
	log.Println("=========================================================")
	log.Println("Final Statistics")
	log.Println("=========================================================")
	log.Printf("Total Rows:  %d", rows)
	log.Printf("Elapsed:     %v", elapsed.Round(time.Second))
	log.Printf("Throughput:  %.2f Mrps", mrps)
	log.Println("=========================================================")
}
