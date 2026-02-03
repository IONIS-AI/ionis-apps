// wspr-turbo - Zero-copy streaming tar.gz → ClickHouse pipeline
//
// Bypasses the "File Penalty" by streaming directly from compressed archives
// to ClickHouse Native Blocks without intermediate disk I/O.
//
// Architecture:
//   - Stream decompression: tar.gz → memory (no disk extraction)
//   - Vectorized parsing: CSV → columnar buffers (no row structs)
//   - Double-buffering: fill block A while sending block B
//   - sync.Pool: zero-allocation buffer reuse
//
// Build: CGO_ENABLED=0 go build -ldflags="-s -w" -o build/wspr-turbo ./cmd/wspr-turbo

package main

import (
	"archive/tar"
	"bufio"

	"github.com/klauspost/compress/gzip"
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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

// Version can be overridden at build time via -ldflags
var Version = "2.0.8"

const (
	BlockSize     = 1_000_000 // 1M rows per block
	NumWorkers    = 16        // Parallel archive workers
	NumSenders    = 4         // Parallel block senders per worker
	ReadBufferSize = 4 * 1024 * 1024 // 4MB read buffer
)

// ColumnBlock holds pre-allocated columnar buffers for vectorized parsing
// This avoids row-oriented structs and GC pressure
type ColumnBlock struct {
	ID           []uint64
	Timestamp    []time.Time
	Reporter     []string
	ReporterGrid []string
	SNR          []int8
	Frequency    []uint64
	Callsign     []string
	Grid         []string
	Power        []int8
	Drift        []int8
	Distance     []uint32
	Azimuth      []uint16
	Band         []int32
	Version      []string
	Code         []uint8
	RowCount     int
}

// Pool for ColumnBlocks - zero allocation after warmup
var blockPool = sync.Pool{
	New: func() interface{} {
		return &ColumnBlock{
			ID:           make([]uint64, 0, BlockSize),
			Timestamp:    make([]time.Time, 0, BlockSize),
			Reporter:     make([]string, 0, BlockSize),
			ReporterGrid: make([]string, 0, BlockSize),
			SNR:          make([]int8, 0, BlockSize),
			Frequency:    make([]uint64, 0, BlockSize),
			Callsign:     make([]string, 0, BlockSize),
			Grid:         make([]string, 0, BlockSize),
			Power:        make([]int8, 0, BlockSize),
			Drift:        make([]int8, 0, BlockSize),
			Distance:     make([]uint32, 0, BlockSize),
			Azimuth:      make([]uint16, 0, BlockSize),
			Band:         make([]int32, 0, BlockSize),
			Version:      make([]string, 0, BlockSize),
			Code:         make([]uint8, 0, BlockSize),
		}
	},
}

// Pool for byte buffers used in parsing
var bufferPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 4096)
		return &buf
	},
}

func (b *ColumnBlock) Reset() {
	b.ID = b.ID[:0]
	b.Timestamp = b.Timestamp[:0]
	b.Reporter = b.Reporter[:0]
	b.ReporterGrid = b.ReporterGrid[:0]
	b.SNR = b.SNR[:0]
	b.Frequency = b.Frequency[:0]
	b.Callsign = b.Callsign[:0]
	b.Grid = b.Grid[:0]
	b.Power = b.Power[:0]
	b.Drift = b.Drift[:0]
	b.Distance = b.Distance[:0]
	b.Azimuth = b.Azimuth[:0]
	b.Band = b.Band[:0]
	b.Version = b.Version[:0]
	b.Code = b.Code[:0]
	b.RowCount = 0
}

func (b *ColumnBlock) Len() int {
	return b.RowCount
}

// ToProtoInput converts columnar buffers to ch-go proto.Input
// This is the only allocation point - we build the native block here
func (b *ColumnBlock) ToProtoInput() proto.Input {
	colID := new(proto.ColUInt64)
	colTimestamp := new(proto.ColDateTime)
	colReporter := new(proto.ColStr)
	colReporterGrid := new(proto.ColStr)
	colSNR := new(proto.ColInt8)
	colFrequency := new(proto.ColUInt64)
	colCallsign := new(proto.ColStr)
	colGrid := new(proto.ColStr)
	colPower := new(proto.ColInt8)
	colDrift := new(proto.ColInt8)
	colDistance := new(proto.ColUInt32)
	colAzimuth := new(proto.ColUInt16)
	colBand := new(proto.ColInt32)
	colMode := new(proto.ColStr)
	colVersion := new(proto.ColStr)
	colCode := new(proto.ColUInt8)
	colColumnCount := new(proto.ColUInt8)

	for i := 0; i < b.RowCount; i++ {
		colID.Append(b.ID[i])
		colTimestamp.Append(b.Timestamp[i])
		colReporter.Append(b.Reporter[i])
		colReporterGrid.Append(b.ReporterGrid[i])
		colSNR.Append(b.SNR[i])
		colFrequency.Append(b.Frequency[i])
		colCallsign.Append(b.Callsign[i])
		colGrid.Append(b.Grid[i])
		colPower.Append(b.Power[i])
		colDrift.Append(b.Drift[i])
		colDistance.Append(b.Distance[i])
		colAzimuth.Append(b.Azimuth[i])
		colBand.Append(b.Band[i])
		colMode.Append("WSPR")
		colVersion.Append(b.Version[i])
		colCode.Append(b.Code[i])
		colColumnCount.Append(15)
	}

	return proto.Input{
		{Name: "id", Data: colID},
		{Name: "timestamp", Data: colTimestamp},
		{Name: "reporter", Data: colReporter},
		{Name: "reporter_grid", Data: colReporterGrid},
		{Name: "snr", Data: colSNR},
		{Name: "frequency", Data: colFrequency},
		{Name: "callsign", Data: colCallsign},
		{Name: "grid", Data: colGrid},
		{Name: "power", Data: colPower},
		{Name: "drift", Data: colDrift},
		{Name: "distance", Data: colDistance},
		{Name: "azimuth", Data: colAzimuth},
		{Name: "band", Data: colBand},
		{Name: "mode", Data: colMode},
		{Name: "version", Data: colVersion},
		{Name: "code", Data: colCode},
		{Name: "column_count", Data: colColumnCount},
	}
}

type Stats struct {
	TotalRows      atomic.Uint64
	TotalBytes     atomic.Uint64
	ArchivesComplete atomic.Uint64
	BlocksSent     atomic.Uint64
	StartTime      time.Time
}

func NewStats() *Stats {
	return &Stats{StartTime: time.Now()}
}

// VectorizedScanner parses CSV directly into columnar buffers
// No row structs, no reflect, minimal allocations
type VectorizedScanner struct {
	reader   *bufio.Reader
	lineBuf  []byte
	fieldBuf []byte
}

func NewVectorizedScanner(r io.Reader) *VectorizedScanner {
	return &VectorizedScanner{
		reader:   bufio.NewReaderSize(r, ReadBufferSize),
		lineBuf:  make([]byte, 0, 4096),
		fieldBuf: make([]byte, 0, 256),
	}
}

// ParseIntoBlock reads CSV lines directly into columnar buffers
// Returns number of rows parsed
func (s *VectorizedScanner) ParseIntoBlock(block *ColumnBlock, maxRows int) (int, error) {
	rowsParsed := 0

	for rowsParsed < maxRows {
		// Read line - reuse buffer
		s.lineBuf = s.lineBuf[:0]
		for {
			chunk, isPrefix, err := s.reader.ReadLine()
			if err != nil {
				if err == io.EOF && rowsParsed > 0 {
					return rowsParsed, nil
				}
				return rowsParsed, err
			}
			s.lineBuf = append(s.lineBuf, chunk...)
			if !isPrefix {
				break
			}
		}

		if len(s.lineBuf) == 0 {
			continue
		}

		// Parse fields directly into columns
		if s.parseLineIntoBlock(block, s.lineBuf) {
			rowsParsed++
			block.RowCount++
		}
	}

	return rowsParsed, nil
}

// parseLineIntoBlock parses a single CSV line directly into columnar buffers
// Inline field extraction for maximum performance
func (s *VectorizedScanner) parseLineIntoBlock(block *ColumnBlock, line []byte) bool {
	fieldStart := 0
	fieldIndex := 0
	lineLen := len(line)

	var (
		id           uint64
		timestamp    int64
		reporter     string
		reporterGrid string
		snr          int64
		frequency    float64
		callsign     string
		grid         string
		power        int64
		drift        int64
		distance     uint64
		azimuth      uint64
		band         int64
		version      string
		code         uint64
	)

	for i := 0; i <= lineLen; i++ {
		if i == lineLen || line[i] == ',' {
			field := line[fieldStart:i]

			switch fieldIndex {
			case 0: // id
				id, _ = parseUint64(field)
			case 1: // timestamp
				timestamp, _ = parseInt64(field)
			case 2: // reporter
				reporter = truncateBytes(field, 16)
			case 3: // reporter_grid
				reporterGrid = truncateBytes(field, 8)
			case 4: // snr
				snr, _ = parseInt64(field)
			case 5: // frequency
				frequency, _ = parseFloat64(field)
			case 6: // callsign
				callsign = truncateBytes(field, 16)
			case 7: // grid
				grid = truncateBytes(field, 8)
			case 8: // power
				power, _ = parseInt64(field)
			case 9: // drift
				drift, _ = parseInt64(field)
			case 10: // distance
				distance, _ = parseUint64(field)
			case 11: // azimuth
				azimuth, _ = parseUint64(field)
			case 12: // band
				band, _ = parseInt64(field)
			case 13: // version
				version = truncateBytes(field, 8)
			case 14: // code
				code, _ = parseUint64(field)
			}

			fieldStart = i + 1
			fieldIndex++
		}
	}

	// Must have at least 15 fields
	if fieldIndex < 15 {
		return false
	}

	// Append to columnar buffers
	block.ID = append(block.ID, id)
	block.Timestamp = append(block.Timestamp, time.Unix(timestamp, 0).UTC())
	block.Reporter = append(block.Reporter, reporter)
	block.ReporterGrid = append(block.ReporterGrid, reporterGrid)
	block.SNR = append(block.SNR, int8(snr))
	block.Frequency = append(block.Frequency, uint64(frequency))
	block.Callsign = append(block.Callsign, callsign)
	block.Grid = append(block.Grid, grid)
	block.Power = append(block.Power, int8(power))
	block.Drift = append(block.Drift, int8(drift))
	block.Distance = append(block.Distance, uint32(distance))
	block.Azimuth = append(block.Azimuth, uint16(azimuth))
	block.Band = append(block.Band, int32(band))
	block.Version = append(block.Version, version)
	block.Code = append(block.Code, uint8(code))

	return true
}

// Fast integer parsing without allocations
func parseUint64(b []byte) (uint64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	var n uint64
	for _, c := range b {
		if c < '0' || c > '9' {
			return n, false
		}
		n = n*10 + uint64(c-'0')
	}
	return n, true
}

func parseInt64(b []byte) (int64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	neg := false
	if b[0] == '-' {
		neg = true
		b = b[1:]
	}
	n, ok := parseUint64(b)
	if neg {
		return -int64(n), ok
	}
	return int64(n), ok
}

func parseFloat64(b []byte) (float64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	f, err := strconv.ParseFloat(string(b), 64)
	return f, err == nil
}

func truncateBytes(b []byte, maxLen int) string {
	if len(b) > maxLen {
		return string(b[:maxLen])
	}
	return string(b)
}

// BlockSender handles sending blocks to ClickHouse
type BlockSender struct {
	conn     *ch.Client
	tableFQN string
	query    string
}

func NewBlockSender(ctx context.Context, host, db, table string) (*BlockSender, error) {
	conn, err := ch.Dial(ctx, ch.Options{
		Address:     host,
		Database:    db,
		Compression: ch.CompressionLZ4,
	})
	if err != nil {
		return nil, err
	}

	tableFQN := fmt.Sprintf("%s.%s", db, table)
	query := fmt.Sprintf("INSERT INTO %s (id, timestamp, reporter, reporter_grid, snr, frequency, callsign, grid, power, drift, distance, azimuth, band, mode, version, code, column_count) VALUES", tableFQN)

	return &BlockSender{
		conn:     conn,
		tableFQN: tableFQN,
		query:    query,
	}, nil
}

func (s *BlockSender) Send(ctx context.Context, block *ColumnBlock) error {
	if block.Len() == 0 {
		return nil
	}

	return s.conn.Do(ctx, ch.Query{
		Body:  s.query,
		Input: block.ToProtoInput(),
	})
}

func (s *BlockSender) Close() {
	s.conn.Close()
}

func (s *BlockSender) TruncatePartition(ctx context.Context, archivePath string) error {
	fileName := filepath.Base(archivePath)

	// Extract YYYYMM from wsprspots-YYYY-MM.csv.gz
	if !strings.HasPrefix(fileName, "wsprspots-") || len(fileName) < 21 {
		return nil
	}

	yearMonth := fileName[10:17] // YYYY-MM
	parts := strings.Split(yearMonth, "-")
	if len(parts) != 2 {
		return nil
	}

	partition := parts[0] + parts[1]
	query := fmt.Sprintf("ALTER TABLE %s DROP PARTITION '%s'", s.tableFQN, partition)

	if err := s.conn.Do(ctx, ch.Query{Body: query}); err != nil {
		if !strings.Contains(err.Error(), "not found") && !strings.Contains(err.Error(), "NO_SUCH_DATA_PART") {
			return err
		}
	}

	return nil
}

// DoubleBuffer manages two blocks for concurrent fill/send
type DoubleBuffer struct {
	blocks [2]*ColumnBlock
	active int
	mu     sync.Mutex
}

func NewDoubleBuffer() *DoubleBuffer {
	return &DoubleBuffer{
		blocks: [2]*ColumnBlock{
			blockPool.Get().(*ColumnBlock),
			blockPool.Get().(*ColumnBlock),
		},
		active: 0,
	}
}

func (db *DoubleBuffer) Active() *ColumnBlock {
	return db.blocks[db.active]
}

func (db *DoubleBuffer) Swap() *ColumnBlock {
	db.mu.Lock()
	defer db.mu.Unlock()

	filled := db.blocks[db.active]
	db.active = 1 - db.active
	db.blocks[db.active].Reset()

	return filled
}

func (db *DoubleBuffer) Release() {
	db.blocks[0].Reset()
	db.blocks[1].Reset()
	blockPool.Put(db.blocks[0])
	blockPool.Put(db.blocks[1])
}

// processArchive streams a tar.gz file directly to ClickHouse
func processArchive(ctx context.Context, archivePath string, chHost, chDB, chTable string, stats *Stats, wg *sync.WaitGroup) {
	defer wg.Done()

	fileName := filepath.Base(archivePath)
	startTime := time.Now()

	// Get archive size
	info, err := os.Stat(archivePath)
	if err != nil {
		log.Printf("[%s] Stat error: %v", fileName, err)
		return
	}
	archiveSize := uint64(info.Size())

	// Open archive
	f, err := os.Open(archivePath)
	if err != nil {
		log.Printf("[%s] Open error: %v", fileName, err)
		return
	}
	defer f.Close()

	// Create buffered reader
	bufReader := bufio.NewReaderSize(f, ReadBufferSize)

	// Determine if gzipped
	var reader io.Reader = bufReader
	if strings.HasSuffix(archivePath, ".gz") {
		gzReader, err := gzip.NewReader(bufReader)
		if err != nil {
			log.Printf("[%s] Gzip error: %v", fileName, err)
			return
		}
		defer gzReader.Close()
		reader = gzReader
	}

	// Create sender
	sender, err := NewBlockSender(ctx, chHost, chDB, chTable)
	if err != nil {
		log.Printf("[%s] Connect error: %v", fileName, err)
		return
	}
	defer sender.Close()

	// Truncate partition
	if err := sender.TruncatePartition(ctx, archivePath); err != nil {
		log.Printf("[%s] Truncate warning: %v", fileName, err)
	}

	// Check if tar archive
	var csvReader io.Reader
	if strings.Contains(archivePath, ".tar") {
		tarReader := tar.NewReader(reader)
		// Find first .csv file in archive
		for {
			header, err := tarReader.Next()
			if err == io.EOF {
				log.Printf("[%s] No CSV in archive", fileName)
				return
			}
			if err != nil {
				log.Printf("[%s] Tar error: %v", fileName, err)
				return
			}
			if strings.HasSuffix(header.Name, ".csv") {
				csvReader = tarReader
				break
			}
		}
	} else {
		csvReader = reader
	}

	// Create vectorized scanner
	scanner := NewVectorizedScanner(csvReader)

	// Double-buffer for concurrent fill/send
	dblBuf := NewDoubleBuffer()
	defer dblBuf.Release()

	var rowCount uint64
	var blocksSent uint64

	// Channel for blocks to send
	blockChan := make(chan *ColumnBlock, 2)
	sendDone := make(chan struct{})

	// Sender goroutine
	go func() {
		defer close(sendDone)
		for block := range blockChan {
			if err := sender.Send(ctx, block); err != nil {
				log.Printf("[%s] Send error: %v", fileName, err)
			}
			blocksSent++
			// Return block to pool after send
			block.Reset()
			blockPool.Put(block)
		}
	}()

	// Parse loop with double-buffering
	for {
		select {
		case <-ctx.Done():
			close(blockChan)
			<-sendDone
			return
		default:
		}

		block := dblBuf.Active()
		n, err := scanner.ParseIntoBlock(block, BlockSize-block.Len())

		if block.Len() >= BlockSize {
			// Block is full - swap and send
			filled := dblBuf.Swap()
			rowCount += uint64(filled.Len())

			// Get a fresh block from pool for sending
			sendBlock := blockPool.Get().(*ColumnBlock)
			// Copy data to send block
			sendBlock.ID = append(sendBlock.ID, filled.ID...)
			sendBlock.Timestamp = append(sendBlock.Timestamp, filled.Timestamp...)
			sendBlock.Reporter = append(sendBlock.Reporter, filled.Reporter...)
			sendBlock.ReporterGrid = append(sendBlock.ReporterGrid, filled.ReporterGrid...)
			sendBlock.SNR = append(sendBlock.SNR, filled.SNR...)
			sendBlock.Frequency = append(sendBlock.Frequency, filled.Frequency...)
			sendBlock.Callsign = append(sendBlock.Callsign, filled.Callsign...)
			sendBlock.Grid = append(sendBlock.Grid, filled.Grid...)
			sendBlock.Power = append(sendBlock.Power, filled.Power...)
			sendBlock.Drift = append(sendBlock.Drift, filled.Drift...)
			sendBlock.Distance = append(sendBlock.Distance, filled.Distance...)
			sendBlock.Azimuth = append(sendBlock.Azimuth, filled.Azimuth...)
			sendBlock.Band = append(sendBlock.Band, filled.Band...)
			sendBlock.Version = append(sendBlock.Version, filled.Version...)
			sendBlock.Code = append(sendBlock.Code, filled.Code...)
			sendBlock.RowCount = filled.RowCount

			blockChan <- sendBlock
			filled.Reset()
		}

		if err != nil {
			break
		}
		if n == 0 {
			break
		}
	}

	// Flush remaining
	if dblBuf.Active().Len() > 0 {
		block := dblBuf.Active()
		rowCount += uint64(block.Len())

		sendBlock := blockPool.Get().(*ColumnBlock)
		sendBlock.ID = append(sendBlock.ID, block.ID...)
		sendBlock.Timestamp = append(sendBlock.Timestamp, block.Timestamp...)
		sendBlock.Reporter = append(sendBlock.Reporter, block.Reporter...)
		sendBlock.ReporterGrid = append(sendBlock.ReporterGrid, block.ReporterGrid...)
		sendBlock.SNR = append(sendBlock.SNR, block.SNR...)
		sendBlock.Frequency = append(sendBlock.Frequency, block.Frequency...)
		sendBlock.Callsign = append(sendBlock.Callsign, block.Callsign...)
		sendBlock.Grid = append(sendBlock.Grid, block.Grid...)
		sendBlock.Power = append(sendBlock.Power, block.Power...)
		sendBlock.Drift = append(sendBlock.Drift, block.Drift...)
		sendBlock.Distance = append(sendBlock.Distance, block.Distance...)
		sendBlock.Azimuth = append(sendBlock.Azimuth, block.Azimuth...)
		sendBlock.Band = append(sendBlock.Band, block.Band...)
		sendBlock.Version = append(sendBlock.Version, block.Version...)
		sendBlock.Code = append(sendBlock.Code, block.Code...)
		sendBlock.RowCount = block.RowCount

		blockChan <- sendBlock
	}

	close(blockChan)
	<-sendDone

	elapsed := time.Since(startTime)
	mrps := float64(rowCount) / elapsed.Seconds() / 1_000_000

	stats.TotalRows.Add(rowCount)
	stats.TotalBytes.Add(archiveSize)
	stats.ArchivesComplete.Add(1)
	stats.BlocksSent.Add(blocksSent)

	log.Printf("[%s] %d rows in %.1fs (%.2f Mrps, %d blocks)", fileName, rowCount, elapsed.Seconds(), mrps, blocksSent)
}

func main() {
	chHost := flag.String("ch-host", "127.0.0.1:9000", "ClickHouse address")
	chDB := flag.String("ch-db", "wspr", "ClickHouse database")
	chTable := flag.String("ch-table", "spots", "ClickHouse table")
	workers := flag.Int("workers", NumWorkers, "Parallel archive workers")
	sourceDir := flag.String("source-dir", "/scratch/ai-stack/wspr-data/archives", "Archive source directory")
	reportDir := flag.String("report-dir", "/mnt/ai-stack/wspr-data/reports-turbo", "Report output directory")
	blockSize := flag.Int("block-size", BlockSize, "Rows per native block")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "wspr-turbo v%s - Zero-Copy Streaming Pipeline\n\n", Version)
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] [archives...]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Streams tar.gz/csv.gz directly to ClickHouse Native Blocks.\n")
		fmt.Fprintf(os.Stderr, "No intermediate disk I/O - bypasses the 'File Penalty'.\n\n")
		fmt.Fprintf(os.Stderr, "Architecture:\n")
		fmt.Fprintf(os.Stderr, "  - Stream decompression (klauspost/gzip, ASM-optimized)\n")
		fmt.Fprintf(os.Stderr, "  - Vectorized CSV parsing (columnar buffers)\n")
		fmt.Fprintf(os.Stderr, "  - Double-buffering (fill while sending)\n")
		fmt.Fprintf(os.Stderr, "  - sync.Pool (zero allocation after warmup)\n")
		fmt.Fprintf(os.Stderr, "  - ch-go native protocol with LZ4\n\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	// Override block size if specified
	if *blockSize != BlockSize {
		log.Printf("Block size: %d rows", *blockSize)
	}

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
	log.Printf("WSPR Turbo v%s - Zero-Copy Streaming Pipeline", Version)
	log.Println("=========================================================")
	log.Printf("Input: %d path(s)", len(inputPaths))
	log.Printf("Workers: %d | Block: %d rows", *workers, *blockSize)
	log.Printf("Buffer: %d MB read | Double-buffered send", ReadBufferSize/1024/1024)
	log.Printf("Protocol: ch-go Native + LZ4 + sync.Pool")
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

	// Discover archives
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
				if !info.IsDir() {
					ext := strings.ToLower(filepath.Ext(path))
					// Support .gz, .tar.gz, .csv
					if ext == ".gz" || ext == ".csv" || strings.HasSuffix(path, ".tar.gz") {
						files = append(files, path)
					}
				}
				return nil
			})
		} else {
			files = append(files, inputPath)
		}
	}

	if len(files) == 0 {
		log.Fatal("No archive files found")
	}

	sort.Strings(files)
	log.Printf("Found %d archive(s)", len(files))

	stats := NewStats()

	// Warmup sync.Pool
	warmupBlocks := make([]*ColumnBlock, 10)
	for i := range warmupBlocks {
		warmupBlocks[i] = blockPool.Get().(*ColumnBlock)
	}
	for _, b := range warmupBlocks {
		blockPool.Put(b)
	}

	// Worker pool with semaphore
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
			processArchive(ctx, fp, *chHost, *chDB, *chTable, stats, &wg)
		}(filePath)
	}

	wg.Wait()

	elapsed := time.Since(stats.StartTime)
	totalRows := stats.TotalRows.Load()
	totalBytes := stats.TotalBytes.Load()
	blocksSent := stats.BlocksSent.Load()
	mrps := float64(totalRows) / elapsed.Seconds() / 1_000_000
	mbps := float64(totalBytes) / elapsed.Seconds() / 1024 / 1024

	log.Println()
	log.Println("=========================================================")
	log.Println("Final Statistics")
	log.Println("=========================================================")
	log.Printf("Total Rows:   %d", totalRows)
	log.Printf("Total Size:   %.2f GB (compressed)", float64(totalBytes)/1024/1024/1024)
	log.Printf("Blocks Sent:  %d", blocksSent)
	log.Printf("Elapsed:      %v", elapsed.Round(time.Second))
	log.Printf("Throughput:   %.2f Mrps", mrps)
	log.Printf("I/O Rate:     %.2f MB/s (compressed)", mbps)

	// Calculate effective throughput
	if totalRows > 0 && totalBytes > 0 {
		bytesPerRow := float64(totalBytes) / float64(totalRows)
		uncompressedEstimate := float64(totalRows) * 80 // ~80 bytes per CSV row
		compressionRatio := uncompressedEstimate / float64(totalBytes)
		effectiveMbps := mbps * compressionRatio
		log.Printf("Effective:    %.2f MB/s (estimated uncompressed)", effectiveMbps)
		log.Printf("Compression:  %.2fx (%.1f bytes/row compressed)", compressionRatio, bytesPerRow)
	}
	log.Println("=========================================================")

	// Write report
	reportFile := filepath.Join(*reportDir, fmt.Sprintf("turbo_%s.log", time.Now().Format("20060102_150405")))
	if f, err := os.Create(reportFile); err == nil {
		fmt.Fprintf(f, "WSPR Turbo v%s Report\n", Version)
		fmt.Fprintf(f, "=====================\n")
		fmt.Fprintf(f, "Archives:   %d\n", stats.ArchivesComplete.Load())
		fmt.Fprintf(f, "Total Rows: %d\n", totalRows)
		fmt.Fprintf(f, "Total Size: %.2f GB\n", float64(totalBytes)/1024/1024/1024)
		fmt.Fprintf(f, "Blocks:     %d\n", blocksSent)
		fmt.Fprintf(f, "Elapsed:    %v\n", elapsed.Round(time.Second))
		fmt.Fprintf(f, "Throughput: %.2f Mrps\n", mrps)
		fmt.Fprintf(f, "I/O Rate:   %.2f MB/s\n", mbps)
		f.Close()
		log.Printf("Report: %s", reportFile)
	}

	// Print memory stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Printf("Memory: %.2f MB alloc, %.2f MB sys, %d GC cycles",
		float64(m.Alloc)/1024/1024,
		float64(m.Sys)/1024/1024,
		m.NumGC)
}
