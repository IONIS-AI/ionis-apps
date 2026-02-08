// contest-ingest — Parse Cabrillo contest logs into ClickHouse
//
// Walks /mnt/contest-logs/{contest}/{yearmode}/*.log, parses Cabrillo headers
// and QSO lines, normalizes band via bands.GetBand(), and batch INSERTs into
// contest.bronze using ch-go native protocol with LZ4 compression.
//
// Optionally extracts GRID-LOCATOR headers and enriches wspr.callsign_grid.
//
// Build: CGO_ENABLED=0 go build -ldflags="-s -w" -o build/contest-ingest ./cmd/contest-ingest

package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
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

var Version = "dev"

const (
	DefaultBatchSize = 100_000
	DefaultWorkers   = 8
)

// callsignRe matches amateur radio callsigns: 1-3 prefix chars, a digit, 0-3 suffix chars, ending with a letter.
// Covers: K1ABC, JA1XYZ, 3DA0NW, VK9DWX, etc.
var callsignRe = regexp.MustCompile(`^[A-Z0-9]{1,3}[0-9][A-Z0-9]{0,3}[A-Z]$`)

// gridRe matches 4- or 6-character Maidenhead grid locators.
var gridRe = regexp.MustCompile(`^[A-R]{2}[0-9]{2}([A-X]{2})?$`)

// Stats tracks ingestion metrics with atomic operations.
type Stats struct {
	TotalRows   atomic.Uint64
	TotalFiles  atomic.Uint64
	SkippedRows atomic.Uint64
	FailedFiles atomic.Uint64
	GridsFound  atomic.Uint64
	StartTime   time.Time
}

// CabrilloHeaders holds parsed header values from a Cabrillo log.
type CabrilloHeaders struct {
	Callsign string
	Contest  string
	Grid     string // from GRID-LOCATOR or HQ-GRID-LOCATOR
}

// QSO represents a single parsed contest QSO.
type QSO struct {
	Timestamp time.Time
	Frequency uint32
	Band      int32
	Mode      string
	Call1     string
	Call2     string
	RSTSent   string
	ExchSent  string
	RSTRcvd   string
	ExchRcvd  string
	Contest   string
	Source    string
}

// ContestBatch holds columnar data for a batch INSERT into contest.bronze.
type ContestBatch struct {
	Timestamp *proto.ColDateTime
	Frequency *proto.ColUInt32
	Band      *proto.ColInt32
	Mode      *proto.ColLowCardinality[string]
	Call1     *proto.ColStr
	Call2     *proto.ColStr
	RSTSent   *proto.ColStr
	ExchSent  *proto.ColStr
	RSTRcvd   *proto.ColStr
	ExchRcvd  *proto.ColStr
	Contest   *proto.ColLowCardinality[string]
	Source    *proto.ColLowCardinality[string]
}

func NewContestBatch() *ContestBatch {
	return &ContestBatch{
		Timestamp: new(proto.ColDateTime),
		Frequency: new(proto.ColUInt32),
		Band:      new(proto.ColInt32),
		Mode:      new(proto.ColStr).LowCardinality(),
		Call1:     new(proto.ColStr),
		Call2:     new(proto.ColStr),
		RSTSent:   new(proto.ColStr),
		ExchSent:  new(proto.ColStr),
		RSTRcvd:   new(proto.ColStr),
		ExchRcvd:  new(proto.ColStr),
		Contest:   new(proto.ColStr).LowCardinality(),
		Source:    new(proto.ColStr).LowCardinality(),
	}
}

func (b *ContestBatch) Reset() {
	b.Timestamp.Reset()
	b.Frequency.Reset()
	b.Band.Reset()
	b.Mode.Reset()
	b.Call1.Reset()
	b.Call2.Reset()
	b.RSTSent.Reset()
	b.ExchSent.Reset()
	b.RSTRcvd.Reset()
	b.ExchRcvd.Reset()
	b.Contest.Reset()
	b.Source.Reset()
}

func (b *ContestBatch) Len() int {
	return b.Timestamp.Rows()
}

func (b *ContestBatch) Input() proto.Input {
	return proto.Input{
		{Name: "timestamp", Data: b.Timestamp},
		{Name: "frequency", Data: b.Frequency},
		{Name: "band", Data: b.Band},
		{Name: "mode", Data: b.Mode},
		{Name: "call_1", Data: b.Call1},
		{Name: "call_2", Data: b.Call2},
		{Name: "rst_sent", Data: b.RSTSent},
		{Name: "exch_sent", Data: b.ExchSent},
		{Name: "rst_rcvd", Data: b.RSTRcvd},
		{Name: "exch_rcvd", Data: b.ExchRcvd},
		{Name: "contest", Data: b.Contest},
		{Name: "source", Data: b.Source},
	}
}

var batchPool = sync.Pool{
	New: func() interface{} { return NewContestBatch() },
}

// isCallsign checks if a string looks like an amateur radio callsign.
// Must contain both a letter and a digit, be 3+ chars, and match the callsign pattern.
// Also accepts callsigns with /suffix (e.g., HB9DAX/QRP, W1AW/4).
func isCallsign(s string) bool {
	// Strip portable/QRP suffixes for pattern matching
	base := strings.ToUpper(s)
	if idx := strings.Index(base, "/"); idx > 0 {
		base = base[:idx]
	}
	if len(base) < 2 {
		return false
	}
	return callsignRe.MatchString(base)
}

// parseQSOLine extracts fields from a Cabrillo QSO line.
// Universal fields (same position in all Cabrillo formats):
//
//	[0]=QSO: [1]=freq [2]=mode [3]=date [4]=time [5]=my_call [6]=rst_sent [7..N-1]=exch_sent
//	[N]=their_call [N+1]=rst_rcvd [N+2..end]=exch_rcvd
//
// Their-call is found by scanning from index 6 for the next callsign that differs from my_call.
func parseQSOLine(fields []string, myCall string) (*QSO, error) {
	if len(fields) < 9 {
		return nil, fmt.Errorf("too few fields: %d", len(fields))
	}

	// Strip "QSO:" prefix if present as field[0] (case-insensitive)
	startIdx := 0
	if strings.EqualFold(strings.TrimRight(fields[0], ":"), "QSO") {
		startIdx = 1
	}

	f := fields[startIdx:]
	if len(f) < 8 {
		return nil, fmt.Errorf("too few fields after QSO: %d", len(f))
	}

	// f[0]=freq f[1]=mode f[2]=date f[3]=time f[4]=my_call f[5..]=rst+exch+their_call+...
	// Frequency may be integer (7000) or decimal (1868.79) depending on logging software
	freqFloat, err := strconv.ParseFloat(f[0], 64)
	if err != nil {
		return nil, fmt.Errorf("bad freq %q: %w", f[0], err)
	}
	freqKHz := uint64(freqFloat)

	mode := strings.ToUpper(f[1])

	// Parse date + time → timestamp
	// Strip trailing 'Z' or 'z' from time (some logs use "1257Z" instead of "1257")
	timeStr := strings.TrimRight(f[3], "Zz")
	// Truncate 6-digit HHMMSS to 4-digit HHMM (some loggers emit seconds)
	if len(timeStr) > 4 {
		timeStr = timeStr[:4]
	}
	// Zero-pad short times (old Cabrillo v1 logs may have "1" instead of "0001")
	for len(timeStr) < 4 {
		timeStr = "0" + timeStr
	}
	// Try ISO date first (2006-01-02), then MM/DD/YYYY (old Cabrillo v1)
	ts, err := time.Parse("2006-01-02 1504", f[2]+" "+timeStr)
	if err != nil {
		ts, err = time.Parse("01/02/2006 1504", f[2]+" "+timeStr)
		if err != nil {
			return nil, fmt.Errorf("bad timestamp %q %q: %w", f[2], f[3], err)
		}
	}

	logCall := strings.ToUpper(f[4])

	// Band: freq is kHz, GetBand expects MHz
	bandID, _ := bands.GetBand(float64(freqKHz) / 1000.0)

	// Find their_call: scan from index 5 (after my_call) looking for a callsign != my_call
	theirIdx := -1
	for i := 5; i < len(f); i++ {
		candidate := strings.ToUpper(f[i])
		if isCallsign(candidate) && !strings.EqualFold(candidate, logCall) {
			theirIdx = i
			break
		}
	}

	if theirIdx < 0 {
		return nil, fmt.Errorf("no their_call found")
	}

	theirCall := strings.ToUpper(f[theirIdx])

	// Extract RST and exchange fields
	rstSent := ""
	exchSent := ""
	rstRcvd := ""
	exchRcvd := ""

	// Fields between my_call and their_call: rst_sent + exch_sent
	if theirIdx > 5 {
		rstSent = f[5]
		if theirIdx > 6 {
			exchSent = strings.Join(f[6:theirIdx], " ")
		}
	}

	// Fields after their_call: rst_rcvd + exch_rcvd
	afterTheir := theirIdx + 1
	if afterTheir < len(f) {
		rstRcvd = f[afterTheir]
		if afterTheir+1 < len(f) {
			exchRcvd = strings.Join(f[afterTheir+1:], " ")
		}
	}

	return &QSO{
		Timestamp: ts,
		Frequency: uint32(freqKHz),
		Band:      bandID,
		Mode:      mode,
		Call1:     logCall,
		Call2:     theirCall,
		RSTSent:   rstSent,
		ExchSent:  exchSent,
		RSTRcvd:   rstRcvd,
		ExchRcvd:  exchRcvd,
	}, nil
}

// parseFile reads a Cabrillo log file and returns headers + QSOs + skipped count.
func parseFile(path, myCallOverride string) (*CabrilloHeaders, []*QSO, int, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, 0, err
	}
	defer file.Close()

	headers := &CabrilloHeaders{}
	var qsos []*QSO
	var skipped int

	scanner := bufio.NewScanner(file)
	// Some contest logs have very long lines
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		// Replace non-breaking spaces (0xA0) with regular spaces.
		// CTESTWIN, UcxLog, and other loggers pad fixed-width fields with NBSP.
		line = strings.ReplaceAll(line, "\xa0", " ")
		trimmed := strings.TrimSpace(line)

		if trimmed == "" {
			continue
		}

		// Check for END-OF-LOG
		if strings.HasPrefix(strings.ToUpper(trimmed), "END-OF-LOG") {
			break
		}

		// Parse headers
		upper := strings.ToUpper(trimmed)
		if strings.HasPrefix(upper, "CALLSIGN:") {
			headers.Callsign = strings.ToUpper(strings.TrimSpace(trimmed[9:]))
		} else if strings.HasPrefix(upper, "CONTEST:") {
			headers.Contest = strings.ToUpper(strings.TrimSpace(trimmed[8:]))
		} else if strings.HasPrefix(upper, "GRID-LOCATOR:") {
			g := strings.ToUpper(strings.TrimSpace(trimmed[13:]))
			if gridRe.MatchString(g) {
				headers.Grid = g
			}
		} else if strings.HasPrefix(upper, "HQ-GRID-LOCATOR:") {
			g := strings.ToUpper(strings.TrimSpace(trimmed[16:]))
			if gridRe.MatchString(g) {
				headers.Grid = g
			}
		} else if strings.HasPrefix(upper, "QSO:") {
			// Parse QSO line
			fields := strings.Fields(trimmed)
			myCall := headers.Callsign
			if myCallOverride != "" {
				myCall = myCallOverride
			}
			if myCall == "" {
				skipped++
				continue
			}

			qso, err := parseQSOLine(fields, myCall)
			if err != nil {
				skipped++
				continue
			}
			qsos = append(qsos, qso)
		}
	}

	if err := scanner.Err(); err != nil {
		return headers, qsos, skipped, fmt.Errorf("scanner error: %w", err)
	}

	if skipped > 0 && len(qsos) == 0 {
		return headers, nil, skipped, fmt.Errorf("all %d QSO lines failed to parse", skipped)
	}

	return headers, qsos, skipped, nil
}

// sourceKey derives a source identifier from a file path relative to srcDir.
// e.g., /mnt/contest-logs/cq-ww/2005cw/k1abc.log → cq-ww/2005cw
func sourceKey(path, srcDir string) string {
	rel, err := filepath.Rel(srcDir, path)
	if err != nil {
		return filepath.Base(filepath.Dir(path))
	}
	// rel = cq-ww/2005cw/k1abc.log → want cq-ww/2005cw
	dir := filepath.Dir(rel)
	return dir
}

// RejectWriter appends failed file paths and error reasons to a reject log.
// Thread-safe via mutex. If no path is configured, writes are silently discarded.
type RejectWriter struct {
	mu   sync.Mutex
	file *os.File
}

func NewRejectWriter(path string) (*RejectWriter, error) {
	if path == "" {
		return &RejectWriter{}, nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("reject log: %w", err)
	}
	return &RejectWriter{file: f}, nil
}

func (rw *RejectWriter) Write(relPath, reason string) {
	if rw.file == nil {
		return
	}
	rw.mu.Lock()
	defer rw.mu.Unlock()
	fmt.Fprintf(rw.file, "%s | %s\n", relPath, reason)
}

func (rw *RejectWriter) Close() {
	if rw.file != nil {
		rw.file.Close()
	}
}

// flushBatch sends the accumulated batch to ClickHouse.
func flushBatch(ctx context.Context, conn *ch.Client, tableFQN string, batch *ContestBatch) error {
	query := fmt.Sprintf(
		"INSERT INTO %s (timestamp, frequency, band, mode, call_1, call_2, "+
			"rst_sent, exch_sent, rst_rcvd, exch_rcvd, contest, source) VALUES",
		tableFQN,
	)
	return conn.Do(ctx, ch.Query{
		Body:  query,
		Input: batch.Input(),
	})
}

// GridEntry holds a callsign→grid mapping for enrichment.
type GridEntry struct {
	Callsign string
	Grid     string
}

// flushGrids batch-inserts callsign→grid mappings into wspr.callsign_grid.
func flushGrids(ctx context.Context, conn *ch.Client, entries []GridEntry) error {
	if len(entries) == 0 {
		return nil
	}

	callsign := new(proto.ColStr)
	grid := &proto.ColFixedStr{Size: 6}
	grid4 := &proto.ColFixedStr{Size: 4}
	spotCount := new(proto.ColUInt32)
	lastSeen := new(proto.ColDate)

	today := time.Now().UTC()

	for _, e := range entries {
		callsign.Append(e.Callsign)

		// Pad grid to 6 chars if it's only 4
		g := e.Grid
		if len(g) == 4 {
			g += "MM" // center of grid square
		}
		if len(g) < 6 {
			g = g + strings.Repeat(" ", 6-len(g))
		}
		grid.Append([]byte(g[:6]))
		grid4.Append([]byte(g[:4]))
		spotCount.Append(0)
		lastSeen.Append(today)
	}

	return conn.Do(ctx, ch.Query{
		Body: "INSERT INTO wspr.callsign_grid (callsign, grid, grid_4, spot_count, last_seen) VALUES",
		Input: proto.Input{
			{Name: "callsign", Data: callsign},
			{Name: "grid", Data: grid},
			{Name: "grid_4", Data: grid4},
			{Name: "spot_count", Data: spotCount},
			{Name: "last_seen", Data: lastSeen},
		},
	})
}

// processFile parses a single Cabrillo log and inserts QSOs using a shared connection.
func processFile(ctx context.Context, conn *ch.Client, path, srcDir, db, table string, batchSize int, enrich bool, stats *Stats, rejectWriter *RejectWriter) {
	fileName := filepath.Base(path)
	src := sourceKey(path, srcDir)
	relPath := filepath.Join(src, fileName)

	headers, qsos, skipped, err := parseFile(path, "")
	if skipped > 0 {
		stats.SkippedRows.Add(uint64(skipped))
	}
	if err != nil {
		log.Printf("[%s] parse error: %v", relPath, err)
		stats.FailedFiles.Add(1)
		rejectWriter.Write(relPath, err.Error())
		return
	}

	if len(qsos) == 0 {
		stats.FailedFiles.Add(1)
		rejectWriter.Write(relPath, "0 QSOs parsed")
		return
	}

	tableFQN := fmt.Sprintf("%s.%s", db, table)

	batch := batchPool.Get().(*ContestBatch)
	defer batchPool.Put(batch)
	batch.Reset()

	contestID := headers.Contest
	var rowCount uint64

	for _, qso := range qsos {
		if ctx.Err() != nil {
			return
		}

		batch.Timestamp.Append(qso.Timestamp)
		batch.Frequency.Append(qso.Frequency)
		batch.Band.Append(qso.Band)
		batch.Mode.Append(qso.Mode)
		batch.Call1.Append(qso.Call1)
		batch.Call2.Append(qso.Call2)
		batch.RSTSent.Append(qso.RSTSent)
		batch.ExchSent.Append(qso.ExchSent)
		batch.RSTRcvd.Append(qso.RSTRcvd)
		batch.ExchRcvd.Append(qso.ExchRcvd)
		batch.Contest.Append(contestID)
		batch.Source.Append(src)

		rowCount++

		if batch.Len() >= batchSize {
			if err := flushBatch(ctx, conn, tableFQN, batch); err != nil {
				log.Printf("[%s] flush error: %v", relPath, err)
			}
			batch.Reset()
		}
	}

	// Flush remainder
	if batch.Len() > 0 {
		if err := flushBatch(ctx, conn, tableFQN, batch); err != nil {
			log.Printf("[%s] final flush error: %v", relPath, err)
		}
		batch.Reset()
	}

	stats.TotalRows.Add(rowCount)
	stats.TotalFiles.Add(1)

	// Grid enrichment
	if enrich && headers.Grid != "" && headers.Callsign != "" {
		entry := GridEntry{
			Callsign: headers.Callsign,
			Grid:     headers.Grid,
		}
		if err := flushGrids(ctx, conn, []GridEntry{entry}); err != nil {
			log.Printf("[%s] grid enrich error: %v", relPath, err)
		} else {
			stats.GridsFound.Add(1)
		}
	}
}

// worker processes files from a channel using a persistent ClickHouse connection.
func worker(ctx context.Context, id int, files <-chan string, srcDir, host, db, table string, batchSize int, enrich bool, stats *Stats, rejectWriter *RejectWriter, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := ch.Dial(ctx, ch.Options{
		Address:     host,
		Database:    db,
		Compression: ch.CompressionLZ4,
	})
	if err != nil {
		log.Printf("[worker-%d] ClickHouse connect error: %v", id, err)
		return
	}
	defer conn.Close()

	for path := range files {
		if ctx.Err() != nil {
			return
		}
		processFile(ctx, conn, path, srcDir, db, table, batchSize, enrich, stats, rejectWriter)
	}
}

// discoverFiles walks srcDir for .log files, optionally filtered by contest key.
func discoverFiles(srcDir, contest string) ([]string, error) {
	var files []string

	if contest != "" {
		// Walk only the specific contest directory
		contestDir := filepath.Join(srcDir, contest)
		err := filepath.Walk(contestDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if !info.IsDir() && strings.HasSuffix(strings.ToLower(path), ".log") {
				// Skip manifest and download log files
				base := strings.ToLower(filepath.Base(path))
				if base == "manifest.txt" || base == "download.log" {
					return nil
				}
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	} else {
		// Walk all subdirectories
		err := filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if !info.IsDir() && strings.HasSuffix(strings.ToLower(path), ".log") {
				base := strings.ToLower(filepath.Base(path))
				if base == "manifest.txt" || base == "download.log" {
					return nil
				}
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
	src := flag.String("src", "/mnt/contest-logs", "Source directory with {contest}/{yearmode}/*.log")
	host := flag.String("host", "192.168.1.90:9000", "ClickHouse host:port")
	db := flag.String("db", "contest", "ClickHouse database")
	table := flag.String("table", "bronze", "ClickHouse table")
	workers := flag.Int("workers", DefaultWorkers, "Parallel file workers")
	batchSize := flag.Int("batch", DefaultBatchSize, "Rows per INSERT batch")
	contest := flag.String("contest", "", "Process only this contest key (empty = all)")
	enrich := flag.Bool("enrich", false, "Also insert GRID-LOCATOR into wspr.callsign_grid")
	rejectLog := flag.String("reject-log", "", "Append failed files to this reject log (empty = disabled)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "contest-ingest v%s — Parse Cabrillo contest logs into ClickHouse\n\n", Version)
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Walks --src/{contest}/{yearmode}/*.log, parses Cabrillo headers\n")
		fmt.Fprintf(os.Stderr, "and QSO lines, normalizes band via ADIF lookup, and inserts into\n")
		fmt.Fprintf(os.Stderr, "ClickHouse using ch-go native protocol with LZ4 compression.\n\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  contest-ingest --contest cq-ww --workers 4\n")
		fmt.Fprintf(os.Stderr, "  contest-ingest --enrich\n")
		fmt.Fprintf(os.Stderr, "  contest-ingest --src /mnt/contest-logs --host 10.60.1.1:9000\n")
	}

	flag.Parse()

	log.Println("=========================================================")
	log.Printf("Contest Ingest v%s", Version)
	log.Println("=========================================================")
	log.Printf("Source:   %s", *src)
	log.Printf("Target:   %s.%s @ %s", *db, *table, *host)
	log.Printf("Workers:  %d | Batch: %d", *workers, *batchSize)
	log.Printf("CPUs:     %d", runtime.NumCPU())
	log.Printf("Enrich:   %v", *enrich)
	if *rejectLog != "" {
		log.Printf("Reject:   %s", *rejectLog)
	}
	if *contest != "" {
		log.Printf("Contest:  %s", *contest)
	} else {
		log.Printf("Contest:  all")
	}

	rejectWriter, err := NewRejectWriter(*rejectLog)
	if err != nil {
		log.Fatalf("Cannot open reject log: %v", err)
	}
	defer rejectWriter.Close()

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

	// Discover .log files
	files, err := discoverFiles(*src, *contest)
	if err != nil {
		log.Fatalf("File discovery failed: %v", err)
	}
	if len(files) == 0 {
		log.Fatal("No .log files found")
	}
	log.Printf("Found %d .log file(s)", len(files))
	log.Println("=========================================================")

	stats := &Stats{StartTime: time.Now()}

	// Worker pool with persistent connections (one CH connection per worker)
	fileChan := make(chan string, *workers*2)
	var wg sync.WaitGroup

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go worker(ctx, i, fileChan, *src, *host, *db, *table, *batchSize, *enrich, stats, rejectWriter, &wg)
	}

	for _, logPath := range files {
		if ctx.Err() != nil {
			break
		}
		fileChan <- logPath
	}
	close(fileChan)

	wg.Wait()

	elapsed := time.Since(stats.StartTime)
	totalRows := stats.TotalRows.Load()
	totalFiles := stats.TotalFiles.Load()
	failedFiles := stats.FailedFiles.Load()
	skippedRows := stats.SkippedRows.Load()
	gridsFound := stats.GridsFound.Load()

	rps := float64(0)
	if elapsed.Seconds() > 0 {
		rps = float64(totalRows) / elapsed.Seconds()
	}

	log.Println()
	log.Println("=========================================================")
	log.Println("Final Statistics")
	log.Println("=========================================================")
	log.Printf("Files OK:       %d", totalFiles)
	log.Printf("Files Failed:   %d", failedFiles)
	log.Printf("QSOs Inserted:  %d", totalRows)
	log.Printf("QSOs Skipped:   %d", skippedRows)
	log.Printf("Grids Enriched: %d", gridsFound)
	log.Printf("Elapsed:        %v", elapsed.Round(time.Second))
	if rps > 1_000_000 {
		log.Printf("Throughput:     %.2f Mrps", rps/1_000_000)
	} else {
		log.Printf("Throughput:     %.0f rows/s", rps)
	}
	if *rejectLog != "" && failedFiles > 0 {
		log.Printf("Reject log:     %s (%d entries)", *rejectLog, failedFiles)
	}
	log.Println("=========================================================")
}

// Ensure net import is used (for ch-go).
var _ = net.Dial
