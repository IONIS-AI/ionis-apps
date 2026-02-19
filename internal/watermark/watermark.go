// Package watermark provides shared ingest watermark tracking for all IONIS data pipelines.
//
// Every data source has an {db}.ingest_log table (ReplacingMergeTree) that tracks which
// files have been loaded. This package provides common functions for loading the watermark,
// inserting log entries, and bootstrapping (priming) existing files.
//
// The pattern follows the pskr-ingest reference implementation:
//   - LoadWatermark returns a map[string]WatermarkEntry for fast lookups
//   - InsertLogEntry records a file after successful ingest
//   - PrimeFiles marks existing files without loading data (row_count=0)
//
// Usage in each ingester:
//
//	wm, err := watermark.LoadWatermark(ctx, host, "rbn")
//	// ... discover files, filter by watermark, process ...
//	watermark.InsertLogEntry(ctx, conn, "rbn", relPath, fileSize, rowCount, elapsedMs)
package watermark

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

// Entry holds a watermark record for a single file.
type Entry struct {
	FileSize uint64
	RowCount uint64
}

// LoadWatermark reads all previously loaded file paths from {db}.ingest_log.
// Returns a map from file_path → Entry (with file_size and row_count).
func LoadWatermark(ctx context.Context, host, db string) (map[string]Entry, error) {
	conn, err := ch.Dial(ctx, ch.Options{
		Address:  host,
		Database: db,
	})
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	wm := make(map[string]Entry)
	colPath := new(proto.ColStr)
	colSize := new(proto.ColUInt64)
	colRows := new(proto.ColUInt64)

	query := fmt.Sprintf("SELECT file_path, file_size, row_count FROM %s.ingest_log FINAL", db)
	err = conn.Do(ctx, ch.Query{
		Body: query,
		Result: proto.Results{
			{Name: "file_path", Data: colPath},
			{Name: "file_size", Data: colSize},
			{Name: "row_count", Data: colRows},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < colPath.Rows(); i++ {
				wm[colPath.Row(i)] = Entry{
					FileSize: colSize.Row(i),
					RowCount: colRows.Row(i),
				}
			}
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	return wm, nil
}

// InsertLogEntry records a processed file in {db}.ingest_log.
// The table uses ReplacingMergeTree(loaded_at), so re-runs update rather than duplicate.
func InsertLogEntry(ctx context.Context, conn *ch.Client, db, filePath string, fileSize, rowCount uint64, elapsedMs uint32) error {
	hostname, _ := os.Hostname()

	colPath := new(proto.ColStr)
	colSize := new(proto.ColUInt64)
	colRows := new(proto.ColUInt64)
	colElapsed := new(proto.ColUInt32)
	colHost := new(proto.ColStr).LowCardinality()

	colPath.Append(filePath)
	colSize.Append(fileSize)
	colRows.Append(rowCount)
	colElapsed.Append(elapsedMs)
	colHost.Append(hostname)

	query := fmt.Sprintf("INSERT INTO %s.ingest_log (file_path, file_size, row_count, elapsed_ms, hostname) VALUES", db)
	return conn.Do(ctx, ch.Query{
		Body: query,
		Input: proto.Input{
			{Name: "file_path", Data: colPath},
			{Name: "file_size", Data: colSize},
			{Name: "row_count", Data: colRows},
			{Name: "elapsed_ms", Data: colElapsed},
			{Name: "hostname", Data: colHost},
		},
	})
}

// PrimeFiles marks a list of files as loaded in the watermark without actually loading data.
// Primed entries have row_count=0 to distinguish them from real loads.
// Files already in the watermark are skipped.
func PrimeFiles(ctx context.Context, host, db string, files []FileInfo) (int, error) {
	wm, err := LoadWatermark(ctx, host, db)
	if err != nil {
		return 0, fmt.Errorf("load watermark: %w", err)
	}

	conn, err := ch.Dial(ctx, ch.Options{
		Address:  host,
		Database: db,
	})
	if err != nil {
		return 0, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	var primed int
	for _, fi := range files {
		if _, ok := wm[fi.RelPath]; ok {
			continue
		}
		if err := InsertLogEntry(ctx, conn, db, fi.RelPath, fi.Size, 0, 0); err != nil {
			log.Printf("[prime] error for %s: %v", fi.RelPath, err)
			continue
		}
		primed++
	}

	return primed, nil
}

// FileInfo holds the minimum information needed to prime a file.
type FileInfo struct {
	RelPath string
	Size    uint64
}
