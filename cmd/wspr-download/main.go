// wspr-download - Download WSPR spot archives from wsprnet.org
//
// Data source: https://wsprnet.org/archive/
// Format: Monthly .csv.gz files (~200MB-1GB each)
// Range: 2008-03 to present
//
// Good neighbor policy:
//   - Configurable parallelism (default 4 workers — appropriate for ~200 large files)
//   - Configurable delay between requests (default 1s)
//   - Honest User-Agent identification
//   - Resume-friendly: ETag-based cache validation detects stale/partial files
//   - Graceful shutdown on Ctrl+C
//
// Build: CGO_ENABLED=0 go build -ldflags="-s -w" -o build/wspr-download ./cmd/wspr-download

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Version can be overridden at build time via -ldflags
var Version = "dev"

var userAgent = fmt.Sprintf("wspr-download/%s (ionis-apps)", Version)

const (
	baseURL    = "https://wsprnet.org/archive"
	filePrefix = "wsprspots"
)

type downloadStats struct {
	Completed atomic.Uint64
	Failed    atomic.Uint64
	// Missing counts archives wsprnet has not published. Tracked apart from Failed so
	// "upstream has no data yet" cannot masquerade as "our download broke".
	Missing atomic.Uint64
	Skipped atomic.Uint64
	Updated atomic.Uint64
	Bytes   atomic.Uint64
}

// remoteETag does a HEAD request and returns the ETag header value.
func remoteETag(ctx context.Context, client *http.Client, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "HEAD", url, nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", userAgent)

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("HTTP HEAD: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", errNotPublished
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	return resp.Header.Get("ETag"), nil
}

// readETag reads a saved ETag from a sidecar file.
func readETag(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// writeETag saves an ETag to a sidecar file.
func writeETag(path, etag string) error {
	return os.WriteFile(path, []byte(etag+"\n"), 0644)
}

func downloadFile(ctx context.Context, client *http.Client, url, destPath string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", userAgent)

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("HTTP GET: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return 0, errNotPublished
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	tmpPath := destPath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return 0, fmt.Errorf("create file: %w", err)
	}

	n, err := io.Copy(f, resp.Body)
	f.Close()

	if err != nil {
		os.Remove(tmpPath)
		return 0, fmt.Errorf("write: %w", err)
	}

	if err := os.Rename(tmpPath, destPath); err != nil {
		os.Remove(tmpPath)
		return 0, fmt.Errorf("rename: %w", err)
	}

	// Save ETag sidecar
	if etag := resp.Header.Get("ETag"); etag != "" {
		writeETag(destPath+".etag", etag)
	}

	return n, nil
}

// errNotPublished marks an archive wsprnet.org has not published. It is NOT a failure:
// generateFileList always enumerates through the CURRENT month, so the in-progress month
// is guaranteed to 404 until wsprnet finalises it. Treating that as an error made this
// unit go red at the start of every single month, by construction — and when wsprnet
// stopped publishing entirely after 2026-05, it went permanently red. Three months of a
// red unit that was reporting upstream's state correctly is how an alarm gets ignored.
//
// A 404 is never something a retry can fix, so it must not gate the exit code. It is
// still reported by name in the summary, and the real signal for "we are missing data"
// is bronze-table staleness in the health check, not this process's exit status.
var errNotPublished = errors.New("not published upstream (404)")

func generateFileList(startYear, startMonth, endYear, endMonth int) []string {
	var files []string

	for year := startYear; year <= endYear; year++ {
		monthStart := 1
		monthEnd := 12

		if year == startYear {
			monthStart = startMonth
		}
		if year == endYear {
			monthEnd = endMonth
		}

		for month := monthStart; month <= monthEnd; month++ {
			filename := fmt.Sprintf("%s-%d-%02d.csv.gz", filePrefix, year, month)
			files = append(files, filename)
		}
	}

	return files
}

func main() {
	destDir := flag.String("dest", "/mnt/wspr-data", "Destination directory")
	workers := flag.Int("workers", 4, "Parallel download workers")
	delay := flag.Duration("delay", 1*time.Second, "Delay between HTTP requests per worker")
	timeout := flag.Duration("timeout", 300*time.Second, "HTTP timeout per download")
	startDate := flag.String("start", "2008-03", "Start date (YYYY-MM)")
	endDate := flag.String("end", "", "End date (YYYY-MM, default: current month)")
	listOnly := flag.Bool("list", false, "List files without downloading")
	force := flag.Bool("force", false, "Re-download all files regardless of ETag")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "wspr-download v%s — WSPR Archive Downloader\n\n", Version)
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Downloads WSPR spot archives from wsprnet.org.\n")
		fmt.Fprintf(os.Stderr, "Archives are monthly .csv.gz files (~200MB-1GB each).\n")
		fmt.Fprintf(os.Stderr, "Uses ETag validation to detect updated files (e.g. end-of-month finalization).\n")
		fmt.Fprintf(os.Stderr, "Good neighbor: configurable workers/delay, resume-friendly.\n\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nData source: %s\n", baseURL)
		fmt.Fprintf(os.Stderr, "Archive range: 2008-03 to present (~200 files)\n")
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  wspr-download                              # Download all, skip unchanged\n")
		fmt.Fprintf(os.Stderr, "  wspr-download --start 2024-01 --end 2024-12  # Download 2024 only\n")
		fmt.Fprintf(os.Stderr, "  wspr-download --list                        # List files without downloading\n")
		fmt.Fprintf(os.Stderr, "  wspr-download --workers 2 --delay 3s        # Be extra polite\n")
		fmt.Fprintf(os.Stderr, "  wspr-download --force                       # Re-download everything\n")
	}

	flag.Parse()

	// Parse start date
	var startYear, startMonth int
	fmt.Sscanf(*startDate, "%d-%d", &startYear, &startMonth)
	if startYear < 2008 || startMonth < 1 || startMonth > 12 {
		fmt.Fprintf(os.Stderr, "Error: Invalid start date. Use YYYY-MM format (earliest: 2008-03)\n")
		os.Exit(1)
	}

	// Parse end date
	var endYear, endMonth int
	if *endDate == "" {
		now := time.Now()
		endYear = now.Year()
		endMonth = int(now.Month())
	} else {
		fmt.Sscanf(*endDate, "%d-%d", &endYear, &endMonth)
		if endYear < 2008 || endMonth < 1 || endMonth > 12 {
			fmt.Fprintf(os.Stderr, "Error: Invalid end date. Use YYYY-MM format\n")
			os.Exit(1)
		}
	}

	files := generateFileList(startYear, startMonth, endYear, endMonth)

	if *listOnly {
		fmt.Printf("wspr-download v%s\n\n", Version)
		fmt.Printf("WSPR Archives (%d files):\n\n", len(files))
		for _, f := range files {
			fmt.Printf("  %s/%s\n", baseURL, f)
		}
		return
	}

	// Set up graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Fprintf(os.Stderr, "\nInterrupt received, finishing current downloads...\n")
		cancel()
	}()

	// Banner
	fmt.Printf("wspr-download v%s\n", Version)
	fmt.Printf("User-Agent: %s\n", userAgent)
	fmt.Printf("Source:      %s\n", baseURL)
	fmt.Printf("Destination: %s\n", *destDir)
	fmt.Printf("Date Range:  %s to %04d-%02d\n", *startDate, endYear, endMonth)
	fmt.Printf("Files:       %d archives\n", len(files))
	fmt.Printf("Workers:     %d parallel\n", *workers)
	fmt.Printf("Delay:       %v per worker\n", *delay)
	fmt.Printf("Timeout:     %v per file\n", *timeout)
	if *force {
		fmt.Printf("Mode:        force (ignoring ETags)\n")
	}
	fmt.Println()

	// Create destination directory
	if err := os.MkdirAll(*destDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Cannot create directory: %v\n", err)
		os.Exit(1)
	}

	// HTTP client
	client := &http.Client{Timeout: *timeout}
	stats := &downloadStats{}
	startTime := time.Now()

	// Worker pool
	sem := make(chan struct{}, *workers)
	// Collected under a mutex so the summary can name the unpublished months rather than
	// just counting them — "which months are missing" is the operator's actual question.
	var missingMu sync.Mutex
	var missingFiles []string

	var wg sync.WaitGroup

	for _, filename := range files {
		if ctx.Err() != nil {
			break
		}

		sem <- struct{}{}
		wg.Add(1)

		go func(fname string) {
			defer func() { <-sem }()
			defer wg.Done()

			if ctx.Err() != nil {
				return
			}

			url := fmt.Sprintf("%s/%s", baseURL, fname)
			destPath := filepath.Join(*destDir, fname)
			etagPath := destPath + ".etag"

			// Check if file exists and validate ETag
			if !*force {
				if info, err := os.Stat(destPath); err == nil && info.Size() > 0 {
					localETag := readETag(etagPath)
					if localETag != "" {
						// We have a saved ETag — do a HEAD to check if remote changed
						remote, err := remoteETag(ctx, client, url)
						if err != nil {
							// HEAD failed — skip rather than re-download blindly
							stats.Skipped.Add(1)
							return
						}
						if remote == localETag {
							stats.Skipped.Add(1)
							return
						}
						// ETag mismatch — file was updated on server, re-download
						fmt.Printf("[%s] ETag changed, re-downloading\n", fname)
						sleepWithContext(ctx, *delay)
					} else {
						// File exists but no ETag saved (pre-ETag download).
						// Do a HEAD to get the ETag and save it, skip the download.
						remote, err := remoteETag(ctx, client, url)
						if err == nil && remote != "" {
							writeETag(etagPath, remote)
						}
						stats.Skipped.Add(1)
						return
					}
				}
			}

			size, err := downloadFile(ctx, client, url, destPath)
			if errors.Is(err, errNotPublished) {
				fmt.Printf("[%s] not published upstream yet\n", fname)
				stats.Missing.Add(1)
				missingMu.Lock()
				missingFiles = append(missingFiles, fname)
				missingMu.Unlock()
			} else if err != nil {
				fmt.Printf("[%s] FAILED: %v\n", fname, err)
				stats.Failed.Add(1)
			} else {
				stats.Bytes.Add(uint64(size))
				stats.Completed.Add(1)
				fmt.Printf("[%s] Downloaded (%s)\n", fname, formatBytes(size))
			}

			sleepWithContext(ctx, *delay)
		}(filename)
	}

	wg.Wait()

	elapsed := time.Since(startTime)
	completed := stats.Completed.Load()
	failed := stats.Failed.Load()
	missing := stats.Missing.Load()
	skipped := stats.Skipped.Load()
	bytes := stats.Bytes.Load()

	fmt.Println()
	fmt.Println("Summary:")
	fmt.Printf("  Downloaded: %d files (%s)\n", completed, formatBytes(int64(bytes)))
	fmt.Printf("  Skipped:    %d (unchanged)\n", skipped)
	fmt.Printf("  Failed:     %d\n", failed)
	if missing > 0 {
		fmt.Printf("  Missing:    %d (not published upstream)\n", missing)
	}
	fmt.Printf("  Elapsed:    %v\n", elapsed.Round(time.Second))
	if completed > 0 && elapsed.Seconds() > 0 {
		fmt.Printf("  Speed:      %.2f MB/s\n", float64(bytes)/elapsed.Seconds()/1024/1024)
	}
	if failed > 0 {
		fmt.Println("  Run again to resume failed files.")
	}
	if missing > 0 {
		sort.Strings(missingFiles)
		fmt.Println()
		fmt.Printf("  Not published upstream (%d) — nothing to retry, wsprnet has not released these:\n", missing)
		for _, f := range missingFiles {
			fmt.Printf("    %s\n", f)
		}
		fmt.Println("  This is upstream state, not a local failure. Data-gap alerting belongs to")
		fmt.Println("  bronze-table staleness in the health check, not to this exit code.")
	}

	// Exit non-zero ONLY for failures a retry could plausibly fix. An unpublished month
	// is upstream's state; failing on it makes the unit permanently red and trains
	// operators to ignore it — which is exactly what happened here for three months.
	if failed > 0 {
		os.Exit(1)
	}
}

// sleepWithContext sleeps for the given duration, returning early if ctx is cancelled.
func sleepWithContext(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}

// formatBytes returns a human-readable byte size.
func formatBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
