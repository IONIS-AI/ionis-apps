// wspr-download - Parallel downloader for WSPR archives from wsprnet.org
//
// Downloads monthly WSPR spot archives in .csv.gz format.
// Supports resume, parallel downloads, and date range filtering.
//
// Build: CGO_ENABLED=0 go build -ldflags="-s -w" -o build/wspr-download ./cmd/wspr-download

package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// Version can be overridden at build time via -ldflags
var Version = "2.0.8"

const (
	BaseURL    = "https://wsprnet.org/archive"
	FilePrefix = "wsprspots"
)

type DownloadStats struct {
	Completed atomic.Uint64
	Failed    atomic.Uint64
	Skipped   atomic.Uint64
	Bytes     atomic.Uint64
}

func downloadFile(url, destPath string, timeout time.Duration, stats *DownloadStats) error {
	// Check if file already exists
	if info, err := os.Stat(destPath); err == nil && info.Size() > 0 {
		stats.Skipped.Add(1)
		return nil
	}

	client := &http.Client{
		Timeout: timeout,
	}

	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("HTTP GET failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("not found (404)")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	// Create temp file
	tmpPath := destPath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create file failed: %w", err)
	}

	n, err := io.Copy(f, resp.Body)
	f.Close()

	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("download failed: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, destPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename failed: %w", err)
	}

	stats.Bytes.Add(uint64(n))
	stats.Completed.Add(1)
	return nil
}

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
			filename := fmt.Sprintf("%s-%d-%02d.csv.gz", FilePrefix, year, month)
			files = append(files, filename)
		}
	}

	return files
}

func main() {
	destDir := flag.String("dest", "/mnt/ai-stack/wspr-data/raw", "Destination directory")
	workers := flag.Int("workers", 4, "Parallel download workers")
	timeout := flag.Duration("timeout", 300*time.Second, "HTTP timeout per download")
	startDate := flag.String("start", "2008-03", "Start date (YYYY-MM)")
	endDate := flag.String("end", "", "End date (YYYY-MM, default: current month)")
	listOnly := flag.Bool("list", false, "List files without downloading")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "wspr-download v%s - WSPR Archive Downloader\n\n", Version)
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Downloads WSPR spot archives from wsprnet.org.\n")
		fmt.Fprintf(os.Stderr, "Archives are monthly .csv.gz files (~200MB-1GB each).\n\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s                           # Download all available\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -start 2024-01 -end 2024-12  # Download 2024 only\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -list                       # List files without downloading\n", os.Args[0])
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
		fmt.Printf("WSPR Archives (%d files):\n\n", len(files))
		for _, f := range files {
			fmt.Printf("  %s/%s\n", BaseURL, f)
		}
		return
	}

	fmt.Println("=========================================================")
	fmt.Printf("WSPR Download v%s\n", Version)
	fmt.Println("=========================================================")
	fmt.Printf("Source:      %s\n", BaseURL)
	fmt.Printf("Destination: %s\n", *destDir)
	fmt.Printf("Date Range:  %s to %04d-%02d\n", *startDate, endYear, endMonth)
	fmt.Printf("Files:       %d archives\n", len(files))
	fmt.Printf("Workers:     %d parallel\n", *workers)
	fmt.Printf("Timeout:     %v per file\n", *timeout)
	fmt.Println()

	// Create destination directory
	if err := os.MkdirAll(*destDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Cannot create directory: %v\n", err)
		os.Exit(1)
	}

	startTime := time.Now()
	stats := &DownloadStats{}

	// Worker pool
	sem := make(chan struct{}, *workers)
	var wg sync.WaitGroup

	for _, filename := range files {
		sem <- struct{}{}
		wg.Add(1)

		go func(fname string) {
			defer func() { <-sem }()
			defer wg.Done()

			url := fmt.Sprintf("%s/%s", BaseURL, fname)
			destPath := filepath.Join(*destDir, fname)

			if err := downloadFile(url, destPath, *timeout, stats); err != nil {
				fmt.Printf("[%s] ERROR: %v\n", fname, err)
				stats.Failed.Add(1)
			} else if stats.Skipped.Load() > 0 {
				// File was skipped (already exists)
			} else {
				fmt.Printf("[%s] Downloaded\n", fname)
			}
		}(filename)
	}

	wg.Wait()

	elapsed := time.Since(startTime)
	completed := stats.Completed.Load()
	failed := stats.Failed.Load()
	skipped := stats.Skipped.Load()
	bytes := stats.Bytes.Load()

	fmt.Println()
	fmt.Println("=========================================================")
	fmt.Println("Download Summary")
	fmt.Println("=========================================================")
	fmt.Printf("Downloaded: %d files (%.2f GB)\n", completed, float64(bytes)/1024/1024/1024)
	fmt.Printf("Skipped:    %d files (already exist)\n", skipped)
	fmt.Printf("Failed:     %d files\n", failed)
	fmt.Printf("Elapsed:    %v\n", elapsed.Round(time.Second))
	if completed > 0 && elapsed.Seconds() > 0 {
		fmt.Printf("Speed:      %.2f MB/s\n", float64(bytes)/elapsed.Seconds()/1024/1024)
	}
	fmt.Println("=========================================================")

	if failed > 0 {
		os.Exit(1)
	}
}
