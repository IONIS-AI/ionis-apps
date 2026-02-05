// rbn-download - Download Reverse Beacon Network daily spot archives
//
// Data source: https://data.reversebeacon.net/rbn_history/
// Format: Daily ZIP files containing CSV (13 columns, ~2.2B total spots)
// Range: 2009-02-21 to present (updated daily)
//
// Good neighbor policy:
//   - Sequential downloads only, zero parallelism
//   - Configurable delay between requests (default 3s)
//   - Honest User-Agent identification
//   - Resume-friendly: skip files that already exist on disk
//   - Graceful shutdown on Ctrl+C
//
// Build: CGO_ENABLED=0 go build -ldflags="-s -w" -o build/rbn-download ./cmd/rbn-download

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

// Version can be overridden at build time via -ldflags
var Version = "2.2.0"

var userAgent = fmt.Sprintf("rbn-download/%s (KI7MT; ki7mt@yahoo.com)", Version)

const (
	baseURL = "https://data.reversebeacon.net/rbn_history/"
	// RBN data starts on 2009-02-21
	startYear  = 2009
	startMonth = 2
	startDay   = 21
)

func main() {
	destDir := flag.String("dest", "/mnt/rbn-data", "Destination directory")
	fromDate := flag.String("from", "2009-02-21", "Start date (YYYY-MM-DD)")
	toDate := flag.String("to", "", "End date (YYYY-MM-DD, default: yesterday)")
	year := flag.Int("year", 0, "Download only this year (0 = all years)")
	delay := flag.Duration("delay", 3*time.Second, "Delay between HTTP requests")
	timeout := flag.Duration("timeout", 120*time.Second, "HTTP request timeout")
	dryRun := flag.Bool("dry-run", false, "Show what would be downloaded, don't fetch")
	listYears := flag.Bool("list", false, "List available year ranges and estimated sizes")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "rbn-download v%s — Reverse Beacon Network Archive Downloader\n\n", Version)
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Downloads daily ZIP archives of CW/RTTY spots from the RBN.\n")
		fmt.Fprintf(os.Stderr, "Each ZIP contains a CSV with 13 columns (callsign, freq, SNR, etc.).\n")
		fmt.Fprintf(os.Stderr, "Good neighbor: sequential requests, configurable delay, resume-friendly.\n\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nData source: %s\n", baseURL)
		fmt.Fprintf(os.Stderr, "Archive range: 2009-02-21 to present (~6,183 files, ~21 GB)\n")
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  rbn-download --list\n")
		fmt.Fprintf(os.Stderr, "  rbn-download --year 2024\n")
		fmt.Fprintf(os.Stderr, "  rbn-download --from 2024-01-01 --to 2024-12-31\n")
		fmt.Fprintf(os.Stderr, "  rbn-download --dry-run\n")
		fmt.Fprintf(os.Stderr, "  rbn-download --delay 5s\n")
	}

	flag.Parse()

	if *listYears {
		printYearSummary()
		return
	}

	// Parse date range
	earliest := time.Date(startYear, time.Month(startMonth), startDay, 0, 0, 0, 0, time.UTC)

	from, err := time.Parse("2006-01-02", *fromDate)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid --from date %q (use YYYY-MM-DD)\n", *fromDate)
		os.Exit(1)
	}
	if from.Before(earliest) {
		from = earliest
	}

	var to time.Time
	if *toDate != "" {
		to, err = time.Parse("2006-01-02", *toDate)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid --to date %q (use YYYY-MM-DD)\n", *toDate)
			os.Exit(1)
		}
	} else {
		// Default to yesterday (today's file may not be available yet)
		to = time.Now().UTC().AddDate(0, 0, -1)
	}

	if to.Before(from) {
		fmt.Fprintf(os.Stderr, "Error: --to (%s) is before --from (%s)\n",
			to.Format("2006-01-02"), from.Format("2006-01-02"))
		os.Exit(1)
	}

	// If --year is set, override date range
	if *year != 0 {
		from = time.Date(*year, 1, 1, 0, 0, 0, 0, time.UTC)
		to = time.Date(*year, 12, 31, 0, 0, 0, 0, time.UTC)
		if from.Before(earliest) {
			from = earliest
		}
		now := time.Now().UTC().AddDate(0, 0, -1)
		if to.After(now) {
			to = now
		}
	}

	// Build date list
	var dates []time.Time
	for d := from; !d.After(to); d = d.AddDate(0, 0, 1) {
		dates = append(dates, d)
	}

	if len(dates) == 0 {
		fmt.Println("No dates in the specified range.")
		return
	}

	// Set up graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Fprintf(os.Stderr, "\nInterrupt received, finishing current download...\n")
		cancel()
	}()

	// HTTP client
	client := &http.Client{Timeout: *timeout}

	// Banner
	fmt.Printf("rbn-download v%s\n", Version)
	fmt.Printf("User-Agent: %s\n", userAgent)
	fmt.Printf("Delay: %v between requests\n", *delay)
	fmt.Printf("Destination: %s\n", *destDir)
	fmt.Printf("Date range: %s to %s (%d days)\n",
		from.Format("2006-01-02"), to.Format("2006-01-02"), len(dates))
	if *dryRun {
		fmt.Printf("Mode: dry-run (no downloads)\n")
	}
	fmt.Println()

	// Download loop
	var downloaded, skipped, failed, totalBytes int64
	var remaining int

	for i, d := range dates {
		if ctx.Err() != nil {
			remaining = len(dates) - i
			fmt.Printf("[%*d/%d] %s — interrupted (Ctrl+C)\n",
				countWidth(len(dates)), i+1, len(dates), d.Format("20060102"))
			break
		}

		filename := d.Format("20060102") + ".zip"
		yearDir := filepath.Join(*destDir, fmt.Sprintf("%d", d.Year()))
		destPath := filepath.Join(yearDir, filename)
		url := baseURL + filename

		// Resume: skip if file already exists
		if info, err := os.Stat(destPath); err == nil && info.Size() > 0 {
			skipped++
			continue
		}

		if *dryRun {
			fmt.Printf("[%*d/%d] %s (would download)\n",
				countWidth(len(dates)), i+1, len(dates), filename)
			continue
		}

		// Ensure year directory exists
		if err := os.MkdirAll(yearDir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "Error: cannot create %s: %v\n", yearDir, err)
			failed++
			continue
		}

		size, err := downloadFile(ctx, client, url, destPath)
		if err != nil {
			fmt.Printf("[%*d/%d] %s FAILED: %v\n",
				countWidth(len(dates)), i+1, len(dates), filename, err)
			failed++
		} else {
			totalBytes += size
			fmt.Printf("[%*d/%d] %s (%s) OK\n",
				countWidth(len(dates)), i+1, len(dates), filename, formatBytes(size))
			downloaded++
		}

		// Delay between requests
		sleepWithContext(ctx, *delay)
	}

	// Summary
	fmt.Println()
	fmt.Println("Summary:")
	fmt.Printf("  Downloaded: %d files (%s)\n", downloaded, formatBytes(totalBytes))
	fmt.Printf("  Skipped:    %d (already on disk)\n", skipped)
	fmt.Printf("  Failed:     %d\n", failed)
	if remaining > 0 {
		fmt.Printf("  Remaining:  %d\n", remaining)
		fmt.Println("  Run again to resume.")
	}

	if failed > 0 {
		os.Exit(1)
	}
}

// downloadFile GETs a URL and writes it atomically via tmp+rename.
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
		// Missing days are normal (RBN may not have data for every day)
		return 0, fmt.Errorf("not found (no data for this date)")
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

	return n, nil
}

// printYearSummary shows the available year ranges with estimated sizes.
func printYearSummary() {
	fmt.Printf("rbn-download v%s\n\n", Version)
	fmt.Printf("RBN Archive: %s\n", baseURL)
	fmt.Printf("Date range: 2009-02-21 to present\n\n")

	type yearInfo struct {
		year    int
		estSize string
		estRows string
		notes   string
	}

	years := []yearInfo{
		{2009, "49 MB", "~5M", "partial year (Feb start)"},
		{2010, "235 MB", "~23M", ""},
		{2011, "505 MB", "~50M", ""},
		{2012, "713 MB", "~71M", ""},
		{2013, "925 MB", "~92M", ""},
		{2014, "1.1 GB", "~111M", ""},
		{2015, "1.2 GB", "~118M", ""},
		{2016, "1.1 GB", "~109M", ""},
		{2017, "1.2 GB", "~118M", ""},
		{2018, "1.3 GB", "~130M", ""},
		{2019, "1.3 GB", "~132M", ""},
		{2020, "1.7 GB", "~169M", "COVID boom"},
		{2021, "1.8 GB", "~176M", ""},
		{2022, "1.8 GB", "~182M", ""},
		{2023, "2.1 GB", "~206M", ""},
		{2024, "2.3 GB", "~230M", "peak solar cycle"},
		{2025, "2.1 GB", "~212M", "est. full year"},
		{2026, "~200 MB", "~20M", "partial year"},
	}

	fmt.Printf("  %-6s  %-10s  %-10s  %s\n", "Year", "Compressed", "Est. Rows", "Notes")
	fmt.Printf("  %-6s  %-10s  %-10s  %s\n", "----", "----------", "---------", "-----")
	for _, y := range years {
		now := time.Now().UTC()
		if y.year > now.Year() {
			break
		}
		fmt.Printf("  %-6d  %-10s  %-10s  %s\n", y.year, y.estSize, y.estRows, y.notes)
	}
	fmt.Printf("\n  Total: ~21 GB compressed, ~135 GB uncompressed, ~2.2B spots\n")
	fmt.Printf("\n  CSV columns: callsign, de_pfx, de_cont, freq, band, dx, dx_pfx,\n")
	fmt.Printf("               dx_cont, mode, db, date, speed, tx_mode\n")
}

// sleepWithContext sleeps for the given duration, returning early if ctx is cancelled.
func sleepWithContext(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}

// countWidth returns the number of digits needed to display n.
func countWidth(n int) int {
	if n <= 0 {
		return 1
	}
	w := 0
	for n > 0 {
		w++
		n /= 10
	}
	return w
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
