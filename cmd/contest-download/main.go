// contest-download - Download public Cabrillo logs from CQ contest sites
//
// Good neighbor policy:
//   - Sequential downloads only, zero parallelism
//   - Configurable delay between requests (default 3s)
//   - Honest User-Agent identification
//   - Resume-friendly: skip files that already exist on disk
//   - Index caching via manifest files
//   - Graceful shutdown on Ctrl+C
//
// Build: CGO_ENABLED=0 go build -ldflags="-s -w" -o build/contest-download ./cmd/contest-download

package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"
)

// Version can be overridden at build time via -ldflags
var Version = "2.2.0"

var userAgent = fmt.Sprintf("contest-download/%s (KI7MT; ki7mt@yahoo.com)", Version)

// Contest defines a CQ contest data source.
type Contest struct {
	Key     string
	Name    string
	BaseURL string
	Modes   []string // e.g. ["ph","cw"] or nil for single-mode contests
	YearMin int
	YearMax int
}

var contests = []Contest{
	{
		Key:     "cq-ww",
		Name:    "CQ WW",
		BaseURL: "https://cqww.com/publiclogs/",
		Modes:   []string{"ph", "cw"},
		YearMin: 2005,
		YearMax: 2025,
	},
	{
		Key:     "cq-wpx",
		Name:    "CQ WPX",
		BaseURL: "https://cqwpx.com/publiclogs/",
		Modes:   []string{"ph", "cw"},
		YearMin: 2008,
		YearMax: 2025,
	},
	{
		Key:     "cq-ww-rtty",
		Name:    "CQ WW RTTY",
		BaseURL: "https://cqwwrtty.com/publiclogs/",
		Modes:   nil,
		YearMin: 2009,
		YearMax: 2024,
	},
	{
		Key:     "cq-wpx-rtty",
		Name:    "CQ WPX RTTY",
		BaseURL: "https://cqwpxrtty.com/publiclogs/",
		Modes:   nil,
		YearMin: 2012,
		YearMax: 2025,
	},
	{
		Key:     "cq-160",
		Name:    "CQ 160",
		BaseURL: "https://cq160.com/publiclogs/",
		Modes:   []string{"ph", "cw"},
		YearMin: 2022,
		YearMax: 2025,
	},
	{
		Key:     "ww-digi",
		Name:    "WW Digi",
		BaseURL: "https://ww-digi.com/publiclogs/",
		Modes:   nil,
		YearMin: 2019,
		YearMax: 2025,
	},
}

// logLinkRe extracts .log file links from an HTML index page.
// Handles both single and double quoted href attributes.
var logLinkRe = regexp.MustCompile(`href=['"]([^'"]+\.log)['"]`)

func main() {
	destDir := flag.String("dest", "/mnt/contest-logs", "Destination directory")
	contestKey := flag.String("contest", "all", "Contest key (cq-ww, cq-wpx, cq-ww-rtty, cq-wpx-rtty, cq-160, ww-digi, all)")
	year := flag.Int("year", 0, "Download only this year (0 = all years)")
	mode := flag.String("mode", "", "Download only this mode: ph, cw (empty = all modes)")
	delay := flag.Duration("delay", 3*time.Second, "Delay between HTTP requests")
	timeout := flag.Duration("timeout", 60*time.Second, "HTTP request timeout")
	refresh := flag.Bool("refresh", false, "Re-fetch index pages even if manifest exists")
	dryRun := flag.Bool("dry-run", false, "Fetch indexes and build manifests only, no log downloads")
	listContests := flag.Bool("list", false, "List available contests and exit")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "contest-download v%s — CQ Contest Log Downloader\n\n", Version)
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Downloads public Cabrillo logs from CQ contest websites.\n")
		fmt.Fprintf(os.Stderr, "Good neighbor: sequential requests, configurable delay, resume-friendly.\n\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nContests:\n")
		for _, c := range contests {
			modes := "(none)"
			if len(c.Modes) > 0 {
				modes = strings.Join(c.Modes, ", ")
			}
			fmt.Fprintf(os.Stderr, "  %-14s %-12s modes: %-8s years: %d–%d\n",
				c.Key, c.Name, modes, c.YearMin, c.YearMax)
		}
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  contest-download --list\n")
		fmt.Fprintf(os.Stderr, "  contest-download --contest cq-ww --year 2024 --mode cw\n")
		fmt.Fprintf(os.Stderr, "  contest-download --dry-run\n")
		fmt.Fprintf(os.Stderr, "  contest-download --delay 5s\n")
	}

	flag.Parse()

	if *listContests {
		fmt.Printf("contest-download v%s\n\n", Version)
		fmt.Printf("Available contests:\n\n")
		for _, c := range contests {
			modes := "(single mode)"
			if len(c.Modes) > 0 {
				modes = strings.Join(c.Modes, ", ")
			}
			fmt.Printf("  %-14s %-12s modes: %-14s years: %d–%d\n",
				c.Key, c.Name, modes, c.YearMin, c.YearMax)
		}
		return
	}

	// Validate --contest flag
	if *contestKey != "all" {
		found := false
		for _, c := range contests {
			if c.Key == *contestKey {
				found = true
				break
			}
		}
		if !found {
			fmt.Fprintf(os.Stderr, "Error: unknown contest %q (use --list to see available contests)\n", *contestKey)
			os.Exit(1)
		}
	}

	// Validate --mode flag
	if *mode != "" && *mode != "ph" && *mode != "cw" {
		fmt.Fprintf(os.Stderr, "Error: invalid mode %q (must be ph or cw)\n", *mode)
		os.Exit(1)
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
	fmt.Printf("contest-download v%s\n", Version)
	fmt.Printf("User-Agent: %s\n", userAgent)
	fmt.Printf("Delay: %v between requests\n", *delay)
	fmt.Printf("Destination: %s\n", *destDir)
	if *dryRun {
		fmt.Printf("Mode: dry-run (manifests only, no log downloads)\n")
	}
	fmt.Println()

	// Build work list: (contest, year, mode-suffix) tuples
	type workItem struct {
		contest Contest
		year    int
		mode    string // "ph", "cw", or "" for single-mode
		subdir  string // e.g. "2024cw" or "2024"
	}

	var work []workItem
	for _, c := range contests {
		if *contestKey != "all" && c.Key != *contestKey {
			continue
		}
		for y := c.YearMin; y <= c.YearMax; y++ {
			if *year != 0 && y != *year {
				continue
			}
			if len(c.Modes) > 0 {
				for _, m := range c.Modes {
					if *mode != "" && m != *mode {
						continue
					}
					work = append(work, workItem{
						contest: c,
						year:    y,
						mode:    m,
						subdir:  fmt.Sprintf("%d%s", y, m),
					})
				}
			} else {
				// Single-mode contest — skip if user requested a specific mode
				if *mode != "" {
					continue
				}
				work = append(work, workItem{
					contest: c,
					year:    y,
					mode:    "",
					subdir:  fmt.Sprintf("%d", y),
				})
			}
		}
	}

	if len(work) == 0 {
		fmt.Println("No matching contest/year/mode combinations found.")
		return
	}

	// Totals across all work items
	var totalDownloaded, totalSkipped, totalFailed, totalRemaining int

	for _, w := range work {
		if ctx.Err() != nil {
			break
		}

		contestDir := filepath.Join(*destDir, w.contest.Key, w.subdir)
		if err := os.MkdirAll(contestDir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "Error: cannot create %s: %v\n", contestDir, err)
			continue
		}

		fmt.Printf("=== %s %s ===\n", w.contest.Key, w.subdir)

		// Phase 1: Get or load manifest
		manifestPath := filepath.Join(contestDir, "manifest.txt")
		var callsigns []string

		if !*refresh {
			if existing, err := readManifest(manifestPath); err == nil && len(existing) > 0 {
				callsigns = existing
				fmt.Printf("  Manifest cached: %d logs\n", len(callsigns))
			}
		}

		if callsigns == nil {
			indexURL := w.contest.BaseURL + w.subdir + "/"
			fmt.Printf("  Fetching index: %s\n", indexURL)

			fetched, err := fetchIndex(ctx, client, indexURL)
			if err != nil {
				fmt.Printf("  WARNING: index fetch failed: %v (skipping)\n", err)
				fmt.Println()
				// Delay even on failure to be polite
				sleepWithContext(ctx, *delay)
				continue
			}

			callsigns = fetched
			fmt.Printf("  %d logs found\n", len(callsigns))

			if err := writeManifest(manifestPath, callsigns); err != nil {
				fmt.Fprintf(os.Stderr, "  WARNING: cannot write manifest: %v\n", err)
			}

			// Delay after index fetch
			sleepWithContext(ctx, *delay)
		}

		if len(callsigns) == 0 {
			fmt.Printf("  No logs to download\n")
			fmt.Println()
			continue
		}

		if *dryRun {
			fmt.Println()
			continue
		}

		// Phase 2: Download logs
		downloaded, skipped, failed := 0, 0, 0
		for i, cs := range callsigns {
			if ctx.Err() != nil {
				remaining := len(callsigns) - i
				totalRemaining += remaining
				fmt.Printf("  [%*d/%d] %s — interrupted (Ctrl+C)\n",
					countWidth(len(callsigns)), i+1, len(callsigns), cs+".log")
				break
			}

			logFile := filepath.Join(contestDir, cs+".log")

			// Resume: skip if file already exists
			if _, err := os.Stat(logFile); err == nil {
				skipped++
				continue
			}

			logURL := w.contest.BaseURL + w.subdir + "/" + cs + ".log"
			size, err := downloadFile(ctx, client, logURL, logFile)
			if err != nil {
				fmt.Printf("  [%*d/%d] %s FAILED: %v\n",
					countWidth(len(callsigns)), i+1, len(callsigns), cs+".log", err)
				failed++
			} else {
				fmt.Printf("  [%*d/%d] %s (%s) OK\n",
					countWidth(len(callsigns)), i+1, len(callsigns), cs+".log", formatBytes(size))
				downloaded++
			}

			// Delay between requests
			sleepWithContext(ctx, *delay)
		}

		totalDownloaded += downloaded
		totalSkipped += skipped
		totalFailed += failed

		fmt.Println()
	}

	// Summary
	fmt.Println("Summary:")
	fmt.Printf("  Downloaded: %d logs\n", totalDownloaded)
	fmt.Printf("  Skipped:    %d (already on disk)\n", totalSkipped)
	fmt.Printf("  Failed:     %d\n", totalFailed)
	if totalRemaining > 0 {
		fmt.Printf("  Remaining:  %d\n", totalRemaining)
		fmt.Println("  Run again to resume.")
	}

	if totalFailed > 0 {
		os.Exit(1)
	}
}

// fetchIndex GETs the contest index page and extracts .log file links.
func fetchIndex(ctx context.Context, client *http.Client, url string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", userAgent)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP GET: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	return parseIndex(body), nil
}

// parseIndex extracts .log filenames from HTML, returning callsigns (without .log extension).
func parseIndex(html []byte) []string {
	matches := logLinkRe.FindAllSubmatch(html, -1)
	seen := make(map[string]bool, len(matches))
	var callsigns []string

	for _, m := range matches {
		filename := string(m[1])
		// Strip directory prefix if present (href might be "path/call.log")
		filename = filepath.Base(filename)
		cs := strings.TrimSuffix(filename, ".log")
		cs = strings.ToLower(cs)
		if cs != "" && !seen[cs] {
			seen[cs] = true
			callsigns = append(callsigns, cs)
		}
	}

	sort.Strings(callsigns)
	return callsigns
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

// writeManifest writes callsigns to a manifest file (one per line).
func writeManifest(path string, callsigns []string) error {
	tmpPath := path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(f)
	for _, cs := range callsigns {
		fmt.Fprintln(w, cs)
	}
	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	f.Close()

	return os.Rename(tmpPath, path)
}

// readManifest reads callsigns from an existing manifest file.
func readManifest(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var callsigns []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			callsigns = append(callsigns, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return callsigns, nil
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
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
