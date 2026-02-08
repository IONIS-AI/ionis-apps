// contest-download - Download public Cabrillo logs from contest sites
//
// Supported sources:
//   - CQ contests (cqww.com, cqwpx.com, etc.) — directory-listing index
//   - ARRL contests (contests.arrl.org) — hash-based public log URLs
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
var Version = "dev"

var userAgent = fmt.Sprintf("contest-download/%s (ki7mt-ai-lab)", Version)

// Contest defines a contest data source.
type Contest struct {
	Key       string
	Name      string
	BaseURL   string
	Modes     []string       // e.g. ["ph","cw"] or nil for single-mode contests
	YearMin   int
	YearMax   int
	IndexType string         // "cq" (directory listing) or "arrl" (hash-based)
	EID       int            // ARRL event ID (only for IndexType "arrl")
	YearIIDs  map[int]int    // ARRL year → instance ID (only for IndexType "arrl")
}

var contests = []Contest{
	// === CQ Contests (directory-listing pattern) ===
	{
		Key:       "cq-ww",
		Name:      "CQ WW",
		BaseURL:   "https://cqww.com/publiclogs/",
		Modes:     []string{"ph", "cw"},
		YearMin:   2005,
		YearMax:   2025,
		IndexType: "cq",
	},
	{
		Key:       "cq-wpx",
		Name:      "CQ WPX",
		BaseURL:   "https://cqwpx.com/publiclogs/",
		Modes:     []string{"ph", "cw"},
		YearMin:   2008,
		YearMax:   2025,
		IndexType: "cq",
	},
	{
		Key:       "cq-ww-rtty",
		Name:      "CQ WW RTTY",
		BaseURL:   "https://cqwwrtty.com/publiclogs/",
		Modes:     nil,
		YearMin:   2009,
		YearMax:   2024,
		IndexType: "cq",
	},
	{
		Key:       "cq-wpx-rtty",
		Name:      "CQ WPX RTTY",
		BaseURL:   "https://cqwpxrtty.com/publiclogs/",
		Modes:     nil,
		YearMin:   2012,
		YearMax:   2025,
		IndexType: "cq",
	},
	{
		Key:       "cq-160",
		Name:      "CQ 160",
		BaseURL:   "https://cq160.com/publiclogs/",
		Modes:     []string{"ph", "cw"},
		YearMin:   2022,
		YearMax:   2025,
		IndexType: "cq",
	},
	{
		Key:       "ww-digi",
		Name:      "WW Digi",
		BaseURL:   "https://ww-digi.com/publiclogs/",
		Modes:     nil,
		YearMin:   2019,
		YearMax:   2025,
		IndexType: "cq",
	},

	// === ARRL Contests (hash-based pattern) ===
	{
		Key:       "arrl-dx-cw",
		Name:      "ARRL DX CW",
		BaseURL:   "https://contests.arrl.org/",
		Modes:     nil,
		YearMin:   2018,
		YearMax:   2025,
		IndexType: "arrl",
		EID:       13,
		YearIIDs:  map[int]int{2018: 82, 2019: 582, 2020: 707, 2021: 960, 2022: 1006, 2023: 1032, 2024: 1059, 2025: 1096},
	},
	{
		Key:       "arrl-dx-ph",
		Name:      "ARRL DX Phone",
		BaseURL:   "https://contests.arrl.org/",
		Modes:     nil,
		YearMin:   2018,
		YearMax:   2025,
		IndexType: "arrl",
		EID:       14,
		YearIIDs:  map[int]int{2018: 106, 2019: 597, 2020: 709, 2021: 961, 2022: 1007, 2023: 1033, 2024: 1060, 2025: 1097},
	},
	{
		Key:       "arrl-10m",
		Name:      "ARRL 10m",
		BaseURL:   "https://contests.arrl.org/",
		Modes:     nil,
		YearMin:   2018,
		YearMax:   2024,
		IndexType: "arrl",
		EID:       21,
		YearIIDs:  map[int]int{2018: 135, 2019: 688, 2020: 725, 2021: 988, 2022: 1013, 2023: 1045, 2024: 1073},
	},
	{
		Key:       "arrl-160m",
		Name:      "ARRL 160m",
		BaseURL:   "https://contests.arrl.org/",
		Modes:     nil,
		YearMin:   2018,
		YearMax:   2024,
		IndexType: "arrl",
		EID:       20,
		YearIIDs:  map[int]int{2018: 164, 2019: 689, 2020: 726, 2021: 989, 2022: 1014, 2023: 1046, 2024: 1074},
	},
	{
		Key:       "arrl-ss-cw",
		Name:      "ARRL SS CW",
		BaseURL:   "https://contests.arrl.org/",
		Modes:     nil,
		YearMin:   2018,
		YearMax:   2024,
		IndexType: "arrl",
		EID:       17,
		YearIIDs:  map[int]int{2018: 18, 2019: 693, 2020: 719, 2021: 991, 2022: 1016, 2023: 1048, 2024: 1070},
	},
	{
		Key:       "arrl-ss-ph",
		Name:      "ARRL SS Phone",
		BaseURL:   "https://contests.arrl.org/",
		Modes:     nil,
		YearMin:   2018,
		YearMax:   2024,
		IndexType: "arrl",
		EID:       18,
		YearIIDs:  map[int]int{2018: 54, 2019: 694, 2020: 720, 2021: 992, 2022: 1017, 2023: 1049, 2024: 1071},
	},
	{
		Key:       "arrl-rtty",
		Name:      "ARRL RTTY RU",
		BaseURL:   "https://contests.arrl.org/",
		Modes:     nil,
		YearMin:   2018,
		YearMax:   2025,
		IndexType: "arrl",
		EID:       6,
		YearIIDs:  map[int]int{2018: 184, 2019: 581, 2020: 705, 2021: 730, 2022: 1004, 2023: 1029, 2024: 1058, 2025: 1095},
	},
	{
		Key:       "arrl-digi",
		Name:      "ARRL Digital",
		BaseURL:   "https://contests.arrl.org/",
		Modes:     nil,
		YearMin:   2022,
		YearMax:   2025,
		IndexType: "arrl",
		EID:       31,
		YearIIDs:  map[int]int{2022: 1023, 2023: 1036, 2024: 1068, 2025: 1100},
	},
	{
		Key:       "iaru-hf",
		Name:      "IARU HF",
		BaseURL:   "https://contests.arrl.org/",
		Modes:     nil,
		YearMin:   2018,
		YearMax:   2025,
		IndexType: "arrl",
		EID:       4,
		YearIIDs:  map[int]int{2018: 202, 2019: 683, 2020: 714, 2021: 997, 2022: 1022, 2023: 1053, 2024: 1064, 2025: 1110},
	},
}

// logLinkRe extracts .log file links from CQ contest index pages.
var logLinkRe = regexp.MustCompile(`href=['"]([^'"]+\.log)['"]`)

// arrlLogRe extracts callsign+hash pairs from ARRL public log pages.
// Format: <a href="showpubliclog.php?q=HASH" target="_new">CALLSIGN</a>
var arrlLogRe = regexp.MustCompile(`showpubliclog\.php\?q=([^"]+)"[^>]*>([^<]+)</a>`)

// logEntry holds a callsign and optional ARRL hash for manifest storage.
type logEntry struct {
	Callsign string
	Hash     string // ARRL only; empty for CQ
}

func main() {
	destDir := flag.String("dest", "/mnt/contest-logs", "Destination directory")
	contestKey := flag.String("contest", "all", "Contest key (use --list to see options, or 'all')")
	year := flag.Int("year", 0, "Download only this year (0 = all years)")
	mode := flag.String("mode", "", "Download only this mode: ph, cw (empty = all modes)")
	delay := flag.Duration("delay", 3*time.Second, "Delay between HTTP requests")
	timeout := flag.Duration("timeout", 60*time.Second, "HTTP request timeout")
	refresh := flag.Bool("refresh", false, "Re-fetch index pages even if manifest exists")
	dryRun := flag.Bool("dry-run", false, "Fetch indexes and build manifests only, no log downloads")
	listContests := flag.Bool("list", false, "List available contests and exit")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "contest-download v%s — Contest Log Downloader\n\n", Version)
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Downloads public Cabrillo logs from CQ and ARRL contest websites.\n")
		fmt.Fprintf(os.Stderr, "Good neighbor: sequential requests, configurable delay, resume-friendly.\n\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nContests:\n")
		printContestTable(os.Stderr)
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  contest-download --list\n")
		fmt.Fprintf(os.Stderr, "  contest-download --contest cq-ww --year 2024 --mode cw\n")
		fmt.Fprintf(os.Stderr, "  contest-download --contest arrl-dx-cw --year 2024\n")
		fmt.Fprintf(os.Stderr, "  contest-download --dry-run\n")
		fmt.Fprintf(os.Stderr, "  contest-download --delay 5s\n")
	}

	flag.Parse()

	if *listContests {
		fmt.Printf("contest-download v%s\n\n", Version)
		fmt.Printf("Available contests:\n\n")
		printContestTable(os.Stdout)
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

	// Build work list
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
			// For ARRL, skip years that don't have an iid mapping
			if c.IndexType == "arrl" {
				if _, ok := c.YearIIDs[y]; !ok {
					continue
				}
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
		var entries []logEntry

		if !*refresh {
			if existing, err := readManifest(manifestPath); err == nil && len(existing) > 0 {
				entries = existing
				fmt.Printf("  Manifest cached: %d logs\n", len(entries))
			}
		}

		if entries == nil {
			var fetched []logEntry
			var err error

			switch w.contest.IndexType {
			case "arrl":
				iid := w.contest.YearIIDs[w.year]
				indexURL := fmt.Sprintf("%spubliclogs.php?eid=%d&iid=%d", w.contest.BaseURL, w.contest.EID, iid)
				fmt.Printf("  Fetching index: %s\n", indexURL)
				fetched, err = fetchARRLIndex(ctx, client, indexURL)
			default: // "cq"
				indexURL := w.contest.BaseURL + w.subdir + "/"
				fmt.Printf("  Fetching index: %s\n", indexURL)
				fetched, err = fetchCQIndex(ctx, client, indexURL)
			}

			if err != nil {
				fmt.Printf("  WARNING: index fetch failed: %v (skipping)\n", err)
				fmt.Println()
				sleepWithContext(ctx, *delay)
				continue
			}

			entries = fetched
			fmt.Printf("  %d logs found\n", len(entries))

			if err := writeManifest(manifestPath, entries); err != nil {
				fmt.Fprintf(os.Stderr, "  WARNING: cannot write manifest: %v\n", err)
			}

			sleepWithContext(ctx, *delay)
		}

		if len(entries) == 0 {
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
		for i, e := range entries {
			if ctx.Err() != nil {
				remaining := len(entries) - i
				totalRemaining += remaining
				fmt.Printf("  [%*d/%d] %s — interrupted (Ctrl+C)\n",
					countWidth(len(entries)), i+1, len(entries), e.Callsign+".log")
				break
			}

			logFile := filepath.Join(contestDir, e.Callsign+".log")

			// Resume: skip if file already exists
			if _, err := os.Stat(logFile); err == nil {
				skipped++
				continue
			}

			// Construct download URL based on index type
			var logURL string
			switch w.contest.IndexType {
			case "arrl":
				logURL = w.contest.BaseURL + "showpubliclog.php?q=" + e.Hash
			default:
				logURL = w.contest.BaseURL + w.subdir + "/" + e.Callsign + ".log"
			}

			size, err := downloadFile(ctx, client, logURL, logFile)
			if err != nil {
				fmt.Printf("  [%*d/%d] %s FAILED: %v\n",
					countWidth(len(entries)), i+1, len(entries), e.Callsign+".log", err)
				failed++
			} else {
				fmt.Printf("  [%*d/%d] %s (%s) OK\n",
					countWidth(len(entries)), i+1, len(entries), e.Callsign+".log", formatBytes(size))
				downloaded++
			}

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

// fetchCQIndex fetches a CQ-style directory listing and extracts .log links.
func fetchCQIndex(ctx context.Context, client *http.Client, url string) ([]logEntry, error) {
	body, err := httpGet(ctx, client, url)
	if err != nil {
		return nil, err
	}
	return parseCQIndex(body), nil
}

// parseCQIndex extracts .log filenames from HTML directory listing.
func parseCQIndex(html []byte) []logEntry {
	matches := logLinkRe.FindAllSubmatch(html, -1)
	seen := make(map[string]bool, len(matches))
	var entries []logEntry

	for _, m := range matches {
		filename := string(m[1])
		filename = filepath.Base(filename)
		cs := strings.TrimSuffix(filename, ".log")
		cs = strings.ToLower(cs)
		if cs != "" && !seen[cs] {
			seen[cs] = true
			entries = append(entries, logEntry{Callsign: cs})
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Callsign < entries[j].Callsign
	})
	return entries
}

// fetchARRLIndex fetches an ARRL public logs page and extracts callsign+hash pairs.
func fetchARRLIndex(ctx context.Context, client *http.Client, url string) ([]logEntry, error) {
	body, err := httpGet(ctx, client, url)
	if err != nil {
		return nil, err
	}
	return parseARRLIndex(body), nil
}

// parseARRLIndex extracts callsign and hash pairs from ARRL HTML.
// Format: showpubliclog.php?q=HASH" target="_new">CALLSIGN</a>
func parseARRLIndex(html []byte) []logEntry {
	matches := arrlLogRe.FindAllSubmatch(html, -1)
	seen := make(map[string]bool, len(matches))
	var entries []logEntry

	for _, m := range matches {
		hash := string(m[1])
		callsign := strings.TrimSpace(string(m[2]))
		callsign = strings.ToLower(callsign)
		// Normalize callsign for filename safety (replace / with -)
		callsign = strings.ReplaceAll(callsign, "/", "-")
		if callsign != "" && !seen[callsign] {
			seen[callsign] = true
			entries = append(entries, logEntry{Callsign: callsign, Hash: hash})
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Callsign < entries[j].Callsign
	})
	return entries
}

// httpGet performs a GET request and returns the response body.
func httpGet(ctx context.Context, client *http.Client, url string) ([]byte, error) {
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

	return body, nil
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

// writeManifest writes log entries to a manifest file.
// CQ entries: one callsign per line.
// ARRL entries: callsign<tab>hash per line.
func writeManifest(path string, entries []logEntry) error {
	tmpPath := path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(f)
	for _, e := range entries {
		if e.Hash != "" {
			fmt.Fprintf(w, "%s\t%s\n", e.Callsign, e.Hash)
		} else {
			fmt.Fprintln(w, e.Callsign)
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	f.Close()

	return os.Rename(tmpPath, path)
}

// readManifest reads log entries from an existing manifest file.
// Supports both formats: "callsign" and "callsign\thash".
func readManifest(path string) ([]logEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var entries []logEntry
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		e := logEntry{Callsign: parts[0]}
		if len(parts) == 2 {
			e.Hash = parts[1]
		}
		entries = append(entries, e)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}

// printContestTable prints the contest list in a formatted table.
func printContestTable(w io.Writer) {
	for _, c := range contests {
		modes := "(single mode)"
		if len(c.Modes) > 0 {
			modes = strings.Join(c.Modes, ", ")
		}
		src := "CQ"
		if c.IndexType == "arrl" {
			src = "ARRL"
		}
		fmt.Fprintf(w, "  %-16s %-14s %-6s modes: %-14s years: %d–%d\n",
			c.Key, c.Name, src, modes, c.YearMin, c.YearMax)
	}
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
