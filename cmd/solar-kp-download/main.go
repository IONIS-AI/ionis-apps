// solar-kp-download fetches the definitive planetary Kp/ap series from GFZ.
//
// WHY THIS EXISTS. Kp reached solar.bronze only through NOAA's
// products/noaa-planetary-k-index.json, which is a SEVEN DAY rolling window. A day
// missed was a day lost -- by the next run the window had moved past it. Five months
// of 2026 went that way: April through August hold SFI on every row and Kp on none,
// and every run reported success.
//
// GFZ publishes Kp_ap_since_1932.txt: the definitive series, 1932 to yesterday,
// ~277k rows, 16 MB, re-fetchable in full at any time. Fetching the whole file every
// run makes "we missed a day" impossible rather than merely unlikely.
//
// ENDPOINT MOVED. GFZ was kp.gfz-potsdam.de and is now kp.gfz.de. The old host
// 301-redirects today and will not forever. That is the third endpoint migration
// this year -- DSCOVR to RTSW, NOAA's Kp format, and this -- and all three failed
// silently, which is why this asserts on CONTENT and not on HTTP status.
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/IONIS-AI/ionis-apps/internal/common"
)

var Version = "dev"

const (
	defaultURL = "https://kp.gfz.de/app/files/Kp_ap_since_1932.txt"
	userAgent  = "ionis-solar-kp-download/1.0 (+https://github.com/IONIS-AI/ionis-apps; KI7MT sovereign AI lab)"
	// The file is ~16 MB. Anything under a megabyte is a redirect page, an error
	// page, or a truncated transfer -- all of which return 200.
	minBytes = 1 << 20
)

func main() {
	var (
		url     = flag.String("url", defaultURL, "GFZ Kp archive URL")
		dest    = flag.String("dest", "", "Destination directory (default: $IONIS_SOLAR_DATA_DIR)")
		timeout = flag.Duration("timeout", 5*time.Minute, "HTTP timeout")
	)
	flag.Parse()

	dir, err := common.ResolvePath(*dest, "IONIS_SOLAR_DATA_DIR", "dest", "solar raw data directory")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Printf("solar-kp-download v%s\n", Version)
	fmt.Printf("  GET %s\n", *url)

	req, _ := http.NewRequest("GET", *url, nil)
	req.Header.Set("User-Agent", userAgent)
	// Redirects are followed by default; log the final URL so a move is visible in
	// the journal rather than only in a diff months later.
	resp, err := (&http.Client{Timeout: *timeout}).Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fetch: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.Request.URL.String() != *url {
		fmt.Printf("  NOTE redirected to %s\n", resp.Request.URL)
	}
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "HTTP %d from %s\n", resp.StatusCode, resp.Request.URL)
		os.Exit(1)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read: %v\n", err)
		os.Exit(1)
	}

	// CONTENT ASSERTIONS. A 200 that returns an HTML error page is the failure mode
	// that cost us five months elsewhere, so size and shape are both checked before
	// anything is written over a good file.
	if len(body) < minBytes {
		fmt.Fprintf(os.Stderr, "got %d bytes, expected at least %d - looks like an error or redirect page, not the archive\n",
			len(body), minBytes)
		os.Exit(1)
	}
	if strings.Contains(strings.ToLower(string(body[:min(512, len(body))])), "<html") {
		fmt.Fprintln(os.Stderr, "response begins with HTML - the endpoint is serving a page, not the archive")
		os.Exit(1)
	}
	if !strings.Contains(string(body[:min(4096, len(body))]), "Kp") {
		fmt.Fprintln(os.Stderr, "header does not mention Kp - the format may have changed; refusing to overwrite")
		os.Exit(1)
	}

	// Write to a temp file and rename, so an interrupted run never leaves a partial
	// archive where the ingester will find it.
	out := filepath.Join(dir, "gfz_kp_ap_since_1932.txt")
	tmp := out + ".partial"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write: %v\n", err)
		os.Exit(1)
	}
	if err := os.Rename(tmp, out); err != nil {
		fmt.Fprintf(os.Stderr, "rename: %v\n", err)
		os.Exit(1)
	}

	lines := strings.Count(string(body), "\n")
	fmt.Printf("  wrote %s (%d bytes, %d lines)\n", out, len(body), lines)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
