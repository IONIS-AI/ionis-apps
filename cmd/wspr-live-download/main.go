// wspr-live-download — fetch WSPR spots from wspr.live into dated files on disk.
//
// The DOWNLOAD half of the standard IONIS download-then-ingest pair. Writes
// /mnt/wspr-data/live/YYYY/MM/DD.jsonl.gz; wspr-live-ingest loads those files and owns
// the wspr.ingest_log watermark. Same shape as pskr-collector -> pskr-ingest.
//
// WHY THE SPLIT MATTERS HERE, beyond convention. The first version of this fused the
// download and the insert, and re-queried a 3-day window from wspr.live EVERY night:
// ~17.6M rows pulled to insert 2. That is a standing load on a volunteer-run service
// with essentially no return. Writing to disk first makes a fetched day permanent — a
// day already on disk is never requested again — so steady-state cost drops to the
// settle window only, and a re-run after a ClickHouse problem costs wspr.live nothing
// at all because the files are already here.
//
// It also restores the local archive. The wsprnet CSVs were re-ingestable; an API is
// not. Without this, ClickHouse became the ONLY copy of every WSPR spot after 2025-01.
//
// BEING A GOOD CITIZEN of wspr.live (and wsprdaemon, same schema):
//   - One request at a time. No parallelism, ever.
//   - A day already on disk is skipped outright, so historical days are fetched once
//     in their lifetime.
//   - Only days inside -settle-days are re-fetched, because WSPR spots keep arriving
//     for minutes after a decode and a day is not final when it ends.
//   - -delay between requests, -max-days to bound any single run.
//   - Identifying User-Agent, so the operators can see who we are and contact us.
//   - Exponential backoff on failure rather than immediate retry.
//
// Build: CGO_ENABLED=0 go build -o build/bin/wspr-live-download ./cmd/wspr-live-download
package main

import (
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

var Version = "dev"

const (
	defaultEndpoint = "https://db1.wspr.live"
	userAgent       = "ionis-wspr-live-download/1.0 (+https://github.com/IONIS-AI/ionis-apps; KI7MT sovereign AI lab)"
)

// dayPath is the on-disk location for one UTC day.
func dayPath(root string, d time.Time) string {
	return filepath.Join(root, d.Format("2006"), d.Format("01"), d.Format("02")+".jsonl.gz")
}

// fetchDay writes one UTC day to a .jsonl.gz. It streams straight from the HTTP body
// through gzip to a temp file, then renames — so a killed process can never leave a
// truncated file that ingest would happily read as complete.
func fetchDay(ctx context.Context, client *http.Client, endpoint, root string, d time.Time) (int64, error) {
	next := d.AddDate(0, 0, 1)
	q := fmt.Sprintf(`SELECT id, toString(time) AS t, rx_sign, rx_loc, snr, frequency,
	                         tx_sign, tx_loc, power, drift, distance, azimuth, version, code
	                  FROM wspr.rx
	                  WHERE time >= '%s' AND time < '%s'
	                  ORDER BY id
	                  FORMAT JSONEachRow`,
		d.Format("2006-01-02"), next.Format("2006-01-02"))

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint+"/?query="+url.QueryEscape(q), nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", userAgent)

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return 0, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	out := dayPath(root, d)
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return 0, err
	}
	tmp := out + ".partial"
	f, err := os.Create(tmp)
	if err != nil {
		return 0, err
	}
	gz := gzip.NewWriter(f)

	n, copyErr := io.Copy(gz, resp.Body)
	gzErr := gz.Close()
	closeErr := f.Close()

	if copyErr != nil || gzErr != nil || closeErr != nil {
		os.Remove(tmp) // never leave a partial where ingest could find it
		return 0, fmt.Errorf("write %s: copy=%v gzip=%v close=%v", tmp, copyErr, gzErr, closeErr)
	}
	// Atomic publish: the file appears complete or not at all.
	if err := os.Rename(tmp, out); err != nil {
		os.Remove(tmp)
		return 0, err
	}
	// Report the ON-DISK size, not the bytes copied. io.Copy counts what went INTO the
	// gzip writer, so logging n would report ~1.3 GB for a 140 MB file — a log that
	// disagrees with `ls` is a log nobody trusts.
	if fi, statErr := os.Stat(out); statErr == nil {
		return fi.Size(), nil
	}
	return n, nil
}

func main() {
	var (
		root     = flag.String("dest", "/mnt/wspr-data/live", "Destination root (YYYY/MM/DD.jsonl.gz)")
		endpoint = flag.String("endpoint", defaultEndpoint, "ClickHouse HTTP endpoint (wspr.live or wsprdaemon — same schema)")
		startStr = flag.String("start", "", "Start date YYYY-MM-DD (default: settle window)")
		endStr   = flag.String("end", "", "End date YYYY-MM-DD exclusive (default: today)")
		settle   = flag.Int("settle-days", 3, "Re-fetch days newer than this; older days on disk are never re-requested")
		delay    = flag.Duration("delay", 60*time.Second, "Pause between requests. Default keeps duty cycle ~24%: a day is ~156 MB and holds the connection ~19s, so a short gap means near-continuous pull.")
		maxDays  = flag.Int("max-days", 0, "Stop after N days this run (0 = no limit)")
		timeout  = flag.Duration("timeout", 300*time.Second, "HTTP timeout per day")
		dryRun   = flag.Bool("dry-run", false, "Report what would be fetched, request nothing")
		force    = flag.Bool("force", false, "Re-fetch even days already on disk outside the settle window")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "wspr-live-download v%s — fetch WSPR spots from wspr.live to disk\n\n", Version)
		fmt.Fprintf(os.Stderr, "Download half of the download-then-ingest pair; wspr-live-ingest loads\n")
		fmt.Fprintf(os.Stderr, "the files and owns the wspr.ingest_log watermark.\n\n")
		fmt.Fprintf(os.Stderr, "A day already on disk and older than -settle-days is NEVER re-requested.\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	log.Printf("wspr-live-download v%s", Version)
	log.Printf("  endpoint: %s", *endpoint)
	log.Printf("  dest:     %s", *root)

	end := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, 1) // include today
	if *endStr != "" {
		t, err := time.Parse("2006-01-02", *endStr)
		if err != nil {
			log.Fatalf("bad -end: %v", err)
		}
		end = t
	}
	start := end.AddDate(0, 0, -(*settle))
	if *startStr != "" {
		t, err := time.Parse("2006-01-02", *startStr)
		if err != nil {
			log.Fatalf("bad -start: %v", err)
		}
		start = t
	}

	// Days at or after this are still settling: WSPR receivers upload a minute or two
	// after each decode, so a day is not final the moment it ends. Re-fetch these;
	// everything older is immutable once on disk.
	settleFrom := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -(*settle))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client := &http.Client{Timeout: *timeout}

	var fetched, skipped, failed, bytes int64
	for d := start; d.Before(end); d = d.AddDate(0, 0, 1) {
		if ctx.Err() != nil {
			log.Printf("interrupted — re-run to continue; completed days are already on disk")
			break
		}
		if *maxDays > 0 && int(fetched) >= *maxDays {
			log.Printf("reached -max-days=%d; remaining days left for the next run", *maxDays)
			break
		}

		path := dayPath(*root, d)
		_, statErr := os.Stat(path)
		onDisk := statErr == nil
		settling := !d.Before(settleFrom)

		// The politeness rule: a complete day already on disk is final. Only the settle
		// window is worth asking about again.
		if onDisk && !settling && !*force {
			skipped++
			continue
		}
		if *dryRun {
			reason := "missing"
			if onDisk {
				reason = "settling — would refresh"
			}
			log.Printf("[%s] would fetch (%s)", d.Format("2006-01-02"), reason)
			fetched++
			continue
		}

		var n int64
		var err error
		// Backoff rather than hammer: a struggling service should see us back off, not
		// retry immediately.
		for attempt := 1; attempt <= 3; attempt++ {
			n, err = fetchDay(ctx, client, *endpoint, *root, d)
			if err == nil || ctx.Err() != nil {
				break
			}
			wait := time.Duration(attempt*attempt) * 10 * time.Second
			log.Printf("[%s] attempt %d failed: %v — backing off %s", d.Format("2006-01-02"), attempt, err, wait)
			select {
			case <-ctx.Done():
			case <-time.After(wait):
			}
		}
		if err != nil {
			log.Printf("[%s] FAILED after retries: %v", d.Format("2006-01-02"), err)
			failed++
			continue
		}

		fetched++
		bytes += n
		log.Printf("[%s] %s (%.1f MB)", d.Format("2006-01-02"), filepath.Base(path), float64(n)/1024/1024)

		select {
		case <-ctx.Done():
		case <-time.After(*delay):
		}
	}

	log.Printf("---------------------------------------------------------")
	log.Printf("Fetched: %d   Skipped (already final): %d   Failed: %d   %.1f MB",
		fetched, skipped, failed, float64(bytes)/1024/1024)
	if *dryRun {
		log.Printf("DRY RUN — no requests were made to %s", *endpoint)
	}
	if failed > 0 {
		os.Exit(1)
	}
}
