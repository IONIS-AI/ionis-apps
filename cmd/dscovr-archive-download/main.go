// dscovr-archive-download mirrors NOAA NCEI's DSCOVR archive to local disk, unchanged.
//
// NCEI publishes DSCOVR as one gzipped netCDF-3 file per day per product, from
// 2016-07-26, about three months behind real time. The files sit in an S3 bucket that
// archive.data.noaa.gov/satellite-spaceweather fronts with a browser app; the same host
// answers standard S3 ListObjectsV2 requests, which is what this uses.
//
//	f1m  DSCOVR/DSCOVR/FC/f1m/YYYY/MM/   Faraday cup plasma, 1-minute averages
//	m1m  DSCOVR/DSCOVR/MAG/m1m/YYYY/MM/  magnetometer, 1-minute averages
//
// The local mirror, $IONIS_SOLAR_DATA_DIR/dscovr/<product>/YYYY/MM/<file>.nc.gz, is the
// archive source of truth: the bytes NOAA served, never edited. dscovr-archive-ingest
// reads it; it never touches the network.
//
// Incremental. A file already present at the listed size is not fetched again. The run
// FAILS if, after it, any listed file is absent or the wrong size -- a partial mirror
// that reports success is the failure this whole plan exists to remove.
package main

import (
	"bytes"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/IONIS-AI/ionis-apps/internal/common"
)

var Version = "dev"

const userAgent = "ionis-dscovr-archive-download/1.0 (+https://github.com/IONIS-AI/ionis-apps; KI7MT sovereign AI lab)"

var prefixes = map[string]string{
	"f1m": "DSCOVR/DSCOVR/FC/f1m/",
	"m1m": "DSCOVR/DSCOVR/MAG/m1m/",
}

type object struct {
	Key  string `xml:"Key"`
	Size int64  `xml:"Size"`
}

type listResult struct {
	Contents              []object `xml:"Contents"`
	IsTruncated           bool     `xml:"IsTruncated"`
	NextContinuationToken string   `xml:"NextContinuationToken"`
}

func main() {
	var (
		dest     = flag.String("dest", "", "Mirror root (default: $IONIS_SOLAR_DATA_DIR/dscovr)")
		base     = flag.String("base", "https://archive.data.noaa.gov/satellite-spaceweather", "Archive endpoint (S3 ListObjectsV2)")
		products = flag.String("products", "f1m,m1m", "Comma-separated products")
		workers  = flag.Int("workers", 4, "Concurrent downloads")
		timeout  = flag.Duration("timeout", 2*time.Minute, "Per-request timeout")
	)
	flag.Parse()

	if !strings.HasPrefix(*base, "https://") {
		fmt.Fprintln(os.Stderr, "-base must be https")
		os.Exit(1)
	}
	root := *dest
	if root == "" {
		sd, err := common.ResolvePath("", "IONIS_SOLAR_DATA_DIR", "dest", "solar raw data directory")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		root = filepath.Join(sd, "dscovr")
	}
	client := &http.Client{Timeout: *timeout}
	fmt.Printf("dscovr-archive-download v%s\n  mirror %s\n", Version, root)

	failed := false
	for _, prod := range strings.Split(*products, ",") {
		prod = strings.TrimSpace(prod)
		prefix, ok := prefixes[prod]
		if !ok {
			fmt.Fprintf(os.Stderr, "unknown product %q\n", prod)
			os.Exit(1)
		}
		objs, err := list(client, *base, prefix)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  %s: list: %v\n", prod, err)
			failed = true
			continue
		}
		var todo []object
		for _, o := range objs {
			if fi, err := os.Stat(localPath(root, prod, prefix, o.Key)); err != nil || fi.Size() != o.Size {
				todo = append(todo, o)
			}
		}
		fmt.Printf("  %s: %d listed, %d to fetch\n", prod, len(objs), len(todo))

		var mu sync.Mutex
		var errs []string
		jobs := make(chan object)
		var wg sync.WaitGroup
		for i := 0; i < *workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for o := range jobs {
					if err := fetch(client, *base, o, localPath(root, prod, prefix, o.Key)); err != nil {
						mu.Lock()
						errs = append(errs, fmt.Sprintf("%s: %v", o.Key, err))
						mu.Unlock()
					}
				}
			}()
		}
		for _, o := range todo {
			jobs <- o
		}
		close(jobs)
		wg.Wait()
		for _, e := range errs {
			fmt.Fprintf(os.Stderr, "  FAIL %s\n", e)
		}

		// The mirror is complete only if every listed file is present at its size.
		missing := 0
		for _, o := range objs {
			if fi, err := os.Stat(localPath(root, prod, prefix, o.Key)); err != nil || fi.Size() != o.Size {
				missing++
			}
		}
		fmt.Printf("  %s: fetched %d, failed %d, mirror complete: %v\n", prod, len(todo)-len(errs), len(errs), missing == 0)
		if missing > 0 {
			fmt.Fprintf(os.Stderr, "  %s: %d listed file(s) absent or wrong size after the run\n", prod, missing)
			failed = true
		}
	}
	if failed {
		os.Exit(1)
	}
}

// localPath maps DSCOVR/DSCOVR/FC/f1m/2016/07/x.nc.gz to <root>/f1m/2016/07/x.nc.gz.
func localPath(root, prod, prefix, key string) string {
	return filepath.Join(root, prod, filepath.FromSlash(strings.TrimPrefix(key, prefix)))
}

func list(c *http.Client, base, prefix string) ([]object, error) {
	var out []object
	token := ""
	for {
		q := url.Values{"list-type": {"2"}, "prefix": {prefix}}
		if token != "" {
			q.Set("continuation-token", token)
		}
		body, err := get(c, base+"?"+q.Encode())
		if err != nil {
			return nil, err
		}
		var r listResult
		if err := xml.Unmarshal(body, &r); err != nil {
			return nil, fmt.Errorf("listing is not S3 XML: %w", err)
		}
		for _, o := range r.Contents {
			if strings.HasSuffix(o.Key, ".nc.gz") {
				out = append(out, o)
			}
		}
		if !r.IsTruncated {
			return out, nil
		}
		if r.NextContinuationToken == "" {
			return nil, fmt.Errorf("listing truncated without a continuation token")
		}
		token = r.NextContinuationToken
	}
}

func fetch(c *http.Client, base string, o object, dst string) error {
	u := base + "/" + (&url.URL{Path: o.Key}).EscapedPath()
	b, err := get(c, u)
	if err != nil {
		return err
	}
	if int64(len(b)) != o.Size {
		return fmt.Errorf("got %d bytes, listing says %d", len(b), o.Size)
	}
	if !bytes.HasPrefix(b, []byte{0x1f, 0x8b}) {
		return fmt.Errorf("not gzip -- refusing to store %s", path.Base(o.Key))
	}
	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, 0o775|os.ModeSetgid); err != nil {
		return err
	}
	// MkdirAll is subject to umask; make the month and year directories group-writable
	// so any account in the data group can add to the mirror. Not fatal: a directory
	// another account owns is already whatever its owner made it.
	for d, i := dir, 0; i < 2; d, i = filepath.Dir(d), i+1 {
		_ = os.Chmod(d, 0o775|os.ModeSetgid)
	}
	tmp := dst + ".partial"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}

// get fetches u, retrying a server error (5xx) or a transport error up to four times
// with backoff. NCEI's proxy returns an occasional HTTP 500 for a file that is fine a
// moment later: one of 7,289 on the first full mirror (2026-09-24).
func get(c *http.Client, u string) ([]byte, error) {
	var last error
	for attempt, wait := 0, 2*time.Second; attempt < 5; attempt, wait = attempt+1, wait*2 {
		if attempt > 0 {
			time.Sleep(wait)
		}
		b, retry, err := getOnce(c, u)
		if err == nil {
			return b, nil
		}
		last = err
		if !retry {
			break
		}
	}
	return nil, last
}

func getOnce(c *http.Client, u string) ([]byte, bool, error) {
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := c.Do(req)
	if err != nil {
		return nil, true, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode >= 500, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	b, err := io.ReadAll(resp.Body)
	return b, err != nil, err
}
