// goes-xrs-download mirrors NOAA NCEI's GOES X-ray (XRS) 1-minute science product to
// local disk, unchanged.
//
// ONE SOURCE. The product is xrsf-l2-avg1m_science, published per satellite as one
// netCDF-4 file per day:
//
//	https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes/
//	    <satellite>/l2/data/xrsf-l2-avg1m_science/YYYY/MM/sci_xrsf-l2-avg1m_gNN_dYYYYMMDD_vX-Y-Z.nc
//
// GOES-16 (2017-02-07..2025-04-06), GOES-17 (2018-06-01..2023-01-10, with two gaps NOAA
// never published), GOES-18 (2022-06-17..) and GOES-19 (2024-09-20..). The satellites
// overlap; the mirror keeps every file of every one.
//
// The local mirror, $IONIS_SOLAR_DATA_DIR/goes-xrs/<satellite>/YYYY/MM/<file>.nc, is the
// archive source of truth: the bytes NOAA served, never edited. goes-xrs-ingest reads it
// and never touches the network.
//
// Incremental: a file already present is not fetched again (the version is part of the
// name, so a reprocessed file arrives under a new name). A download is checked against
// the server's Content-Length and the HDF5 signature before it is renamed into place. The
// run FAILS if, afterwards, any file the server lists is absent from the mirror.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/IONIS-AI/ionis-apps/internal/common"
)

var Version = "dev"

const (
	userAgent = "ionis-goes-xrs-download/1.0 (+https://github.com/IONIS-AI/ionis-apps; KI7MT sovereign AI lab)"
	product   = "xrsf-l2-avg1m_science"
)

var (
	yearRe  = regexp.MustCompile(`href="(\d{4})/"`)
	monthRe = regexp.MustCompile(`href="(\d{2})/"`)
	fileRe  = regexp.MustCompile(`href="(sci_xrsf-l2-avg1m_g\d+_d\d{8}_v[0-9-]+\.nc)"`)
	hdf5Sig = []byte{0x89, 'H', 'D', 'F', '\r', '\n', 0x1a, '\n'}
)

type remote struct{ sat, year, month, name string }

func (r remote) local(root string) string {
	return filepath.Join(root, r.sat, r.year, r.month, r.name)
}

func main() {
	var (
		dest    = flag.String("dest", "", "Mirror root (default: $IONIS_SOLAR_DATA_DIR/goes-xrs)")
		base    = flag.String("base", "https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes", "NCEI GOES root")
		sats    = flag.String("satellites", "goes16,goes17,goes18,goes19", "Comma-separated satellites")
		workers = flag.Int("workers", 4, "Concurrent downloads")
		timeout = flag.Duration("timeout", 2*time.Minute, "Per-request timeout")
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
		root = filepath.Join(sd, "goes-xrs")
	}
	c := &http.Client{Timeout: *timeout}
	fmt.Printf("goes-xrs-download v%s\n  mirror %s\n", Version, root)

	failed := false
	for _, sat := range strings.Split(*sats, ",") {
		sat = strings.TrimSpace(sat)
		listed, err := list(c, *base, sat)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  %s: list: %v\n", sat, err)
			failed = true
			continue
		}
		var todo []remote
		for _, r := range listed {
			if fi, err := os.Stat(r.local(root)); err != nil || fi.Size() == 0 {
				todo = append(todo, r)
			}
		}
		fmt.Printf("  %s: %d listed, %d to fetch\n", sat, len(listed), len(todo))

		var mu sync.Mutex
		var errs []string
		jobs := make(chan remote)
		var wg sync.WaitGroup
		for i := 0; i < *workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for r := range jobs {
					u := fmt.Sprintf("%s/%s/l2/data/%s/%s/%s/%s", *base, r.sat, product, r.year, r.month, r.name)
					if err := fetch(c, u, root, r); err != nil {
						mu.Lock()
						errs = append(errs, fmt.Sprintf("%s: %v", r.name, err))
						mu.Unlock()
					}
				}
			}()
		}
		for _, r := range todo {
			jobs <- r
		}
		close(jobs)
		wg.Wait()
		sort.Strings(errs)
		for _, e := range errs {
			fmt.Fprintf(os.Stderr, "  FAIL %s\n", e)
		}

		missing := 0
		for _, r := range listed {
			if fi, err := os.Stat(r.local(root)); err != nil || fi.Size() == 0 {
				missing++
			}
		}
		fmt.Printf("  %s: fetched %d, failed %d, mirror complete: %v\n", sat, len(todo)-len(errs), len(errs), missing == 0)
		if missing > 0 {
			fmt.Fprintf(os.Stderr, "  %s: %d listed file(s) absent after the run\n", sat, missing)
			failed = true
		}
	}
	if failed {
		os.Exit(1)
	}
}

// list walks the server's year and month indexes for one satellite.
func list(c *http.Client, base, sat string) ([]remote, error) {
	top := fmt.Sprintf("%s/%s/l2/data/%s/", base, sat, product)
	b, err := get(c, top)
	if err != nil {
		return nil, err
	}
	var out []remote
	for _, y := range uniq(yearRe.FindAllSubmatch(b, -1)) {
		yb, err := get(c, top+y+"/")
		if err != nil {
			return nil, fmt.Errorf("%s: %w", y, err)
		}
		for _, m := range uniq(monthRe.FindAllSubmatch(yb, -1)) {
			mb, err := get(c, top+y+"/"+m+"/")
			if err != nil {
				return nil, fmt.Errorf("%s/%s: %w", y, m, err)
			}
			for _, f := range uniq(fileRe.FindAllSubmatch(mb, -1)) {
				out = append(out, remote{sat, y, m, f})
			}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no files listed under %s", top)
	}
	return out, nil
}

func uniq(ms [][][]byte) []string {
	seen := map[string]bool{}
	var out []string
	for _, m := range ms {
		s := string(m[1])
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

func fetch(c *http.Client, u, root string, r remote) error {
	b, err := get(c, u)
	if err != nil {
		return err
	}
	if !bytes.HasPrefix(b, hdf5Sig) {
		return fmt.Errorf("not an HDF5/netCDF-4 file -- refusing to store it")
	}
	dst := r.local(root)
	// Every directory level the service may create is group-writable with setgid, from
	// the mirror root down, so a new satellite or year never needs a hand-made directory.
	for _, d := range []string{root, filepath.Join(root, r.sat), filepath.Join(root, r.sat, r.year), filepath.Dir(dst)} {
		if err := os.MkdirAll(d, 0o775|os.ModeSetgid); err != nil {
			return err
		}
		_ = os.Chmod(d, 0o775|os.ModeSetgid) // umask; not fatal on a directory another account owns
	}
	tmp := dst + ".partial"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}

// get fetches u; a 5xx or transport error is retried up to four times with backoff, and
// a body shorter than the server's Content-Length is an error, not a file.
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
	if err != nil {
		return nil, true, err
	}
	if resp.ContentLength > 0 && int64(len(b)) != resp.ContentLength {
		return nil, true, fmt.Errorf("got %d bytes, Content-Length %d", len(b), resp.ContentLength)
	}
	return b, false, nil
}
