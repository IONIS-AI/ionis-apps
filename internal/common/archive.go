package common

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// FetchArchive downloads a published archive file and writes it atomically.
//
// It asserts on CONTENT, not on HTTP status. Every solar endpoint failure this
// pipeline has suffered returned 200: NOAA changed the Kp payload shape, GFZ moved
// host and served a 301 page, and the DSCOVR products prefix was retired. A status
// check would have passed all three. So this checks size, rejects HTML, and requires
// a caller-supplied marker string to appear near the top of the body.
//
// It writes to <dest>.partial and renames, so an interrupted transfer never leaves a
// truncated archive where an ingester will find it and load half a series.
func FetchArchive(url, destPath, marker string, minBytes int, timeout time.Duration, ua string) (int, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", ua)

	resp, err := (&http.Client{Timeout: timeout}).Do(req)
	if err != nil {
		return 0, fmt.Errorf("fetch: %w", err)
	}
	defer resp.Body.Close()

	if final := resp.Request.URL.String(); final != url {
		fmt.Printf("  NOTE redirected to %s\n", final)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HTTP %d from %s", resp.StatusCode, resp.Request.URL)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("read: %w", err)
	}
	if len(body) < minBytes {
		return 0, fmt.Errorf("got %d bytes, expected at least %d — an error page or truncated transfer, not the archive",
			len(body), minBytes)
	}
	head := string(body[:minInt(4096, len(body))])
	if strings.Contains(strings.ToLower(head[:minInt(512, len(head))]), "<html") {
		return 0, fmt.Errorf("response begins with HTML — the endpoint is serving a page, not the archive")
	}
	if marker != "" && !strings.Contains(head, marker) {
		return 0, fmt.Errorf("header does not contain %q — the format may have changed; refusing to overwrite the good copy", marker)
	}

	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return 0, err
	}
	tmp := destPath + ".partial"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return 0, fmt.Errorf("write: %w", err)
	}
	if err := os.Rename(tmp, destPath); err != nil {
		return 0, fmt.Errorf("rename: %w", err)
	}
	return len(body), nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
