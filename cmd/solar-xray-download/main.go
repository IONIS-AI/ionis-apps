// solar-xray-download fetches the GOES X-ray flux series from NOAA SWPC.
//
// THIS ONE HAS NO DEEP ARCHIVE AT THIS ENDPOINT. SWPC serves 7 days of 1-minute
// samples and nothing older. The long history lives at NCEI
// (ncei.noaa.gov/data/goes-space-environment-monitor) as per-satellite, per-month
// files, which is a different fetch shape and a separate tool.
//
// So this is the LIVE feed and it must run daily or data is lost -- the same
// structural weakness that cost five months of Kp. The difference is that Kp had a
// definitive archive to rebuild from and X-ray does not, at least not from here.
// Every raw file is therefore kept on disk under its own date, not overwritten, so
// the 1-minute series accumulates even though the table stores 3-hour aggregates.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/IONIS-AI/ionis-apps/internal/common"
)

var Version = "dev"

func main() {
	var (
		url     = flag.String("url", "https://services.swpc.noaa.gov/json/goes/primary/xrays-7-day.json", "GOES X-ray 7-day JSON")
		dest    = flag.String("dest", "", "Destination directory (default: $IONIS_SOLAR_DATA_DIR)")
		timeout = flag.Duration("timeout", 5*time.Minute, "HTTP timeout")
		keep    = flag.Bool("dated", true, "Also keep a dated copy so the 1-minute series accumulates")
	)
	flag.Parse()

	dir, err := common.ResolvePath(*dest, "IONIS_SOLAR_DATA_DIR", "dest", "solar raw data directory")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	out := filepath.Join(dir, "goes_xray_7day.json")
	fmt.Printf("solar-xray-download v%s\n  GET %s\n", Version, *url)

	n, err := common.FetchArchive(*url, out, "time_tag", 1<<18, *timeout,
		"ionis-solar-xray-download/1.0 (+https://github.com/IONIS-AI/ionis-apps; KI7MT sovereign AI lab)")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("  wrote %s (%d bytes)\n", out, n)

	// A dated copy, because this endpoint's window is only 7 days wide. Overwriting
	// one rolling file means a missed week is unrecoverable; keeping dated copies
	// means the archive grows even though the table aggregates.
	if *keep {
		archDir := filepath.Join(dir, "xray-archive")
		if err := os.MkdirAll(archDir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "  WARN could not create %s: %v\n", archDir, err)
			return
		}
		dated := filepath.Join(archDir, fmt.Sprintf("goes_xray_%s.json", time.Now().UTC().Format("20060102")))
		if b, err := os.ReadFile(out); err == nil {
			if err := os.WriteFile(dated, b, 0o644); err != nil {
				fmt.Fprintf(os.Stderr, "  WARN dated copy failed: %v\n", err)
			} else {
				fmt.Printf("  kept %s\n", dated)
			}
		}
	}
}
