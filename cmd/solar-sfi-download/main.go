// solar-sfi-download fetches the Penticton 10.7cm flux table from NRC Canada.
//
// PENTICTON IS THE SOURCE OF RECORD, not NOAA. NOAA's 10cm-flux-30-day.json is a
// 30-day window; this is the observatory's own published series, 2004-10-28 to
// today, re-fetchable in full. 2004-10-28 is 11 months before our earliest contest
// QSO, so it covers everything we hold.
//
// It also carries THREE observations a day (17, 20, 23 UTC) where NOAA publishes a
// daily rollup, and both observed and adjusted flux.
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
		url     = flag.String("url", "https://www.spaceweather.gc.ca/solar_flux_data/daily_flux_values/fluxtable.txt", "Penticton flux table URL")
		dest    = flag.String("dest", "", "Destination directory (default: $IONIS_SOLAR_DATA_DIR)")
		timeout = flag.Duration("timeout", 5*time.Minute, "HTTP timeout")
	)
	flag.Parse()

	dir, err := common.ResolvePath(*dest, "IONIS_SOLAR_DATA_DIR", "dest", "solar raw data directory")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	out := filepath.Join(dir, "penticton_fluxtable.txt")
	fmt.Printf("solar-sfi-download v%s\n  GET %s\n", Version, *url)

	n, err := common.FetchArchive(*url, out, "fluxdate", 1<<19, *timeout,
		"ionis-solar-sfi-download/1.0 (+https://github.com/IONIS-AI/ionis-apps; KI7MT sovereign AI lab)")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("  wrote %s (%d bytes)\n", out, n)
}
