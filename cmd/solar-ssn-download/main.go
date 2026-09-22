// solar-ssn-download fetches the SIDC/SILSO daily total sunspot number series.
//
// SIDC (Royal Observatory of Belgium) is the world authority on sunspot number and
// publishes the full daily series from 1818 -- 76k rows, 2.8 MB. We take the whole
// file; restricting it to our era would save 2 MB and discard the only cheap thing
// about it.
//
// SSN IS REVISED. SILSO reprocesses the series, so a value published today can
// change, and rows carry a definitive flag. That is why the destination is a
// ReplacingMergeTree and why re-fetching in full is the normal operation rather
// than an exception.
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
		url     = flag.String("url", "https://www.sidc.be/SILSO/INFO/sndtotcsv.php", "SIDC daily SSN CSV URL")
		dest    = flag.String("dest", "", "Destination directory (default: $IONIS_SOLAR_DATA_DIR)")
		timeout = flag.Duration("timeout", 5*time.Minute, "HTTP timeout")
	)
	flag.Parse()

	dir, err := common.ResolvePath(*dest, "IONIS_SOLAR_DATA_DIR", "dest", "solar raw data directory")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	out := filepath.Join(dir, "sidc_ssn_daily.csv")
	fmt.Printf("solar-ssn-download v%s\n  GET %s\n", Version, *url)

	// Marker "1818" is the first data year and appears in the opening bytes. It is a
	// weaker assertion than a header string, because this file has no header -- but
	// it still distinguishes the series from an error page.
	n, err := common.FetchArchive(*url, out, "1818", 1<<20, *timeout,
		"ionis-solar-ssn-download/1.0 (+https://github.com/IONIS-AI/ionis-apps; KI7MT sovereign AI lab)")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("  wrote %s (%d bytes)\n", out, n)
}
