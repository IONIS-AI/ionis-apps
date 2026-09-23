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
	//
	// A FAILED COPY FAILS THE RUN. It used to print a WARN and exit 0, so the unit
	// reported success while the only X-ray history stopped growing -- which is what
	// would have happened from the first timer run on 2026-09-24: xray-archive/ had
	// been created by a hand run as 0755, and the service user could not write to it
	// (IONIS-AI/ionis-apps#34). The ingest does not run when this fails, and the
	// unit's Restart=on-failure retries.
	if *keep {
		dated, err := keepDatedCopy(out, filepath.Join(dir, "xray-archive"), time.Now().UTC())
		if err != nil {
			fmt.Fprintf(os.Stderr, "  dated copy failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("  kept %s\n", dated)
	}
}

// keepDatedCopy copies src into archDir as goes_xray_YYYYMMDD.json. The directory is
// made group-writable with setgid (mode 2775) when this creates it, so every account in
// the data group can add to it; the file is written beside its final name and
// renamed, so a copy made earlier the same day by another account is replaced, not
// refused.
func keepDatedCopy(src, archDir string, now time.Time) (string, error) {
	if _, err := os.Stat(archDir); os.IsNotExist(err) {
		if err := os.MkdirAll(archDir, 0o775|os.ModeSetgid); err != nil {
			return "", err
		}
		if err := os.Chmod(archDir, 0o775|os.ModeSetgid); err != nil { // MkdirAll is subject to umask; Go spells setgid os.ModeSetgid, not 02000
			return "", err
		}
	}
	b, err := os.ReadFile(src)
	if err != nil {
		return "", err
	}
	dated := filepath.Join(archDir, fmt.Sprintf("goes_xray_%s.json", now.Format("20060102")))
	tmp := dated + ".partial"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, dated); err != nil {
		os.Remove(tmp)
		return "", err
	}
	return dated, nil
}
