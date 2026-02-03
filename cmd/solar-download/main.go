// solar-download - Download solar flux data from NOAA and SIDC
//
// Data sources:
//   - SIDC SILSO: Daily sunspot numbers from Royal Observatory of Belgium
//   - NOAA SWPC: Solar cycle indices from Space Weather Prediction Center
//
// Build: CGO_ENABLED=0 go build -ldflags="-s -w" -o build/solar-download ./cmd/solar-download

package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// Version can be overridden at build time via -ldflags
var Version = "2.0.8"

// DataSource defines a solar data source
type DataSource struct {
	Name     string
	URL      string
	Filename string
	Desc     string
}

var sources = []DataSource{
	{
		Name:     "sidc_daily",
		URL:      "https://www.sidc.be/SILSO/DATA/SN_d_tot_V2.0.csv",
		Filename: "sidc_ssn_daily.csv",
		Desc:     "SIDC daily sunspot numbers (1818-present)",
	},
	{
		Name:     "sidc_monthly",
		URL:      "https://www.sidc.be/SILSO/DATA/SN_m_tot_V2.0.csv",
		Filename: "sidc_ssn_monthly.csv",
		Desc:     "SIDC monthly sunspot numbers (1749-present)",
	},
	{
		Name:     "noaa_sfi",
		URL:      "https://services.swpc.noaa.gov/json/solar-cycle/observed-solar-cycle-indices.json",
		Filename: "sfi_daily_flux.txt",
		Desc:     "NOAA solar cycle indices (F10.7 flux, SSN)",
	},
	{
		Name:     "noaa_predicted",
		URL:      "https://services.swpc.noaa.gov/json/solar-cycle/predicted-solar-cycle.json",
		Filename: "sfi_predicted.json",
		Desc:     "NOAA predicted solar cycle",
	},
	{
		Name:     "penticton_daily",
		URL:      "https://www.spaceweather.gc.ca/forecast-prevision/solar-solaire/solarflux/sx-5-flux-en.php?type=d",
		Filename: "penticton_flux.txt",
		Desc:     "Penticton 10.7cm daily flux",
	},
	// Solar Flux (Live)
	{
		Name:     "noaa_sfi_summary",
		URL:      "https://services.swpc.noaa.gov/products/summary/10cm-flux.json",
		Filename: "noaa_solar_flux.json",
		Desc:     "NOAA 10.7cm solar flux summary (live)",
	},
	{
		Name:     "noaa_sfi_30day",
		URL:      "https://services.swpc.noaa.gov/products/10cm-flux-30-day.json",
		Filename: "noaa_sfi_30day.json",
		Desc:     "NOAA 10.7cm solar flux (30-day history)",
	},
	// Geomagnetic indices (Earth Response)
	{
		Name:     "noaa_kp",
		URL:      "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json",
		Filename: "noaa_kp_index.json",
		Desc:     "NOAA planetary K-index (3-hourly geomagnetic)",
	},
	// X-Ray flux (Radio Blackouts)
	{
		Name:     "goes_xray",
		URL:      "https://services.swpc.noaa.gov/json/goes/primary/xrays-6-hour.json",
		Filename: "goes_xray_flux.json",
		Desc:     "GOES X-ray flux (6-hour rolling window)",
	},
	{
		Name:     "goes_xray_7day",
		URL:      "https://services.swpc.noaa.gov/json/goes/primary/xrays-7-day.json",
		Filename: "goes_xray_7day.json",
		Desc:     "GOES X-ray flux (7-day history for training)",
	},
}

func downloadFile(url, destPath string, timeout time.Duration) error {
	client := &http.Client{
		Timeout: timeout,
	}

	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("HTTP GET failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	// Create temp file
	tmpPath := destPath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create file failed: %w", err)
	}

	n, err := io.Copy(f, resp.Body)
	f.Close()

	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("download failed: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, destPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename failed: %w", err)
	}

	fmt.Printf("  Downloaded %s (%d bytes)\n", filepath.Base(destPath), n)
	return nil
}

func main() {
	destDir := flag.String("dest", "/mnt/ai-stack/solar-data/raw", "Destination directory")
	timeout := flag.Duration("timeout", 60*time.Second, "HTTP timeout per download")
	listSources := flag.Bool("list", false, "List available data sources")
	source := flag.String("source", "all", "Source to download (or 'all')")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "solar-download v%s - Solar Data Downloader\n\n", Version)
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Downloads solar flux data from NOAA and SIDC sources.\n\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nData Sources:\n")
		for _, s := range sources {
			fmt.Fprintf(os.Stderr, "  %-15s %s\n", s.Name, s.Desc)
		}
	}

	flag.Parse()

	if *listSources {
		fmt.Printf("Available solar data sources:\n\n")
		for _, s := range sources {
			fmt.Printf("  %-15s %s\n", s.Name, s.Desc)
			fmt.Printf("                  URL: %s\n", s.URL)
			fmt.Printf("                  File: %s\n\n", s.Filename)
		}
		return
	}

	fmt.Println("=========================================================")
	fmt.Printf("Solar Download v%s\n", Version)
	fmt.Println("=========================================================")
	fmt.Printf("Destination: %s\n", *destDir)
	fmt.Printf("Timeout:     %v\n", *timeout)
	fmt.Println()

	// Create destination directory
	if err := os.MkdirAll(*destDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Cannot create directory: %v\n", err)
		os.Exit(1)
	}

	startTime := time.Now()
	downloaded := 0
	failed := 0

	for _, src := range sources {
		if *source != "all" && *source != src.Name {
			continue
		}

		destPath := filepath.Join(*destDir, src.Filename)
		fmt.Printf("[%s] Downloading from %s...\n", src.Name, src.URL)

		if err := downloadFile(src.URL, destPath, *timeout); err != nil {
			fmt.Printf("  ERROR: %v\n", err)
			failed++
		} else {
			downloaded++
		}
	}

	elapsed := time.Since(startTime)

	fmt.Println()
	fmt.Println("=========================================================")
	fmt.Println("Download Summary")
	fmt.Println("=========================================================")
	fmt.Printf("Downloaded: %d files\n", downloaded)
	fmt.Printf("Failed:     %d files\n", failed)
	fmt.Printf("Elapsed:    %v\n", elapsed.Round(time.Millisecond))
	fmt.Println("=========================================================")

	if failed > 0 {
		os.Exit(1)
	}
}
