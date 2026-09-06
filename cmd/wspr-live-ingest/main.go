// wspr-live-ingest — load wspr-live-download's files into wspr.bronze.
//
// The INGEST half of the standard download-then-ingest pair, modelled on pskr-ingest.
// Reads /mnt/wspr-data/live/YYYY/MM/DD.jsonl.gz, inserts into wspr.bronze, and records
// each file in wspr.ingest_log via the shared internal/watermark package.
//
// WHY THIS EXISTS. The first recovery tool wrote straight to ClickHouse and tracked
// progress in a private /tmp state file. That put ~1.1B rows into wspr.bronze with NO
// ingest_log entry: every other IONIS source can answer "where did this row come from
// and when did it load", and WSPR could not, for a third of its corpus. It also left
// ClickHouse as the ONLY copy of every WSPR spot after 2025-01, because an API — unlike
// the wsprnet CSVs it replaced — is not a re-ingestable artifact.
//
// TWO-KEY IDEMPOTENCE. Unlike pskr-ingest, whose hourly files are immutable once closed,
// a WSPR day is NOT final when it ends: receivers upload a minute or two after each
// decode, so wspr-live-download re-fetches recent days and REPLACES their files. So the
// watermark alone is not a sufficient skip test — a file can legitimately grow.
//
//  1. Watermark (file_path + file_size): unchanged file -> skip entirely, zero work.
//  2. Id set: when a file IS new or has grown, insert only ids not already held for
//     that day.
//
// The second key is load-bearing, not belt-and-braces: wspr.bronze is a plain MergeTree
// with NO deduplication, so a re-read of a grown file would otherwise duplicate every
// row it had already contributed. An earlier version of this pipeline did exactly that
// on a narrower scope and left 1,522 duplicates behind.
//
// Build: CGO_ENABLED=0 go build -o build/bin/wspr-live-ingest ./cmd/wspr-live-ingest
package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"

	"github.com/IONIS-AI/ionis-apps/internal/bands"
	"github.com/IONIS-AI/ionis-apps/internal/watermark"
)

var Version = "dev"

type spot struct {
	ID       uint64
	Time     time.Time
	Reporter string
	RxGrid   string
	SNR      int8
	FreqHz   uint64
	Callsign string
	TxGrid   string
	Power    int8
	Drift    int8
	Distance uint32
	Azimuth  uint16
	Band     int32
	Version  string
	Code     uint8
}

// writeFreq reproduces wspr-turbo's frequency convention DELIBERATELY.
//
// wspr-turbo does uint64(frequencyMHz), storing 7 for a 7.04009 MHz spot even though the
// column is documented "Frequency in Hz" (it should be 7040090). That is a real bug
// affecting all existing rows, tracked separately. It does NOT affect `band`, which is
// computed from the full-precision float, and the IONIS signatures key on band.
//
// Writing correct Hz for only the rows this tool loads would leave two encodings in one
// column, distinguishable only by which loader wrote them. A uniformly wrong column can
// be fixed in a single pass; a half-corrected one is a trap for anyone who queries it in
// between.
func writeFreq(mhz float64) uint64 { return uint64(mhz) }

func atoiDefault(s string) int {
	if s == "" {
		return 0
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return int(f)
	}
	return 0
}

// parseFile decodes one .jsonl.gz into spots, mapped into OUR schema.
func parseFile(path string) ([]spot, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("gzip %s: %w", path, err)
	}
	defer gz.Close()

	var out []spot
	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<22)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var r struct {
			ID       json.Number `json:"id"`
			T        string      `json:"t"`
			RxSign   string      `json:"rx_sign"`
			RxLoc    string      `json:"rx_loc"`
			SNR      json.Number `json:"snr"`
			Freq     json.Number `json:"frequency"`
			TxSign   string      `json:"tx_sign"`
			TxLoc    string      `json:"tx_loc"`
			Power    json.Number `json:"power"`
			Drift    json.Number `json:"drift"`
			Distance json.Number `json:"distance"`
			Azimuth  json.Number `json:"azimuth"`
			Version  string      `json:"version"`
			Code     json.Number `json:"code"`
		}
		if err := json.Unmarshal(line, &r); err != nil {
			continue // isolate a bad row; never abort a file for one
		}
		id, err := strconv.ParseUint(r.ID.String(), 10, 64)
		if err != nil {
			continue
		}
		ts, err := time.Parse("2006-01-02 15:04:05", r.T)
		if err != nil {
			continue
		}
		freqHz, _ := strconv.ParseUint(r.Freq.String(), 10, 64)
		// Band is RECOMPUTED, never copied: wspr.live's band is MHz (7, 14, 21) while
		// ours is an ADIF band ID (105, 107, 109). Copying it would corrupt the exact
		// column the IONIS signatures are keyed on.
		band, _ := bands.GetBand(float64(freqHz) / 1_000_000.0)

		out = append(out, spot{
			ID: id, Time: ts,
			Reporter: r.RxSign, RxGrid: r.RxLoc,
			SNR:      int8(atoiDefault(r.SNR.String())),
			FreqHz:   writeFreq(float64(freqHz) / 1_000_000.0),
			Callsign: r.TxSign, TxGrid: r.TxLoc,
			Power:    int8(atoiDefault(r.Power.String())),
			Drift:    int8(atoiDefault(r.Drift.String())),
			Distance: uint32(atoiDefault(r.Distance.String())),
			Azimuth:  uint16(atoiDefault(r.Azimuth.String())),
			Band:     band, Version: r.Version,
			Code: uint8(atoiDefault(r.Code.String())),
		})
	}
	return out, sc.Err()
}

// dayFromPath recovers the UTC day a file covers from YYYY/MM/DD.jsonl.gz.
func dayFromPath(rel string) (time.Time, error) {
	parts := strings.Split(filepath.ToSlash(rel), "/")
	if len(parts) < 3 {
		return time.Time{}, fmt.Errorf("unexpected layout: %s", rel)
	}
	dd := strings.TrimSuffix(parts[len(parts)-1], ".jsonl.gz")
	return time.Parse("2006/01/02", parts[len(parts)-3]+"/"+parts[len(parts)-2]+"/"+dd)
}

// existingIDs returns ids already held for a day, widened an hour each side. The window
// is wider than the file because the upstream sometimes carries one spot id at two
// timestamps a minute apart; a day-exact lookup misses the copy sitting at :59 of the
// previous day and re-inserts it.
func existingIDs(ctx context.Context, conn *ch.Client, table string, day time.Time) (map[uint64]struct{}, error) {
	next := day.AddDate(0, 0, 1)
	ids := make(map[uint64]struct{}, 1<<21)
	var col proto.ColUInt64
	err := conn.Do(ctx, ch.Query{
		Body: fmt.Sprintf(`SELECT id FROM %s WHERE timestamp >= toDateTime('%s') - INTERVAL 1 HOUR AND timestamp < toDateTime('%s') + INTERVAL 1 HOUR`,
			table, day.Format("2006-01-02 15:04:05"), next.Format("2006-01-02 15:04:05")),
		Result: proto.Results{{Name: "id", Data: &col}},
		OnResult: func(ctx context.Context, b proto.Block) error {
			for _, v := range col {
				ids[v] = struct{}{}
			}
			return nil
		},
	})
	return ids, err
}

func fixed(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s + strings.Repeat("\x00", n-len(s))
}

func insertSpots(ctx context.Context, conn *ch.Client, table string, spots []spot) error {
	var (
		cID                                    proto.ColUInt64
		cTime                                  proto.ColDateTime
		cRep, cRepG, cCall, cGrid, cMode, cVer proto.ColFixedStr
		cSNR, cPow, cDrift                     proto.ColInt8
		cFreq                                  proto.ColUInt64
		cDist                                  proto.ColUInt32
		cAz                                    proto.ColUInt16
		cBand                                  proto.ColInt32
		cCode, cCols                           proto.ColUInt8
	)
	cRep.SetSize(16)
	cRepG.SetSize(8)
	cCall.SetSize(16)
	cGrid.SetSize(8)
	cMode.SetSize(8)
	cVer.SetSize(8)

	for _, s := range spots {
		cID.Append(s.ID)
		cTime.Append(s.Time)
		cRep.Append([]byte(fixed(s.Reporter, 16)))
		cRepG.Append([]byte(fixed(s.RxGrid, 8)))
		cSNR.Append(s.SNR)
		cFreq.Append(s.FreqHz)
		cCall.Append([]byte(fixed(s.Callsign, 16)))
		cGrid.Append([]byte(fixed(s.TxGrid, 8)))
		cPow.Append(s.Power)
		cDrift.Append(s.Drift)
		cDist.Append(s.Distance)
		cAz.Append(s.Azimuth)
		cBand.Append(s.Band)
		cMode.Append([]byte(fixed("WSPR", 8)))
		cVer.Append([]byte(fixed(s.Version, 8)))
		cCode.Append(s.Code)
		cCols.Append(15)
	}
	return conn.Do(ctx, ch.Query{
		Body: fmt.Sprintf("INSERT INTO %s (id, timestamp, reporter, reporter_grid, snr, frequency, callsign, grid, power, drift, distance, azimuth, band, mode, version, code, column_count) VALUES", table),
		Input: proto.Input{
			{Name: "id", Data: &cID}, {Name: "timestamp", Data: &cTime},
			{Name: "reporter", Data: &cRep}, {Name: "reporter_grid", Data: &cRepG},
			{Name: "snr", Data: &cSNR}, {Name: "frequency", Data: &cFreq},
			{Name: "callsign", Data: &cCall}, {Name: "grid", Data: &cGrid},
			{Name: "power", Data: &cPow}, {Name: "drift", Data: &cDrift},
			{Name: "distance", Data: &cDist}, {Name: "azimuth", Data: &cAz},
			{Name: "band", Data: &cBand}, {Name: "mode", Data: &cMode},
			{Name: "version", Data: &cVer}, {Name: "code", Data: &cCode},
			{Name: "column_count", Data: &cCols},
		},
	})
}

func discoverFiles(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // a missing subtree is not fatal; report what exists
		}
		if !d.IsDir() && strings.HasSuffix(p, ".jsonl.gz") {
			files = append(files, p)
		}
		return nil
	})
	sort.Strings(files)
	return files, err
}

func main() {
	var (
		src    = flag.String("src", "/mnt/wspr-data/live", "Source root (YYYY/MM/DD.jsonl.gz)")
		host   = flag.String("host", "10.60.1.1:9000", "ClickHouse host:port")
		db     = flag.String("db", "wspr", "ClickHouse database")
		table  = flag.String("table", "bronze", "ClickHouse table")
		dryRun = flag.Bool("dry-run", false, "List files that would load; write nothing")
		prime  = flag.Bool("prime", false, "Mark all existing files as loaded without loading (bootstrap)")
		full   = flag.Bool("full", false, "Ignore the watermark and reconsider every file (id-dedup still applies)")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "wspr-live-ingest v%s — load wspr-live-download files into wspr.bronze\n\n", Version)
		fmt.Fprintf(os.Stderr, "  wspr-live-ingest                  # load new/grown files\n")
		fmt.Fprintf(os.Stderr, "  wspr-live-ingest --dry-run        # show what would load\n")
		fmt.Fprintf(os.Stderr, "  wspr-live-ingest --prime          # bootstrap watermark, load nothing\n")
		fmt.Fprintf(os.Stderr, "  wspr-live-ingest --full           # reconsider every file\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	log.Printf("wspr-live-ingest v%s", Version)
	fq := *db + "." + *table

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	files, err := discoverFiles(*src)
	if err != nil {
		log.Fatalf("discover %s: %v", *src, err)
	}
	log.Printf("Found %d file(s) under %s", len(files), *src)

	if *prime {
		var infos []watermark.FileInfo
		for _, f := range files {
			fi, err := os.Stat(f)
			if err != nil {
				continue
			}
			rel, _ := filepath.Rel(*src, f)
			infos = append(infos, watermark.FileInfo{RelPath: rel, Size: uint64(fi.Size())})
		}
		n, err := watermark.PrimeFiles(ctx, *host, *db, infos)
		if err != nil {
			log.Fatalf("prime: %v", err)
		}
		log.Printf("Primed %d file(s) — nothing loaded", n)
		return
	}

	wm, err := watermark.LoadWatermark(ctx, *host, *db)
	if err != nil {
		log.Fatalf("load watermark: %v", err)
	}
	log.Printf("Watermark holds %d file(s)", len(wm))

	conn, err := ch.Dial(ctx, ch.Options{Address: *host})
	if err != nil {
		log.Fatalf("ClickHouse dial %s: %v", *host, err)
	}
	defer conn.Close()

	var loaded, skipped, inserted, held int
	for _, path := range files {
		if ctx.Err() != nil {
			log.Printf("interrupted — re-run to continue")
			break
		}
		rel, _ := filepath.Rel(*src, path)
		fi, err := os.Stat(path)
		if err != nil {
			continue
		}

		// Key 1 — watermark. A file whose size matches what we already loaded cannot
		// have gained rows, so it is skipped without touching ClickHouse at all.
		if !*full {
			if e, ok := wm[rel]; ok && e.FileSize == uint64(fi.Size()) {
				skipped++
				continue
			}
		}

		day, err := dayFromPath(rel)
		if err != nil {
			log.Printf("[%s] skipping — %v", rel, err)
			continue
		}

		spots, err := parseFile(path)
		if err != nil {
			log.Printf("[%s] parse failed: %v", rel, err)
			continue
		}

		if *dryRun {
			log.Printf("[%s] would load %d row(s)", rel, len(spots))
			loaded++
			continue
		}

		// Key 2 — id set. The file may be a REPLACEMENT of one already partly loaded
		// (a settling day that grew). bronze is a plain MergeTree with no dedup, so
		// inserting the whole file again would duplicate every row it already gave us.
		have, err := existingIDs(ctx, conn, fq, day)
		if err != nil {
			log.Printf("[%s] existing-id lookup failed: %v", rel, err)
			continue
		}
		fresh := spots[:0:0]
		seen := make(map[uint64]struct{}, len(spots))
		for _, s := range spots {
			if _, ok := have[s.ID]; ok {
				continue
			}
			if _, ok := seen[s.ID]; ok {
				continue // upstream can repeat an id within one response
			}
			seen[s.ID] = struct{}{}
			fresh = append(fresh, s)
		}

		start := time.Now()
		if len(fresh) > 0 {
			if err := insertSpots(ctx, conn, fq, fresh); err != nil {
				log.Printf("[%s] INSERT FAILED: %v", rel, err)
				continue
			}
		}
		elapsed := uint32(time.Since(start).Milliseconds())

		// Watermark only AFTER a successful insert. A failed file stays unwatermarked so
		// the next run retries it rather than silently leaving a hole.
		if err := watermark.InsertLogEntry(ctx, conn, *db, rel, uint64(fi.Size()), uint64(len(fresh)), elapsed); err != nil {
			log.Printf("[%s] loaded %d rows but WATERMARK FAILED: %v", rel, len(fresh), err)
		}

		loaded++
		inserted += len(fresh)
		held += len(spots) - len(fresh)
		log.Printf("[%s] parsed=%-8d new=%-8d already-held=%-8d %dms", rel, len(spots), len(fresh), len(spots)-len(fresh), elapsed)
	}

	log.Printf("---------------------------------------------------------")
	log.Printf("Files loaded: %d   skipped (watermark): %d", loaded, skipped)
	log.Printf("Rows inserted: %d   already held: %d", inserted, held)
	if *dryRun {
		log.Printf("DRY RUN — nothing was written")
	}
}
