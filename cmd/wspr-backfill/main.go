// wspr-backfill — recover the WSPR spots wsprnet's monthly CSV export never published.
//
// THE DEFECT (upstream, precisely dated). From 2023-10-16 onward, wsprnet.org's monthly
// archive export omits most spots in the first ~10 minutes of every hour. Measured
// retention per 2-minute WSPR slot (2026-03, ours vs the complete stream):
//
//	:00  0.9%   :02  0.8%   :04  1.4%   :06 13.3%   :08 69.1%   :10+ 100.0%
//
// The ramp is the signature of an upload-latency cutoff — receivers upload a minute or
// two after each transmission window, and the export reads its window before the late
// arrivals land. It is NOT our bug: the depletion is present in the raw .csv.gz, our
// row counts match those files exactly (2026-03: 203,240,942 both sides), and our
// bronze minute-distribution matches the file's. See KI7MT/fleet-ops#118 for how it
// went unnoticed for ~3 years (the fleet health check was itself dead).
//
// THE RECOVERY. wspr.live ingests the live stream and never touches the CSV export, so
// it holds the missing spots. We already hold :10-:58 byte-identical to it, so this
// tool pulls ONLY minute<10 and only for the affected era. That is ~786M rows instead
// of ~6B — an 8x smaller ask on a volunteer-run service, which is the point.
//
// SAFETY. wspr.bronze is a plain MergeTree with NO deduplication, so a careless re-run
// would duplicate rows permanently. Two properties make this safe:
//   - wspr.live shares our `id` namespace (verified identical row-for-row on a slot we
//     hold complete), so "already have it" is an exact test, not a heuristic.
//   - Every insert is filtered against the ids already present for that day.
//
// Band is RECOMPUTED via bands.GetBand, never copied: wspr.live's `band` is MHz
// (7, 14, 21) while ours is an ADIF band ID (105, 107, 109). Copying it would silently
// corrupt the column the IONIS signatures are keyed on.
//
// Frequency deliberately reproduces the existing (wrong) convention — see writeFreq.
//
// Build: CGO_ENABLED=0 go build -o build/wspr-backfill ./cmd/wspr-backfill
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"

	"github.com/IONIS-AI/ionis-apps/internal/bands"
)

var Version = "dev"

const (
	liveEndpoint = "https://db1.wspr.live/"
	// The export defect begins here. 2023-10-15 is the last clean day (:00 slot at 76%
	// of the :10 slot, the healthy baseline); 2023-10-16 drops to 1.3% overnight.
	defectStart = "2023-10-16"
	userAgent   = "ionis-wspr-backfill/1.0 (+https://github.com/IONIS-AI/ionis-apps; KI7MT sovereign AI lab)"
)

// spot is one recovered row, already mapped into OUR schema.
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

// writeFreq reproduces wspr-turbo's frequency convention ON PURPOSE.
//
// wspr-turbo does `uint64(frequencyMHz)`, storing 7 for a 7.04009 MHz spot even though
// the column is documented "Frequency in Hz" (it should be 7040090). That is a real bug
// affecting all 11.48B existing rows, filed separately — but it does NOT affect `band`
// (computed from the full-precision float before truncation) and the IONIS signatures
// key on band, not frequency, so nothing published depends on it.
//
// Writing correct Hz for only the ~786M backfilled rows would make the column
// self-inconsistent: two encodings in one column, distinguishable only by knowing which
// rows came from which loader. A uniformly wrong column can be fixed in one pass; a
// half-corrected one is a trap for anyone who queries it in between. So we match the
// existing convention here and fix the whole column deliberately, later.
func writeFreq(mhz float64) uint64 { return uint64(mhz) }

// fetchDay pulls minute<10 spots for a single UTC day from wspr.live.
func fetchDay(ctx context.Context, client *http.Client, day time.Time) ([]spot, error) {
	next := day.AddDate(0, 0, 1)
	// JSONEachRow keeps parsing streaming and per-row, so a malformed row is isolated
	// rather than poisoning a whole day's decode.
	q := fmt.Sprintf(`SELECT id, toString(time) AS t, rx_sign, rx_loc, snr, frequency,
	                         tx_sign, tx_loc, power, drift, distance, azimuth, version, code
	                  FROM wspr.rx
	                  WHERE time >= '%s' AND time < '%s' AND toMinute(time) < 10
	                  ORDER BY id
	                  FORMAT JSONEachRow`,
		day.Format("2006-01-02"), next.Format("2006-01-02"))

	req, err := http.NewRequestWithContext(ctx, "GET",
		liveEndpoint+"?query="+url.QueryEscape(q), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("wspr.live request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("wspr.live HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var out []spot
	sc := bufio.NewScanner(resp.Body)
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
			continue // isolate a bad row; never abort a day for one
		}
		id, err := strconv.ParseUint(r.ID.String(), 10, 64)
		if err != nil {
			continue
		}
		ts, err := time.Parse("2006-01-02 15:04:05", r.T)
		if err != nil {
			continue
		}
		// wspr.live's `frequency` is genuine Hz; band normalisation wants MHz.
		freqHz, _ := strconv.ParseUint(r.Freq.String(), 10, 64)
		band, _ := bands.GetBand(float64(freqHz) / 1_000_000.0)

		out = append(out, spot{
			ID:       id,
			Time:     ts,
			Reporter: r.RxSign,
			RxGrid:   r.RxLoc,
			SNR:      int8(atoiDefault(r.SNR.String())),
			FreqHz:   writeFreq(float64(freqHz) / 1_000_000.0),
			Callsign: r.TxSign,
			TxGrid:   r.TxLoc,
			Power:    int8(atoiDefault(r.Power.String())),
			Drift:    int8(atoiDefault(r.Drift.String())),
			Distance: uint32(atoiDefault(r.Distance.String())),
			Azimuth:  uint16(atoiDefault(r.Azimuth.String())),
			Band:     band,
			Version:  r.Version,
			Code:     uint8(atoiDefault(r.Code.String())),
		})
	}
	return out, sc.Err()
}

func atoiDefault(s string) int {
	if s == "" {
		return 0
	}
	// Values arrive as JSON numbers; a float (e.g. "-12.0") must not become 0.
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return int(f)
	}
	return 0
}

// existingIDs returns the ids we ALREADY hold for the whole day — deliberately NOT just
// the minute<10 window we fetch. This is what makes the tool idempotent against a
// MergeTree with no dedup.
//
// Scoping this to minute<10 (the first version) let 1,522 duplicates through on a real
// run. wspr.live sometimes carries the SAME spot id at two timestamps a minute apart —
// e.g. id 9346106285 at both 17:59:00 and 18:00:00. We already held the :59 copy from
// the CSV, but a minute<10 lookup could not see it, so the :00 copy was inserted as
// "new". Widening the lookup to the full day closes that: an id we hold at ANY minute
// counts as held.
func existingIDs(ctx context.Context, conn *ch.Client, table string, day time.Time) (map[uint64]struct{}, error) {
	next := day.AddDate(0, 0, 1)
	ids := make(map[uint64]struct{}, 1<<21)
	var col proto.ColUInt64
	// Widened by one hour on each side as well: a cross-midnight duplicate pair (23:59
	// / 00:00) is the same failure one day boundary over.
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
		cID    proto.ColUInt64
		cTime  proto.ColDateTime
		cRep   proto.ColFixedStr
		cRepG  proto.ColFixedStr
		cSNR   proto.ColInt8
		cFreq  proto.ColUInt64
		cCall  proto.ColFixedStr
		cGrid  proto.ColFixedStr
		cPow   proto.ColInt8
		cDrift proto.ColInt8
		cDist  proto.ColUInt32
		cAz    proto.ColUInt16
		cBand  proto.ColInt32
		cMode  proto.ColFixedStr
		cVer   proto.ColFixedStr
		cCode  proto.ColUInt8
		cCols  proto.ColUInt8
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
		cCols.Append(15) // CSV-shape artifact; constant for this era
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

func main() {
	var (
		chHost   = flag.String("ch-host", "10.60.1.1:9000", "ClickHouse native address")
		table    = flag.String("table", "wspr.bronze", "Destination table")
		startStr = flag.String("start", defectStart, "Start date YYYY-MM-DD")
		endStr   = flag.String("end", "", "End date YYYY-MM-DD, exclusive (default: today)")
		delay    = flag.Duration("delay", 3*time.Second, "Pause between days — be kind to wspr.live")
		timeout  = flag.Duration("timeout", 180*time.Second, "HTTP timeout per day")
		dryRun   = flag.Bool("dry-run", false, "Fetch and map, but do not insert")
		state    = flag.String("state", "", "Checkpoint file for resume (default: none)")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "wspr-backfill v%s — recover WSPR spots wsprnet never published\n\n", Version)
		fmt.Fprintf(os.Stderr, "Pulls ONLY minute<10 spots (the slots wsprnet's export drops since\n")
		fmt.Fprintf(os.Stderr, "%s) from wspr.live, and inserts only ids not already held.\n\n", defectStart)
		flag.PrintDefaults()
	}
	flag.Parse()

	log.Printf("=========================================================")
	log.Printf("wspr-backfill v%s", Version)
	log.Printf("=========================================================")

	start, err := time.Parse("2006-01-02", *startStr)
	if err != nil {
		log.Fatalf("bad -start: %v", err)
	}
	end := time.Now().UTC().Truncate(24 * time.Hour)
	if *endStr != "" {
		if end, err = time.Parse("2006-01-02", *endStr); err != nil {
			log.Fatalf("bad -end: %v", err)
		}
	}
	if !start.Before(end) {
		log.Fatalf("-start (%s) must precede -end (%s)", start.Format("2006-01-02"), end.Format("2006-01-02"))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, err := ch.Dial(ctx, ch.Options{Address: *chHost})
	if err != nil {
		log.Fatalf("ClickHouse dial %s: %v", *chHost, err)
	}
	defer conn.Close()

	client := &http.Client{Timeout: *timeout}
	done := loadState(*state)

	var totFetched, totInserted, totSkipped, totDays int
	for d := start; d.Before(end); d = d.AddDate(0, 0, 1) {
		key := d.Format("2006-01-02")
		if _, ok := done[key]; ok {
			continue
		}
		if ctx.Err() != nil {
			log.Printf("interrupted — resume with -state %s", *state)
			break
		}

		spots, err := fetchDay(ctx, client, d)
		if err != nil {
			// Do NOT checkpoint a failed day: leaving it unmarked is what makes a
			// resume correct rather than silently skipping a hole.
			log.Printf("[%s] FETCH FAILED: %v", key, err)
			time.Sleep(*delay)
			continue
		}

		have, err := existingIDs(ctx, conn, *table, d)
		if err != nil {
			log.Printf("[%s] existing-id lookup failed: %v", key, err)
			time.Sleep(*delay)
			continue
		}

		// Two filters, not one. `have` covers ids already in the table; `seen` covers
		// ids repeated WITHIN this fetch — wspr.live can return the same id twice in a
		// single response, and without this the batch duplicates itself on insert.
		fresh := spots[:0:0]
		seen := make(map[uint64]struct{}, len(spots))
		for _, s := range spots {
			if _, ok := have[s.ID]; ok {
				continue
			}
			if _, ok := seen[s.ID]; ok {
				continue
			}
			seen[s.ID] = struct{}{}
			fresh = append(fresh, s)
		}

		if !*dryRun && len(fresh) > 0 {
			if err := insertSpots(ctx, conn, *table, fresh); err != nil {
				log.Printf("[%s] INSERT FAILED: %v", key, err)
				time.Sleep(*delay)
				continue
			}
		}

		totFetched += len(spots)
		totInserted += len(fresh)
		totSkipped += len(spots) - len(fresh)
		totDays++
		log.Printf("[%s] fetched=%-7d new=%-7d already-held=%-7d", key, len(spots), len(fresh), len(spots)-len(fresh))

		if !*dryRun {
			markState(*state, key)
		}
		time.Sleep(*delay)
	}

	log.Printf("---------------------------------------------------------")
	log.Printf("Days processed: %d", totDays)
	log.Printf("Fetched:        %d", totFetched)
	log.Printf("Inserted:       %d", totInserted)
	log.Printf("Already held:   %d", totSkipped)
	if *dryRun {
		log.Printf("DRY RUN — nothing was written")
	}
}

func loadState(path string) map[string]struct{} {
	done := map[string]struct{}{}
	if path == "" {
		return done
	}
	f, err := os.Open(path)
	if err != nil {
		return done
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if l := strings.TrimSpace(sc.Text()); l != "" {
			done[l] = struct{}{}
		}
	}
	return done
}

func markState(path, key string) {
	if path == "" {
		return
	}
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintln(f, key)
}
