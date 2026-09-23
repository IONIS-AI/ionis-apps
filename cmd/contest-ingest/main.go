// contest-ingest — Parse Cabrillo contest logs into ClickHouse
//
// Walks /mnt/contest-logs/{contest}/{yearmode}/*.log, parses Cabrillo headers
// and QSO lines, normalizes band via bands.GetBand(), and batch INSERTs into
// contest.bronze using ch-go native protocol with LZ4 compression.
//
// Uses a watermark table (contest.ingest_log) to track which files have been
// loaded, so only new files are processed on each run. Supports --full (reload
// all), --prime (bootstrap watermark), and --dry-run (list pending files).
//
// Optionally extracts GRID-LOCATOR headers and enriches wspr.callsign_grid.
//
// Build: CGO_ENABLED=0 go build -ldflags="-s -w" -o build/contest-ingest ./cmd/contest-ingest

package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"github.com/IONIS-AI/ionis-apps/internal/common"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/IONIS-AI/ionis-apps/internal/bands"
	"github.com/IONIS-AI/ionis-apps/internal/watermark"
)

var Version = "dev"

const (
	DefaultBatchSize = 100_000
	DefaultWorkers   = 8
)

// theirCallIdx gives the 0-based index of the RECEIVED CALLSIGN in a QSO line after
// the "QSO:" token is stripped, per contest.
//
// THERE IS A CABRILLO TEMPLATE PER CONTEST, and the received callsign does not sit
// in one place across them. The generic template is
//
//	freq mo date time call rst exch call rst exch t          -> their call at f[7]
//
// but Sweepstakes carries a four-part exchange:
//
//	freq mo date time call nr p ck sec call nr p ck sec      -> their call at f[9]
//
// so a single hardcoded index writes the SECTION into call_2 for every SS QSO. RTTY
// and digital variants differ again.
//
// Verified against the mirror rather than taken on faith: for each contest, the first
// field after my_call holding a callsign that is not my_call, over 3,000 lines per
// contest. Every contest came back 100% on ONE index, and ARRL-SS came back f[9] --
// the published template page says the received call is at "position 10", which
// counted against a real SS line is the section, not the call. The data wins.
//
// Contests absent here fall back to the generic f[7].
var theirCallIdx = map[string]int{
	"ARRL-SS-CW":  9,
	"ARRL-SS-SSB": 9,
	"CQ-WW-RTTY":  8,
	"WW-DIGI":     6,
	"ARRL-DIGI":   6,
}

const genericTheirCallIdx = 7

// specTheirCallIdx returns the template position for a contest, or the generic one.
func specTheirCallIdx(contestID string) int {
	if i, ok := theirCallIdx[contestID]; ok {
		return i
	}
	return genericTheirCallIdx
}

// callsignRe matches amateur radio callsigns: 1-3 prefix chars, a digit, then a
// suffix ending in a letter. Covers K1ABC, JA1XYZ, 3DA0NW, VK9DWX.
//
// THE SUFFIX RUNS TO SIX, NOT THREE. Special-event and commemorative calls carry long
// suffixes -- SN0MARCONI, HG24TISZA, OH100SRAL, DL60RRDXA -- and a 3-char cap rejected
// them, so parseQSOLine reported "no their_call found" and SKIPPED the QSO outright.
// Measured at 6,773 in a 15.2M-field sample (~0.045%).
//
// Widening is safe because this regex is also how the worked station is LOCATED among
// the fields: if it matched an exchange, the wrong field would become their_call.
// Checked against rst_sent, exch_sent, rst_rcvd and exch_rcvd over 7,209,211 QSO
// lines -- the wider form matches ZERO that the narrow form did not. An exchange is
// digits, or letters with no digit, and neither satisfies digit-then-letter-ending.
var callsignRe = regexp.MustCompile(`^[A-Z0-9]{1,3}[0-9][A-Z0-9]{0,6}[A-Z]$`)

// gridRe matches 4- or 6-character Maidenhead grid locators.
var gridRe = regexp.MustCompile(`^[A-R]{2}[0-9]{2}([A-X]{2})?$`)

// Stats tracks ingestion metrics with atomic operations.
type Stats struct {
	TotalRows   atomic.Uint64
	TotalFiles  atomic.Uint64
	SkippedRows atomic.Uint64
	// LabelMismatches counts files whose CONTEST: header disagreed with the
	// directory. Not an error -- see the Contest label block -- but a rising count
	// means the mirror layout and the headers have drifted apart, which is worth
	// knowing before it is worth acting on.
	LabelMismatches atomic.Uint64
	Quarantined     atomic.Uint64
	FailedFiles     atomic.Uint64
	GridsFound      atomic.Uint64
	StartTime       time.Time
}

// CabrilloHeaders holds parsed header values from a Cabrillo log.
type CabrilloHeaders struct {
	Callsign string
	Contest  string
	Grid     string // from GRID-LOCATOR or HQ-GRID-LOCATOR
}

// QSO represents a single parsed contest QSO.
type QSO struct {
	Timestamp time.Time
	Frequency uint32
	Band      int32
	Mode      string
	Call1     string
	Call2     string
	RSTSent   string
	ExchSent  string
	RSTRcvd   string
	ExchRcvd  string
	Contest   string
	Source    string
}

// ContestBatch holds columnar data for a batch INSERT into contest.bronze.
type ContestBatch struct {
	Timestamp *proto.ColDateTime
	Frequency *proto.ColUInt32
	Band      *proto.ColInt32
	Mode      *proto.ColLowCardinality[string]
	Call1     *proto.ColStr
	Call2     *proto.ColStr
	RSTSent   *proto.ColStr
	ExchSent  *proto.ColStr
	RSTRcvd   *proto.ColStr
	ExchRcvd  *proto.ColStr
	Contest   *proto.ColLowCardinality[string]
	Source    *proto.ColLowCardinality[string]
}

func NewContestBatch() *ContestBatch {
	return &ContestBatch{
		Timestamp: new(proto.ColDateTime),
		Frequency: new(proto.ColUInt32),
		Band:      new(proto.ColInt32),
		Mode:      new(proto.ColStr).LowCardinality(),
		Call1:     new(proto.ColStr),
		Call2:     new(proto.ColStr),
		RSTSent:   new(proto.ColStr),
		ExchSent:  new(proto.ColStr),
		RSTRcvd:   new(proto.ColStr),
		ExchRcvd:  new(proto.ColStr),
		Contest:   new(proto.ColStr).LowCardinality(),
		Source:    new(proto.ColStr).LowCardinality(),
	}
}

func (b *ContestBatch) Reset() {
	b.Timestamp.Reset()
	b.Frequency.Reset()
	b.Band.Reset()
	b.Mode.Reset()
	b.Call1.Reset()
	b.Call2.Reset()
	b.RSTSent.Reset()
	b.ExchSent.Reset()
	b.RSTRcvd.Reset()
	b.ExchRcvd.Reset()
	b.Contest.Reset()
	b.Source.Reset()
}

func (b *ContestBatch) Len() int {
	return b.Timestamp.Rows()
}

func (b *ContestBatch) Input() proto.Input {
	return proto.Input{
		{Name: "timestamp", Data: b.Timestamp},
		{Name: "frequency", Data: b.Frequency},
		{Name: "band", Data: b.Band},
		{Name: "mode", Data: b.Mode},
		{Name: "call_1", Data: b.Call1},
		{Name: "call_2", Data: b.Call2},
		{Name: "rst_sent", Data: b.RSTSent},
		{Name: "exch_sent", Data: b.ExchSent},
		{Name: "rst_rcvd", Data: b.RSTRcvd},
		{Name: "exch_rcvd", Data: b.ExchRcvd},
		{Name: "contest", Data: b.Contest},
		{Name: "source", Data: b.Source},
	}
}

// isCallsign checks if a string looks like an amateur radio callsign.
// Must contain both a letter and a digit, be 3+ chars, and match the callsign pattern.
// Also accepts callsigns with /suffix (e.g., HB9DAX/QRP, W1AW/4).
// hasAlnum reports whether s contains at least one letter or digit. It is the whole
// test applied to a logging station: anything else would be judging a callsign by a
// pattern, and the patterns keep being wrong about real stations.
func hasAlnum(s string) bool {
	for _, r := range s {
		if (r >= '0' && r <= '9') || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') {
			return true
		}
	}
	return false
}

// baseCall returns the actual callsign inside a compound callsign, or "" if the
// string holds none.
//
// WHICH SIDE OF THE SLASH IS THE CALLSIGN DEPENDS ON THE FORM, so this cannot just
// take one side -- which is what it used to do, keeping everything before the first
// slash:
//
//	LX/ON9TT      location prefix  the call is AFTER  (ON9TT operating in Luxembourg)
//	KI7MT/KP4     location suffix  the call is BEFORE (KI7MT operating in KP4)
//	K1ABC/4       call-area suffix the call is BEFORE
//	DL2AW/P       operating suffix the call is BEFORE (/P portable, /M mobile,
//	                               /MM maritime, /AM aeronautical, /QRP low power)
//	PA/DL2AW/P    both             the call is in the MIDDLE
//
// Taking the leading component turned LX/ON9TT into "LX", which matches no callsign
// pattern, so parseQSOLine reported "no their_call found" and the whole QSO was
// SKIPPED rather than mis-parsed. Those are systematically the DX -- operators away
// from their home country -- which is the population this data exists to describe.
//
// The rule that works on every form: split on "/" and keep the components that are
// callsign-shaped. A DXCC prefix is not (KP4, WP4, EA5 end in a digit; LX, 9A, PA
// have no digit-then-letter), and neither is an operating suffix (P, M, MM, AM,
// QRP), so in practice exactly one component survives. When two do -- VP2E/K1ABC, where the
// prefix is itself a valid call -- the longer one is the operator and the shorter
// the location, so length breaks the tie.
func baseCall(s string) string {
	best := ""
	for _, part := range strings.Split(strings.ToUpper(s), "/") {
		if len(part) < 2 || !callsignRe.MatchString(part) {
			continue
		}
		if len(part) > len(best) {
			best = part
		}
	}
	return best
}

func isCallsign(s string) bool {
	return baseCall(s) != ""
}

// cabrilloBandKHz maps the Cabrillo 3.0 band designators allowed in the QSO frequency
// field above 1 GHz to the band's lower edge in kHz. A log may give the band instead
// of a frequency there; `2.3G` is legal Cabrillo, not a malformed number. LIGHT has no
// frequency to store and is left to fail.
var cabrilloBandKHz = map[string]uint64{
	"1.2G": 1_240_000, "2.3G": 2_300_000, "3.4G": 3_300_000, "5.7G": 5_650_000,
	"10G": 10_000_000, "24G": 24_000_000, "47G": 47_000_000, "75G": 75_500_000,
	"122G": 122_250_000, "134G": 134_000_000, "241G": 241_000_000,
}

// gluedFreqModeRe matches a frequency with its Cabrillo mode glued on: 21170CW, 14000PH.
var gluedFreqModeRe = regexp.MustCompile(`^([0-9]+(?:\.[0-9]+)?)(CW|PH|FM|RY|DG)$`)

// parseQSOLine extracts fields from a Cabrillo QSO line.
// Universal fields (same position in all Cabrillo formats):
//
//	[0]=QSO: [1]=freq [2]=mode [3]=date [4]=time [5]=my_call [6]=rst_sent [7..N-1]=exch_sent
//	[N]=their_call [N+1]=rst_rcvd [N+2..end]=exch_rcvd
//
// Their-call is found by scanning from index 6 for the next callsign that differs from my_call.
func parseQSOLine(fields []string, myCall, contestID string) (*QSO, error) {
	// THE TAG IS NOT ALWAYS ITS OWN FIELD. Some logs write no space after the colon:
	//
	//	QSO:14080 RY 2020-02-08 0501  UR8EQ  599  111 RK0UT    599  098
	//
	// strings.Fields then yields "QSO:14080" as one token. Matching only the bare
	// "QSO:" left the frequency glued to the tag, so every such line failed as a bad
	// frequency and the QSO was dropped -- 266 of the 577 lines in ur8eq.log alone,
	// and that file mixes both spellings, so it is not even consistent within itself.
	//
	// Split the tag off wherever it is, then apply the field-count check to what
	// remains. Checking the count first would reject a line whose fields are all
	// present but whose first two share a token.
	f := fields
	if len(f) > 0 && len(f[0]) >= 4 && strings.EqualFold(f[0][:4], "QSO:") {
		if rest := f[0][4:]; rest != "" {
			f = append([]string{rest}, f[1:]...)
		} else {
			f = f[1:]
		}
	}

	// THE MODE IS NOT ALWAYS ITS OWN FIELD EITHER. `QSO: 21170CW 2005-...` -- the
	// same missing space as the glued QSO: tag above, one field later. Split only on a
	// Cabrillo mode, so `3500P` (a typo, not a mode) still fails as a bad frequency.
	if len(f) > 0 {
		if m := gluedFreqModeRe.FindStringSubmatch(f[0]); m != nil {
			f = append([]string{m[1], m[2]}, f[1:]...)
		}
	}

	if len(f) < 8 {
		return nil, fmt.Errorf("too few fields: %d", len(f))
	}

	// f[0]=freq f[1]=mode f[2]=date f[3]=time f[4]=my_call f[5..]=rst+exch+their_call+...
	// Frequency may be integer (7000) or decimal (1868.79) depending on logging software,
	// or, above 1 GHz, a Cabrillo band designator (2.3G) rather than a frequency.
	freqKHz, ok := cabrilloBandKHz[strings.ToUpper(f[0])]
	if !ok {
		freqFloat, err := strconv.ParseFloat(f[0], 64)
		if err != nil {
			return nil, fmt.Errorf("bad freq %q: %w", f[0], err)
		}
		freqKHz = uint64(freqFloat)
	}

	mode := strings.ToUpper(f[1])

	// Parse date + time → timestamp
	// Strip trailing 'Z' or 'z' from time (some logs use "1257Z" instead of "1257")
	timeStr := strings.TrimRight(f[3], "Zz")
	// Truncate 6-digit HHMMSS to 4-digit HHMM (some loggers emit seconds)
	if len(timeStr) > 4 {
		timeStr = timeStr[:4]
	}
	// Zero-pad short times (old Cabrillo v1 logs may have "1" instead of "0001")
	for len(timeStr) < 4 {
		timeStr = "0" + timeStr
	}
	// Try ISO date first (2006-01-02), then MM/DD/YYYY (old Cabrillo v1)
	ts, err := time.Parse("2006-01-02 1504", f[2]+" "+timeStr)
	if err != nil {
		ts, err = time.Parse("01/02/2006 1504", f[2]+" "+timeStr)
		if err != nil {
			return nil, fmt.Errorf("bad timestamp %q %q: %w", f[2], f[3], err)
		}
	}

	// THE LINE CARRIES THE LOGGING STATION, NOT THE HEADER. Cabrillo puts the sent
	// callsign in the QSO line itself, so a file with no CALLSIGN: header is still
	// fully attributable -- 253 files in the mirror have no such header and 107,793
	// of their 107,794 QSO lines name their station here.
	//
	// The header is the fallback for the reverse case: a line whose own field is
	// junk. Until now myCall was passed in and never read, so the header could only
	// ever reject a QSO, never rescue one.
	// THE LINE'S OWN CALLSIGN IS TAKEN AS WRITTEN. It is not held to callsignRe,
	// because that regex demands a trailing letter and real callsigns do not always
	// have one: LM1814 (Norwegian constitution bicentenary), SZ3PC20, 7Q1. Gating on
	// it here rejected the WHOLE FILE for those stations -- 226 QSOs for LM1814, 110
	// for SZ3PC20 -- which is the same mistake as refusing a QSO worked with 7Q1,
	// made one field to the left.
	//
	// callsignRe exists to LOCATE the worked station among variable-width exchange
	// fields. It is a locator, not a validator, and it has now twice been used as
	// one. Bronze is a faithful ingest: f[4] is what the log says logged the QSO.
	//
	// The only refusal is a field with nothing callsign-like in it at all -- the
	// "*************" template placeholder -- which is not a spelling of a station.
	logCall := strings.ToUpper(f[4])
	if !hasAlnum(logCall) {
		if mc := strings.ToUpper(strings.TrimSpace(myCall)); hasAlnum(mc) {
			logCall = mc
		} else {
			return nil, fmt.Errorf("no logging station: %q", f[4])
		}
	}

	// Band: freq is kHz, GetBand expects MHz
	bandID, _ := bands.GetBand(float64(freqKHz) / 1000.0)

	// Find their_call: scan from index 5 (after my_call) looking for a callsign != my_call
	theirIdx := -1
	for i := 5; i < len(f); i++ {
		candidate := strings.ToUpper(f[i])
		// Compare the calls, not the raw fields: a station logged as KI7MT in the
		// header and KI7MT/KP4 on the line is still itself.
		if c := baseCall(candidate); c != "" && c != baseCall(logCall) {
			theirIdx = i
			break
		}
	}

	// POSITIONAL FALLBACK -- a QSO is never dropped because a callsign looks odd.
	//
	// The scan above is a heuristic for logs whose exchange width varies; Cabrillo
	// itself is positional, and f[7] IS the worked station in the standard layout
	//
	//     freq mode date time mycall rst_s exch_s THEIRCALL rst_r exch_r
	//
	// Returning an error here dropped the whole row, and the shape test it depended
	// on is not a fact about callsigns. 7Q1 -- a licensed Malawi call, 2,461 QSOs in
	// one contest-year -- ends in a digit, and so do others; the regex demands a
	// trailing letter. The regex cannot simply be relaxed to allow a trailing digit
	// because then 599, 14 and 37 match it and the wrong field becomes their_call.
	//
	// So the shape test keeps its job of LOCATING the field when the layout is
	// irregular, and loses its power to veto the row. Bronze is a faithful ingest:
	// what the log says goes in, and judging it is silver's business. Measured on
	// cq-ww/2024ph -- 3,509 rows had no field pass the shape test and all 3,509 have
	// a non-empty f[7].
	if theirIdx < 0 {
		if i := specTheirCallIdx(contestID); i < len(f) && strings.TrimSpace(f[i]) != "" {
			theirIdx = i
		}
	}
	if theirIdx < 0 {
		return nil, fmt.Errorf("no their_call found")
	}

	theirCall := strings.ToUpper(f[theirIdx])

	// Extract RST and exchange fields
	rstSent := ""
	exchSent := ""
	rstRcvd := ""
	exchRcvd := ""

	// Fields between my_call and their_call: rst_sent + exch_sent
	if theirIdx > 5 {
		rstSent = f[5]
		if theirIdx > 6 {
			exchSent = strings.Join(f[6:theirIdx], " ")
		}
	}

	// Fields after their_call: rst_rcvd + exch_rcvd
	afterTheir := theirIdx + 1
	if afterTheir < len(f) {
		rstRcvd = f[afterTheir]
		if afterTheir+1 < len(f) {
			exchRcvd = strings.Join(f[afterTheir+1:], " ")
		}
	}

	return &QSO{
		Timestamp: ts,
		Frequency: uint32(freqKHz),
		Band:      bandID,
		Mode:      mode,
		Call1:     logCall,
		Call2:     theirCall,
		RSTSent:   rstSent,
		ExchSent:  exchSent,
		RSTRcvd:   rstRcvd,
		ExchRcvd:  exchRcvd,
	}, nil
}

// ParseReject is one QSO line the parser could not read. A skip used to be a
// counter and nothing else -- after a run over 823,467 files there was no way to ask
// which file had dropped anything, or why. Three real defects hid behind that for
// months. A skip is now a record.
type ParseReject struct {
	LineNo  uint32
	Reason  string // a fixed category, safe for LowCardinality
	Detail  string // the parser error in full, including the offending value
	RawLine string
}

// rejectReason maps a parser error to one of a FIXED SET of categories.
//
// The error text carries the offending value -- bad freq "notafreq":
// strconv.ParseFloat: parsing "notafreq": invalid syntax -- so storing it directly in
// a LowCardinality column would mint a new distinct string per bad value, which makes
// LowCardinality worse than String and makes grouping by reason useless. The full
// error goes to detail; this returns the bucket.
func rejectReason(err error) string {
	if err == nil {
		return "unknown"
	}
	msg := err.Error()
	switch {
	case strings.HasPrefix(msg, "bad freq"):
		return "bad frequency"
	case strings.HasPrefix(msg, "bad timestamp"):
		return "bad timestamp"
	case strings.HasPrefix(msg, "too few fields"):
		return "too few fields"
	case strings.Contains(msg, "no their_call"):
		return "no worked station"
	default:
		return "other"
	}
}

// parseFile reads a Cabrillo log file and returns one header set PER LOG SECTION,
// plus every QSO in the file and the skipped count.
//
// A file may hold several operators' logs end to end -- r0hq.log in the IARU mirror
// is the one that surfaced it. This used to `break` at the first END-OF-LOG, so the
// file parsed, reported no error, and silently contributed only its first station's
// QSOs. Bronze is supposed to hold what the archive holds, so sections are walked
// rather than stopped at.
//
// Section boundaries are START-OF-LOG and END-OF-LOG, and either is enough on its
// own: publishers concatenate both ways, and some final logs carry no END-OF-LOG at
// all. Headers reset at each boundary, which is what attributes each section's QSOs
// to the station that logged them instead of to the first station in the file.
func parseFile(path, myCallOverride, contestID string) ([]*CabrilloHeaders, []*QSO, []ParseReject, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, nil, err
	}
	defer file.Close()

	var sections []*CabrilloHeaders
	headers := &CabrilloHeaders{}
	var qsos []*QSO
	var rejects []ParseReject
	lineNo := uint32(0)

	// The raw line is kept for review, but a pathological file must not put a
	// megabyte into one row.
	const maxRawLine = 512
	reject := func(reason, detail, raw string) {
		if len(raw) > maxRawLine {
			raw = raw[:maxRawLine]
		}
		if len(detail) > maxRawLine {
			detail = detail[:maxRawLine]
		}
		rejects = append(rejects, ParseReject{
			LineNo: lineNo, Reason: reason, Detail: detail, RawLine: raw,
		})
	}

	// A section counts as real only once it carries something; that keeps a trailing
	// END-OF-LOG, or a publisher footer after the last log, from becoming a phantom.
	seen := false
	// Whether this section has produced a QSO yet. A boundary marker only ENDS a log
	// that actually contained QSOs -- see closeSection.
	sectionHasQSO := false
	closeSection := func() {
		// A MARKER BEFORE ANY QSO DOES NOT END A LOG, IT IS MISPLACED.
		//
		// 889 files in the mirror, all from 2020, put END-OF-LOG after the header
		// block and before the first QSO line:
		//
		//	START-OF-LOG: 3.0
		//	CALLSIGN: W7GF
		//	...
		//	END-OF-LOG:          <- here
		//	QSO: 14000 CW ...
		//
		// Taking that at face value resets the headers, so every QSO after it has no
		// callsign to attribute and the whole file is lost -- 106 QSOs for W7GF, 817
		// for WR2G, and so on across 893 files.
		//
		// It is not a logger bug: those files were written by N1MM, N3FJP, CTESTWIN
		// and QARTest among others, and eight vendors do not independently move a
		// terminator. 889 of 893 are from 2020 alone, which makes it a publisher-side
		// artifact of that year's archives -- the same class as the 2017 logs
		// republished under 2018 that contest.quarantine exists for.
		//
		// So a boundary closes a log only once that log has produced a QSO. A marker
		// arriving before any QSO is treated as part of the header block and the
		// headers are kept.
		if !sectionHasQSO {
			return
		}
		if seen {
			sections = append(sections, headers)
		}
		headers = &CabrilloHeaders{}
		seen = false
		sectionHasQSO = false
	}

	scanner := bufio.NewScanner(file)
	// Some contest logs have very long lines
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		lineNo++
		line := scanner.Text()
		// Replace non-breaking spaces (0xA0) with regular spaces.
		// CTESTWIN, UcxLog, and other loggers pad fixed-width fields with NBSP.
		line = strings.ReplaceAll(line, "\xa0", " ")
		trimmed := strings.TrimSpace(line)

		if trimmed == "" {
			continue
		}

		upper := strings.ToUpper(trimmed)

		// Section boundaries. END-OF-LOG closes the current log; START-OF-LOG opens
		// the next one and also closes any log that never wrote an END-OF-LOG.
		if strings.HasPrefix(upper, "END-OF-LOG") {
			closeSection()
			continue
		}
		if strings.HasPrefix(upper, "START-OF-LOG") {
			closeSection()
			continue
		}

		// Parse headers
		if strings.HasPrefix(upper, "CALLSIGN:") {
			seen = true
			headers.Callsign = strings.ToUpper(strings.TrimSpace(trimmed[9:]))
		} else if strings.HasPrefix(upper, "CONTEST:") {
			seen = true
			headers.Contest = strings.ToUpper(strings.TrimSpace(trimmed[8:]))
		} else if strings.HasPrefix(upper, "GRID-LOCATOR:") {
			g := strings.ToUpper(strings.TrimSpace(trimmed[13:]))
			if gridRe.MatchString(g) {
				headers.Grid = g
			}
		} else if strings.HasPrefix(upper, "HQ-GRID-LOCATOR:") {
			g := strings.ToUpper(strings.TrimSpace(trimmed[16:]))
			if gridRe.MatchString(g) {
				headers.Grid = g
			}
		} else if strings.HasPrefix(upper, "QSO:") {
			// Parse QSO line
			fields := strings.Fields(trimmed)
			myCall := headers.Callsign
			if myCallOverride != "" {
				myCall = myCallOverride
			}
			qso, err := parseQSOLine(fields, myCall, contestID)
			if err != nil {
				reject(rejectReason(err), err.Error(), trimmed)
				continue
			}
			seen = true
			sectionHasQSO = true
			qsos = append(qsos, qso)
		}
	}

	// The last log usually has an END-OF-LOG and is already closed; some do not.
	closeSection()

	if err := scanner.Err(); err != nil {
		return sections, qsos, rejects, fmt.Errorf("scanner error: %w", err)
	}

	if len(rejects) > 0 && len(qsos) == 0 {
		return sections, nil, rejects, fmt.Errorf("all %d QSO lines failed to parse", len(rejects))
	}

	return sections, qsos, rejects, nil
}

// sourceKey derives a source identifier from a file path relative to srcDir.
// e.g., /mnt/contest-logs/cq-ww/2005cw/k1abc.log → cq-ww/2005cw
func sourceKey(path, srcDir string) string {
	rel, err := filepath.Rel(srcDir, path)
	if err != nil {
		return filepath.Base(filepath.Dir(path))
	}
	// rel = cq-ww/2005cw/k1abc.log → want cq-ww/2005cw
	dir := filepath.Dir(rel)
	return dir
}

// relPathFrom returns the path relative to srcDir.
func relPathFrom(srcDir, fullPath string) string {
	rel, err := filepath.Rel(srcDir, fullPath)
	if err != nil {
		return fullPath
	}
	return rel
}

// RejectWriter appends failed file paths and error reasons to a reject log.
// Thread-safe via mutex. If no path is configured, writes are silently discarded.
type RejectWriter struct {
	mu   sync.Mutex
	file *os.File
}

func NewRejectWriter(path string) (*RejectWriter, error) {
	if path == "" {
		return &RejectWriter{}, nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("reject log: %w", err)
	}
	return &RejectWriter{file: f}, nil
}

func (rw *RejectWriter) Write(relPath, reason string) {
	if rw.file == nil {
		return
	}
	rw.mu.Lock()
	defer rw.mu.Unlock()
	fmt.Fprintf(rw.file, "%s | %s\n", relPath, reason)
}

func (rw *RejectWriter) Close() {
	if rw.file != nil {
		rw.file.Close()
	}
}

// flushBatch sends the accumulated batch to ClickHouse.
func flushBatch(ctx context.Context, conn *ch.Client, tableFQN string, batch *ContestBatch) error {
	query := fmt.Sprintf(
		"INSERT INTO %s (timestamp, frequency, band, mode, call_1, call_2, "+
			"rst_sent, exch_sent, rst_rcvd, exch_rcvd, contest, source) VALUES",
		tableFQN,
	)
	return conn.Do(ctx, ch.Query{
		Body:  query,
		Input: batch.Input(),
	})
}

// GridEntry holds a callsign→grid mapping for enrichment.
type GridEntry struct {
	Callsign string
	Grid     string
}

// flushGrids batch-inserts callsign→grid mappings into wspr.callsign_grid.
func flushGrids(ctx context.Context, conn *ch.Client, entries []GridEntry) error {
	if len(entries) == 0 {
		return nil
	}

	callsign := new(proto.ColStr)
	grid := &proto.ColFixedStr{Size: 6}
	grid4 := &proto.ColFixedStr{Size: 4}
	spotCount := new(proto.ColUInt32)
	lastSeen := new(proto.ColDate)

	today := time.Now().UTC()

	for _, e := range entries {
		callsign.Append(e.Callsign)

		// Pad grid to 6 chars if it's only 4
		g := e.Grid
		if len(g) == 4 {
			g += "MM" // center of grid square
		}
		if len(g) < 6 {
			g = g + strings.Repeat(" ", 6-len(g))
		}
		grid.Append([]byte(g[:6]))
		grid4.Append([]byte(g[:4]))
		spotCount.Append(0)
		lastSeen.Append(today)
	}

	return conn.Do(ctx, ch.Query{
		Body: "INSERT INTO wspr.callsign_grid (callsign, grid, grid_4, spot_count, last_seen) VALUES",
		Input: proto.Input{
			{Name: "callsign", Data: callsign},
			{Name: "grid", Data: grid},
			{Name: "grid_4", Data: grid4},
			{Name: "spot_count", Data: spotCount},
			{Name: "last_seen", Data: lastSeen},
		},
	})
}

// processFile parses a single Cabrillo log and inserts QSOs using a shared connection.
// ---------------------------------------------------------------------------
// Date validation
//
// A QSO must be dated inside the year its source directory declares. That is the
// whole rule, and it is deliberately all of it.
//
// It exists because upstream sites republish neighbouring years under the wrong
// path. cqwpxrtty.com/publiclogs/2018/ serves 3,146 logs that are 2017
// submissions -- fetch 2017/aa7v.log and 2018/aa7v.log and the QSOs are identical
// 2017-02-11/12 content. contest-download mirrors the site faithfully, so those
// rows arrive twice under two source keys, and contest.bronze is a plain
// MergeTree that collapses nothing. One event contributed 533,506 duplicates.
// The same rule catches corrupted year fields in operator logs (2008-11-29
// recorded as 2088-11-29) and dates that failed to parse into the Unix epoch.
//
// What the rule does NOT do is assert when any contest was held. Contest dates,
// log publication dates, and the date a file lands on a website are three
// different things. A hardcoded calendar of 15 series across 20 years would be
// one wrong entry away from silently rejecting a legitimate year, and would need
// maintaining every January forever -- the same trap as the downloader\'s
// hand-kept map of ARRL instance IDs.
//
// The two days of slack at each boundary are load-bearing, not padding: ARRL RTTY
// Roundup runs the first weekend of January, and arrl-rtty/2021 legitimately
// holds 190,174 QSOs dated 2021-01-02.
// ---------------------------------------------------------------------------

var (
	// Past this many, the mismatch log is noise: the count still climbs.
	labelMismatchLogLimit uint64 = 20

	quarantineTable   = "contest.quarantine"
	rejectTable       = "contest.parse_rejects"
	hostName, _       = os.Hostname()
	quarantineEnabled = true
	declaredYearRe    = regexp.MustCompile(`(\d{4})`)
)

// sourceDeclaredYear reports the year a source key claims: "cq-ww/2005cw" -> 2005.
// Returns false when the key carries no plausible year, in which case the QSO is
// admitted -- an unrecognised layout must not silently quarantine a whole feed.
func sourceDeclaredYear(src string) (int, bool) {
	m := declaredYearRe.FindStringSubmatch(src)
	if m == nil {
		return 0, false
	}
	y, err := strconv.Atoi(m[1])
	if err != nil || y < 1990 || y > 2100 {
		return 0, false
	}
	return y, true
}

// ---------------------------------------------------------------------------
// Contest label
//
// The CONTEST: header is operator-typed free text and cannot be trusted as a
// dimension. Taking it verbatim -- which is what this ingester used to do --
// produced 41 distinct labels for 15 contests across 37,495 rows:
//
//   CQ-WW-CW, CQ-WW-CW 2005, CQ-WW-CW 05, CQ-WW-CW CONTEST, CQ-WW CW,
//   CQ- WW- CW, CQWW SSB, CQ-WW-SSB MODE: SSB, CQ-WW-SSB VOICE,
//   2008 CQ WORLD WIDE DX CONTEST, CW, WW 2010, ...
//
// and, from logs whose SOAPBOX prose wrapped onto a line beginning "CONTEST:",
// entries like "TIMES BECAUSE UNABLE TO CATCH ANY SIGNAL. SORRY FOR THE MANY
// REPEAT". A GROUP BY contest over that is not a report, it is a word cloud.
//
// THE DIRECTORY IS THE AUTHORITY, not the header. contest-download mirrors each
// publisher's site into <series>/<season>, so "cq-ww/2015cw" states the contest
// and the mode as a fact about where the file came from -- our own mirror
// structure, not something an operator typed at 3am after 36 hours of CW. It is
// already trusted for exactly this reason by sourceDeclaredYear above.
//
// UNKNOWN SERIES FAIL LOUDLY. A directory this map does not know means the mirror
// grew a series we have not classified, and guessing a label for it is how the
// dimension got polluted in the first place. The file is rejected with its path,
// which is actionable; a silently mislabelled corpus is not.
// ---------------------------------------------------------------------------

// canonicalContest maps a series directory to its canonical label. Series that
// split CW and phone into separate seasons (<year>cw / <year>ph) map to a pair;
// the rest carry their mode in the series name and use the same label for every
// season.
var canonicalContest = map[string]struct{ cw, ph string }{
	"cq-ww":       {"CQ-WW-CW", "CQ-WW-SSB"},
	"cq-wpx":      {"CQ-WPX-CW", "CQ-WPX-SSB"},
	"cq-160":      {"CQ-160-CW", "CQ-160-SSB"},
	"cq-ww-rtty":  {"CQ-WW-RTTY", "CQ-WW-RTTY"},
	"cq-wpx-rtty": {"CQ-WPX-RTTY", "CQ-WPX-RTTY"},
	"arrl-dx-cw":  {"ARRL-DX-CW", "ARRL-DX-CW"},
	"arrl-dx-ph":  {"ARRL-DX-SSB", "ARRL-DX-SSB"},
	"arrl-rtty":   {"ARRL-RTTY", "ARRL-RTTY"},
	"arrl-ss-cw":  {"ARRL-SS-CW", "ARRL-SS-CW"},
	"arrl-ss-ph":  {"ARRL-SS-SSB", "ARRL-SS-SSB"},
	"arrl-10m":    {"ARRL-10", "ARRL-10"},
	"arrl-160m":   {"ARRL-160", "ARRL-160"},
	"arrl-digi":   {"ARRL-DIGI", "ARRL-DIGI"},
	"iaru-hf":     {"IARU-HF", "IARU-HF"},
	"ww-digi":     {"WW-DIGI", "WW-DIGI"},
}

// contestFromSource resolves a source key to its canonical contest label:
// "cq-ww/2015cw" -> "CQ-WW-CW". Reports false for a series this map does not
// know, so the caller can reject the file by name rather than invent a label.
func contestFromSource(src string) (string, bool) {
	series, season, found := strings.Cut(src, "/")
	if !found {
		return "", false
	}
	pair, ok := canonicalContest[strings.ToLower(series)]
	if !ok {
		return "", false
	}
	if strings.HasSuffix(strings.ToLower(season), "ph") {
		return pair.ph, true
	}
	return pair.cw, true
}

// inDeclaredYear reports whether ts falls within year y, allowing two days either
// side so a contest straddling New Year is not rejected.
func inDeclaredYear(ts time.Time, y int) bool {
	lo := time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -2)
	hi := time.Date(y+1, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, 2)
	return !ts.Before(lo) && ts.Before(hi)
}

// flushQuarantine writes held-back QSOs to the quarantine table. Failure here is
// logged but never fatal: quarantine is a safety net, and a net that takes the
// whole ingest down when it tears is worse than no net.
// maxRejectsPerFile caps how many unreadable lines are RECORDED for one file. The
// count is never capped -- it goes to contest.ingest_log.skipped_rows in full.
//
// A systematically mis-parsed contest would otherwise write tens of millions of rows
// and turn a diagnostic into an outage. Beyond the cap the samples stop being
// informative anyway: a file sitting at its cap is not telling you about bad lines,
// it is telling you the parser is wrong about that file.
const maxRejectsPerFile = 100

// flushParseRejects records the lines the parser could not read, so a skip can be
// reviewed instead of merely counted.
func flushParseRejects(ctx context.Context, conn *ch.Client, table string,
	rejects []ParseReject, filePath, contestID, host string) error {
	if len(rejects) == 0 {
		return nil
	}
	if len(rejects) > maxRejectsPerFile {
		rejects = rejects[:maxRejectsPerFile]
	}
	var (
		colPath proto.ColStr
		colLine proto.ColUInt32
		colCon  = new(proto.ColStr).LowCardinality()
		colWhy  = new(proto.ColStr).LowCardinality()
		colDet  proto.ColStr
		colRaw  proto.ColStr
		colHost = new(proto.ColStr).LowCardinality()
	)
	for _, r := range rejects {
		colPath.Append(filePath)
		colLine.Append(r.LineNo)
		colCon.Append(contestID)
		colWhy.Append(r.Reason)
		colDet.Append(r.Detail)
		colRaw.Append(r.RawLine)
		colHost.Append(host)
	}
	return conn.Do(ctx, ch.Query{
		Body: fmt.Sprintf("INSERT INTO %s (file_path, line_no, contest, reason, detail, raw_line, hostname) VALUES", table),
		Input: proto.Input{
			{Name: "file_path", Data: colPath},
			{Name: "line_no", Data: colLine},
			{Name: "contest", Data: colCon},
			{Name: "reason", Data: colWhy},
			{Name: "detail", Data: colDet},
			{Name: "raw_line", Data: colRaw},
			{Name: "hostname", Data: colHost},
		},
	})
}

func flushQuarantine(ctx context.Context, conn *ch.Client, rows []*QSO, filePath string, year int, reason string) error {
	if len(rows) == 0 {
		return nil
	}
	var (
		colTS   proto.ColStr
		colFreq proto.ColUInt32
		colBand proto.ColInt32
		colMode = new(proto.ColStr).LowCardinality()
		colC1   proto.ColStr
		colC2   proto.ColStr
		colRSTS proto.ColStr
		colExS  proto.ColStr
		colRSTR proto.ColStr
		colExR  proto.ColStr
		colCon  = new(proto.ColStr).LowCardinality()
		colSrc  = new(proto.ColStr).LowCardinality()
		colPath proto.ColStr
		colYear proto.ColUInt16
		colWhy  = new(proto.ColStr).LowCardinality()
	)
	for _, q := range rows {
		colTS.Append(q.Timestamp.UTC().Format(time.RFC3339))
		colFreq.Append(q.Frequency)
		colBand.Append(q.Band)
		colMode.Append(q.Mode)
		colC1.Append(q.Call1)
		colC2.Append(q.Call2)
		colRSTS.Append(q.RSTSent)
		colExS.Append(q.ExchSent)
		colRSTR.Append(q.RSTRcvd)
		colExR.Append(q.ExchRcvd)
		colCon.Append(q.Contest)
		colSrc.Append(q.Source)
		colPath.Append(filePath)
		colYear.Append(uint16(year))
		colWhy.Append(reason)
	}
	return conn.Do(ctx, ch.Query{
		Body: fmt.Sprintf("INSERT INTO %s (timestamp, frequency, band, mode, call_1, call_2, "+
			"rst_sent, exch_sent, rst_rcvd, exch_rcvd, contest, source, file_path, "+
			"declared_year, reason) VALUES", quarantineTable),
		Input: proto.Input{
			{Name: "timestamp", Data: colTS},
			{Name: "frequency", Data: colFreq},
			{Name: "band", Data: colBand},
			{Name: "mode", Data: colMode},
			{Name: "call_1", Data: colC1},
			{Name: "call_2", Data: colC2},
			{Name: "rst_sent", Data: colRSTS},
			{Name: "exch_sent", Data: colExS},
			{Name: "rst_rcvd", Data: colRSTR},
			{Name: "exch_rcvd", Data: colExR},
			{Name: "contest", Data: colCon},
			{Name: "source", Data: colSrc},
			{Name: "file_path", Data: colPath},
			{Name: "declared_year", Data: colYear},
			{Name: "reason", Data: colWhy},
		},
	})
}

func processFile(ctx context.Context, conn *ch.Client, path, srcDir string, pend *pendingBatch, enrich bool, stats *Stats, rejectWriter *RejectWriter) uint64 {
	fileName := filepath.Base(path)
	src := sourceKey(path, srcDir)
	relPath := filepath.Join(src, fileName)

	// The contest must be known BEFORE parsing: its Cabrillo template decides where
	// the received callsign sits, and Sweepstakes does not put it where CQ-WW does.
	contestID, known := contestFromSource(src)
	if !known {
		log.Printf("[%s] unknown contest series in source key %q - not ingested", relPath, src)
		stats.FailedFiles.Add(1)
		rejectWriter.Write(relPath, fmt.Sprintf("unknown contest series %q: add it to canonicalContest", src))
		return 0
	}

	sections, qsos, rejects, err := parseFile(path, "", contestID)
	if len(rejects) > 0 {
		stats.SkippedRows.Add(uint64(len(rejects)))
	}

	// RECORD THE REJECTS EVEN WHEN THE FILE FAILS ENTIRELY -- especially then.
	//
	// parseFile returns an error when NOTHING parsed, and the error paths below
	// return early, so the files most worth diagnosing were the ones writing nothing
	// to contest.parse_rejects. That is exactly backwards: "all 106 QSO lines failed
	// to parse" tells you a file died and not one thing about why, which is how the
	// misplaced-END-OF-LOG defect had to be diagnosed by hand from the archive.
	//
	// Done before the error checks so every path is covered, and never fatal: a
	// diagnostic must not be the reason a file is reported differently.
	if rejectTable != "" && len(rejects) > 0 {
		if e := flushParseRejects(ctx, conn, rejectTable, rejects, relPath, contestID, hostName); e != nil {
			log.Printf("[%s] parse-reject insert error: %v", relPath, e)
		}
	}

	if err != nil {
		log.Printf("[%s] parse error: %v", relPath, err)
		stats.FailedFiles.Add(1)
		rejectWriter.Write(relPath, err.Error())
		return 0
	}

	if len(qsos) == 0 {
		stats.FailedFiles.Add(1)
		rejectWriter.Write(relPath, "0 QSOs parsed")
		return 0
	}

	// The directory names the contest; the header only gets to disagree in the log.
	// See the Contest label block above for why the header is not trusted here.
	// A mismatch is worth seeing but never fatal: the usual cause is operator
	// free-text, and the occasional real one is a misfiled log upstream, which is
	// a finding about the mirror rather than a reason to drop good QSOs.
	// One file can hold several logs, so every section's header gets the same
	// treatment -- a concatenated file used to be judged by its first log alone.
	for _, hdr := range sections {
		h := strings.TrimSpace(hdr.Contest)
		if h == "" || h == contestID {
			continue
		}
		stats.LabelMismatches.Add(1)
		if stats.LabelMismatches.Load() <= labelMismatchLogLimit {
			log.Printf("[%s] CONTEST: header %q -> %s (directory wins)", relPath, h, contestID)
		}
	}

	var rowCount uint64
	startTime := time.Now()

	declaredYear, haveYear := sourceDeclaredYear(src)

	// Pass 1: classify. Nothing is written until the whole file's disposition is
	// known. Interleaving the two writes meant a quarantine failure could leave
	// already-flushed bronze rows behind with no watermark, so the retry inserted
	// them a second time - the guard against duplicates creating duplicates.
	var held, keep []*QSO
	for _, qso := range qsos {
		if quarantineEnabled && haveYear && !inDeclaredYear(qso.Timestamp, declaredYear) {
			qso.Contest = contestID
			qso.Source = src
			held = append(held, qso)
			continue
		}
		keep = append(keep, qso)
	}

	// Pass 2: quarantine first. If it fails, not one bronze row exists yet, so the
	// file is cleanly retryable as a whole.
	if len(held) > 0 {
		if err := flushQuarantine(ctx, conn, held, relPath, declaredYear, "off-declared-year"); err != nil {
			log.Printf("[%s] quarantine insert FAILED (%v) - no rows written, file left unwatermarked for retry", relPath, err)
			stats.FailedFiles.Add(1)
			rejectWriter.Write(relPath, fmt.Sprintf("quarantine insert failed: %v", err))
			return 0
		}
		stats.Quarantined.Add(uint64(len(held)))
		rejectWriter.Write(relPath, fmt.Sprintf("%d QSO(s) dated outside declared year %d", len(held), declaredYear))
	}

	// Pass 3: bronze -- appended to the worker's batch, not written here. The file
	// goes in whole or not at all: the cancellation check is before the first row,
	// never between rows, so a batch can not hold half a file.
	if ctx.Err() != nil {
		return 0
	}
	batch := pend.batch
	for _, qso := range keep {
		batch.Timestamp.Append(qso.Timestamp)
		batch.Frequency.Append(qso.Frequency)
		batch.Band.Append(qso.Band)
		batch.Mode.Append(qso.Mode)
		batch.Call1.Append(qso.Call1)
		batch.Call2.Append(qso.Call2)
		batch.RSTSent.Append(qso.RSTSent)
		batch.ExchSent.Append(qso.ExchSent)
		batch.RSTRcvd.Append(qso.RSTRcvd)
		batch.ExchRcvd.Append(qso.ExchRcvd)
		batch.Contest.Append(contestID)
		batch.Source.Append(src)
		rowCount++
	}

	// Parse time only: the INSERT that carries these rows happens later, for many files.
	elapsedMs := uint32(time.Since(startTime).Milliseconds())

	fi, _ := os.Stat(path)
	fileSize := uint64(0)
	if fi != nil {
		fileSize = uint64(fi.Size())
	}

	pend.entries = append(pend.entries, watermark.LogEntry{
		FilePath:    relPathFrom(srcDir, path),
		FileSize:    fileSize,
		RowCount:    rowCount,
		SkippedRows: uint64(len(rejects)),
		ElapsedMs:   elapsedMs,
	})
	pend.rows += rowCount

	// Grid enrichment -- one entry per log section. A concatenated file used to
	// enrich only its first station and drop the grids of every station after it.
	if enrich {
		for _, hdr := range sections {
			if hdr.Grid == "" || hdr.Callsign == "" {
				continue
			}
			pend.grids = append(pend.grids, GridEntry{Callsign: hdr.Callsign, Grid: hdr.Grid})
		}
	}

	return rowCount
}

// pendingBatch is one worker's bronze batch plus the files whose rows are in it.
//
// THE BATCH SPANS FILES. It used to live inside processFile, so -batch (100,000)
// could never fire: a contest log is a few hundred QSOs, and every file became its
// own ~400-row INSERT plus its own watermark INSERT. A full reload was 825K files x
// two round trips -- latency-bound, the machine idle (IONIS-AI/ionis-apps#22).
//
// A FILE IS WATERMARKED ONLY AFTER ITS ROWS ARE IN BRONZE. The old code logged a
// failed bronze flush and watermarked the file anyway, so its rows were lost and an
// incremental run would never retry them. Now a failed flush fails every file in the
// batch, none is watermarked, and the next run picks them up.
type pendingBatch struct {
	batch   *ContestBatch
	entries []watermark.LogEntry
	grids   []GridEntry
	rows    uint64
}

func (p *pendingBatch) reset() {
	p.batch.Reset()
	p.entries = p.entries[:0]
	p.grids = p.grids[:0]
	p.rows = 0
}

// commit writes the batch, then the watermark for every file in it, then any grids.
func (p *pendingBatch) commit(ctx context.Context, conn *ch.Client, db, tableFQN string, stats *Stats, rejectWriter *RejectWriter) {
	defer p.reset()
	if len(p.entries) == 0 {
		return
	}

	if p.batch.Len() > 0 {
		if err := flushBatch(ctx, conn, tableFQN, p.batch); err != nil {
			log.Printf("bronze insert FAILED for %d file(s), %d row(s): %v - none watermarked, retryable", len(p.entries), p.rows, err)
			for _, e := range p.entries {
				stats.FailedFiles.Add(1)
				rejectWriter.Write(e.FilePath, fmt.Sprintf("bronze insert failed: %v", err))
			}
			return
		}
	}

	if err := watermark.InsertLogEntries(ctx, conn, db, p.entries); err != nil {
		log.Printf("watermark insert error for %d file(s): %v", len(p.entries), err)
	}

	stats.TotalRows.Add(p.rows)
	stats.TotalFiles.Add(uint64(len(p.entries)))

	if len(p.grids) > 0 {
		if err := flushGrids(ctx, conn, p.grids); err != nil {
			log.Printf("grid enrich error for %d file(s): %v", len(p.entries), err)
		} else {
			stats.GridsFound.Add(uint64(len(p.grids)))
		}
	}
}

// worker processes files from a channel using a persistent ClickHouse connection.
func worker(ctx context.Context, id int, files <-chan string, srcDir, host, db, table string, batchSize int, enrich bool, stats *Stats, rejectWriter *RejectWriter, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := ch.Dial(ctx, ch.Options{
		Address:     host,
		Database:    db,
		Compression: ch.CompressionLZ4,
	})
	if err != nil {
		log.Printf("[worker-%d] ClickHouse connect error: %v", id, err)
		return
	}
	defer conn.Close()

	tableFQN := fmt.Sprintf("%s.%s", db, table)
	pend := &pendingBatch{batch: NewContestBatch()}

	for path := range files {
		if ctx.Err() != nil {
			return // pending files are unwatermarked, so the next run retries them
		}
		processFile(ctx, conn, path, srcDir, pend, enrich, stats, rejectWriter)
		if pend.batch.Len() >= batchSize {
			pend.commit(ctx, conn, db, tableFQN, stats, rejectWriter)
		}
	}
	if ctx.Err() == nil {
		pend.commit(ctx, conn, db, tableFQN, stats, rejectWriter)
	}
}

// discoverFiles walks srcDir for .log files, optionally filtered by contest key.
func discoverFiles(srcDir, contest string) ([]string, error) {
	var files []string

	if contest != "" {
		// Walk only the specific contest directory
		contestDir := filepath.Join(srcDir, contest)
		err := filepath.Walk(contestDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if !info.IsDir() && strings.HasSuffix(strings.ToLower(path), ".log") {
				// Skip manifest and download log files
				base := strings.ToLower(filepath.Base(path))
				if base == "manifest.txt" || base == "download.log" {
					return nil
				}
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	} else {
		// Walk all subdirectories
		err := filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if !info.IsDir() && strings.HasSuffix(strings.ToLower(path), ".log") {
				base := strings.ToLower(filepath.Base(path))
				if base == "manifest.txt" || base == "download.log" {
					return nil
				}
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	sort.Strings(files)
	return files, nil
}

func main() {
	src := flag.String("src", "", "Source directory with {contest}/{yearmode}/*.log (default: $IONIS_CONTEST_LOGS_DIR)")
	host := flag.String("host", "192.168.1.90:9000", "ClickHouse host:port")
	db := flag.String("db", "contest", "ClickHouse database")
	table := flag.String("table", "bronze", "ClickHouse table")
	workers := flag.Int("workers", DefaultWorkers, "Parallel file workers")
	batchSize := flag.Int("batch", DefaultBatchSize, "Rows per INSERT batch")
	contest := flag.String("contest", "", "Process only this contest key (empty = all)")
	enrich := flag.Bool("enrich", false, "Also insert GRID-LOCATOR into wspr.callsign_grid")
	rejectLog := flag.String("reject-log", "", "Append failed files to this reject log (empty = disabled)")
	fullMode := flag.Bool("full", false, "Full reload: re-ingest all files, update watermark")
	prime := flag.Bool("prime", false, "Bootstrap watermark for existing log files without loading data")
	dryRun := flag.Bool("dry-run", false, "List files that would be processed, then exit")
	qTable := flag.String("quarantine-table", "contest.quarantine", "Table receiving QSOs dated outside their directory's year")
	rjTable := flag.String("reject-table", "contest.parse_rejects", "Table receiving QSO lines the parser could not read (empty = disabled)")
	noQuarantine := flag.Bool("no-quarantine", false, "Admit every QSO regardless of date (disables the date guard)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "contest-ingest v%s — Parse Cabrillo contest logs into ClickHouse\n\n", Version)
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Walks --src/{contest}/{yearmode}/*.log, parses Cabrillo headers\n")
		fmt.Fprintf(os.Stderr, "and QSO lines, normalizes band via ADIF lookup, and inserts into\n")
		fmt.Fprintf(os.Stderr, "ClickHouse using ch-go native protocol with LZ4 compression.\n\n")
		fmt.Fprintf(os.Stderr, "Uses a watermark table (contest.ingest_log) to track loaded files\n")
		fmt.Fprintf(os.Stderr, "for incremental processing.\n\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  contest-ingest --prime                   # Bootstrap watermark\n")
		fmt.Fprintf(os.Stderr, "  contest-ingest --dry-run                 # Show new files\n")
		fmt.Fprintf(os.Stderr, "  contest-ingest                           # Incremental load\n")
		fmt.Fprintf(os.Stderr, "  contest-ingest --full                    # Full reload\n")
		fmt.Fprintf(os.Stderr, "  contest-ingest --contest cq-ww --workers 4\n")
		fmt.Fprintf(os.Stderr, "  contest-ingest --enrich\n")
		fmt.Fprintf(os.Stderr, "  contest-ingest --host 10.60.1.1:9000\n")
	}

	flag.Parse()

	if v, err := common.ResolvePath(*src, "IONIS_CONTEST_LOGS_DIR", "src", "contest log directory"); err != nil {
		log.Fatal(err)
	} else {
		*src = v
	}

	log.Println("=========================================================")
	log.Printf("Contest Ingest v%s", Version)
	log.Println("=========================================================")
	log.Printf("Source:   %s", *src)
	log.Printf("Target:   %s.%s @ %s", *db, *table, *host)
	log.Printf("Workers:  %d | Batch: %d", *workers, *batchSize)
	log.Printf("CPUs:     %d", runtime.NumCPU())
	log.Printf("Enrich:   %v", *enrich)
	if *rejectLog != "" {
		log.Printf("Reject:   %s", *rejectLog)
	}
	if *contest != "" {
		log.Printf("Contest:  %s", *contest)
	} else {
		log.Printf("Contest:  all")
	}
	if *dryRun {
		log.Printf("Mode:     DRY-RUN (no data will be loaded)")
	} else if *prime {
		log.Printf("Mode:     PRIME (bootstrap watermark only)")
	} else if *fullMode {
		log.Printf("Mode:     FULL (re-ingest all files)")
	} else {
		log.Printf("Mode:     INCREMENTAL (skip watermarked files)")
	}

	rejectWriter, err := NewRejectWriter(*rejectLog)
	if err != nil {
		log.Fatalf("Cannot open reject log: %v", err)
	}
	defer rejectWriter.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("\nShutdown requested...")
		cancel()
	}()

	// Test ClickHouse connection
	log.Printf("Connecting to ClickHouse at %s...", *host)
	testConn, err := ch.Dial(ctx, ch.Options{
		Address:  *host,
		Database: *db,
	})
	if err != nil {
		log.Fatalf("ClickHouse connection failed: %v", err)
	}
	testConn.Close()
	log.Println("Connection OK")

	// Discover .log files
	allFiles, err := discoverFiles(*src, *contest)
	if err != nil {
		log.Fatalf("File discovery failed: %v", err)
	}
	if len(allFiles) == 0 {
		log.Fatal("No .log files found")
	}
	log.Printf("Found %d .log file(s) on disk", len(allFiles))

	// Prime mode: mark existing files and exit
	if *prime {
		log.Println("=========================================================")
		log.Println("Priming watermark...")
		var primeFiles []watermark.FileInfo
		for _, fp := range allFiles {
			fi, err := os.Stat(fp)
			if err != nil {
				continue
			}
			primeFiles = append(primeFiles, watermark.FileInfo{
				RelPath: relPathFrom(*src, fp),
				Size:    uint64(fi.Size()),
			})
		}
		primed, err := watermark.PrimeFiles(ctx, *host, *db, primeFiles)
		if err != nil {
			log.Fatalf("Prime failed: %v", err)
		}
		log.Printf("Primed %d file(s) in watermark (row_count=0)", primed)
		log.Println("=========================================================")
		return
	}

	// Load watermark
	wm, err := watermark.LoadWatermark(ctx, *host, *db)
	if err != nil {
		log.Fatalf("Load watermark failed: %v", err)
	}
	quarantineTable = *qTable
	rejectTable = *rjTable
	quarantineEnabled = !*noQuarantine
	if quarantineEnabled {
		log.Printf("Date guard: ON  (off-year QSOs -> %s)", quarantineTable)
	} else {
		log.Printf("Date guard: OFF (--no-quarantine)")
	}

	log.Printf("Watermark: %d file(s) already loaded", len(wm))

	// Filter files based on mode
	var filesToProcess []string
	if *fullMode {
		// Full mode: process all files
		filesToProcess = allFiles
	} else {
		// Incremental mode: skip watermarked files
		// Contest logs are static — once downloaded, they don't change
		for _, fp := range allFiles {
			rel := relPathFrom(*src, fp)
			if _, ok := wm[rel]; ok {
				continue
			}
			filesToProcess = append(filesToProcess, fp)
		}
	}

	if len(filesToProcess) == 0 {
		log.Println("0 new files to process")
		log.Println("=========================================================")
		return
	}

	log.Printf("%d file(s) to process", len(filesToProcess))
	log.Println("=========================================================")

	// Dry-run mode: list files and exit
	if *dryRun {
		for _, fp := range filesToProcess {
			rel := relPathFrom(*src, fp)
			fi, _ := os.Stat(fp)
			fmt.Printf("  %s (%d bytes)\n", rel, fi.Size())
		}
		log.Printf("\nDry-run complete: %d file(s) would be loaded", len(filesToProcess))
		return
	}

	stats := &Stats{StartTime: time.Now()}

	// Worker pool with persistent connections (one CH connection per worker)
	fileChan := make(chan string, *workers*2)
	var wg sync.WaitGroup

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go worker(ctx, i, fileChan, *src, *host, *db, *table, *batchSize, *enrich, stats, rejectWriter, &wg)
	}

	for _, logPath := range filesToProcess {
		if ctx.Err() != nil {
			break
		}
		fileChan <- logPath
	}
	close(fileChan)

	wg.Wait()

	elapsed := time.Since(stats.StartTime)
	totalRows := stats.TotalRows.Load()
	totalFiles := stats.TotalFiles.Load()
	failedFiles := stats.FailedFiles.Load()
	skippedRows := stats.SkippedRows.Load()
	gridsFound := stats.GridsFound.Load()

	rps := float64(0)
	if elapsed.Seconds() > 0 {
		rps = float64(totalRows) / elapsed.Seconds()
	}

	log.Println()
	log.Println("=========================================================")
	log.Println("Final Statistics")
	log.Println("=========================================================")
	log.Printf("Files OK:       %d", totalFiles)
	log.Printf("Files Failed:   %d", failedFiles)
	log.Printf("QSOs Inserted:  %d", totalRows)
	log.Printf("QSOs Skipped:   %d", skippedRows)
	if q := stats.Quarantined.Load(); q > 0 {
		log.Printf("QSOs Held:      %d (dated outside declared year -> %s)", q, quarantineTable)
	}
	log.Printf("Grids Enriched: %d", gridsFound)
	log.Printf("Elapsed:        %v", elapsed.Round(time.Second))
	if rps > 1_000_000 {
		log.Printf("Throughput:     %.2f Mrps", rps/1_000_000)
	} else {
		log.Printf("Throughput:     %.0f rows/s", rps)
	}
	if *rejectLog != "" && failedFiles > 0 {
		log.Printf("Reject log:     %s (%d entries)", *rejectLog, failedFiles)
	}
	log.Println("=========================================================")
}

// Ensure net import is used (for ch-go).
var _ = net.Dial
