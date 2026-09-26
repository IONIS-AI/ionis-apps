// pskr-capture-ingest loads pskr-capture's files into pskr.capture_bronze.
//
// It reads $IONIS_PSKR_DATA_DIR/capture/YYYY/MM/DD/capture-*.jsonl.gz and never touches
// the network. ONE ROW PER LINE, message or event (ionis-core 50-pskr_capture_bronze.sql):
// every payload field in its own column, any field the feed adds later in `extra` (raw
// JSON text), a known field of an unexpected type also in `extra` -- no value is dropped
// and no line is rejected. A line that is not readable at all -- e.g. the cut-off last
// line of a crashed hour -- is a row with raw_line and parse_error.
//
// WHICH FILES. Completed hours (capture-*.jsonl.gz). A .partial file is the hour being
// written, or an hour whose process crashed: it is loaded only once its mtime is older
// than -stale-partial, i.e. once nothing is writing it, and its path keeps the .partial
// suffix so the loss is visible in bronze. A .partial that is later completed would be a
// different path; that cannot happen, because pskr-capture never reopens a file.
//
// Watermarked per file in pskr.capture_ingest_log, only after the file's rows are in.
package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/IONIS-AI/ionis-apps/internal/bands"
	"github.com/IONIS-AI/ionis-apps/internal/common"
	"github.com/IONIS-AI/ionis-apps/internal/watermark"
)

var Version = "dev"

// row is one line of a capture file.
type row struct {
	lineNo                     uint32
	rx                         *time.Time
	topic, event, detail       string
	count                      *uint64
	sq, f                      *uint64
	md, sc, sl, rc, rl, b      string
	rp                         *int16
	t, ttx                     *time.Time
	sa, ra                     *uint16
	extra                      map[string]string
	bandADIF                   *int32
	payloadText, raw, parseErr string
}

type line struct {
	RX          string          `json:"rx"`
	Topic       string          `json:"topic"`
	Payload     json.RawMessage `json:"payload"`
	PayloadText *string         `json:"payload_text"`
	Event       string          `json:"event"`
	Detail      string          `json:"detail"`
	Count       *uint64         `json:"count"`
}

func parseLine(n uint32, raw []byte) row {
	r := row{lineNo: n, extra: map[string]string{}}
	var l line
	if err := json.Unmarshal(raw, &l); err != nil {
		r.raw, r.parseErr = string(raw), "line: "+err.Error()
		return r
	}
	if ts, err := time.Parse(time.RFC3339Nano, l.RX); err == nil {
		r.rx = &ts
	} else {
		r.raw, r.parseErr = string(raw), "rx: "+err.Error()
	}
	r.topic, r.event, r.detail, r.count = l.Topic, l.Event, l.Detail, l.Count
	if l.PayloadText != nil {
		r.payloadText = *l.PayloadText
	}
	if len(l.Payload) == 0 {
		return r
	}
	var p map[string]json.RawMessage
	if err := json.Unmarshal(l.Payload, &p); err != nil {
		r.raw, r.parseErr = string(raw), "payload: "+err.Error()
		return r
	}
	for k, v := range p {
		if !field(&r, k, v) {
			r.extra[k] = string(v) // unknown field, or known field of an unexpected type
		}
	}
	if r.f != nil {
		if id, _ := bands.GetBand(float64(*r.f) / 1e6); id != 0 {
			r.bandADIF = &id
		}
	}
	return r
}

// field stores one known payload field; false means "keep it in extra instead".
func field(r *row, k string, v json.RawMessage) bool {
	u := func(bits int) (uint64, bool) {
		x, err := strconv.ParseUint(string(v), 10, bits)
		return x, err == nil
	}
	i := func(bits int) (int64, bool) {
		x, err := strconv.ParseInt(string(v), 10, bits)
		return x, err == nil
	}
	s := func(dst *string) bool { return json.Unmarshal(v, dst) == nil }
	switch k {
	case "sq", "f":
		x, ok := u(64)
		if ok {
			if k == "sq" {
				r.sq = &x
			} else {
				r.f = &x
			}
		}
		return ok
	case "rp":
		x, ok := i(16)
		if ok {
			y := int16(x)
			r.rp = &y
		}
		return ok
	case "t", "t_tx":
		x, ok := i(64)
		if ok {
			ts := time.Unix(x, 0).UTC()
			if k == "t" {
				r.t = &ts
			} else {
				r.ttx = &ts
			}
		}
		return ok
	case "sa", "ra":
		x, ok := u(16)
		if ok {
			y := uint16(x)
			if k == "sa" {
				r.sa = &y
			} else {
				r.ra = &y
			}
		}
		return ok
	case "md":
		return s(&r.md)
	case "sc":
		return s(&r.sc)
	case "sl":
		return s(&r.sl)
	case "rc":
		return s(&r.rc)
	case "rl":
		return s(&r.rl)
	case "b":
		return s(&r.b)
	}
	return false
}

// parseFile reads every line; a gzip stream cut short by a crash yields what it holds,
// its last (possibly cut) line kept as a row with parse_error, and truncated=true.
func parseFile(path string) (rows []row, truncated bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return nil, false, err
	}
	br := bufio.NewReaderSize(zr, 1<<20)
	var n uint32
	for {
		b, rerr := br.ReadBytes('\n')
		if len(b) > 0 {
			n++
			rows = append(rows, parseLine(n, bytes.TrimRight(b, "\n")))
		}
		if rerr == io.EOF {
			return rows, false, nil
		}
		if rerr != nil {
			if errorsIsTruncation(rerr) {
				return rows, true, nil
			}
			return nil, false, rerr
		}
	}
}

func errorsIsTruncation(err error) bool {
	return err == io.ErrUnexpectedEOF || strings.Contains(err.Error(), "unexpected EOF")
}

func opt[T any](p *T) proto.Nullable[T] {
	if p == nil {
		return proto.Null[T]()
	}
	return proto.NewNullable(*p)
}

type batch struct {
	path                          *proto.ColLowCardinality[string]
	lineNo                        proto.ColUInt32
	rx                            *proto.ColNullable[time.Time]
	topic, detail, sc, sl, rc, rl proto.ColStr
	event, md, b                  *proto.ColLowCardinality[string]
	count, sq, f                  *proto.ColNullable[uint64]
	rp                            *proto.ColNullable[int16]
	t, ttx                        *proto.ColNullable[time.Time]
	sa, ra                        *proto.ColNullable[uint16]
	extra                         *proto.ColMap[string, string]
	bandADIF                      *proto.ColNullable[int32]
	payloadText, raw, parseErr    proto.ColStr
	entries                       []watermark.LogEntry
	rows                          int
}

func newBatch() *batch {
	lc := func() *proto.ColLowCardinality[string] { return new(proto.ColStr).LowCardinality() }
	dt := func() *proto.ColNullable[time.Time] {
		return proto.NewColNullable[time.Time](new(proto.ColDateTime))
	}
	return &batch{
		path: lc(), event: lc(), md: lc(), b: lc(),
		rx:    proto.NewColNullable[time.Time](new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano).WithLocation(time.UTC)),
		count: proto.NewColNullable[uint64](new(proto.ColUInt64)), sq: proto.NewColNullable[uint64](new(proto.ColUInt64)),
		f: proto.NewColNullable[uint64](new(proto.ColUInt64)), rp: proto.NewColNullable[int16](new(proto.ColInt16)),
		t: dt(), ttx: dt(),
		sa: proto.NewColNullable[uint16](new(proto.ColUInt16)), ra: proto.NewColNullable[uint16](new(proto.ColUInt16)),
		extra:    proto.NewMap[string, string](new(proto.ColStr), new(proto.ColStr)),
		bandADIF: proto.NewColNullable[int32](new(proto.ColInt32)),
	}
}

func (b *batch) add(rel string, rows []row) {
	for _, r := range rows {
		b.path.Append(rel)
		b.lineNo.Append(r.lineNo)
		b.rx.Append(opt(r.rx))
		b.topic.Append(r.topic)
		b.event.Append(r.event)
		b.detail.Append(r.detail)
		b.count.Append(opt(r.count))
		b.sq.Append(opt(r.sq))
		b.f.Append(opt(r.f))
		b.md.Append(r.md)
		b.rp.Append(opt(r.rp))
		b.t.Append(opt(r.t))
		b.ttx.Append(opt(r.ttx))
		b.sc.Append(r.sc)
		b.sl.Append(r.sl)
		b.rc.Append(r.rc)
		b.rl.Append(r.rl)
		b.sa.Append(opt(r.sa))
		b.ra.Append(opt(r.ra))
		b.b.Append(r.b)
		b.extra.Append(r.extra)
		b.bandADIF.Append(opt(r.bandADIF))
		b.payloadText.Append(r.payloadText)
		b.raw.Append(r.raw)
		b.parseErr.Append(r.parseErr)
	}
	b.rows += len(rows)
}

func (b *batch) input() proto.Input {
	return proto.Input{
		{Name: "file_path", Data: b.path}, {Name: "line_no", Data: &b.lineNo}, {Name: "rx", Data: b.rx},
		{Name: "topic", Data: &b.topic}, {Name: "event", Data: b.event}, {Name: "event_detail", Data: &b.detail},
		{Name: "event_count", Data: b.count}, {Name: "sq", Data: b.sq}, {Name: "f", Data: b.f},
		{Name: "md", Data: b.md}, {Name: "rp", Data: b.rp}, {Name: "t", Data: b.t}, {Name: "t_tx", Data: b.ttx},
		{Name: "sc", Data: &b.sc}, {Name: "sl", Data: &b.sl}, {Name: "rc", Data: &b.rc}, {Name: "rl", Data: &b.rl},
		{Name: "sa", Data: b.sa}, {Name: "ra", Data: b.ra}, {Name: "b", Data: b.b}, {Name: "extra", Data: b.extra},
		{Name: "band_adif", Data: b.bandADIF}, {Name: "payload_text", Data: &b.payloadText},
		{Name: "raw_line", Data: &b.raw}, {Name: "parse_error", Data: &b.parseErr},
	}
}

func (b *batch) commit(ctx context.Context, conn *ch.Client, table, logTable string) error {
	if len(b.entries) == 0 {
		return nil
	}
	in := b.input()
	names := make([]string, len(in))
	for i, c := range in {
		names[i] = c.Name
	}
	if err := conn.Do(ctx, ch.Query{Body: fmt.Sprintf("INSERT INTO %s (%s) VALUES", table, strings.Join(names, ", ")), Input: in}); err != nil {
		return fmt.Errorf("insert %d rows: %w (none of these %d files watermarked)", b.rows, err, len(b.entries))
	}
	return watermark.InsertLogEntriesTable(ctx, conn, logTable, b.entries)
}

func main() {
	var (
		src      = flag.String("src", "", "Capture root (default: $IONIS_PSKR_DATA_DIR/capture)")
		host     = flag.String("host", "", "ClickHouse host:port (default: $IONIS_CH_HOST)")
		table    = flag.String("table", "pskr.capture_bronze", "Destination table")
		logTable = flag.String("log-table", "pskr.capture_ingest_log", "Watermark table")
		batchSz  = flag.Int("batch", 500000, "Rows per INSERT")
		workers  = flag.Int("workers", 8, "Files decompressed and parsed concurrently")
		stale    = flag.Duration("stale-partial", 2*time.Hour, "Load a .partial file once untouched this long (a crashed hour)")
	)
	flag.Parse()
	root := *src
	if root == "" {
		d, err := common.ResolvePath("", "IONIS_PSKR_DATA_DIR", "src", "PSKR data directory")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		root = filepath.Join(d, "capture")
	}
	chHost, err := common.ResolvePath(*host, "IONIS_CH_HOST", "host", "ClickHouse endpoint")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("pskr-capture-ingest v%s\n  %s -> %s\n", Version, root, *table)
	ctx := context.Background()
	wm, err := watermark.LoadWatermarkTable(ctx, chHost, *logTable)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load watermark: %v\n", err)
		os.Exit(1)
	}
	conn, err := ch.Dial(ctx, ch.Options{Address: chHost})
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial %s: %v\n", chHost, err)
		os.Exit(1)
	}
	defer conn.Close()

	var todo []string
	onDisk, done, active := 0, 0, 0
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasPrefix(d.Name(), "capture-") {
			return nil
		}
		complete := strings.HasSuffix(path, ".jsonl.gz")
		partial := strings.HasSuffix(path, ".jsonl.gz.partial")
		if !complete && !partial {
			return nil
		}
		onDisk++
		if partial {
			if fi, err := d.Info(); err != nil || time.Since(fi.ModTime()) < *stale {
				active++ // being written now, or too recently touched to call it abandoned
				return nil
			}
		}
		rel, _ := filepath.Rel(root, path)
		if _, ok := wm[rel]; ok {
			done++
			return nil
		}
		todo = append(todo, path)
		return nil
	})
	sort.Strings(todo)

	type result struct {
		rel       string
		rows      []row
		truncated bool
		size      int64
		elapsed   time.Duration
		err       error
	}
	jobs, results := make(chan string), make(chan result)
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range jobs {
				start := time.Now()
				rel, _ := filepath.Rel(root, path)
				rows, trunc, err := parseFile(path)
				var size int64
				if fi, e := os.Stat(path); e == nil {
					size = fi.Size()
				}
				results <- result{rel, rows, trunc, size, time.Since(start), err}
			}
		}()
	}
	go func() {
		for _, p := range todo {
			jobs <- p
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	b := newBatch()
	loaded, rows, failed, truncated := 0, 0, 0, 0
	for r := range results {
		if r.err != nil {
			fmt.Fprintf(os.Stderr, "  FAIL %s: %v\n", r.rel, r.err)
			failed++
			continue
		}
		if r.truncated {
			truncated++
			fmt.Printf("  %s: gzip stream cut short (crashed hour) -- %d lines readable, loaded as such\n", r.rel, len(r.rows))
		}
		b.add(r.rel, r.rows)
		b.entries = append(b.entries, watermark.LogEntry{FilePath: r.rel, FileSize: uint64(r.size),
			RowCount: uint64(len(r.rows)), ElapsedMs: uint32(r.elapsed.Milliseconds())})
		loaded++
		rows += len(r.rows)
		if b.rows >= *batchSz {
			if err := b.commit(ctx, conn, *table, *logTable); err != nil {
				fmt.Fprintln(os.Stderr, "  "+err.Error())
				os.Exit(1)
			}
			b = newBatch()
		}
	}
	if err := b.commit(ctx, conn, *table, *logTable); err != nil {
		fmt.Fprintln(os.Stderr, "  "+err.Error())
		os.Exit(1)
	}
	fmt.Printf("  %d capture files on disk: %d already loaded, %d still being written (skipped), %d loaded now (%d rows, %d crashed hours)\n",
		onDisk, done, active, loaded, rows, truncated)
	if failed > 0 {
		fmt.Fprintf(os.Stderr, "%d file(s) could not be read -- not loaded, not watermarked\n", failed)
		os.Exit(1)
	}
}
