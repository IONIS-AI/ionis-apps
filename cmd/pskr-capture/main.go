// pskr-capture records the PSK Reporter MQTT feed exactly as it arrives.
//
// WHY THIS REPLACES pskr-collector (Judge, 2026-09-26). The old collector was the
// archive writer and it was not faithful: it dropped every non-HF spot (--hf-only
// defaulted on, ~0.4% of the feed), erased every grid that was not 4 or 6 characters
// (28% of grid values in a 60 s sample -- PSK Reporter sends 8- and 10-character grids),
// turned a missing timestamp into 1970, kept 8 of the 13 fields each message carries
// (sequence number, transmit time, both DXCC entities and the band label were never
// stored), rewrote everything into its own JSON, connected in plain TCP, and could
// drop spots silently when its buffer filled. What it filtered is gone for good.
//
// WHAT THIS WRITES. One JSON line per MQTT message, hourly-rotated gzip:
//
//	{"rx":"<receive time, RFC3339 nanoseconds>","topic":"pskr/filter/v2/...","payload":{...exact bytes...}}
//
// The payload is embedded byte-for-byte when it is valid, compact JSON; otherwise it is
// kept as a string in "payload_text", so either way the original bytes are recoverable. Nothing is filtered, renamed or normalised --
// that is the ingester's job, done with named patches, so the archive stays upstream's
// bytes (DATA-DICTIONARY §0).
//
// EVENTS ARE RECORDED, NOT LOGGED AWAY. Connect, connection loss, reconnect and any
// message dropped because the write buffer was full go into the same file as event
// lines ({"rx":...,"event":"..."}), so a gap in the capture is evidence in the archive,
// not a line in a journal that rotates away. Together with PSK Reporter's per-message
// sequence number (sq), that makes capture loss measurable going forward.
//
// A FILE IS COMPLETE ONLY WHEN RENAMED. Each hour is written to <name>.jsonl.gz.partial
// and renamed to <name>.jsonl.gz only after a clean close, so a crash leaves a file
// marked incomplete by its name -- not a truncated gzip that reads as a short hour.
//
// DELIVERY. TLS to mqtt.pskreporter.info:1884. QoS 1 is requested; the broker grants it
// on the subscription but PSK Reporter publishes at QoS 0, so delivery is at most once.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/IONIS-AI/ionis-apps/internal/common"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var Version = "dev"

// record is one line of the capture file.
type record struct {
	RX          string          `json:"rx"`
	Topic       string          `json:"topic,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	PayloadText *string         `json:"payload_text,omitempty"`
	Event       string          `json:"event,omitempty"`
	Detail      string          `json:"detail,omitempty"`
	Count       int64           `json:"count,omitempty"`
}

type capture struct {
	dir     string
	rotate  time.Duration
	lines   chan record
	dropped atomic.Int64 // dropped since the last drop event was written
	stats   struct{ received, written, dropped, events atomic.Int64 }

	mu       sync.Mutex
	f        *os.File
	gz       *gzip.Writer
	enc      *json.Encoder
	partial  string
	final    string
	openedAt time.Time
}

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// onMessage runs on the MQTT client's goroutine: it copies the bytes and never blocks.
func (c *capture) onMessage(_ mqtt.Client, m mqtt.Message) {
	c.stats.received.Add(1)
	p := m.Payload()
	r := record{RX: now(), Topic: m.Topic()}
	// Embedded as JSON only when the encoder will write back exactly these bytes: it
	// compacts embedded JSON, so a payload with any insignificant whitespace is kept as
	// text instead. PSK Reporter's payloads are compact today; the guarantee must not
	// depend on that.
	var compact bytes.Buffer
	if json.Valid(p) && json.Compact(&compact, p) == nil && bytes.Equal(compact.Bytes(), p) {
		r.Payload = append(json.RawMessage(nil), p...)
	} else {
		s := string(p)
		r.PayloadText = &s
	}
	select {
	case c.lines <- r:
	default:
		c.dropped.Add(1) // recorded as an event line by the writer, never silent
		c.stats.dropped.Add(1)
	}
}

func (c *capture) event(name, detail string) {
	c.stats.events.Add(1)
	c.lines <- record{RX: now(), Event: name, Detail: detail} // events block rather than drop
}

func (c *capture) open(t time.Time) error {
	day := filepath.Join(c.dir, t.Format("2006"), t.Format("01"), t.Format("02"))
	for _, d := range []string{c.dir, filepath.Join(c.dir, t.Format("2006")), filepath.Join(c.dir, t.Format("2006"), t.Format("01")), day} {
		if err := os.MkdirAll(d, 0o775|os.ModeSetgid); err != nil {
			return err
		}
		_ = os.Chmod(d, 0o775|os.ModeSetgid)
	}
	c.final = filepath.Join(day, "capture-"+t.Format("150405")+".jsonl.gz")
	c.partial = c.final + ".partial"
	f, err := os.OpenFile(c.partial, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	c.f, c.gz = f, gzip.NewWriter(f)
	c.enc = json.NewEncoder(c.gz)
	c.enc.SetEscapeHTML(false)
	c.openedAt = t
	return nil
}

// close finishes the gzip stream, syncs, and only then renames the file to its final name.
func (c *capture) close() error {
	if c.f == nil {
		return nil
	}
	err1 := c.gz.Close()
	err2 := c.f.Sync()
	err3 := c.f.Close()
	c.f = nil
	for _, e := range []error{err1, err2, err3} {
		if e != nil {
			return fmt.Errorf("closing %s (left as .partial): %w", c.partial, e)
		}
	}
	return os.Rename(c.partial, c.final)
}

func (c *capture) write(r record) {
	t := time.Now().UTC()
	if c.f == nil || t.Sub(c.openedAt) >= c.rotate {
		if err := c.close(); err != nil {
			log.Print(err)
		}
		if err := c.open(t); err != nil {
			log.Printf("open: %v -- exiting so systemd restarts and the gap is visible", err)
			os.Exit(1)
		}
	}
	if n := c.dropped.Swap(0); n > 0 {
		if err := c.enc.Encode(record{RX: now(), Event: "dropped", Detail: "write buffer full", Count: n}); err != nil {
			log.Printf("write: %v", err)
			os.Exit(1)
		}
	}
	if err := c.enc.Encode(r); err != nil {
		log.Printf("write: %v -- exiting", err)
		os.Exit(1)
	}
	c.stats.written.Add(1)
}

// writer owns the file. It stops on stop (not on the signal context), so main can
// disconnect and write the final "stopped" event while the writer is still reading.
func (c *capture) writer(stop <-chan struct{}, done chan<- struct{}) {
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		select {
		case r := <-c.lines:
			c.mu.Lock()
			c.write(r)
			c.mu.Unlock()
		case <-tick.C:
			c.mu.Lock()
			if c.f != nil {
				_ = c.gz.Flush()
				_ = c.f.Sync()
			}
			c.mu.Unlock()
		case <-stop:
			c.mu.Lock()
			for {
				select {
				case r := <-c.lines:
					c.write(r)
					continue
				default:
				}
				break
			}
			if err := c.close(); err != nil {
				log.Print(err)
			}
			c.mu.Unlock()
			close(done)
			return
		}
	}
}

func main() {
	var (
		broker = flag.String("broker", "ssl://mqtt.pskreporter.info:1884", "MQTT broker URL (TLS)")
		topic  = flag.String("topic", "pskr/filter/v2/#", "MQTT topic filter -- everything by default")
		outDir = flag.String("outdir", "", "Capture root (default: $IONIS_PSKR_DATA_DIR/capture)")
		rotate = flag.Duration("rotate", time.Hour, "File rotation interval")
		buf    = flag.Int("buffer", 1_000_000, "Messages buffered between the MQTT callback and the writer")
		every  = flag.Duration("stats", time.Minute, "Stats log interval")
	)
	flag.Parse()
	dir := *outDir
	if dir == "" {
		d, err := common.ResolvePath("", "IONIS_PSKR_DATA_DIR", "outdir", "PSKR data directory")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		dir = filepath.Join(d, "capture")
	}
	c := &capture{dir: dir, rotate: *rotate, lines: make(chan record, *buf)}
	log.Printf("pskr-capture v%s  broker %s  topic %s  -> %s", Version, *broker, *topic, dir)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	stop, done := make(chan struct{}), make(chan struct{})
	go c.writer(stop, done)

	host, _ := os.Hostname()
	opts := mqtt.NewClientOptions().AddBroker(*broker).
		SetClientID(fmt.Sprintf("ionis-capture-%s-%d", host, os.Getpid())).
		SetTLSConfig(&tls.Config{ServerName: "mqtt.pskreporter.info", MinVersion: tls.VersionTLS12}).
		SetAutoReconnect(true).SetCleanSession(true).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) { c.event("connection_lost", err.Error()) }).
		SetReconnectingHandler(func(_ mqtt.Client, _ *mqtt.ClientOptions) { c.event("reconnecting", "") }).
		SetOnConnectHandler(func(cl mqtt.Client) {
			c.event("connected", *broker)
			tok := cl.Subscribe(*topic, 1, c.onMessage)
			tok.Wait()
			if err := tok.Error(); err != nil {
				c.event("subscribe_failed", err.Error())
				return
			}
			c.event("subscribed", fmt.Sprintf("%s granted %v", *topic, tok.(*mqtt.SubscribeToken).Result()))
		})
	cl := mqtt.NewClient(opts)
	if t := cl.Connect(); t.Wait() && t.Error() != nil {
		log.Printf("connect: %v", t.Error())
		os.Exit(1)
	}
	go func() {
		t := time.NewTicker(*every)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				log.Printf("received=%d written=%d dropped=%d events=%d buffered=%d",
					c.stats.received.Load(), c.stats.written.Load(), c.stats.dropped.Load(), c.stats.events.Load(), len(c.lines))
			case <-ctx.Done():
				return
			}
		}
	}()
	<-ctx.Done()
	cl.Disconnect(500)
	c.lines <- record{RX: now(), Event: "stopped", Detail: "shutdown signal"}
	close(stop)
	<-done
	log.Printf("stopped: received=%d written=%d dropped=%d", c.stats.received.Load(), c.stats.written.Load(), c.stats.dropped.Load())
}
