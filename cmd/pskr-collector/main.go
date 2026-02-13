// pskr-collector — PSK Reporter MQTT real-time spot collector
//
// Subscribes to the PSK Reporter MQTT feed (mqtt.pskreporter.info),
// parses JSON spots, normalizes band via bands.GetBand(), and writes
// hourly-rotated gzip JSONL files to /mnt/pskr-data/YYYY/MM/DD/.
//
// This is stage 1 of a two-stage pipeline:
//   1. Collect: MQTT → gzip JSONL files on disk (this tool)
//   2. Ingest:  JSONL → ClickHouse pskr.bronze (future pskr-ingest tool)
//
// The disk-first design provides durability (no data loss if ClickHouse is
// down), replayability (re-ingest after schema changes), and consistency
// with the WSPR/RBN/contest pipeline pattern.
//
// MQTT JSON fields (short-form from PSK Reporter):
//   t  = Unix timestamp
//   sc = sender callsign
//   rc = receiver callsign
//   sl = sender locator (grid)
//   rl = receiver locator (grid)
//   rp = signal report (SNR dB)
//   f  = frequency (Hz)
//   md = mode (FT8, FT4, WSPR, etc.)
//
// Build: CGO_ENABLED=0 go build -ldflags="-s -w" -o build/pskr-collector ./cmd/pskr-collector

package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/IONIS-AI/ionis-apps/internal/bands"
)

var (
	Version   = "dev"
	GitCommit = "unknown"
	BuildDate = "unknown"
)

// mqttSpot is the raw JSON from PSK Reporter MQTT.
type mqttSpot struct {
	Timestamp    int64  `json:"t"`
	SenderCall   string `json:"sc"`
	ReceiverCall string `json:"rc"`
	SenderGrid   string `json:"sl"`
	ReceiverGrid string `json:"rl"`
	SNR          int    `json:"rp"`
	Frequency    uint64 `json:"f"`
	Mode         string `json:"md"`
}

// outputSpot is the normalized format written to JSONL files.
type outputSpot struct {
	Timestamp    string `json:"ts"`
	SenderCall   string `json:"sc"`
	SenderGrid   string `json:"sg"`
	ReceiverCall string `json:"rc"`
	ReceiverGrid string `json:"rg"`
	FreqHz       uint64 `json:"f"`
	Band         int32  `json:"band"`
	Mode         string `json:"mode"`
	SNR          int    `json:"snr"`
}

var gridRe = regexp.MustCompile(`^[A-R]{2}[0-9]{2}([a-x]{2})?$`)

// collector manages MQTT subscription and JSONL file output.
type collector struct {
	outDir   string
	rotate   time.Duration
	hfOnly   bool
	bufSize  int

	spots    chan outputSpot
	stats    stats

	mu       sync.Mutex
	file     *os.File
	gzWriter *gzip.Writer
	encoder  *json.Encoder
	fileTime time.Time
}

type stats struct {
	received  atomic.Uint64
	written   atomic.Uint64
	filtered  atomic.Uint64
	invalid   atomic.Uint64
	errors    atomic.Uint64
}

func newCollector(outDir string, rotate time.Duration, hfOnly bool, bufSize int) *collector {
	return &collector{
		outDir:  outDir,
		rotate:  rotate,
		hfOnly:  hfOnly,
		bufSize: bufSize,
		spots:   make(chan outputSpot, bufSize),
	}
}

// filePath returns the output file path for the given time.
func (c *collector) filePath(t time.Time) string {
	return filepath.Join(
		c.outDir,
		t.Format("2006"),
		t.Format("01"),
		t.Format("02"),
		fmt.Sprintf("spots-%s.jsonl.gz", t.Format("150405")),
	)
}

// openFile opens a new gzip JSONL output file for the given time.
func (c *collector) openFile(t time.Time) error {
	path := c.filePath(t)
	dir := filepath.Dir(path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}

	gz, err := gzip.NewWriterLevel(f, gzip.BestSpeed)
	if err != nil {
		f.Close()
		return fmt.Errorf("gzip writer: %w", err)
	}

	c.file = f
	c.gzWriter = gz
	c.encoder = json.NewEncoder(gz)
	c.fileTime = t

	log.Printf("Opened %s", path)
	return nil
}

// closeFile flushes and closes the current output file.
func (c *collector) closeFile() error {
	if c.gzWriter != nil {
		if err := c.gzWriter.Close(); err != nil {
			return err
		}
	}
	if c.file != nil {
		return c.file.Close()
	}
	return nil
}

// shouldRotate returns true if it's time to open a new file.
func (c *collector) shouldRotate(now time.Time) bool {
	if c.file == nil {
		return true
	}
	return now.Sub(c.fileTime) >= c.rotate
}

// handleMessage is the MQTT message callback. It parses the JSON spot,
// validates fields, normalizes band, and sends to the writer channel.
func (c *collector) handleMessage(_ mqtt.Client, msg mqtt.Message) {
	c.stats.received.Add(1)

	var raw mqttSpot
	if err := json.Unmarshal(msg.Payload(), &raw); err != nil {
		c.stats.invalid.Add(1)
		return
	}

	// Validate required fields
	if raw.SenderCall == "" || raw.ReceiverCall == "" || raw.Frequency == 0 {
		c.stats.invalid.Add(1)
		return
	}

	// Band normalization: frequency is in Hz, GetBand expects MHz
	freqMHz := float64(raw.Frequency) / 1e6
	bandID, _ := bands.GetBand(freqMHz)

	// HF-only filter: bands 102 (160m) through 111 (10m)
	if c.hfOnly && (bandID < 102 || bandID > 111) {
		c.stats.filtered.Add(1)
		return
	}

	// Validate grids if present (4 or 6 char Maidenhead)
	senderGrid := raw.SenderGrid
	if senderGrid != "" && !gridRe.MatchString(senderGrid) {
		senderGrid = ""
	}
	receiverGrid := raw.ReceiverGrid
	if receiverGrid != "" && !gridRe.MatchString(receiverGrid) {
		receiverGrid = ""
	}

	ts := time.Unix(raw.Timestamp, 0).UTC()

	spot := outputSpot{
		Timestamp:    ts.Format("2006-01-02T15:04:05Z"),
		SenderCall:   raw.SenderCall,
		SenderGrid:   senderGrid,
		ReceiverCall: raw.ReceiverCall,
		ReceiverGrid: receiverGrid,
		FreqHz:       raw.Frequency,
		Band:         bandID,
		Mode:         raw.Mode,
		SNR:          raw.SNR,
	}

	// Non-blocking send — drop spot if channel is full
	select {
	case c.spots <- spot:
	default:
		c.stats.errors.Add(1)
	}
}

// writer drains the spot channel and writes JSONL to rotated gzip files.
func (c *collector) writer(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case spot := <-c.spots:
			c.mu.Lock()
			now := time.Now().UTC()
			if c.shouldRotate(now) {
				if err := c.closeFile(); err != nil {
					log.Printf("Close error: %v", err)
				}
				if err := c.openFile(now); err != nil {
					log.Printf("Open error: %v", err)
					c.stats.errors.Add(1)
					c.mu.Unlock()
					continue
				}
			}
			if err := c.encoder.Encode(spot); err != nil {
				c.stats.errors.Add(1)
			} else {
				c.stats.written.Add(1)
			}
			c.mu.Unlock()

		case <-ticker.C:
			// Periodic flush to prevent data sitting in buffers
			c.mu.Lock()
			if c.gzWriter != nil {
				_ = c.gzWriter.Flush()
			}
			if c.file != nil {
				_ = c.file.Sync()
			}
			c.mu.Unlock()

		case <-ctx.Done():
			// Drain remaining spots
			for {
				select {
				case spot := <-c.spots:
					c.mu.Lock()
					now := time.Now().UTC()
					if c.shouldRotate(now) {
						_ = c.closeFile()
						if err := c.openFile(now); err != nil {
							c.stats.errors.Add(1)
							c.mu.Unlock()
							continue
						}
					}
					if err := c.encoder.Encode(spot); err != nil {
						c.stats.errors.Add(1)
					} else {
						c.stats.written.Add(1)
					}
					c.mu.Unlock()
				default:
					c.mu.Lock()
					_ = c.closeFile()
					c.mu.Unlock()
					return
				}
			}
		}
	}
}

// reportStats logs periodic metrics.
func (c *collector) reportStats(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastWritten uint64

	for {
		select {
		case <-ticker.C:
			written := c.stats.written.Load()
			delta := written - lastWritten
			rate := float64(delta) / interval.Seconds()
			lastWritten = written

			log.Printf("Stats: received=%d written=%d filtered=%d invalid=%d errors=%d (%.1f spots/sec)",
				c.stats.received.Load(),
				written,
				c.stats.filtered.Load(),
				c.stats.invalid.Load(),
				c.stats.errors.Load(),
				rate,
			)
		case <-ctx.Done():
			return
		}
	}
}

func main() {
	broker := flag.String("broker", "mqtt.pskreporter.info:1883", "MQTT broker address")
	topic := flag.String("topic", "pskr/filter/v2/#", "MQTT topic filter")
	outDir := flag.String("outdir", "/mnt/pskr-data", "Output directory for JSONL files")
	rotate := flag.Duration("rotate", 1*time.Hour, "File rotation interval")
	bufSize := flag.Int("buffer", 100000, "Channel buffer size")
	hfOnly := flag.Bool("hf-only", true, "Filter to HF bands only (160m-10m)")
	statsInterval := flag.Duration("stats", 60*time.Second, "Stats reporting interval")
	clientID := flag.String("client-id", "", "MQTT client ID (default: auto-generated)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "pskr-collector v%s — PSK Reporter MQTT real-time spot collector\n\n", Version)
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Subscribes to PSK Reporter MQTT feed and writes hourly-rotated\n")
		fmt.Fprintf(os.Stderr, "gzip JSONL files to --outdir/YYYY/MM/DD/spots-HHMMSS.jsonl.gz.\n\n")
		fmt.Fprintf(os.Stderr, "This is a long-running daemon. Use Ctrl+C or SIGTERM for graceful shutdown.\n\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  pskr-collector\n")
		fmt.Fprintf(os.Stderr, "  pskr-collector --hf-only=false\n")
		fmt.Fprintf(os.Stderr, "  pskr-collector --rotate 30m --outdir /tmp/pskr-test\n")
		fmt.Fprintf(os.Stderr, "  pskr-collector --topic 'pskr/filter/v2/20m/#'\n")
	}

	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	log.Println("=========================================================")
	log.Printf("PSK Reporter Collector v%s (%s, %s)", Version, GitCommit, BuildDate)
	log.Println("=========================================================")
	log.Printf("Broker:   %s", *broker)
	log.Printf("Topic:    %s", *topic)
	log.Printf("Output:   %s", *outDir)
	log.Printf("Rotate:   %s", *rotate)
	log.Printf("HF-Only:  %v", *hfOnly)
	log.Printf("Buffer:   %d", *bufSize)

	// Verify output directory exists
	if err := os.MkdirAll(*outDir, 0755); err != nil {
		log.Fatalf("Cannot create output directory %s: %v", *outDir, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	c := newCollector(*outDir, *rotate, *hfOnly, *bufSize)

	// Start writer goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.writer(ctx)
	}()

	// Start stats reporter
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.reportStats(ctx, *statsInterval)
	}()

	// MQTT client setup
	mqttClientID := *clientID
	if mqttClientID == "" {
		mqttClientID = fmt.Sprintf("ki7mt-ionis-pskr-collector-v%s", Version)
	}

	topicFilter := *topic

	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s", *broker))
	opts.SetClientID(mqttClientID)
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(60 * time.Second)
	opts.SetCleanSession(true)
	opts.SetOrderMatters(false)
	opts.SetDefaultPublishHandler(c.handleMessage)

	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		log.Printf("MQTT disconnected: %v — will auto-reconnect", err)
	})

	opts.SetOnConnectHandler(func(client mqtt.Client) {
		log.Printf("MQTT connected, subscribing to %s", topicFilter)
		token := client.Subscribe(topicFilter, 0, nil)
		token.Wait()
		if token.Error() != nil {
			log.Printf("Subscribe error: %v", token.Error())
		} else {
			log.Printf("Subscribed successfully")
		}
	})

	log.Printf("Connecting to MQTT broker %s ...", *broker)
	client := mqtt.NewClient(opts)
	token := client.Connect()
	token.Wait()
	if token.Error() != nil {
		log.Fatalf("MQTT connect failed: %v", token.Error())
	}

	log.Println("=========================================================")
	log.Println("Collecting spots... (Ctrl+C to stop)")
	log.Println("=========================================================")

	// Wait for shutdown signal
	<-sigChan
	log.Println()
	log.Println("Shutdown requested — flushing buffers...")

	// Disconnect MQTT first to stop new messages
	client.Disconnect(5000)
	log.Println("MQTT disconnected")

	// Cancel context to drain writer
	cancel()
	wg.Wait()

	// Final stats
	log.Println()
	log.Println("=========================================================")
	log.Println("Final Statistics")
	log.Println("=========================================================")
	log.Printf("Received: %d", c.stats.received.Load())
	log.Printf("Written:  %d", c.stats.written.Load())
	log.Printf("Filtered: %d (non-HF)", c.stats.filtered.Load())
	log.Printf("Invalid:  %d", c.stats.invalid.Load())
	log.Printf("Errors:   %d", c.stats.errors.Load())
	log.Println("=========================================================")
}
