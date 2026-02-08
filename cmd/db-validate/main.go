// db-validate — Validate ClickHouse database table row counts
//
// Connects to ClickHouse and checks that key tables exist and have expected
// minimum row counts. Use flags to select which databases to validate.
//
// Build: CGO_ENABLED=0 go build -ldflags="-s -w" -o build/db-validate ./cmd/db-validate

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

var Version = "dev"

// TableCheck defines a table validation target.
type TableCheck struct {
	Database string
	Table    string
	MinRows  uint64 // expected minimum row count (0 = just check existence)
	Group    string // wspr, solar, rbn, contest
}

var checks = []TableCheck{
	// Bronze tables
	{"wspr", "bronze", 10_000_000_000, "wspr"},
	{"solar", "bronze", 10_000, "solar"},
	{"rbn", "bronze", 2_000_000_000, "rbn"},
	{"contest", "bronze", 200_000_000, "contest"},

	// Derived wspr tables
	{"wspr", "callsign_grid", 30_000, "wspr"},
	{"wspr", "silver", 0, "wspr"},
	{"wspr", "signatures_v1", 0, "wspr"},
	{"wspr", "gold_stratified", 0, "wspr"},
	{"wspr", "gold_continuous", 0, "wspr"},
	{"wspr", "gold_v6", 0, "wspr"},

	// Management
	{"data_mgmt", "config", 0, "solar"},
	{"data_mgmt", "lab_versions", 0, "solar"},
}

func main() {
	host := flag.String("host", "192.168.1.90:9000", "ClickHouse host:port")
	doWSPR := flag.Bool("wspr", false, "Validate wspr tables")
	doSolar := flag.Bool("solar", false, "Validate solar tables")
	doRBN := flag.Bool("rbn", false, "Validate rbn tables")
	doContest := flag.Bool("contest", false, "Validate contest tables")
	doAll := flag.Bool("all", false, "Validate all tables (default if no flags)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "db-validate v%s — Validate ClickHouse table row counts\n\n", Version)
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  db-validate --all\n")
		fmt.Fprintf(os.Stderr, "  db-validate --wspr --solar\n")
		fmt.Fprintf(os.Stderr, "  db-validate --rbn --host 10.60.1.1:9000\n")
	}

	flag.Parse()

	// Default to --all if no group flags specified
	if !*doWSPR && !*doSolar && !*doRBN && !*doContest && !*doAll {
		*doAll = true
	}

	activeGroups := map[string]bool{
		"wspr":    *doWSPR || *doAll,
		"solar":   *doSolar || *doAll,
		"rbn":     *doRBN || *doAll,
		"contest": *doContest || *doAll,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	log.Printf("=========================================================")
	log.Printf("db-validate v%s", Version)
	log.Printf("=========================================================")
	log.Printf("Host: %s", *host)

	conn, err := ch.Dial(ctx, ch.Options{
		Address: *host,
	})
	if err != nil {
		log.Fatalf("ClickHouse connection failed: %v", err)
	}
	defer conn.Close()
	log.Println("Connection OK")
	log.Println("=========================================================")

	start := time.Now()
	passed := 0
	warned := 0
	failed := 0

	for _, tc := range checks {
		if !activeGroups[tc.Group] {
			continue
		}

		count, err := getRowCount(ctx, conn, tc.Database, tc.Table)
		if err != nil {
			log.Printf("  FAIL  %s.%-18s  error: %v", tc.Database, tc.Table, err)
			failed++
			continue
		}

		if tc.MinRows > 0 && count < tc.MinRows {
			log.Printf("  WARN  %s.%-18s  %s rows (expected >= %s)",
				tc.Database, tc.Table, fmtNum(count), fmtNum(tc.MinRows))
			warned++
		} else if count == 0 {
			log.Printf("  ----  %s.%-18s  empty", tc.Database, tc.Table)
			warned++
		} else {
			log.Printf("  OK    %s.%-18s  %s rows", tc.Database, tc.Table, fmtNum(count))
			passed++
		}
	}

	elapsed := time.Since(start)
	log.Println()
	log.Println("=========================================================")
	log.Printf("Results: %d passed, %d warnings, %d failed  (%v)", passed, warned, failed, elapsed.Round(time.Millisecond))
	log.Println("=========================================================")

	if failed > 0 {
		os.Exit(1)
	}
}

func getRowCount(ctx context.Context, conn *ch.Client, database, table string) (uint64, error) {
	var count proto.ColUInt64
	query := fmt.Sprintf("SELECT count() FROM %s.%s", database, table)

	err := conn.Do(ctx, ch.Query{
		Body: query,
		Result: proto.Results{
			{Name: "count()", Data: &count},
		},
	})
	if err != nil {
		return 0, err
	}

	if count.Rows() == 0 {
		return 0, nil
	}
	return count.Row(0), nil
}

func fmtNum(n uint64) string {
	if n >= 1_000_000_000 {
		return fmt.Sprintf("%.2fB", float64(n)/1_000_000_000)
	}
	if n >= 1_000_000 {
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	}
	if n >= 1_000 {
		return fmt.Sprintf("%.1fK", float64(n)/1_000)
	}
	return fmt.Sprintf("%d", n)
}
