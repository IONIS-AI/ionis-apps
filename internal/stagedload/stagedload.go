// Package stagedload replaces a bronze table with an exact copy of one source file,
// atomically, and reports what actually landed.
//
// The house pattern for full-file sources (Watson, #46): load into <table>_staging,
// read back count(), and only if it equals what the file holds run EXCHANGE TABLES.
// A short or failed load never becomes the live table, a rerun never duplicates, and
// the number printed is the count read from the table -- never the count sent, which
// is how 49 lost SFI observations once read as "inserted".
package stagedload

import (
	"context"
	"fmt"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

// Replace creates <table>_staging with the live table's schema, calls insert to fill
// it, checks it holds exactly want rows, swaps it in, and returns the live count.
func Replace(ctx context.Context, conn *ch.Client, table string, want int, insert func(staging string) error) (int, error) {
	staging := table + "_staging"
	for _, q := range []string{
		"DROP TABLE IF EXISTS " + staging,
		fmt.Sprintf("CREATE TABLE %s AS %s", staging, table),
	} {
		if err := conn.Do(ctx, ch.Query{Body: q}); err != nil {
			return 0, fmt.Errorf("%s: %w", q, err)
		}
	}
	if err := insert(staging); err != nil {
		return 0, fmt.Errorf("insert into %s: %w (live table untouched)", staging, err)
	}
	landed, err := Count(ctx, conn, staging)
	if err != nil {
		return 0, fmt.Errorf("count %s: %w (live table untouched)", staging, err)
	}
	if landed != want {
		return 0, fmt.Errorf("%s holds %d rows, the source has %d -- not swapping (live table untouched)", staging, landed, want)
	}
	if err := conn.Do(ctx, ch.Query{Body: fmt.Sprintf("EXCHANGE TABLES %s AND %s", table, staging)}); err != nil {
		return 0, fmt.Errorf("exchange: %w", err)
	}
	_ = conn.Do(ctx, ch.Query{Body: "DROP TABLE IF EXISTS " + staging})
	return Count(ctx, conn, table)
}

// Count returns count() of a table.
func Count(ctx context.Context, conn *ch.Client, table string) (int, error) {
	var c proto.ColUInt64
	if err := conn.Do(ctx, ch.Query{Body: "SELECT count() FROM " + table,
		Result: proto.Results{{Name: "count()", Data: &c}}}); err != nil {
		return 0, err
	}
	if c.Rows() == 0 {
		return 0, fmt.Errorf("count() returned no row")
	}
	return int(c.Row(0)), nil
}
