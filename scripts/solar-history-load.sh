#!/bin/bash
# =============================================================================
# Name............: solar-history-load
# Version.........: 3.0.0
# Description.....: Load NOAA SWPC solar data into ClickHouse solar.bronze
# Usage...........: solar-history-load [--download]
#
# This script:
#   1. Optionally downloads fresh 7-day X-ray, 7-day Kp, 30-day SFI from NOAA
#   2. Aggregates 1-minute X-ray flux into 3-hour buckets (max per bucket)
#   3. Merges Kp (already 3-hourly) and daily SFI into the same time grid
#   4. Inserts 3 streams into a temp staging table, then merges via
#      GROUP BY (date, time) + max() before inserting into solar.bronze.
#      This prevents ReplacingMergeTree from collapsing partial rows.
#
# Data sources:
#   - GOES X-ray flux (7 days, 1-min): services.swpc.noaa.gov
#   - Planetary Kp index (7 days, 3-hr): services.swpc.noaa.gov
#   - F10.7 solar flux (30 days, daily): services.swpc.noaa.gov
#
# =============================================================================
set -e

SOLAR_DATA_DIR="${SOLAR_DATA_DIR:-/mnt/ai-stack/solar-data/raw}"
DO_DOWNLOAD=false

# NOAA SWPC endpoints
XRAY_URL="https://services.swpc.noaa.gov/json/goes/primary/xrays-7-day.json"
KP_URL="https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json"
SFI_URL="https://services.swpc.noaa.gov/products/10cm-flux-30-day.json"

# Local file paths
XRAY_FILE="$SOLAR_DATA_DIR/goes_xray_7day.json"
KP_FILE="$SOLAR_DATA_DIR/noaa_kp_7day.json"
SFI_FILE="$SOLAR_DATA_DIR/noaa_sfi_30day.json"

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        --download|-d)
            DO_DOWNLOAD=true
            shift
            ;;
        --help|-h)
            printf "Usage: %s [--download]\n" "$(basename "$0")"
            printf "  --download    Download fresh data from NOAA SWPC first\n"
            printf "\nWithout --download, uses existing files in %s\n" "$SOLAR_DATA_DIR"
            exit 0
            ;;
        *)
            shift
            ;;
    esac
done

# ─────────────────────────────────────────────────────────────────────────────
# 1. Download (optional)
# ─────────────────────────────────────────────────────────────────────────────
if $DO_DOWNLOAD; then
    printf "[%s] Downloading NOAA SWPC data...\n" "$(date '+%Y-%m-%d %H:%M:%S')"

    curl -sS -o "$XRAY_FILE" "$XRAY_URL"
    XRAY_COUNT=$(jq 'length' "$XRAY_FILE")
    printf "  X-ray 7-day:  %s records\n" "$XRAY_COUNT"

    curl -sS -o "$KP_FILE" "$KP_URL"
    KP_COUNT=$(jq 'length - 1' "$KP_FILE")  # subtract header row
    printf "  Kp 7-day:     %s records\n" "$KP_COUNT"

    curl -sS -o "$SFI_FILE" "$SFI_URL"
    SFI_COUNT=$(jq 'length - 1' "$SFI_FILE")  # subtract header row
    printf "  SFI 30-day:   %s records\n" "$SFI_COUNT"
fi

# Validate files exist
for f in "$XRAY_FILE" "$KP_FILE" "$SFI_FILE"; do
    if [[ ! -f "$f" ]]; then
        printf "ERROR: %s not found. Run with --download first.\n" "$f" >&2
        exit 1
    fi
done

# ─────────────────────────────────────────────────────────────────────────────
# 2. Process X-ray data (aggregate 1-min → 3-hour buckets)
# ─────────────────────────────────────────────────────────────────────────────
printf "[%s] Processing X-ray flux data...\n" "$(date '+%Y-%m-%d %H:%M:%S')"

# Aggregate: group by date + 3-hour bucket, take max flux per energy band
# Output: date, hour (bucket start), max_xray_short, max_xray_long
XRAY_CSV=$(jq -r '
  # Group by date and 3-hour bucket
  group_by(.time_tag[:13])
  | map(
      {
        time_tag: .[0].time_tag,
        short: [.[] | select(.energy == "0.05-0.4nm") | .flux] | max,
        long:  [.[] | select(.energy == "0.1-0.8nm")  | .flux] | max
      }
    )
  # Re-group into 3-hour windows
  | group_by(.time_tag[:11] + ((.time_tag[11:13] | tonumber / 3 | floor * 3) | tostring | if length == 1 then "0" + . else . end))
  | map(
      {
        date: .[0].time_tag[:10],
        hour: (.[0].time_tag[11:13] | tonumber / 3 | floor * 3),
        xray_short: [.[].short // 0] | max,
        xray_long:  [.[].long  // 0] | max
      }
    )
  | .[]
  | "\(.date)\t\(.date) \(if .hour < 10 then "0" else "" end)\(.hour):00:00\t0\t0\t0\t0\t0\tgoes_xray_7day.json\t\(.xray_short)\t\(.xray_long)"
' "$XRAY_FILE")

XRAY_ROWS=$(printf "%s\n" "$XRAY_CSV" | wc -l)
printf "  Aggregated to %s 3-hour X-ray buckets\n" "$XRAY_ROWS"

# ─────────────────────────────────────────────────────────────────────────────
# 3. Process Kp data (already 3-hourly)
# ─────────────────────────────────────────────────────────────────────────────
printf "[%s] Processing Kp index data...\n" "$(date '+%Y-%m-%d %H:%M:%S')"

# Skip header row [0], parse remaining
# Kp is field [1], ap_running is field [2]
# Time format: "2026-01-27 00:00:00.000" → need "2026-01-27 00:00:00"
KP_CSV=$(jq -r '
  .[1:][]
  | {
      date: .[0][:10],
      time: .[0][:19],
      kp:   (.[1] | tonumber),
      ap:   (.[2] | tonumber)
    }
  | "\(.date)\t\(.time)\t0\t0\t0\t\(.kp)\t\(.ap)\tnoaa_kp_7day.json\t0\t0"
' "$KP_FILE")

KP_ROWS=$(printf "%s\n" "$KP_CSV" | wc -l)
printf "  Loaded %s Kp records\n" "$KP_ROWS"

# ─────────────────────────────────────────────────────────────────────────────
# 4. Process SFI data (daily → assign to all 8 buckets per day)
# ─────────────────────────────────────────────────────────────────────────────
printf "[%s] Processing F10.7 solar flux data...\n" "$(date '+%Y-%m-%d %H:%M:%S')"

# Expand daily SFI into 8 3-hourly rows so every bucket has flux data
SFI_CSV=$(jq -r '
  .[1:][]
  | {date: .[0][:10], flux: (.[1] | tonumber)}
  | . as $d
  | range(0; 24; 3)
  | "\($d.date)\t\($d.date) \(if . < 10 then "0" else "" end)\(.):00:00\t\($d.flux)\t0\t0\t0\t0\tnoaa_sfi_30day.json\t0\t0"
' "$SFI_FILE")

SFI_ROWS=$(printf "%s\n" "$SFI_CSV" | wc -l)
printf "  Expanded to %s SFI rows (8 per day)\n" "$SFI_ROWS"

# ─────────────────────────────────────────────────────────────────────────────
# 5. Stage all 3 streams into a temp table
# ─────────────────────────────────────────────────────────────────────────────
# solar.bronze uses ReplacingMergeTree ORDER BY (date, time). Inserting 3
# partial rows per (date, time) key would collapse to 1 row after merge,
# losing 2 of 3 streams. Instead: stage → GROUP BY merge → single insert.

STAGING_TABLE="solar._tmp_history_load"

printf "[%s] Creating staging table %s...\n" "$(date '+%Y-%m-%d %H:%M:%S')" "$STAGING_TABLE"

clickhouse-client --query "DROP TABLE IF EXISTS $STAGING_TABLE"
clickhouse-client --query "
    CREATE TABLE $STAGING_TABLE (
        date Date32,
        time DateTime,
        observed_flux Float32 DEFAULT 0,
        adjusted_flux Float32 DEFAULT 0,
        ssn Float32 DEFAULT 0,
        kp_index Float32 DEFAULT 0,
        ap_index Float32 DEFAULT 0,
        source_file LowCardinality(String),
        xray_short Float32 DEFAULT 0,
        xray_long Float32 DEFAULT 0
    ) ENGINE = MergeTree ORDER BY (date, time)
"

INSERT_COLS="date, time, observed_flux, adjusted_flux, ssn, kp_index, ap_index, source_file, xray_short, xray_long"

# Insert X-ray data into staging
if [[ -n "$XRAY_CSV" ]]; then
    printf "%s\n" "$XRAY_CSV" | clickhouse-client --query "INSERT INTO $STAGING_TABLE ($INSERT_COLS) FORMAT TabSeparated"
    printf "  Staged %s X-ray rows\n" "$XRAY_ROWS"
fi

# Insert Kp data into staging
if [[ -n "$KP_CSV" ]]; then
    printf "%s\n" "$KP_CSV" | clickhouse-client --query "INSERT INTO $STAGING_TABLE ($INSERT_COLS) FORMAT TabSeparated"
    printf "  Staged %s Kp rows\n" "$KP_ROWS"
fi

# Insert SFI data into staging
if [[ -n "$SFI_CSV" ]]; then
    printf "%s\n" "$SFI_CSV" | clickhouse-client --query "INSERT INTO $STAGING_TABLE ($INSERT_COLS) FORMAT TabSeparated"
    printf "  Staged %s SFI rows\n" "$SFI_ROWS"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 6. Merge streams and insert into solar.bronze
# ─────────────────────────────────────────────────────────────────────────────
# GROUP BY (date, time) + max() produces one complete row per key with all
# 3 streams combined. source_file is set to 'noaa_swpc_7day' since the merged
# row contains data from all 3 NOAA feeds.

printf "[%s] Merging staged data and inserting into solar.bronze...\n" "$(date '+%Y-%m-%d %H:%M:%S')"

MERGED_ROWS=$(clickhouse-client --query "SELECT count() FROM (SELECT date, time FROM $STAGING_TABLE GROUP BY date, time)")

clickhouse-client --query "
    INSERT INTO solar.bronze (date, time, observed_flux, adjusted_flux, ssn, kp_index, ap_index, source_file, xray_short, xray_long)
    SELECT
        date, time,
        max(observed_flux),
        max(adjusted_flux),
        max(ssn),
        max(kp_index),
        max(ap_index),
        'noaa_swpc_7day',
        max(xray_short),
        max(xray_long)
    FROM $STAGING_TABLE
    GROUP BY date, time
"

printf "  Inserted %s merged rows into solar.bronze\n" "$MERGED_ROWS"

# Clean up staging table
clickhouse-client --query "DROP TABLE IF EXISTS $STAGING_TABLE"

# ─────────────────────────────────────────────────────────────────────────────
# 7. Verify
# ─────────────────────────────────────────────────────────────────────────────
MIN_DATE=$(jq -r '.[0].time_tag[:10]' "$XRAY_FILE")
MAX_DATE=$(jq -r '.[-1].time_tag[:10]' "$XRAY_FILE")

printf "\n[%s] Verification (NOAA 7-day range: %s to %s):\n" "$(date '+%Y-%m-%d %H:%M:%S')" "$MIN_DATE" "$MAX_DATE"

clickhouse-client --query "
    SELECT
        date,
        count() AS rows,
        countIf(kp_index > 0) AS has_kp,
        countIf(xray_long > 0) AS has_xray,
        countIf(observed_flux > 0) AS has_sfi,
        max(kp_index) AS max_kp,
        max(xray_long) AS max_xray,
        max(observed_flux) AS max_sfi
    FROM solar.bronze FINAL
    WHERE source_file = 'noaa_swpc_7day'
      AND date >= '$MIN_DATE' AND date <= '$MAX_DATE'
    GROUP BY date
    ORDER BY date
    FORMAT PrettyCompact
"

printf "\n[%s] Solar history load complete.\n" "$(date '+%Y-%m-%d %H:%M:%S')"
