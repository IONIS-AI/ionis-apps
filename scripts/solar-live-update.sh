#!/bin/bash
# =============================================================================
# Name............: solar-live-update
# Version.........: 2.3.3
# Description.....: Update live_conditions table for Now-Casting
# Usage...........: solar-live-update [--refresh]
#
# This script:
#   1. Optionally runs solar-refresh to get fresh data
#   2. Extracts latest Kp, SFI, X-ray from downloaded JSON files
#   3. Updates wspr.live_conditions table (Memory engine)
#
# For cron: Run every 15 minutes
#   */15 * * * * /path/to/solar-live-update.sh >> /var/log/solar-live.log 2>&1
#
# =============================================================================
set -e

SOLAR_DATA_DIR="${SOLAR_DATA_DIR:-/mnt/ai-stack/solar-data/raw}"
DO_REFRESH=false

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        --refresh|-r)
            DO_REFRESH=true
            shift
            ;;
        --help|-h)
            echo "Usage: $(basename "$0") [--refresh]"
            echo "  --refresh    Run solar-download first"
            exit 0
            ;;
        *)
            shift
            ;;
    esac
done

# Optionally refresh data first
if $DO_REFRESH; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Downloading fresh solar data..."
    solar-download -dest "$SOLAR_DATA_DIR" > /dev/null 2>&1 || true
fi

# Local files (populated by solar-download)
KP_FILE="$SOLAR_DATA_DIR/noaa_kp_index.json"
XRAY_FILE="$SOLAR_DATA_DIR/goes_xray_flux.json"
SFI_FILE="$SOLAR_DATA_DIR/noaa_solar_flux.json"

# Default values
KP_INDEX=0
AP_INDEX=0
SOLAR_FLUX=0
XRAY_SHORT=0
XRAY_LONG=0
CONDITIONS="Unknown"

# NO FALLBACK CONSTANT. There was one — SFI_FALLBACK=145, "Cycle 25 high-mean" — and it is
# the reason this bug survived for months. When the parse below started returning nothing,
# the script substituted 145, logged a WARNING nobody reads, wrote the constant to
# ClickHouse, printed "updated successfully" and exited 0. Every 15 minutes. ham-stats
# published 145 as a measurement for as long as that went on.
#
# A plausible-looking number is the most expensive possible failure mode: it cannot be
# distinguished from data. Missing input now fails the run, loudly, and leaves the previous
# row in place rather than overwriting real data with a guess.

fail() {
    printf "ERROR: %s\n" "$1" >&2
    printf "ERROR: refusing to write live_conditions — the existing row is left alone.\n" >&2
    exit 1
}

# ── Parse SFI (from solar-download: summary/10cm-flux.json) ─────────────────
# NOAA SWPC switched this endpoint to array-of-objects (~Apr 2026), the same migration
# already handled in solar-history-load.sh:
#     was:  {"Flux":"174","TimeStamp":"2026-02-02 20:00:00"}
#     now:  [{"flux":110,"time_tag":"2026-09-08T20:00:00"}]
# `jq '.Flux'` against an array does not return null, it ERRORS — so `// 0` never fired and
# the 2>/dev/null on the old line swallowed the only evidence. history-load was fixed for
# this and the live path was not, which is why solar.bronze stayed correct while the front
# page did not.
[[ -f "$SFI_FILE" ]] || fail "SFI file missing: $SFI_FILE"
SOLAR_FLUX=$(jq -er '.[-1].flux' "$SFI_FILE" 2>/dev/null) \
    || fail "could not read .[-1].flux from $SFI_FILE (NOAA JSON shape changed again?)"
SFI_TIME=$(jq -er '.[-1].time_tag' "$SFI_FILE" 2>/dev/null) \
    || fail "could not read .[-1].time_tag from $SFI_FILE"

# ── Parse Kp and Ap (from solar-download: noaa-planetary-k-index.json) ──────
#     was:  [["time_tag","Kp",...], ["2026-02-02T00:00:00","3.67",...]]   (header + rows)
#     now:  [{"time_tag":"2026-09-09T09:00:00","Kp":2.67,"a_running":12,"station_count":8}]
# `.[-1][1]` on an object errors with "Cannot index object with number", so KP_INDEX fell to
# 0 on every run — and Kp=0 is a legal value, so it read as "perfectly quiet" rather than as
# a failure. ap_index was never parsed at all and was written as a hardcoded 0; a_running is
# right there in the same record, and is what solar-history-load.sh already uses for ap.
[[ -f "$KP_FILE" ]] || fail "Kp file missing: $KP_FILE"
KP_INDEX=$(jq -er '.[-1].Kp' "$KP_FILE" 2>/dev/null) \
    || fail "could not read .[-1].Kp from $KP_FILE (NOAA JSON shape changed again?)"
AP_INDEX=$(jq -er '.[-1].a_running' "$KP_FILE" 2>/dev/null) \
    || fail "could not read .[-1].a_running from $KP_FILE"
KP_TIME=$(jq -er '.[-1].time_tag' "$KP_FILE" 2>/dev/null) \
    || fail "could not read .[-1].time_tag from $KP_FILE"

# ── Range checks ───────────────────────────────────────────────────────────
# A parse that succeeds can still be wrong. Kp is bounded 0-9 by definition and F10.7 has
# never been observed outside roughly 60-400 sfu, so a value outside those is a shape change
# that happens to parse — the exact failure being fixed here, one layer up.
[[ "$KP_INDEX"   =~ ^[0-9]+(\.[0-9]+)?$ ]] || fail "Kp is not a number: '$KP_INDEX'"
[[ "$AP_INDEX"   =~ ^[0-9]+(\.[0-9]+)?$ ]] || fail "Ap is not a number: '$AP_INDEX'"
[[ "$SOLAR_FLUX" =~ ^[0-9]+(\.[0-9]+)?$ ]] || fail "SFI is not a number: '$SOLAR_FLUX'"
(( $(echo "$KP_INDEX >= 0 && $KP_INDEX <= 9" | bc -l) )) \
    || fail "Kp out of range 0-9: $KP_INDEX"
(( $(echo "$SOLAR_FLUX >= 60 && $SOLAR_FLUX <= 400" | bc -l) )) \
    || fail "SFI out of plausible range 60-400 sfu: $SOLAR_FLUX"

# ── Freshness ──────────────────────────────────────────────────────────────
# Genuine-but-old is NOT the same as missing, and is not a reason to refuse. NOAA publishes
# F10.7 a few times a day and Kp every three hours, so some age is normal. We publish the
# real value with its real observation time and warn; downstream can now see the age instead
# of having to trust that a number is current. Fiction is what we refuse, not staleness.
now_epoch=$(date -u +%s)
warn_if_stale() {   # $1=label  $2=ISO time_tag  $3=max age hours
    local age_h
    age_h=$(( (now_epoch - $(date -u -d "$2" +%s)) / 3600 ))
    if (( age_h > $3 )); then
        printf "  WARNING: %s observation is %sh old (%s), max expected %sh.\n" \
            "$1" "$age_h" "$2" "$3"
    fi
}
warn_if_stale SFI "$SFI_TIME" 48
warn_if_stale Kp  "$KP_TIME"  12

# ── Parse X-ray (from solar-download: goes xrays-6-hour.json) ───────────────
if [[ -f "$XRAY_FILE" ]]; then
    # Format: array of objects with energy and flux fields, 2 bands per timestamp
    XRAY_LONG=$(jq -r '[.[] | select(.energy == "0.1-0.8nm")] | .[-1].flux // 0' "$XRAY_FILE" 2>/dev/null || echo "0")
    XRAY_SHORT=$(jq -r '[.[] | select(.energy == "0.05-0.4nm")] | .[-1].flux // 0' "$XRAY_FILE" 2>/dev/null || echo "0")
    if ! [[ "$XRAY_LONG" =~ ^[0-9.eE+-]+$ ]]; then
        XRAY_LONG=0
    fi
    if ! [[ "$XRAY_SHORT" =~ ^[0-9.eE+-]+$ ]]; then
        XRAY_SHORT=0
    fi
fi

# Determine conditions based on Kp
if (( $(echo "$KP_INDEX < 3" | bc -l) )); then
    CONDITIONS="Quiet"
elif (( $(echo "$KP_INDEX < 5" | bc -l) )); then
    CONDITIONS="Unsettled"
elif (( $(echo "$KP_INDEX < 7" | bc -l) )); then
    CONDITIONS="Storm"
else
    CONDITIONS="Severe Storm"
fi

# Check X-ray for blackout
if (( $(echo "$XRAY_LONG > 0.00001" | bc -l 2>/dev/null) )); then
    CONDITIONS="$CONDITIONS + Radio Blackout"
fi

# Update ClickHouse live_conditions table (Memory engine — recreate if lost after restart)
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Updating live_conditions: Kp=$KP_INDEX, SFI=$SOLAR_FLUX, X-ray=$XRAY_LONG, $CONDITIONS"

# The table carried NO timestamp of any kind, which is the second half of why this went
# unnoticed: a consumer reading solar_flux=145 had no way to ask how old it was, and neither
# did anyone looking at the table directly. sfi_observed_at / kp_observed_at are NOAA's own
# time_tags for the samples used; updated_at is when this run wrote the row. Staleness is now
# a readable fact rather than something you have to already suspect.
#
# DROP rather than CREATE IF NOT EXISTS: the old three-column table is still resident in
# memory on any host that has not restarted ClickHouse, and IF NOT EXISTS would silently keep
# it — the INSERT would then fail on unknown columns every 15 minutes. Memory engine holds
# exactly one row that this script rewrites anyway, so there is nothing to preserve.
clickhouse-client --query "
    DROP TABLE IF EXISTS wspr.live_conditions;
    CREATE TABLE wspr.live_conditions (
        kp_index Float32, ap_index Float32, solar_flux Float32,
        xray_short Float64, xray_long Float64, conditions String,
        sfi_observed_at DateTime, kp_observed_at DateTime, updated_at DateTime
    ) ENGINE = Memory;
    INSERT INTO wspr.live_conditions
        (kp_index, ap_index, solar_flux, xray_short, xray_long, conditions,
         sfi_observed_at, kp_observed_at, updated_at)
    VALUES ($KP_INDEX, $AP_INDEX, $SOLAR_FLUX, $XRAY_SHORT, $XRAY_LONG, '$CONDITIONS',
            '${SFI_TIME/T/ }', '${KP_TIME/T/ }', now());
"

# Also write to JSON file for direct HTTP access
JSON_OUTPUT="$SOLAR_DATA_DIR/live_conditions.json"
cat > "$JSON_OUTPUT" <<ENDJSON
{
    "timestamp": "$(date -u '+%Y-%m-%dT%H:%M:%SZ')",
    "sfi_observed_at": "$SFI_TIME",
    "kp_observed_at": "$KP_TIME",
    "kp_index": $KP_INDEX,
    "ap_index": $AP_INDEX,
    "solar_flux": $SOLAR_FLUX,
    "xray_short": $XRAY_SHORT,
    "xray_long": $XRAY_LONG,
    "conditions": "$CONDITIONS"
}
ENDJSON

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Live conditions updated successfully"
