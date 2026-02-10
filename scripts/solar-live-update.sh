#!/bin/bash
# =============================================================================
# Name............: solar-live-update
# Version.........: 1.0.0
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

# Cycle 25 high-mean fallback (Feb 2026) - used if SFI is 0 or missing
SFI_FALLBACK=145

# Default values
KP_INDEX=0
AP_INDEX=0
SOLAR_FLUX=0
XRAY_SHORT=0
XRAY_LONG=0
CONDITIONS="Unknown"

# ── Parse SFI (from solar-download: summary/10cm-flux.json) ─────────────────
if [[ -f "$SFI_FILE" ]]; then
    # Format: {"Flux":"174","TimeStamp":"2026-02-02 20:00:00"}
    SOLAR_FLUX=$(jq -r '.Flux // 0' "$SFI_FILE" 2>/dev/null || echo "0")
fi
# Validate and apply fallback if zero or missing
if ! [[ "$SOLAR_FLUX" =~ ^[0-9.]+$ ]] || [[ "$SOLAR_FLUX" == "0" ]]; then
    printf "  WARNING: SFI data missing or zero. Using Cycle 25 fallback (%s).\n" "$SFI_FALLBACK"
    SOLAR_FLUX=$SFI_FALLBACK
fi

# ── Parse Kp (from solar-download: noaa-planetary-k-index.json) ─────────────
if [[ -f "$KP_FILE" ]]; then
    # Format: array of arrays, header + data rows: ["time_tag","Kp","a_running","station_count"]
    KP_INDEX=$(jq -r '.[-1][1] // 0' "$KP_FILE" 2>/dev/null || echo "0")
    if ! [[ "$KP_INDEX" =~ ^[0-9.]+$ ]]; then
        KP_INDEX=0
    fi
fi

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

clickhouse-client --query "
    CREATE TABLE IF NOT EXISTS wspr.live_conditions (
        kp_index Float32, ap_index Float32, solar_flux Float32,
        xray_short Float64, xray_long Float64, conditions String
    ) ENGINE = Memory;
    TRUNCATE TABLE wspr.live_conditions;
    INSERT INTO wspr.live_conditions (kp_index, ap_index, solar_flux, xray_short, xray_long, conditions)
    VALUES ($KP_INDEX, $AP_INDEX, $SOLAR_FLUX, $XRAY_SHORT, $XRAY_LONG, '$CONDITIONS');
"

# Also write to JSON file for direct HTTP access
JSON_OUTPUT="$SOLAR_DATA_DIR/live_conditions.json"
cat > "$JSON_OUTPUT" <<ENDJSON
{
    "timestamp": "$(date -u '+%Y-%m-%dT%H:%M:%SZ')",
    "kp_index": $KP_INDEX,
    "ap_index": $AP_INDEX,
    "solar_flux": $SOLAR_FLUX,
    "xray_short": $XRAY_SHORT,
    "xray_long": $XRAY_LONG,
    "conditions": "$CONDITIONS"
}
ENDJSON

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Live conditions updated successfully"
