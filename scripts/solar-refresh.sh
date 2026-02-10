#!/bin/bash
# =============================================================================
# Name............: solar-refresh
# Version.........: 2.3.2
# Description.....: Download and ingest solar/geomagnetic data
# Usage...........: solar-refresh [OPTIONS]
#
# Data Sources:
#   - SIDC daily/monthly sunspot numbers (SSN)
#   - NOAA solar flux indices (SFI)
#   - NOAA planetary K-index (Kp/Ap) - 3-hourly geomagnetic
#   - GOES X-ray flux - radio blackout indicator
#
# =============================================================================
set -e

VERSION="2.3.2"
SOLAR_DATA_DIR="${SOLAR_DATA_DIR:-/mnt/ai-stack/solar-data/raw}"

usage() {
    printf "solar-refresh v%s - Solar Data Refresh Utility\n\n" "$VERSION"
    printf "Usage: %s [OPTIONS]\n\n" "$(basename "$0")"
    printf "Downloads fresh solar/geomagnetic data and performs a clean reload\n"
    printf "into ClickHouse (truncate + ingest).\n\n"
    printf "Options:\n"
    printf "  -d, --dest DIR     Destination directory (default: %s)\n" "$SOLAR_DATA_DIR"
    printf "  -n, --no-truncate  Append instead of truncate + reload\n"
    printf "  -q, --quiet        Suppress output\n"
    printf "  -h, --help         Show this help message\n\n"
    printf "Environment:\n"
    printf "  SOLAR_DATA_DIR     Override default data directory\n\n"
    printf "Examples:\n"
    printf "  %s                 # Full refresh (truncate + reload)\n" "$(basename "$0")"
    printf "  %s -n              # Append new data only\n" "$(basename "$0")"
    printf "  %s -d /tmp/solar   # Use alternate directory\n" "$(basename "$0")"
}

# Defaults
TRUNCATE=true
QUIET=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--dest)
            SOLAR_DATA_DIR="$2"
            shift 2
            ;;
        -n|--no-truncate)
            TRUNCATE=false
            shift
            ;;
        -q|--quiet)
            QUIET=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            printf "Unknown option: %s\n" "$1"
            usage
            exit 1
            ;;
    esac
done

# Build ingest flags
INGEST_FLAGS="-source-dir $SOLAR_DATA_DIR"
if $TRUNCATE; then
    INGEST_FLAGS="$INGEST_FLAGS -truncate"
fi

# Run pipeline
if $QUIET; then
    solar-download -dest "$SOLAR_DATA_DIR" > /dev/null 2>&1
    solar-ingest $INGEST_FLAGS > /dev/null 2>&1
else
    printf "=================================================================\n"
    printf "Solar Refresh v%s\n" "$VERSION"
    printf "=================================================================\n"
    printf "Data Directory: %s\n" "$SOLAR_DATA_DIR"
    printf "Mode:           %s\n" "$($TRUNCATE && echo 'Truncate + Reload' || echo 'Append')"
    printf "=================================================================\n\n"

    solar-download -dest "$SOLAR_DATA_DIR"
    printf "\n"
    solar-ingest $INGEST_FLAGS

    printf "\n=================================================================\n"
    printf "Solar Refresh Complete\n"
    printf "=================================================================\n"
fi
