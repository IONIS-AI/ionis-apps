#!/bin/bash
#------------------------------------------------------------------------------
# WSPR Fast Parallel Decompression Script
#
# Purpose: Decompress all *.csv.gz files in parallel to saturate I/O
#          Uses pigz (parallel gzip) for maximum throughput
#
# Author:  KI7MT AI Lab
# Version: 2.3.2
#
# Dependencies: pigz (preferred) or gzip + GNU parallel
#------------------------------------------------------------------------------

set -euo pipefail

readonly SCRIPT_VERSION="2.3.2"
readonly DEFAULT_DIR="/mnt/ai-stack/wspr-data/raw"

# Color codes
readonly GREEN='\033[0;32m'
readonly BLUE='\033[0;34m'
readonly YELLOW='\033[1;33m'
readonly NC='\033[0m'

log_info() { printf "${BLUE}[INFO]${NC} %s\n" "$1"; }
log_success() { printf "${GREEN}[OK]${NC} %s\n" "$1"; }
log_warn() { printf "${YELLOW}[WARN]${NC} %s\n" "$1"; }

show_help() {
    cat << EOF
wspr-decompress.sh v${SCRIPT_VERSION} - Fast Parallel Decompression

Usage: $0 [OPTIONS] [directory]

Options:
  -j, --jobs N    Number of parallel jobs (default: nproc)
  -k, --keep      Keep original .gz files after decompression
  -d, --delete    Delete .csv files (re-compress)
  -h, --help      Show this help

Examples:
  $0                           # Decompress all in default directory
  $0 /path/to/data             # Decompress all in specified directory
  $0 -j 8 /path/to/data        # Use 8 parallel jobs
  $0 -k /path/to/data          # Keep .gz files after decompressing

EOF
}

decompress_with_pigz() {
    local dir="$1"
    local jobs="$2"
    local keep="$3"

    local count
    count=$(find "$dir" -maxdepth 1 -name "*.csv.gz" | wc -l)

    if [[ "$count" -eq 0 ]]; then
        log_warn "No .csv.gz files found in $dir"
        return 0
    fi

    log_info "Decompressing $count files with pigz ($jobs threads)..."

    local start_time
    start_time=$(date +%s.%N)

    local keep_flag=""
    [[ "$keep" == "true" ]] && keep_flag="-k"

    # Use pigz with maximum threads per file
    find "$dir" -maxdepth 1 -name "*.csv.gz" -print0 | \
        xargs -0 -P "$jobs" -I {} pigz -d $keep_flag -f {}

    local end_time
    end_time=$(date +%s.%N)
    local elapsed
    elapsed=$(echo "$end_time - $start_time" | bc)

    log_success "Decompressed $count files in ${elapsed}s"
}

decompress_with_parallel() {
    local dir="$1"
    local jobs="$2"
    local keep="$3"

    local count
    count=$(find "$dir" -maxdepth 1 -name "*.csv.gz" | wc -l)

    if [[ "$count" -eq 0 ]]; then
        log_warn "No .csv.gz files found in $dir"
        return 0
    fi

    log_info "Decompressing $count files with GNU parallel ($jobs jobs)..."

    local start_time
    start_time=$(date +%s.%N)

    local keep_flag=""
    [[ "$keep" == "true" ]] && keep_flag="-k"

    find "$dir" -maxdepth 1 -name "*.csv.gz" | \
        parallel -j "$jobs" "gzip -d $keep_flag -f {}"

    local end_time
    end_time=$(date +%s.%N)
    local elapsed
    elapsed=$(echo "$end_time - $start_time" | bc)

    log_success "Decompressed $count files in ${elapsed}s"
}

compress_files() {
    local dir="$1"
    local jobs="$2"

    local count
    count=$(find "$dir" -maxdepth 1 -name "*.csv" ! -name "*.csv.gz" | wc -l)

    if [[ "$count" -eq 0 ]]; then
        log_warn "No .csv files found to compress in $dir"
        return 0
    fi

    log_info "Compressing $count files with pigz ($jobs threads)..."

    local start_time
    start_time=$(date +%s.%N)

    find "$dir" -maxdepth 1 -name "*.csv" ! -name "*.csv.gz" -print0 | \
        xargs -0 -P "$jobs" -I {} pigz -f {}

    local end_time
    end_time=$(date +%s.%N)
    local elapsed
    elapsed=$(echo "$end_time - $start_time" | bc)

    log_success "Compressed $count files in ${elapsed}s"
}

main() {
    local jobs
    jobs=$(nproc)
    local keep="false"
    local delete="false"
    local dir="$DEFAULT_DIR"

    while [[ $# -gt 0 ]]; do
        case "$1" in
            -j|--jobs)
                jobs="$2"
                shift 2
                ;;
            -k|--keep)
                keep="true"
                shift
                ;;
            -d|--delete)
                delete="true"
                shift
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                dir="$1"
                shift
                ;;
        esac
    done

    if [[ ! -d "$dir" ]]; then
        echo "Error: Directory not found: $dir" >&2
        exit 1
    fi

    echo "==========================================================="
    printf "WSPR Fast Parallel Decompression v%s\n" "$SCRIPT_VERSION"
    echo "==========================================================="
    log_info "Directory: $dir"
    log_info "Jobs: $jobs"
    log_info "Keep .gz: $keep"
    echo

    if [[ "$delete" == "true" ]]; then
        compress_files "$dir" "$jobs"
    elif command -v pigz &> /dev/null; then
        decompress_with_pigz "$dir" "$jobs" "$keep"
    elif command -v parallel &> /dev/null; then
        decompress_with_parallel "$dir" "$jobs" "$keep"
    else
        log_warn "Neither pigz nor GNU parallel found. Using single-threaded gzip."
        find "$dir" -maxdepth 1 -name "*.csv.gz" -exec gzip -d -f {} \;
    fi

    # Show result
    echo
    log_info "CSV files in $dir:"
    ls -lh "$dir"/*.csv 2>/dev/null | head -10 || log_warn "No .csv files found"
    local csv_count
    csv_count=$(find "$dir" -maxdepth 1 -name "*.csv" | wc -l)
    log_info "Total: $csv_count CSV files"
}

main "$@"
