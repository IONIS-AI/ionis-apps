#!/bin/bash
#------------------------------------------------------------------------------
# WSPR CSV to Parquet Converter
#
# Purpose: Convert WSPR CSV files to Parquet format using DuckDB
#          Handles malformed rows gracefully with ignore_errors
#
# Author:  KI7MT AI Lab
# Version: 2.3.2
#
# Dependencies: duckdb
#------------------------------------------------------------------------------

set -euo pipefail

readonly SCRIPT_VERSION="2.3.2"
DEFAULT_CSV_SRC="/scratch/ai-stack/wspr-data/csv"
DEFAULT_PARQUET_DEST="/scratch/ai-stack/wspr-data/parquet"
DEFAULT_THREADS=32
DEFAULT_PARALLEL_JOBS=4

# Color codes
readonly GREEN='\033[0;32m'
readonly BLUE='\033[0;34m'
readonly YELLOW='\033[1;33m'
readonly RED='\033[0;31m'
readonly NC='\033[0m'

log_info() { printf "${BLUE}[INFO]${NC} %s\n" "$1"; }
log_success() { printf "${GREEN}[OK]${NC} %s\n" "$1"; }
log_warn() { printf "${YELLOW}[WARN]${NC} %s\n" "$1"; }
log_error() { printf "${RED}[ERROR]${NC} %s\n" "$1" >&2; }

show_help() {
    cat << EOF
wspr-csv-to-parquet.sh v${SCRIPT_VERSION} - Convert WSPR CSV to Parquet

Usage: $0 [OPTIONS]

Options:
  -s, --source DIR      Source CSV directory (default: $DEFAULT_CSV_SRC)
  -d, --dest DIR        Destination Parquet directory (default: $DEFAULT_PARQUET_DEST)
  -j, --jobs N          Parallel conversion jobs (default: $DEFAULT_PARALLEL_JOBS)
  -t, --threads N       DuckDB threads per job (default: $DEFAULT_THREADS)
  -f, --file FILE       Convert single file only
  -h, --help            Show this help

Environment Variables:
  CSV_SRC               Source directory
  PARQUET_DEST          Destination directory
  THREADS               DuckDB threads
  PARALLEL_JOBS         Parallel jobs

Examples:
  $0                                    # Convert all CSVs
  $0 -f wsprspots-2025-01.csv          # Convert single file
  $0 -j 8 -t 16                        # 8 parallel jobs, 16 threads each

EOF
}

convert_file() {
    local csv_file="$1"
    local base_name
    base_name=$(basename "$csv_file" .csv)
    local parquet_file="${PARQUET_DEST}/${base_name}.parquet"

    # Skip if already converted
    if [[ -f "$parquet_file" ]]; then
        log_info "Skipping ${base_name} (exists)"
        return 0
    fi

    local start_time
    start_time=$(date +%s.%N)

    # DuckDB conversion with ignore_errors for malformed rows
    duckdb -c "
SET threads = ${THREADS};
SET preserve_insertion_order = false;

COPY (
    SELECT
        column0::UBIGINT AS id,
        column1::BIGINT AS timestamp,
        column2::VARCHAR AS reporter,
        column3::VARCHAR AS reporter_grid,
        column4::SMALLINT AS snr,
        column5::DOUBLE AS frequency,
        column6::VARCHAR AS callsign,
        column7::VARCHAR AS grid,
        column8::SMALLINT AS power,
        column9::SMALLINT AS drift,
        column10::UINTEGER AS distance,
        column11::USMALLINT AS azimuth,
        column12::INTEGER AS band,
        column13::VARCHAR AS version,
        column14::UTINYINT AS code
    FROM read_csv(
        '${csv_file}',
        header = false,
        auto_detect = false,
        delim = ',',
        quote = '\"',
        escape = '\"',
        columns = {
            'column0': 'UBIGINT',
            'column1': 'BIGINT',
            'column2': 'VARCHAR',
            'column3': 'VARCHAR',
            'column4': 'SMALLINT',
            'column5': 'DOUBLE',
            'column6': 'VARCHAR',
            'column7': 'VARCHAR',
            'column8': 'SMALLINT',
            'column9': 'SMALLINT',
            'column10': 'UINTEGER',
            'column11': 'USMALLINT',
            'column12': 'INTEGER',
            'column13': 'VARCHAR',
            'column14': 'UTINYINT'
        },
        ignore_errors = true
    )
) TO '${parquet_file}'
(FORMAT PARQUET, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 1000000);
" 2>/dev/null

    local end_time
    end_time=$(date +%s.%N)
    local elapsed
    elapsed=$(echo "$end_time - $start_time" | bc)

    # Get file sizes
    local csv_size parquet_size ratio
    csv_size=$(stat -c %s "$csv_file" 2>/dev/null || echo 0)
    parquet_size=$(stat -c %s "$parquet_file" 2>/dev/null || echo 0)

    if [[ "$csv_size" -gt 0 && "$parquet_size" -gt 0 ]]; then
        ratio=$(echo "scale=2; $csv_size / $parquet_size" | bc)
        csv_mb=$(echo "scale=1; $csv_size / 1048576" | bc)
        parquet_mb=$(echo "scale=1; $parquet_size / 1048576" | bc)
        log_success "${base_name}: ${csv_mb}MB -> ${parquet_mb}MB (${ratio}x) in ${elapsed}s"
    else
        log_warn "${base_name}: conversion may have failed"
    fi
}

export -f convert_file log_info log_success log_warn log_error
export GREEN BLUE YELLOW RED NC

main() {
    local source_dir="$DEFAULT_CSV_SRC"
    local dest_dir="$DEFAULT_PARQUET_DEST"
    local jobs="$DEFAULT_PARALLEL_JOBS"
    local threads="$DEFAULT_THREADS"
    local single_file=""

    while [[ $# -gt 0 ]]; do
        case "$1" in
            -s|--source)
                source_dir="$2"
                shift 2
                ;;
            -d|--dest)
                dest_dir="$2"
                shift 2
                ;;
            -j|--jobs)
                jobs="$2"
                shift 2
                ;;
            -t|--threads)
                threads="$2"
                shift 2
                ;;
            -f|--file)
                single_file="$2"
                shift 2
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done

    # Export for parallel subprocesses
    export PARQUET_DEST="$dest_dir"
    export THREADS="$threads"

    echo "==========================================================="
    printf "WSPR CSV to Parquet Converter v%s\n" "$SCRIPT_VERSION"
    echo "==========================================================="
    log_info "Source:      $source_dir"
    log_info "Destination: $dest_dir"
    log_info "Jobs:        $jobs parallel"
    log_info "Threads:     $threads per job"
    echo

    # Create destination directory
    mkdir -p "$dest_dir"

    # Check for duckdb
    if ! command -v duckdb &> /dev/null; then
        log_error "duckdb not found. Install with: pip install duckdb-cli"
        exit 1
    fi

    local start_time
    start_time=$(date +%s)

    if [[ -n "$single_file" ]]; then
        # Single file mode
        local full_path="${source_dir}/${single_file}"
        if [[ ! -f "$full_path" ]]; then
            log_error "File not found: $full_path"
            exit 1
        fi
        convert_file "$full_path"
    else
        # Batch mode - convert all CSVs
        local csv_count
        csv_count=$(find "$source_dir" -maxdepth 1 -name "*.csv" | wc -l)

        if [[ "$csv_count" -eq 0 ]]; then
            log_error "No CSV files found in $source_dir"
            exit 1
        fi

        log_info "Found $csv_count CSV files to convert"
        echo

        # Process files in parallel using xargs
        find "$source_dir" -maxdepth 1 -name "*.csv" -print0 | sort -z | \
            xargs -0 -P "$jobs" -I {} bash -c 'convert_file "$@"' _ {}
    fi

    local end_time
    end_time=$(date +%s)
    local total_elapsed=$((end_time - start_time))

    echo
    echo "==========================================================="
    log_info "Conversion complete!"
    log_info "Elapsed: ${total_elapsed}s"

    # Show totals
    local csv_total parquet_total
    csv_total=$(find "$source_dir" -maxdepth 1 -name "*.csv" -exec stat -c %s {} \; | awk '{s+=$1} END {print s}')
    parquet_total=$(find "$dest_dir" -maxdepth 1 -name "*.parquet" -exec stat -c %s {} \; 2>/dev/null | awk '{s+=$1} END {print s}')

    if [[ -n "$csv_total" && -n "$parquet_total" && "$parquet_total" -gt 0 ]]; then
        local csv_gb parquet_gb ratio
        csv_gb=$(echo "scale=2; $csv_total / 1073741824" | bc)
        parquet_gb=$(echo "scale=2; $parquet_total / 1073741824" | bc)
        ratio=$(echo "scale=2; $csv_total / $parquet_total" | bc)
        log_info "CSV Total:     ${csv_gb} GB"
        log_info "Parquet Total: ${parquet_gb} GB"
        log_info "Compression:   ${ratio}x"
    fi
    echo "==========================================================="
}

main "$@"
