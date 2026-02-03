# ki7mt-ai-lab-apps

High-performance Go applications for KI7MT AI Lab WSPR/Solar data processing.

## Overview

This package provides command-line tools for ingesting and processing amateur radio propagation data from WSPR (Weak Signal Propagation Reporter) and NOAA solar indices. All ingestion tools use the ch-go native ClickHouse protocol with LZ4 compression for maximum throughput.

## Applications

### WSPR Ingestion Tools

| Command | Source Format | Throughput | Description |
|---------|---------------|------------|-------------|
| `wspr-turbo` | Compressed .gz | 24.67 Mrps | Zero-copy streaming from archives (champion) |
| `wspr-shredder` | Uncompressed CSV | 21.81 Mrps | Fastest for pre-extracted CSV files |
| `wspr-parquet-native` | Parquet | 17.02 Mrps | Native Go Parquet reader |
| `wspr-ingest` | CSV | - | Standard CSV ingestion |
| `wspr-ingest-cpu` | CSV | - | CPU-optimized CSV ingestion |
| `wspr-ingest-fast` | Compressed .gz | - | Fast streaming ingestion |
| `wspr-download` | - | - | Parallel archive downloader |

**Band Normalization (v2.1.0)**: All CSV ingesters normalize the band column from the frequency field using `internal/bands.GetBand()`, producing correct ADIF band IDs (102-111 for HF). This fixes the legacy band encoding bug where raw CSV band values (MHz frequencies) were stored directly.

### Solar Tools

| Command | Description |
|---------|-------------|
| `solar-ingest` | NOAA solar flux data ingestion into ClickHouse |

## Benchmarks (Threadripper 9975WX, 10.8B rows, 24 workers)

| Tool | Source Size | Time | Throughput |
|------|-------------|------|------------|
| **wspr-turbo** | 185 GB (.gz) | **7m18s** | **24.67 Mrps** |
| wspr-shredder | 878 GB (CSV) | 8m15s | 21.81 Mrps |
| wspr-parquet-native | 109 GB (Parquet) | 9m39s | 17.02 Mrps |

### End-to-End Pipeline Comparison

| Pipeline | Total Time | Disk Written |
|----------|------------|--------------|
| **wspr-turbo streaming** | **7m18s** | **0 GB** |
| pigz + wspr-shredder | 9m31s | 878 GB |

## Requirements

- Go 1.24+
- ClickHouse server
- ki7mt-ai-lab-core (database schemas)

## Building

```bash
# Build all binaries
make all

# Build WSPR tools only
make wspr

# Build Solar tools only
make solar

# Show help
make help
```

## Installation

```bash
# Install to /usr/bin
sudo make install

# Or stage for packaging
DESTDIR=/tmp/stage make install
```

## COPR Installation (Fedora/RHEL/Rocky)

```bash
# Enable COPR repository
sudo dnf copr enable ki7mt/ai-lab

# Install packages
sudo dnf install ki7mt-ai-lab-apps-wspr
sudo dnf install ki7mt-ai-lab-apps-solar
```

## Usage Examples

### wspr-turbo (Recommended for archived data)

Stream directly from compressed archives - no intermediate disk I/O:

```bash
# Ingest all .gz files from directory
wspr-turbo -source-dir /path/to/archives

# Specify ClickHouse connection
wspr-turbo -ch-host 192.168.1.100:9000 -ch-db wspr -ch-table spots \
  -source-dir /path/to/archives
```

### wspr-shredder (Fastest for pre-extracted CSV)

```bash
# Ingest uncompressed CSV files
wspr-shredder -source-dir /path/to/csv

# With custom workers
wspr-shredder -workers 16 -source-dir /path/to/csv
```

### wspr-parquet-native (For Parquet sources)

```bash
# Ingest Parquet files
wspr-parquet-native -source-dir /path/to/parquet
```

## Architecture

### wspr-turbo Pipeline

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  .csv.gz file   │────▶│  Stream Decomp   │────▶│  Vectorized     │
│  (on disk)      │     │  (klauspost/gzip)│     │  CSV Parser     │
└─────────────────┘     └──────────────────┘     └────────┬────────┘
                                                          │
                                                          ▼
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  ClickHouse     │◀────│  ch-go Native    │◀────│  Double Buffer  │
│                 │     │  Blocks + LZ4    │     │  Fill A/Send B  │
└─────────────────┘     └──────────────────┘     └─────────────────┘
```

**Key Features:**
- **Stream Decompress**: klauspost/gzip (ASM-optimized)
- **Vectorized Parse**: Columnar buffers, no row structs
- **Double-Buffer**: Fill block A while sending block B
- **Zero-Alloc**: sync.Pool for 1M-row ColumnBlocks
- **Native Protocol**: Binary wire format with LZ4

## Performance Tuning

- **Workers**: 16 optimal for 64 GB RAM (each worker uses ~4 GB)
- **Block Size**: 1M rows per native block
- **I/O Separation**: Best performance with source on separate device from ClickHouse
- **Memory**: sync.Pool holds ~56 GB of buffers at 16 workers

## License

GPL-3.0-or-later

## Author

Greg Beam, KI7MT - ki7mt@outlook.com
