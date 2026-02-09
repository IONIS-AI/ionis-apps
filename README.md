# ki7mt-ai-lab-apps

High-performance Go data ingesters for the KI7MT Sovereign AI Lab.

Part of the [IONIS](https://github.com/KI7MT/ki7mt-ai-lab-docs) (Ionospheric Neural Inference System) project.

## Overview

Command-line tools for ingesting and processing amateur radio propagation data from multiple sources: WSPR, Reverse Beacon Network, contest logs (CQ/ARRL), and NOAA solar indices. All ingestion tools use the ch-go native ClickHouse protocol with LZ4 compression for maximum throughput.

**Current version:** 2.3.1

## Applications

### WSPR Tools

| Command | Source Format | Throughput | Description |
|---------|--------------|------------|-------------|
| `wspr-turbo` | Compressed .gz | 22.55 Mrps | Zero-copy streaming from archives |
| `wspr-shredder` | Uncompressed CSV | 21.81 Mrps | Fastest for pre-extracted CSV |
| `wspr-parquet-native` | Parquet | 17.02 Mrps | Native Go Parquet reader |
| `wspr-ingest` | CSV | — | Standard CSV ingestion |
| `wspr-ingest-cpu` | CSV | — | CPU-optimized CSV ingestion |
| `wspr-ingest-fast` | Compressed .gz | — | Fast streaming ingestion |
| `wspr-download` | — | — | Parallel archive downloader |

### Contest Tools

| Command | Description |
|---------|-------------|
| `contest-download` | Downloads public Cabrillo logs (CQ WW, WPX, ARRL DX, and 12 other contests) |
| `contest-ingest` | Parses and ingests Cabrillo v1/v2/v3 into ClickHouse (258K rps) |

### RBN Tools

| Command | Description |
|---------|-------------|
| `rbn-download` | Downloads daily RBN ZIP archives (2009–present) |
| `rbn-ingest` | Ingests RBN CSV into ClickHouse (10.32 Mrps) |

### Solar Tools

| Command | Description |
|---------|-------------|
| `solar-ingest` | NOAA solar flux data ingestion |
| `solar-backfill` | GFZ Potsdam historical SSN/SFI/Kp (2000–present) |
| `solar-download` | NOAA/SIDC solar data downloader |

### Utility Tools

| Command | Description |
|---------|-------------|
| `db-validate` | ClickHouse schema and data validation |

## Data Pipeline Summary

**Total dataset: 13.18B+ observations** — the largest curated amateur radio propagation dataset in existence.

| Source | Volume | Tool | Throughput |
|--------|--------|------|------------|
| WSPR | 10.8B spots | `wspr-turbo` | 22.55 Mrps, 7m27s |
| RBN | 2.18B spots | `rbn-ingest` | 10.32 Mrps, 3m32s |
| Contest Logs | 195M QSOs | `contest-ingest` | 258K rps, 12m37s |
| Solar Indices | 76K rows | `solar-backfill` | 2.88M rps |

### Data Quality

WSPR balloon/telemetry contamination (2.56% of spots) is identified and filtered at the ClickHouse layer via `wspr.balloon_callsigns` (see [ki7mt-ai-lab-core](https://github.com/KI7MT/ki7mt-ai-lab-core)). Pico balloon trackers (Type 2 telemetry) encode GPS coordinates as synthetic callsigns — these are flagged by velocity analysis (>=45 grids/day), Rosetta Stone exclusion, and reserved prefix detection. V14+ model training uses `wspr.signatures_v2_terrestrial` which excludes all flagged callsigns.

## Benchmarks (Threadripper 9975WX, 16 workers)

| Tool | Source Size | Time | Throughput |
|------|-----------|------|------------|
| `wspr-turbo` | 185 GB (.gz) | ~8 min | 22.55 Mrps |
| `wspr-shredder` | 878 GB (CSV) | 8m15s | 21.81 Mrps |
| `wspr-parquet-native` | 109 GB (Parquet) | 9m39s | 17.02 Mrps |
| `rbn-ingest` | 21 GB (6,183 ZIPs) | 3m32s | 10.32 Mrps |
| `contest-ingest` | 3 GB (407K files) | 12m37s | 258K rps |

## Band Normalization (v2.1.0+)

All CSV ingesters normalize the band column from the frequency field using `internal/bands.GetBand()`, producing correct ADIF band IDs (102–111 for HF). This fixed the legacy band encoding bug where raw CSV frequency values were stored as band IDs.

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

### Contest Parser

Handles Cabrillo v1, v2, and v3 formats with known edge cases:
- NBSP (0xA0) padding from CTESTWIN/UcxLog/N1MM
- HHMMSS 6-digit time from some loggers
- MM/DD/YYYY dates from old v1 Cabrillo (2005 era)
- Grid extraction from `HQ-GRID-LOCATOR` headers (160K callsign→grid mappings)

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

### From COPR (Recommended)

```bash
sudo dnf copr enable ki7mt/ai-lab
sudo dnf install ki7mt-ai-lab-apps-wspr
sudo dnf install ki7mt-ai-lab-apps-solar
```

### From Source

```bash
sudo make install
```

## Related Repositories

| Repository | Purpose |
|------------|---------|
| [ki7mt-ai-lab-core](https://github.com/KI7MT/ki7mt-ai-lab-core) | DDL schemas, SQL scripts |
| [ki7mt-ai-lab-cuda](https://github.com/KI7MT/ki7mt-ai-lab-cuda) | CUDA signature embedding engine |
| [ki7mt-ai-lab-training](https://github.com/KI7MT/ki7mt-ai-lab-training) | PyTorch model training |
| [ki7mt-ai-lab-docs](https://github.com/KI7MT/ki7mt-ai-lab-docs) | Documentation site |

## License

GPL-3.0-or-later — See [COPYING](COPYING)

## Author

Greg Beam, KI7MT
