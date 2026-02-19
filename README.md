# ionis-apps

High-performance Go data ingesters for the IONIS project.

## Overview

Command-line tools for ingesting and processing amateur radio propagation data from multiple sources: WSPR, Reverse Beacon Network, contest logs (CQ/ARRL), PSK Reporter, and NOAA solar indices. All ingestion tools use the ch-go native ClickHouse protocol with LZ4 compression for maximum throughput. The PSK Reporter MQTT collector writes to disk (gzip JSONL) for durability and replayability.

## Applications

### WSPR Tools

| Command | Source Format | Throughput | Description |
|---------|--------------|------------|-------------|
| `wspr-turbo` | Compressed .gz | 22.55 Mrps | Zero-copy streaming from archives (watermark) |
| `wspr-shredder` | Uncompressed CSV | 21.81 Mrps | Fastest for pre-extracted CSV |
| `wspr-parquet-native` | Parquet | 17.02 Mrps | Native Go Parquet reader |
| `wspr-download` | — | — | Parallel archive downloader |

### Contest Tools

| Command | Description |
|---------|-------------|
| `contest-download` | Downloads public Cabrillo logs (CQ WW, WPX, ARRL DX, and 12 other contests) |
| `contest-ingest` | Parses and ingests Cabrillo v1/v2/v3 into ClickHouse (258K rps, watermark) |

### RBN Tools

| Command | Description |
|---------|-------------|
| `rbn-download` | Downloads daily RBN ZIP archives (2009-present) |
| `rbn-ingest` | Ingests RBN CSV into ClickHouse (10.32 Mrps, watermark) |

### PSK Reporter Tools

| Command | Description |
|---------|-------------|
| `pskr-collector` | MQTT real-time spot collector → gzip JSONL (~250 spots/sec HF) |
| `pskr-ingest` | Incremental JSONL→ClickHouse loader with watermark tracking |

### Solar Tools

| Command | Description |
|---------|-------------|
| `solar-ingest` | NOAA solar flux data ingestion |
| `solar-backfill` | GFZ Potsdam historical SSN/SFI/Kp (2000-present) |
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
| PSK Reporter | ~22M/day (collecting) | `pskr-collector` | ~250 HF spots/sec |

## Idempotent Pipeline (v3.0.5+)

Every data source follows a paired **download → ingest** pattern. Downloaders write files to disk (never touch ClickHouse). Ingesters read files from disk and load into ClickHouse (never touch the network). All ingesters track progress via `{db}.ingest_log` watermark tables (ReplacingMergeTree).

| Source | Downloader | Ingester | Watermark Table |
|--------|-----------|----------|-----------------|
| WSPR | `wspr-download` | `wspr-turbo` | `wspr.ingest_log` |
| RBN | `rbn-download` | `rbn-ingest` | `rbn.ingest_log` |
| Contest | `contest-download` | `contest-ingest` | `contest.ingest_log` |
| PSKR | `pskr-collector` | `pskr-ingest` | `pskr.ingest_log` |
| Solar | `solar-download` | `solar-ingest` | — (ReplacingMergeTree dedup) |

### Ingest Modes

All watermark-enabled ingesters support three modes:

```text
--incremental   (default) Skip files already in watermark. For WSPR, re-ingest
                if file has grown (cumulative monthly archives).
--full          Process all files. Drop partitions before reload, update watermark.
--prime         Bootstrap watermark for existing files without loading data.
--dry-run       List files that would be processed, then exit.
```

### Watermark Schema

Each `{db}.ingest_log` table uses the same schema:

```sql
CREATE TABLE {db}.ingest_log (
    file_path    String,         -- Relative path from source dir
    file_size    UInt64,         -- Bytes at load time (detects growth)
    row_count    UInt64,         -- Rows loaded (0 = primed entry)
    loaded_at    DateTime,       -- UTC timestamp
    elapsed_ms   UInt32,         -- Processing time
    hostname     LowCardinality(String)  -- Which host ingested
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (file_path);
```

## Band Normalization (v2.1.0+)

All CSV ingesters normalize the band column from the frequency field using `internal/bands.GetBand()`, producing correct ADIF band IDs (102-111 for HF).

## Requirements

- Go 1.24+
- ClickHouse server
- ionis-core (database schemas)

## Building

```bash
make all       # Build all binaries
make wspr      # Build WSPR tools only
make solar     # Build Solar tools only
make pskr      # Build PSK Reporter tools only
make help      # Show help
```

## Installation

### From COPR (Recommended)

```bash
sudo dnf copr enable ki7mt/ionis-ai
sudo dnf install ionis-apps-wspr
sudo dnf install ionis-apps-solar
```

### Upgrading from ki7mt-ai-lab-apps

The `ionis-apps` package includes `Obsoletes: ki7mt-ai-lab-apps` for seamless upgrade:

```bash
sudo dnf copr enable ki7mt/ionis-ai
sudo dnf upgrade --refresh
```

### From Source

```bash
sudo make install
```

## Related Repositories

| Repository | Purpose |
|------------|---------|
| [ionis-core](https://github.com/IONIS-AI/ionis-core) | DDL schemas, SQL scripts |
| [ionis-cuda](https://github.com/IONIS-AI/ionis-cuda) | CUDA signature embedding engine |
| [ionis-training](https://github.com/IONIS-AI/ionis-training) | PyTorch model training |
| [ionis-validate](https://github.com/IONIS-AI/ionis-validate) | Model validation suite (PyPI) |
| [ionis-docs](https://github.com/IONIS-AI/ionis-docs) | Documentation site |

## License

GPL-3.0-or-later — See [COPYING](COPYING)

## Author

Greg Beam, KI7MT
