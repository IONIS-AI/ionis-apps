# Disable debug package (Go binaries are statically linked)
%global debug_package %{nil}

# Go import path
%global goipath         github.com/KI7MT/ki7mt-ai-lab-apps

Name:           ki7mt-ai-lab-apps
Version:        2.2.0
Release:        1%{?dist}
Summary:        High-performance WSPR/Solar data ingestion tools for ClickHouse

License:        GPL-3.0-or-later
URL:            https://github.com/KI7MT/ki7mt-ai-lab-apps
Source0:        https://github.com/KI7MT/%{name}/archive/refs/tags/v%{version}.tar.gz#/%{name}-%{version}.tar.gz

# Architecture-specific (Go compiles to native binaries)
ExclusiveArch:  x86_64 aarch64

# Build requirements
BuildRequires:  golang >= 1.22
BuildRequires:  git
BuildRequires:  make

# Runtime requirements
Requires:       ki7mt-ai-lab-core >= 2.0.0
Requires:       %{name}-wspr = %{version}-%{release}
Requires:       %{name}-solar = %{version}-%{release}

%description
High-performance Go applications for KI7MT AI Lab WSPR (Weak Signal
Propagation Reporter) and Solar flux data processing. Optimized for
10+ billion row datasets with ClickHouse backend.

Ingestion tools use ch-go native protocol with LZ4 compression for
maximum throughput. Benchmarks on Ryzen 9 9950X3D:
- wspr-shredder: 14.4 Mrps (uncompressed CSV)
- wspr-turbo: 8.8 Mrps (streaming from .gz archives)
- wspr-parquet-native: 8.4 Mrps (Parquet files)

%package wspr
Summary:        WSPR data processing tools
Requires:       %{name} = %{version}-%{release}

%description wspr
WSPR (Weak Signal Propagation Reporter) data processing applications:

- wspr-shredder:       High-performance uncompressed CSV ingester (14.4 Mrps)
- wspr-turbo:          Zero-copy streaming ingester for .gz archives (8.8 Mrps)
- wspr-parquet-native: Native Parquet file ingester (8.4 Mrps)
- wspr-download:       Parallel archive downloader from wsprnet.org

All ingestion tools use ch-go native protocol with LZ4 compression.

%package solar
Summary:        Solar flux data processing tools
Requires:       %{name} = %{version}-%{release}

%description solar
Solar and geomagnetic data processing applications:
- solar-download:     Multi-source solar data downloader (SIDC, NOAA, GOES)
- solar-ingest:       Solar/geomagnetic data ingestion (SFI, SSN, Kp, Ap, X-ray)
- solar-refresh:      Download + truncate + ingest pipeline script
- solar-live-update:  Now-Casting live conditions updater (15-min cron)
- solar-history-load: Historical solar data loader for training (6-hour cron)

%prep
%autosetup -n %{name}-%{version}

%build
make all VERSION=%{version}

%install
make install DESTDIR=%{buildroot} PREFIX=%{_prefix}

%files
%license COPYING
%doc README.md
%dir %{_datadir}/%{name}
%dir %{_sysconfdir}/%{name}

%files wspr
%{_bindir}/wspr-shredder
%{_bindir}/wspr-turbo
%{_bindir}/wspr-parquet-native
%{_bindir}/wspr-download

%files solar
%{_bindir}/solar-ingest
%{_bindir}/solar-download
%{_bindir}/solar-backfill
%{_bindir}/solar-refresh
%{_bindir}/solar-live-update
%{_bindir}/solar-history-load

%changelog
* Tue Feb 04 2026 Greg Beam <ki7mt@yahoo.com> - 2.2.0-1
- Add solar-backfill: GFZ Potsdam historical SSN/SFI/Kp backfill (1932-present)
- Add solar-backfill to RPM solar subpackage
- Phase 4.1 SSN Signal Recovery: backfill 25+ years of solar indices

* Mon Feb 03 2026 Greg Beam <ki7mt@yahoo.com> - 2.1.0-1
- Normalize band from frequency in all 5 CSV ingesters via bands.GetBand()
- Add internal/bands package: ADIF-MCP v3.1.6 full-spectrum band lookup
- Remove legacy Python ingester (source of band column contamination)
- Update benchmarks: wspr-turbo 24.67 Mrps, wspr-shredder 21.81 Mrps (9975WX)
- Align version across all lab packages at 2.1.0

* Mon Feb 03 2026 Greg Beam <ki7mt@yahoo.com> - 2.0.8-1
- Fix Silent Sun: solar-download now fetches NOAA SFI summary endpoint
- Add 3 new NOAA data sources (SFI live, SFI 30-day, X-ray 7-day)
- Add solar-live-update.sh for Now-Casting (15-min cron)
- Add solar-history-load.sh for training data (6-hour cron)
- Fix solar-live-update to parse correct SFI file with Cycle 25 fallback
- solar-download expanded from 7 to 10 data sources

* Mon Jan 20 2025 Greg Beam <ki7mt@yahoo.com> - 2.0.3-1
- Sync version across all lab packages
- Fix maintainer email in changelog

* Sun Jan 18 2025 Greg Beam <ki7mt@yahoo.com> - 2.0.0-1
- Major release with high-performance ingestion tools
- Add wspr-shredder: 14.4 Mrps uncompressed CSV ingester
- Add wspr-turbo: 8.8 Mrps streaming .gz ingester with klauspost/gzip
- Add wspr-parquet-native: 8.4 Mrps Parquet ingester
- All tools use ch-go native protocol with LZ4 compression
- Remove CUDA/GPU dependencies (CPU-only, static binaries)
- Update to Go 1.24

* Sat Jan 17 2025 Greg Beam <ki7mt@yahoo.com> - 1.0.0-1
- Initial package release
- Add wspr-ingest: CSV ingestion with dual-path (GPU/CPU)
- Add wspr-download: Parallel WSPR archive downloader
- Add solar-ingest: NOAA solar flux data ingestion
