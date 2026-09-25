# Makefile for ionis-apps
#
# High-performance Go applications for IONIS WSPR/Solar data processing
#
# Usage:
#   make              # Show help
#   make all          # Build all binaries
#   make install      # Install to system (requires sudo)
#   make test         # Run tests
#   make clean        # Remove build artifacts

SHELL := /bin/bash
.PHONY: help all build install uninstall test clean lint fmt vet wspr solar contest pskr check-spec

# =============================================================================
# Package Metadata
# =============================================================================
NAME        := ionis-apps
VERSION     := $(shell cat VERSION 2>/dev/null || echo "0.0.0")
GIT_COMMIT  := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE  := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

# =============================================================================
# Go Configuration (CPU-only, static binaries)
# =============================================================================
GO          := go
GOFLAGS     := -trimpath
CGO_ENABLED := 0
LDFLAGS     := -s -w \
               -X main.Version=$(VERSION) \
               -X main.GitCommit=$(GIT_COMMIT) \
               -X main.BuildDate=$(BUILD_DATE)

# =============================================================================
# Installation Paths (FHS-compliant)
# =============================================================================
PREFIX      := /usr
BINDIR      := $(PREFIX)/bin
DATADIR     := $(PREFIX)/share/$(NAME)
SYSCONFDIR  := /etc/$(NAME)

# =============================================================================
# Build Directories
# =============================================================================
BINDIR_BUILD := bin
DISTDIR      := dist

# =============================================================================
# Commands to Build
# =============================================================================
# Note: Legacy tools (wspr-ingest, wspr-ingest-cpu, wspr-ingest-fast) removed
#       due to clickhouse-go/v2 API incompatibility. Replaced by faster tools.
WSPR_CMDS    := wspr-shredder wspr-turbo wspr-parquet-native wspr-download wspr-backfill wspr-live-download wspr-live-ingest
SOLAR_CMDS   := solar-download dscovr-ingest dscovr-archive-download dscovr-archive-ingest \
                solar-kp-download solar-kp-ingest \
                solar-sfi-download solar-sfi-ingest \
                solar-ssn-download solar-ssn-ingest \
                goes-xrs-download goes-xrs-ingest
CONTEST_CMDS := contest-download rbn-download rbn-ingest contest-ingest
PSKR_CMDS    := pskr-collector pskr-ingest
UTIL_CMDS    := db-validate
ALL_CMDS     := $(WSPR_CMDS) $(SOLAR_CMDS) $(CONTEST_CMDS) $(PSKR_CMDS) $(UTIL_CMDS)

# Shell scripts to install
SOLAR_SCRIPTS := solar-live-update.sh

# =============================================================================
# Default Target
# =============================================================================
.DEFAULT_GOAL := help

help:
	@printf "\n"
	@printf "┌─────────────────────────────────────────────────────────────────┐\n"
	@printf "│  ionis-apps v$(VERSION)                                              │\n"
	@printf "│  High-Performance WSPR/Solar Data Ingestion Tools              │\n"
	@printf "└─────────────────────────────────────────────────────────────────┘\n"
	@printf "\n"
	@printf "WSPR Ingestion Tools:\n"
	@printf "  wspr-shredder        Uncompressed CSV ingester (14.4 Mrps)\n"
	@printf "  wspr-turbo           Streaming .gz ingester (8.8 Mrps)\n"
	@printf "  wspr-parquet-native  Parquet file ingester (8.4 Mrps)\n"
	@printf "  wspr-download        WSPR archive downloader\n"
	@printf "\n"
	@printf "Solar Tools:\n"
	@printf "  solar-{kp,sfi,ssn}-download / -ingest   one bronze table per source\n"
	@printf "  goes-xrs-download / -ingest    GOES X-ray 1-min archive (NCEI, 2017-) -> solar.goes_xrs_1m_bronze\n"
	@printf "  solar-download       SWPC nowcast JSON for solar-live-update\n"
	@printf "  dscovr-ingest        DSCOVR L1 solar wind, live RTSW feed\n"
	@printf "  dscovr-archive-download / -ingest   DSCOVR NCEI archive 2016- -> solar.dscovr_{f1m,m1m}_bronze\n"
	@printf "\n"
	@printf "Contest Tools:\n"
	@printf "  contest-download     CQ contest Cabrillo log downloader\n"
	@printf "  rbn-download         Reverse Beacon Network archive downloader\n"
	@printf "  rbn-ingest           RBN ZIP→CSV→ClickHouse ingester\n"
	@printf "  contest-ingest       Cabrillo log→ClickHouse ingester\n"
	@printf "\n"
	@printf "PSK Reporter Tools:\n"
	@printf "  pskr-collector       MQTT real-time spot collector\n"
	@printf "  pskr-ingest          JSONL→ClickHouse incremental ingester\n"
	@printf "\n"
	@printf "Utility Tools:\n"
	@printf "  db-validate          Validate ClickHouse table row counts\n"
	@printf "\n"
	@printf "Usage: make [target]\n"
	@printf "\n"
	@printf "Targets:\n"
	@printf "  help          Show this help message\n"
	@printf "  all           Build all binaries\n"
	@printf "  wspr          Build WSPR binaries only\n"
	@printf "  solar         Build Solar binaries only\n"
	@printf "  contest       Build Contest binaries only\n"
	@printf "  pskr          Build PSK Reporter binaries only\n"
	@printf "  install       Install to system (PREFIX=$(PREFIX))\n"
	@printf "  uninstall     Remove installed files\n"
	@printf "  test          Run Go tests\n"
	@printf "  lint          Run golangci-lint\n"
	@printf "  fmt           Format Go code\n"
	@printf "  vet           Run go vet\n"
	@printf "  clean         Remove build artifacts\n"
	@printf "  deps          Download dependencies\n"
	@printf "\n"
	@printf "Variables:\n"
	@printf "  PREFIX        Installation prefix (default: /usr)\n"
	@printf "  DESTDIR       Staging directory for packaging\n"
	@printf "\n"
	@printf "Examples:\n"
	@printf "  make all                         # Build everything\n"
	@printf "  sudo make install                # Install to /usr/bin\n"
	@printf "  DESTDIR=/tmp/stage make install  # Stage for RPM packaging\n"
	@printf "\n"

# =============================================================================
# Build Targets
# =============================================================================

# Every binary the RPM spec installs into %{_bindir} must be in ALL_CMDS, or the
# build succeeds and the PACKAGE fails at %files with "File not found" -- which is
# what v4.2.0 did: eight solar-*-{download,ingest} commands were added to cmd/ and
# to the spec, and nobody added them here. Cheap to check, so check it.
check-spec:
	@spec=$(NAME).spec; \
	missing=""; \
	installed="$(ALL_CMDS) $$(echo '$(SOLAR_SCRIPTS)' | sed 's/\.sh//g')"; \
	for b in $$(grep -oE '%\{_bindir\}/[a-z0-9-]+' $$spec | sed 's|.*/||' | sort -u); do \
	  echo "$$installed" | tr ' ' '\n' | grep -qx "$$b" || missing="$$missing $$b"; \
	done; \
	if [ -n "$$missing" ]; then \
	  echo "ERROR: $$spec installs binaries that ALL_CMDS does not build:"; \
	  for b in $$missing; do echo "         $$b"; done; \
	  echo "       add them to the appropriate *_CMDS variable in this Makefile."; \
	  exit 1; \
	fi; \
	echo "check-spec: every $$spec %{_bindir} entry is built by ALL_CMDS or installed from SOLAR_SCRIPTS"

all: check-spec $(addprefix $(BINDIR_BUILD)/,$(ALL_CMDS))
	@printf "\nBuild complete:\n"
	@for cmd in $(ALL_CMDS); do printf "  $(BINDIR_BUILD)/$$cmd\n"; done

wspr: $(addprefix $(BINDIR_BUILD)/,$(WSPR_CMDS))
	@printf "\nWSPR tools built:\n"
	@for cmd in $(WSPR_CMDS); do printf "  $(BINDIR_BUILD)/$$cmd\n"; done

solar: $(addprefix $(BINDIR_BUILD)/,$(SOLAR_CMDS))
	@printf "\nSolar tools built:\n"
	@for cmd in $(SOLAR_CMDS); do printf "  $(BINDIR_BUILD)/$$cmd\n"; done

contest: $(addprefix $(BINDIR_BUILD)/,$(CONTEST_CMDS))
	@printf "\nContest tools built:\n"
	@for cmd in $(CONTEST_CMDS); do printf "  $(BINDIR_BUILD)/$$cmd\n"; done

pskr: $(addprefix $(BINDIR_BUILD)/,$(PSKR_CMDS))
	@printf "\nPSK Reporter tools built:\n"
	@for cmd in $(PSKR_CMDS); do printf "  $(BINDIR_BUILD)/$$cmd\n"; done

# Generic build rule for all commands
$(BINDIR_BUILD)/%: cmd/%/main.go | $(BINDIR_BUILD)
	@printf "Building $*...\n"
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $@ ./cmd/$*

# Create build directory
$(BINDIR_BUILD):
	@mkdir -p $(BINDIR_BUILD)

# =============================================================================
# Dependencies
# =============================================================================
deps:
	@printf "Downloading dependencies...\n"
	$(GO) mod download
	$(GO) mod tidy
	$(GO) mod verify
	@printf "Dependencies ready.\n"

# =============================================================================
# Code Quality
# =============================================================================
fmt:
	@printf "Formatting Go code...\n"
	$(GO) fmt ./...

vet:
	@printf "Running go vet...\n"
	$(GO) vet ./...

lint:
	@printf "Running golangci-lint...\n"
	@if command -v golangci-lint &>/dev/null; then \
		golangci-lint run ./...; \
	else \
		printf "golangci-lint not installed, skipping.\n"; \
	fi

# =============================================================================
# Testing
# =============================================================================
test:
	@printf "Running tests...\n"
	$(GO) test -v -race -coverprofile=coverage.out ./...
	@printf "\nTest coverage:\n"
	$(GO) tool cover -func=coverage.out | tail -1

test-short:
	@printf "Running short tests...\n"
	$(GO) test -short -v ./...

# =============================================================================
# Installation
# =============================================================================
install: all
	@printf "Installing $(NAME) v$(VERSION) to $(DESTDIR)$(PREFIX)...\n"
	@printf "\n"
	install -d $(DESTDIR)$(BINDIR)
	install -d $(DESTDIR)$(DATADIR)
	install -d $(DESTDIR)$(SYSCONFDIR)
	@for cmd in $(ALL_CMDS); do \
		printf "  Installing $$cmd...\n"; \
		install -m 755 $(BINDIR_BUILD)/$$cmd $(DESTDIR)$(BINDIR)/; \
	done
	@for script in $(SOLAR_SCRIPTS); do \
		printf "  Installing $${script%.sh}...\n"; \
		install -m 755 scripts/$$script $(DESTDIR)$(BINDIR)/$${script%.sh}; \
	done
	@printf "\nInstalled $(words $(ALL_CMDS)) binaries + $(words $(SOLAR_SCRIPTS)) scripts to $(DESTDIR)$(BINDIR)/\n"

uninstall:
	@printf "Uninstalling $(NAME) from $(DESTDIR)$(PREFIX)...\n"
	@for cmd in $(ALL_CMDS); do \
		rm -f $(DESTDIR)$(BINDIR)/$$cmd; \
	done
	@for script in $(SOLAR_SCRIPTS); do \
		rm -f $(DESTDIR)$(BINDIR)/$${script%.sh}; \
	done
	rm -rf $(DESTDIR)$(DATADIR)
	rm -rf $(DESTDIR)$(SYSCONFDIR)
	@printf "Uninstall complete.\n"

# =============================================================================
# Packaging
# =============================================================================
dist: clean all
	@printf "Creating distribution archive...\n"
	mkdir -p $(DISTDIR)
	tar -czvf $(DISTDIR)/$(NAME)-$(VERSION)-linux-amd64.tar.gz \
		-C $(BINDIR_BUILD) $(ALL_CMDS)
	@printf "Created: $(DISTDIR)/$(NAME)-$(VERSION)-linux-amd64.tar.gz\n"

# =============================================================================
# Cleanup
# =============================================================================
clean:
	@printf "Cleaning build artifacts...\n"
	rm -rf $(BINDIR_BUILD) $(DISTDIR) build
	rm -f coverage.out
	@printf "Clean complete.\n"
