#!/bin/bash
# Checks for solar-live-update's NOAA parsers.
#
#     tests/solar-live-update.test.sh
#
# Runs the REAL script against fixture data with a stub clickhouse-client, so what is
# checked is the shipped parse logic, not a copy of it.
#
# WHY THIS EXISTS. NOAA SWPC changed two endpoints from array-of-arrays / bare-object to
# array-of-objects (~Apr 2026). solar-history-load.sh was updated; this script was not.
# `jq '.Flux'` against an array ERRORS rather than returning null, and `2>/dev/null` ate the
# evidence — so SFI fell to a hardcoded 145 and Kp to 0, and the run exited 0 every fifteen
# minutes. ham-stats published both as measurements. The fixtures below pin BOTH shapes:
# the current one must parse, and the retired one must FAIL rather than invent a number.

set -u
SCRIPT="$(cd "$(dirname "$0")/.." && pwd)/scripts/solar-live-update.sh"
pass=0; fail=0

check() {   # check <label> <got> <want>
    if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; pass=$((pass+1))
    else echo "  FAIL  $1: got '$2', want '$3'"; fail=$((fail+1)); fi
}

# A stub clickhouse-client that records the SQL instead of running it, so the test needs no
# database and cannot touch the live table.
setup() {
    WORK=$(mktemp -d)
    mkdir -p "$WORK/bin" "$WORK/raw"
    cat > "$WORK/bin/clickhouse-client" <<'EOF'
#!/bin/bash
while [[ $# -gt 0 ]]; do [[ "$1" == "--query" ]] && { echo "$2" >> "$CH_LOG"; shift; }; shift; done
EOF
    chmod +x "$WORK/bin/clickhouse-client"
    export CH_LOG="$WORK/ch.sql"
    : > "$CH_LOG"
}
teardown() { rm -rf "$WORK"; }

run_script() {
    SOLAR_DATA_DIR="$WORK/raw" PATH="$WORK/bin:$PATH" bash "$SCRIPT" > "$WORK/out" 2> "$WORK/err"
    echo $?
}

xray_fixture() {
    cat > "$WORK/raw/goes_xray_flux.json" <<'EOF'
[{"time_tag":"2026-09-09T14:00:00Z","energy":"0.05-0.4nm","flux":2.7e-9},
 {"time_tag":"2026-09-09T14:00:00Z","energy":"0.1-0.8nm","flux":3.7e-7}]
EOF
}

# ── the shape NOAA serves today ────────────────────────────────────────────
echo "== current NOAA shape parses to the real values =="
setup
NOW=$(date -u '+%Y-%m-%dT%H:00:00')
echo "[{\"flux\":110,\"time_tag\":\"$NOW\"}]" > "$WORK/raw/noaa_solar_flux.json"
echo "[{\"time_tag\":\"$NOW\",\"Kp\":2.67,\"a_running\":12,\"station_count\":8}]" > "$WORK/raw/noaa_kp_index.json"
xray_fixture
rc=$(run_script)
check "exits 0" "$rc" "0"
sql=$(cat "$CH_LOG")
check "SFI is the observed 110, not the old 145 constant" \
      "$(grep -c 'VALUES (2.67, 12, 110,' <<<"$sql")" "1"
check "conditions derived from a real Kp of 2.67" \
      "$(grep -c "'Quiet'" <<<"$sql")" "1"
check "the row carries NOAA's own observation times" \
      "$(grep -c "${NOW/T/ }" <<<"$sql")" "1"
check "no fallback warning" "$(grep -c 'fallback' "$WORK/out")" "0"
teardown

# ── the shapes NOAA retired: must fail, must not invent ─────────────────────
echo "== retired SFI shape fails loudly instead of substituting 145 =="
setup
echo '{"Flux":"174","TimeStamp":"2026-02-02 20:00:00"}' > "$WORK/raw/noaa_solar_flux.json"
echo "[{\"time_tag\":\"$NOW\",\"Kp\":2.67,\"a_running\":12}]" > "$WORK/raw/noaa_kp_index.json"
xray_fixture
rc=$(run_script)
check "exits non-zero" "$([[ $rc -ne 0 ]] && echo yes || echo no)" "yes"
check "wrote nothing to ClickHouse" "$(wc -c < "$CH_LOG" | tr -d ' ')" "0"
check "says what it could not read" "$(grep -c 'flux' "$WORK/err")" "1"
check "145 appears nowhere" "$(grep -c '145' "$WORK/out" "$WORK/err" | grep -c ':[1-9]')" "0"
teardown

echo "== retired Kp shape fails loudly instead of reporting a quiet 0 =="
setup
echo "[{\"flux\":110,\"time_tag\":\"$NOW\"}]" > "$WORK/raw/noaa_solar_flux.json"
cat > "$WORK/raw/noaa_kp_index.json" <<'EOF'
[["time_tag","Kp","a_running","station_count"],["2026-02-02T00:00:00","3.67","25","8"]]
EOF
xray_fixture
rc=$(run_script)
check "exits non-zero" "$([[ $rc -ne 0 ]] && echo yes || echo no)" "yes"
check "wrote nothing to ClickHouse" "$(wc -c < "$CH_LOG" | tr -d ' ')" "0"
teardown

# ── a value that parses but cannot be true ─────────────────────────────────
echo "== parses-but-implausible is still refused =="
setup
echo "[{\"flux\":0,\"time_tag\":\"$NOW\"}]" > "$WORK/raw/noaa_solar_flux.json"
echo "[{\"time_tag\":\"$NOW\",\"Kp\":2.67,\"a_running\":12}]" > "$WORK/raw/noaa_kp_index.json"
xray_fixture
rc=$(run_script)
check "SFI 0 is refused, not replaced" "$([[ $rc -ne 0 ]] && echo yes || echo no)" "yes"
check "wrote nothing to ClickHouse" "$(wc -c < "$CH_LOG" | tr -d ' ')" "0"
teardown

setup
echo "[{\"flux\":110,\"time_tag\":\"$NOW\"}]" > "$WORK/raw/noaa_solar_flux.json"
echo "[{\"time_tag\":\"$NOW\",\"Kp\":42,\"a_running\":12}]" > "$WORK/raw/noaa_kp_index.json"
xray_fixture
rc=$(run_script)
check "Kp above the 0-9 scale is refused" "$([[ $rc -ne 0 ]] && echo yes || echo no)" "yes"
teardown

# ── genuine but old: publish it, say so ────────────────────────────────────
echo "== stale-but-real is published with its true age, not refused =="
setup
OLD=$(date -u -d '5 days ago' '+%Y-%m-%dT%H:00:00')
echo "[{\"flux\":110,\"time_tag\":\"$OLD\"}]" > "$WORK/raw/noaa_solar_flux.json"
echo "[{\"time_tag\":\"$NOW\",\"Kp\":2.67,\"a_running\":12}]" > "$WORK/raw/noaa_kp_index.json"
xray_fixture
rc=$(run_script)
check "exits 0 — the value is real, just old" "$rc" "0"
check "warns about the age" "$(grep -c 'WARNING: SFI observation is' "$WORK/out")" "1"
check "publishes the observed 110" "$(grep -c ', 110,' "$CH_LOG")" "1"
check "records the real observation time, not now" "$(grep -c "${OLD/T/ }" "$CH_LOG")" "1"
teardown

# ── a missing file is missing, not zero ────────────────────────────────────
echo "== a missing input is not a value =="
setup
echo "[{\"time_tag\":\"$NOW\",\"Kp\":2.67,\"a_running\":12}]" > "$WORK/raw/noaa_kp_index.json"
xray_fixture
rc=$(run_script)
check "exits non-zero when the SFI file is absent" "$([[ $rc -ne 0 ]] && echo yes || echo no)" "yes"
check "wrote nothing to ClickHouse" "$(wc -c < "$CH_LOG" | tr -d ' ')" "0"
teardown

echo
echo "  $pass passed, $fail failed"
[[ $fail -eq 0 ]] || exit 1
