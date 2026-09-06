#!/usr/bin/env bash
# wspr-source-crosscheck — compare wspr.bronze against an INDEPENDENT WSPR database.
#
# WHY. Every internal check we had said wspr.bronze was healthy while it was missing 14%
# of its spots for three years — and each of those checks was correct on its own terms.
# Row counts grew. Freshness was fine. Ingest was byte-exact against its source file. The
# data was faithfully copied from a source that was itself wrong, and nothing internal can
# detect that. Only a second, independently-collected copy can.
#
# It earned its place immediately: the first run of this comparison found our 2026 short by
# 573M rows, because the recovery tool had been pulling only minute<10 for months where
# wsprnet published no CSV at all. Nothing else would have surfaced that.
#
# wsprdaemon (wd1/wd2) and wspr.live share a schema but are SEPARATE collectors, so neither
# is a superset — each drops spots the other caught. Small two-way deltas are normal and
# expected. A large one-way delta is the signal.
#
# Read-only. One aggregate query per side.
#
# Usage: ./wspr-source-crosscheck.sh [months_back]
set -euo pipefail

CH="${CH_ENDPOINT:-http://10.60.1.1:8123}"
PEER="${PEER_ENDPOINT:-https://wd1.wsprdaemon.org}"
MONTHS="${1:-6}"
# Threshold on the peer's own volume, not an absolute count, so it stays meaningful as
# traffic grows. 5% is far above normal collector-to-collector drift (observed <1%) and
# far below a real structural gap (today's was 34%).
ALERT_PCT="${ALERT_PCT:-5}"

since=$(date -u -d "${MONTHS} months ago" +%Y-%m-01)

ours=$(curl -sS --max-time 300 -G "$CH/" --data-urlencode \
  "query=SELECT substring(toString(toStartOfMonth(timestamp)),1,7) m, count() c
         FROM wspr.bronze WHERE timestamp >= '$since' GROUP BY m ORDER BY m FORMAT TSV")

peer=$(curl -sS --max-time 300 -G "$PEER/" --data-urlencode \
  "query=SELECT substring(toString(toStartOfMonth(time)),1,7) m, count() c
         FROM wspr.rx WHERE time >= '$since' GROUP BY m ORDER BY m FORMAT TSV")

printf '  %-9s %14s %14s %12s\n' month ours peer delta
echo "  ------------------------------------------------------------"

alerts=0
while IFS=$'\t' read -r m c; do
    [ -z "${m:-}" ] && continue
    # Skip the CURRENT month: it is still filling on both sides, and whichever collector
    # ran most recently trivially "wins". Comparing a partial month reports a difference
    # in ingest timing as if it were a difference in data.
    [ "$m" = "$(date -u +%Y-%m)" ] && continue
    p=$(echo "$peer" | awk -F'\t' -v k="$m" '$1==k{print $2}')
    [ -z "${p:-}" ] && continue
    awk -v m="$m" -v o="$c" -v p="$p" -v t="$ALERT_PCT" 'BEGIN{
        d = p - o
        pct = (p > 0) ? (d / p * 100) : 0
        flag = (pct > t) ? "  <<< WE ARE SHORT" : ""
        printf "  %-9s %14d %14d %+12d%s\n", m, o, p, d, flag
    }'
    over=$(awk -v o="$c" -v p="$p" -v t="$ALERT_PCT" 'BEGIN{print (p>0 && (p-o)/p*100 > t) ? 1 : 0}')
    alerts=$((alerts + over))
done <<<"$ours"

echo
if [ "$alerts" -gt 0 ]; then
    echo "  $alerts month(s) where the peer holds >${ALERT_PCT}% more than us — investigate."
    echo "  Usual cause: an ingest window that is too narrow, not upstream data loss."
    exit 1
fi
echo "  No month differs by more than ${ALERT_PCT}% — sources agree."
