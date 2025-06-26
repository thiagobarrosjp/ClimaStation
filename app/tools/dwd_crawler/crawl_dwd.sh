#!/bin/bash
set -e  # Exit on error
set -x  # Print commands as they execute

# === Configuration ===
BASE_URL="https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="$SCRIPT_DIR/../../../data/dwd_structure_logs"
TIMESTAMP=$(date "+%Y-%m-%d_%H-%M")

# === Prepare Output Paths ===
mkdir -p "$OUTPUT_DIR"
URL_LOG="$OUTPUT_DIR/${TIMESTAMP}_urls.txt"
TREE_LOG="$OUTPUT_DIR/${TIMESTAMP}_tree.txt"

echo "📦 Starting DWD crawl: $BASE_URL"
echo "🕒 Timestamp: $TIMESTAMP"
echo "Saving logs to: $OUTPUT_DIR"

# === Run wget in spider mode to get all URLs ===
wget \
  --recursive \
  --level=inf \
  --no-parent \
  --no-directories \
  --spider \
  --wait=0.1 \
  --random-wait \
  --output-file="$URL_LOG" \
  "$BASE_URL"

# === Extract URLs from log file ===
grep --only-matching "https://[^ ]*" "$URL_LOG" | sort -u > "$URL_LOG.tmp"
mv "$URL_LOG.tmp" "$URL_LOG"

# === Generate tree view from URLs ===
sed "s|$BASE_URL||" "$URL_LOG" | \
awk -F'/' '{
  for (i = 1; i <= NF; i++) {
    indent = i - 1;
    printf("%*s%s\n", indent * 4, "", $i);
  }
}' | sort -u > "$TREE_LOG"

echo "✅ Crawl complete!"
echo "→ Full URL list saved to: $URL_LOG"
echo "→ Tree view saved to: $TREE_LOG"
