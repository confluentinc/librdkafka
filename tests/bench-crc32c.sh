#!/bin/bash
# CRC32C benchmark: compile + run + format markdown table for PR.
#
# Usage:
#   cd tests && ./bench-crc32c.sh

set -e

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="$ROOT/src"
TST="$ROOT/tests"
BENCH_BIN="$TST/crc32c_bench"
BENCH_SRC="$TST/crc32c_bench.c"
LIB="$SRC/librdkafka.a"

# ── build library if needed ──
if [ ! -f "$LIB" ]; then
    echo "Building librdkafka..."
    make -C "$SRC" -j
fi

# ── compile benchmark ──
echo "Compiling benchmark..."
gcc -g -O2 -Wall \
    -I"$SRC" \
    -o "$BENCH_BIN" \
    "$BENCH_SRC" \
    "$LIB" \
    -lpthread -lrt -ldl -lm -lz -lssl -lcrypto -lzstd -lcurl -llz4

# ── run ──
echo "Running..."
OUTPUT=$("$BENCH_BIN" 2>/dev/null)

# ── print markdown table ──
echo ""
echo "| Size | Before (MB/s) | After (MB/s) | Speedup |"
echo "|------|---------------|-------------|---------|"

echo "$OUTPUT" | while IFS= read -r line; do
    case "$line" in
        *"B  "*|*"KB "*|*"MB "*)
            # $1 $2 = size, $3 = before, $4 = MB/s, $5 = after, $6 = MB/s, $7 = speedup
            sz=$(echo "$line"   | awk '{print $1, $2}')
            before=$(echo "$line" | awk '{print $3}')
            after=$(echo "$line"  | awk '{print $5}')
            speedup=$(echo "$line" | awk '{print $7}')
            printf "| %-5s | %13s | %11s | %6s |\n" \
                "$sz" "${before} MB/s" "${after} MB/s" "$speedup"
            ;;
    esac
done

echo ""
echo "Done."
