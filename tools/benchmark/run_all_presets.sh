#!/usr/bin/env bash
# run_all_presets.sh
#
# Runs every built-in preset against a Docker broker using all default settings.
# Results (JSON, CSV, HTML) are written to ./results/run_all_<timestamp>/
#
# Usage:
#   ./run_all_presets.sh                        # confluent client (default)
#   ./run_all_presets.sh --client rdkafka_perf  # local librdkafka build
#   ./run_all_presets.sh --output-dir /my/path  # custom output dir
#
# Options passed to this script are forwarded to benchmark.py as-is,
# so any benchmark.py flag works here too.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
OUTPUT_DIR="${SCRIPT_DIR}/results/run_all_${TIMESTAMP}"

# Presets to run in order.
# 'custom' is excluded (user template).
# 'eos'    is excluded for --client rdkafka_perf (no transactional support),
#           but included for the default confluent client.
ALL_PRESETS=(high_throughput low_latency balanced sustained_throughput eos)
RDKAFKA_PRESETS=(high_throughput low_latency balanced sustained_throughput)

CLIENT="confluent"
EXTRA_ARGS=()

# Simple argument pass-through: detect --client to pick preset list
for arg in "$@"; do
    if [[ "$arg" == "--client" ]]; then
        :  # next arg is the value, handled below
    fi
done

# Parse --client and --output-dir from forwarded args
i=0
args=("$@")
while [[ $i -lt ${#args[@]} ]]; do
    case "${args[$i]}" in
        --client)
            i=$((i+1))
            CLIENT="${args[$i]}"
            EXTRA_ARGS+=("--client" "$CLIENT")
            ;;
        --output-dir)
            i=$((i+1))
            OUTPUT_DIR="${args[$i]}"
            EXTRA_ARGS+=("--output-dir" "$OUTPUT_DIR")
            ;;
        *)
            EXTRA_ARGS+=("${args[$i]}")
            ;;
    esac
    i=$((i+1))
done

# Pick preset list based on client
if [[ "$CLIENT" == "rdkafka_perf" ]]; then
    PRESETS=("${RDKAFKA_PRESETS[@]}")
    # Rebuild binary before running
    echo "==> Building rdkafka_benchmark from local source..."
    make -C "${SCRIPT_DIR}/../../examples" rdkafka_benchmark
    echo ""
else
    PRESETS=("${ALL_PRESETS[@]}")
fi

echo "============================================================"
echo "  librdkafka Benchmark Suite"
echo "  client  : $CLIENT"
echo "  presets : ${PRESETS[*]}"
echo "  output  : $OUTPUT_DIR"
echo "============================================================"
echo ""

FAILED=()

# Compute last preset index — bash 3.x compatible (no negative subscripts)
LAST_PRESET="${PRESETS[$((${#PRESETS[@]}-1))]}"

for preset in "${PRESETS[@]}"; do
    echo "------------------------------------------------------------"
    echo "  Preset: $preset"
    echo "------------------------------------------------------------"

    # --keep-broker: broker starts on first preset, stays for the rest.
    # Last preset does NOT get --keep-broker so Docker stops automatically.
    BROKER_FLAG="--auto-broker --keep-broker"
    if [[ "$preset" == "$LAST_PRESET" ]]; then
        BROKER_FLAG="--auto-broker"
    fi

    # shellcheck disable=SC2086
    if python3 "${SCRIPT_DIR}/benchmark.py" \
        --preset "$preset" \
        $BROKER_FLAG \
        --output-dir "$OUTPUT_DIR" \
        "${EXTRA_ARGS[@]}"; then
        echo "  [OK] $preset"
    else
        echo "  [FAILED] $preset"
        FAILED+=("$preset")
    fi
    echo ""
done

echo "============================================================"
echo "  Results written to: $OUTPUT_DIR"
echo ""

# Generate combined HTML report across all presets
echo "==> Generating combined report..."
if python3 "${SCRIPT_DIR}/combine_results.py" "$OUTPUT_DIR"; then
    echo "  Open: ${OUTPUT_DIR}/combined_report.html"
else
    echo "  [warn] Combined report generation failed — individual reports still available."
fi
echo ""

if [[ ${#FAILED[@]} -gt 0 ]]; then
    echo "  FAILED presets: ${FAILED[*]}"
    exit 1
else
    echo "  All presets completed successfully."
    exit 0
fi
