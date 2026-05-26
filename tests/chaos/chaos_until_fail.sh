#!/bin/bash
#
# Loop the chaos.py share-consumer workload until verify.txt
# reports VERDICT: FAIL, then stop with the failing run's artifacts
# intact.
#
# Default cluster shape: 5 brokers, 6 partitions, 100 rec/s,
# 3 cycles, share-consumer-verify in explicit-ack mode, broker-roll
# leader-change mode. Compared to the chaos.py defaults, the runner
# trims pre-roll/warmup/cooldown/drain and uses unclean (SIGKILL)
# shutdown so each iteration finishes faster and each broker death
# is harsher (more race surface). Drain is 30 s — analysis of past
# failing runs showed every healthy partition reached its broker
# HWM before drain even started (idle-at-drain-start <= 45 s for
# healthy, >= 90 s for stuck), so drain length doesn't gate the
# verdict; it just flushes any in-flight records after the producer
# stops. Dwell times during the roll itself (--stop-s, --up-s,
# --cycles) are left alone — those are the leader-migration timings
# the bug depends on.
#
# Each iteration overwrites /tmp/chaos-logs from scratch, so
# disk usage stays bounded; the only run kept is the one that broke.
#
# Usage:
#   ./chaos_until_fail.sh                    # 100 iterations
#   ./chaos_until_fail.sh 500                # 500 iterations
#   ./chaos_until_fail.sh 100 --cycles 5     # extra args forwarded to
#                                            # chaos.py (override
#                                            # the runner's defaults)
#
# Exit codes:
#   0  all iterations passed
#   1  failure detected (VERDICT: FAIL, missing verify.txt, or
#      chaos.py non-zero exit)
#   130 interrupted (Ctrl-C)

set -uo pipefail

ITERATIONS=${1:-100}
shift || true
EXTRA_ARGS=("$@")

# Trimmed dwell times + unclean shutdown. Listed first so any
# matching flag in EXTRA_ARGS overrides them (argparse "last wins").
RUNNER_DEFAULTS=(
    --unclean-stop
    --warmup-s 5
    --pre-roll-s 10
    --cooldown-s 10
    --drain-s 30
)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TESTS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR=/tmp/chaos-logs
PREV_LOG_DIR=/tmp/chaos-prev
TRIVUP_INSTANCES="$TESTS_DIR/tmp/LibrdkafkaTestCluster"
HISTORY_FILE="$SCRIPT_DIR/run-history.tsv"
LOG_FILE="$SCRIPT_DIR/run-history.log"
# Per-runner-invocation report archive. Each iter's small report
# files (verify.txt, summary.txt, leader_changes.txt, etc.) are
# copied here so they survive across iterations. Detailed debug logs
# (consumer-0.stderr) live only in LOG_DIR (current iter) and
# PREV_LOG_DIR (previous iter), each capped at ~200 MB.
RUN_ID="$(date -u '+%Y%m%dT%H%M%SZ')-pid$$"
REPORTS_DIR="$SCRIPT_DIR/runs/$RUN_ID"
mkdir -p "$REPORTS_DIR"

# One-line per-iteration summary appended to HISTORY_FILE so we have
# a record of successful runs (the runner wipes LOG_DIR each
# iteration, so without this we lose all evidence of passing runs).
if [[ ! -f "$HISTORY_FILE" ]]; then
    printf 'iter\tstart_utc\telapsed_s\tseed\tcycles\tproduce_rate\textra_args\tverdict\tsummary\n' \
        > "$HISTORY_FILE"
fi

# Full stdout + stderr capture to LOG_FILE (still echoed to the
# terminal via tee). Each invocation of the runner appends a banner
# so multiple runs are distinguishable inside the same log file.
{
    printf '\n========================================================\n'
    printf '= run started %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf '= argv: ITERATIONS=%s extra=[%s]\n' "$ITERATIONS" "${EXTRA_ARGS[*]:-}"
    printf '========================================================\n\n'
} >> "$LOG_FILE"
exec > >(tee -a "$LOG_FILE") 2>&1

cd "$SCRIPT_DIR"

# Kill any leftover Kafka brokers from a previous (crashed) run so
# new trivup ports don't collide. Narrow to kafka.Kafka (the broker
# main class) + kafka-storage owned by the current user — avoids
# clobbering unrelated kafka work.
cleanup_cluster() {
    local matched
    matched=$(pgrep -u "$USER" -f 'kafka\.Kafka|kafka-storage|trivup\.kafka' 2>/dev/null || true)
    if [[ -n "$matched" ]]; then
        echo "[runner] killing stranded Kafka procs:" $matched
        pkill -u "$USER" -f 'kafka\.Kafka|kafka-storage|trivup\.kafka' 2>/dev/null || true
        sleep 2
        pkill -9 -u "$USER" -f 'kafka\.Kafka|kafka-storage|trivup\.kafka' 2>/dev/null || true
    fi

    # Per-run instance dirs (numeric names) — remove. Keep
    # KafkaBrokerApp/ which is the shared tarball cache.
    if [[ -d "$TRIVUP_INSTANCES" ]]; then
        find "$TRIVUP_INSTANCES" -mindepth 1 -maxdepth 1 -type d \
             -regex '.*/[0-9]+$' -exec rm -rf {} +
    fi
}

# Archive iteration N's small report files (verify.txt, summary.txt,
# leader_changes.txt, leader_history.log, metadata_triggers.txt) into
# REPORTS_DIR/iter-NNN-<verdict>/ so all runs' reports survive across
# iterations. Idempotent — safe to call even if some files are
# missing.
archive_reports() {
    local iter=$1 verdict=$2
    local dest
    dest=$(printf '%s/iter-%03d-%s' "$REPORTS_DIR" "$iter" "$verdict")
    mkdir -p "$dest"
    for f in verify.txt summary.txt metadata_triggers.txt \
             leader_changes.txt leader_history.log; do
        if [[ -f "$LOG_DIR/$f" ]]; then
            cp "$LOG_DIR/$f" "$dest/"
        fi
    done
    echo "[runner] iter $iter reports archived to $dest"
}

# Rotate the current iteration's LOG_DIR into PREV_LOG_DIR so the
# user always has the last two iterations on disk (current + prev)
# without paying for all of them.
rotate_log_dir() {
    rm -rf "$PREV_LOG_DIR"
    if [[ -d "$LOG_DIR" ]]; then
        mv "$LOG_DIR" "$PREV_LOG_DIR"
    fi
}

START_EPOCH=$(date +%s)
trap 'echo "[runner] interrupted at iteration $i"; cleanup_cluster; exit 130' INT

# Pull useful values from EXTRA_ARGS so the history TSV captures what
# varied between sessions. These greps don't validate; they just
# surface whatever was passed (or empty string if not present).
extract_arg() {
    local target=$1
    shift
    local prev=""
    for token in "$@"; do
        if [[ "$prev" == "$target" ]]; then
            printf '%s' "$token"
            return
        fi
        prev=$token
    done
}
EXTRA_SEED=$(extract_arg --seed "${EXTRA_ARGS[@]}")
EXTRA_CYCLES=$(extract_arg --cycles "${EXTRA_ARGS[@]}")
EXTRA_RATE=$(extract_arg --produce-rate "${EXTRA_ARGS[@]}")
EXTRA_JOINED="${EXTRA_ARGS[*]:-}"

append_history() {
    local iter=$1 start_utc=$2 elapsed=$3 verdict=$4 summary=$5
    # Strip any tabs/newlines from the summary so the TSV stays
    # parseable by `cut`.
    summary=$(printf '%s' "$summary" | tr '\t\n' '  ')
    printf '%d\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$iter" "$start_utc" "$elapsed" \
        "${EXTRA_SEED:-auto}" "${EXTRA_CYCLES:-default}" \
        "${EXTRA_RATE:-default}" "$EXTRA_JOINED" \
        "$verdict" "$summary" >> "$HISTORY_FILE"
}

for ((i=1; i<=ITERATIONS; i++)); do
    echo "==========================================================="
    echo "[runner] iteration $i / $ITERATIONS — $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    echo "==========================================================="

    cleanup_cluster
    # Note: do NOT rm $LOG_DIR here. At the end of the previous iter
    # we rotated it into $PREV_LOG_DIR; if Ctrl-C happens during the
    # next chaos.py the current iter's partial state still lives at
    # $LOG_DIR. chaos.py will recreate $LOG_DIR as needed.

    iter_start_utc=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
    iter_start=$(date +%s)
    if ! python3 "$SCRIPT_DIR/chaos.py" \
            "${RUNNER_DEFAULTS[@]}" "${EXTRA_ARGS[@]}"; then
        iter_elapsed=$(( $(date +%s) - iter_start ))
        append_history "$i" "$iter_start_utc" "$iter_elapsed" \
            "ERROR" "chaos.py exited non-zero"
        archive_reports "$i" "ERROR"
        echo "[runner] iteration $i: chaos.py exited non-zero"
        echo "[runner] current iter logs at $LOG_DIR; reports at $REPORTS_DIR"
        exit 1
    fi
    iter_elapsed=$(( $(date +%s) - iter_start ))

    verify="$LOG_DIR/verify.txt"
    if [[ ! -f "$verify" ]]; then
        append_history "$i" "$iter_start_utc" "$iter_elapsed" \
            "ERROR" "verify.txt missing"
        archive_reports "$i" "ERROR"
        echo "[runner] iteration $i: $verify missing — treating as failure"
        exit 1
    fi

    verdict_line=$(grep '^VERDICT' "$verify" | head -1)

    if grep -q '^VERDICT: FAIL' "$verify"; then
        append_history "$i" "$iter_start_utc" "$iter_elapsed" \
            "FAIL" "$verdict_line"
        archive_reports "$i" "FAIL"
        echo "[runner] iteration $i: VERDICT: FAIL detected after ${iter_elapsed}s"
        grep '^VERDICT' "$verify" | sed 's/^/[runner]   /'
        echo "[runner] current iter logs at $LOG_DIR; reports at $REPORTS_DIR"
        exit 1
    fi

    append_history "$i" "$iter_start_utc" "$iter_elapsed" \
        "OK" "$verdict_line"
    archive_reports "$i" "OK"
    grep '^VERDICT' "$verify" | sed 's/^/[runner]   /'
    total_elapsed=$(( $(date +%s) - START_EPOCH ))
    avg=$(( total_elapsed / i ))
    remaining=$(( (ITERATIONS - i) * avg ))
    printf '[runner] iteration %d PASS in %ds (avg=%ds, remaining ~%ds)\n' \
        "$i" "$iter_elapsed" "$avg" "$remaining"
    # Rotate AFTER the iter's report files are archived. From this
    # point until the next chaos.py creates a fresh $LOG_DIR, the
    # just-completed iter's full debug logs live in $PREV_LOG_DIR
    # for inspection.
    rotate_log_dir
done

echo "[runner] all $ITERATIONS iterations passed"