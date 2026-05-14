#!/bin/bash
set -e

# Use env vars matching run-all-tests.sh convention,
# with fallback to positional args for backward compatibility.
export TEST_KAFKA_GIT_REF="${TEST_KAFKA_GIT_REF:-${1:-4.2.0}}"
export TEST_CP_VERSION="${TEST_CP_VERSION:-${2:-8.2.0}}"
TEST_CONFIGURATION="${TEST_CONFIGURATION:---kraft}"

if [[ "$(uname)" == "Darwin" ]]; then
    CONFIGURE_ARGS="--install-deps --source-deps-only"
else
    source /home/user/venv/bin/activate
    CONFIGURE_ARGS="--install-deps --enable-werror"
fi
./configure ${CONFIGURE_ARGS}
make -j all
make -j -C tests build

# Print system specs once before tests start.
echo "=== sysinfo ==="
uname -a || true
if [[ "$(uname)" == "Darwin" ]]; then
    sysctl -n hw.ncpu hw.memsize 2>/dev/null || true
else
    nproc 2>/dev/null || true
    grep -E '^(MemTotal|MemAvailable)' /proc/meminfo 2>/dev/null || true
fi
echo "==============="

# Background system load monitor: prints CPU/mem snapshot every 5s to stdout
# (and therefore the Semaphore job log). Killed on script exit.
(
    while true; do
        ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
        if [[ "$(uname)" == "Darwin" ]]; then
            load=$(sysctl -n vm.loadavg 2>/dev/null)
            cpu_top=$(top -l 1 -n 5 -o cpu -stats pid,command,cpu 2>/dev/null | tail -n +12)
            mem_top=$(top -l 1 -n 5 -o mem  -stats pid,command,mem 2>/dev/null | tail -n +12)
            echo "[SYSMON ${ts}] load=${load}"
            echo "[SYSMON ${ts}] top-cpu:"; echo "${cpu_top}" | sed 's/^/[SYSMON]   /'
            echo "[SYSMON ${ts}] top-mem:"; echo "${mem_top}" | sed 's/^/[SYSMON]   /'
        else
            load=$(cat /proc/loadavg 2>/dev/null)
            mem=$(awk '/^Mem(Total|Available|Free):/ {printf "%s=%s ", $1, $2}' /proc/meminfo 2>/dev/null)
            cpu_top=$(top -bn1 -o %CPU 2>/dev/null | awk 'NR>=7 && NR<=12')
            echo "[SYSMON ${ts}] load=${load} mem(kB)=${mem}"
            echo "${cpu_top}" | sed 's/^/[SYSMON]   /'
        fi
        sleep 5
    done
) &
SYSMON_PID=$!
trap "kill ${SYSMON_PID} 2>/dev/null || true" EXIT

echo "arguments: $TEST_ARGS"
# TODO KIP-932: Change 0157 to 0155 after after leader migration is fixed
(cd tests && python3 -m trivup.clusters.KafkaCluster $TEST_CONFIGURATION \
 --conf '["group.share.min.record.lock.duration.ms=1000", "transaction.state.log.replication.factor=1", "transaction.state.log.min.isr=1"]' \
 --version "$TEST_KAFKA_GIT_REF" \
 --cpversion "$TEST_CP_VERSION" \
 --cmd "TESTS_SKIP_BEFORE=0157 python run-test-batches.py $TEST_ARGS")
