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

echo "arguments: $TEST_ARGS"

# Run share consumer tests
# Test 0172 is run separately with debug logging enabled to diagnose
# intermittent failures. Debug logs are saved to artifacts if test fails.
TEST_0172_LOG="${PWD}/artifacts/test_0172_debug.log"
TEST_0172_FAILED=0

run_test_0172_with_debug() {
    echo "Running test 0172 with debug logging..."
    cd tests
    # Run 0172 with full debug logging, capture output
    TEST_DEBUG=all TESTS=0172 python3 -m trivup.clusters.KafkaCluster $TEST_CONFIGURATION \
        --version "$TEST_KAFKA_GIT_REF" \
        --cpversion "$TEST_CP_VERSION" \
        --cmd "./run-test.sh -D" 2>&1 | tee "$TEST_0172_LOG"
    TEST_0172_FAILED=${PIPESTATUS[0]}
    cd ..

    if [ $TEST_0172_FAILED -eq 0 ]; then
        echo "Test 0172 passed, removing debug log"
        rm -f "$TEST_0172_LOG"
    else
        echo "Test 0172 FAILED, debug log saved to: $TEST_0172_LOG"
        echo "Upload as artifact for debugging"
    fi
}

# Run all other share consumer tests (skip 0172)
(cd tests && python3 -m trivup.clusters.KafkaCluster $TEST_CONFIGURATION \
 --conf '["group.share.min.record.lock.duration.ms=1000"]' \
 --version "$TEST_KAFKA_GIT_REF" \
 --cpversion "$TEST_CP_VERSION" \
 --cmd "TESTS_SKIP_BEFORE=0170 TESTS=0170,0171,0173- python run-test-batches.py $TEST_ARGS")

# Run test 0172 separately with debug logging
run_test_0172_with_debug

# Exit with failure if 0172 failed
if [ $TEST_0172_FAILED -ne 0 ]; then
    echo "Test 0172 failed with exit code: $TEST_0172_FAILED"
    exit $TEST_0172_FAILED
fi
