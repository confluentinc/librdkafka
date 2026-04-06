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

# Run share consumer tests with debug logging enabled only for test 0172.
# All tests run together (0170, 0171, 0172, 0173+) since 0172 only fails
# intermittently when run alongside other tests.
# TEST_DEBUG_0172 enables debug only for test 0172 consumers (not all tests).
# Debug logs are captured and saved to artifacts if any test fails.
DEBUG_LOG="${PWD}/artifacts/test_0172_debug.log"
mkdir -p "${PWD}/artifacts"

# Run all share consumer tests together
# TEST_DEBUG_0172=all enables debug output only for test 0172's consumers
(cd tests && TEST_DEBUG_0172=all python3 -m trivup.clusters.KafkaCluster $TEST_CONFIGURATION \
 --version "$TEST_KAFKA_GIT_REF" \
 --cpversion "$TEST_CP_VERSION" \
 --cmd "TESTS_SKIP_BEFORE=0170 TESTS=0170- python run-test-batches.py $TEST_ARGS" 2>&1 | tee "$DEBUG_LOG")
TEST_RESULT=${PIPESTATUS[0]}

if [ $TEST_RESULT -eq 0 ]; then
    echo "All share consumer tests passed, removing debug log"
    rm -f "$DEBUG_LOG"
else
    echo "Share consumer tests FAILED (exit code: $TEST_RESULT)"
    echo "Debug log saved to: $DEBUG_LOG"
    exit $TEST_RESULT
fi
