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

# Run share consumer tests with debug logging enabled for all tests.
# All tests run together (0170, 0171, 0172, 0173+) since 0172 only fails
# intermittently when run alongside other tests.
# TEST_DEBUG=all enables debug output for all share consumer tests.
# Debug logs are output to console and captured in Semaphore job logs.

# Run all share consumer tests together with debug logging
(cd tests && python3 -m trivup.clusters.KafkaCluster $TEST_CONFIGURATION \
 --conf '["group.share.min.record.lock.duration.ms=1000"]' \
 --version "$TEST_KAFKA_GIT_REF" \
 --cpversion "$TEST_CP_VERSION" \
 --cmd "TESTS_SKIP_BEFORE=0170 python run-test-batches.py $TEST_ARGS")
