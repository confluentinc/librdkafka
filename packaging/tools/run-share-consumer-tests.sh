#!/bin/bash
set -e

# Use env vars matching run-all-tests.sh convention,
# with fallback to positional args for backward compatibility.
export TEST_KAFKA_GIT_REF="${TEST_KAFKA_GIT_REF:-${1:-4.3.0}}"
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

# Pass TEST_DEBUG through to the test runner inside trivup's --cmd shell
# (env vars set in this script are not automatically inherited by the
# subprocess that trivup spawns for --cmd). Set TEST_DEBUG in the parent
# env to enable librdkafka debug logging — e.g.
#   TEST_DEBUG=cgrp,fetch,topic,msg,broker ./run-share-consumer-tests.sh
# Empty by default so this is a no-op when not set.
TEST_DEBUG_PASS=""
if [[ -n "${TEST_DEBUG:-}" ]]; then
    TEST_DEBUG_PASS="TEST_DEBUG=${TEST_DEBUG} "
fi

(cd tests && python3 -m trivup.clusters.KafkaCluster $TEST_CONFIGURATION \
 --conf '["group.share.min.record.lock.duration.ms=1000", "transaction.state.log.replication.factor=1", "transaction.state.log.min.isr=1"]' \
 --version "$TEST_KAFKA_GIT_REF" \
 --cpversion "$TEST_CP_VERSION" \
 --cmd "${TEST_DEBUG_PASS}TESTS_SKIP_BEFORE=0155 python run-test-batches.py $TEST_ARGS")
