#!/bin/bash
set -e

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <kafka-version> <cp-version> [<consumer_group_protocol>]"
    echo "Example: $0 3.9.0 7.9.0"
    exit 1
fi

KAFKA_VERSION=$1
CP_VERSION=$2
if [ -n "$3" ]; then
    export TEST_CONSUMER_GROUP_PROTOCOL=$3
fi

if [[ "$(uname)" == "Darwin" ]]; then
    # macOS: dependencies come from --source-deps-only, no Linux venv, no ldd.
    CONFIGURE_ARGS="--install-deps --source-deps-only --enable-devel"
    SHARED_LIB_SUFFIX="dylib"
    SHARED_LIB_TOOL="otool -L"
else
    source /home/user/venv/bin/activate
    CONFIGURE_ARGS="--install-deps --enable-werror --enable-devel"
    SHARED_LIB_SUFFIX="so.1"
    SHARED_LIB_TOOL="ldd"
fi

./configure ${CONFIGURE_ARGS}
./packaging/tools/rdutcoverage.sh
make copyright-check
make -j all examples check
echo "Verifying that CONFIGURATION.md does not have manual changes"
git diff --exit-code CONFIGURATION.md
examples/rdkafka_example -X builtin.features
${SHARED_LIB_TOOL} src/librdkafka.${SHARED_LIB_SUFFIX}
${SHARED_LIB_TOOL} src-cpp/librdkafka++.${SHARED_LIB_SUFFIX}
make -j -C tests build
make -C tests run_local_quick
DESTDIR="$PWD/dest" make install
(cd tests && python3 -m trivup.clusters.KafkaCluster --kraft \
 --version ${KAFKA_VERSION} \
 --cpversion ${CP_VERSION} --cmd 'make quick')
