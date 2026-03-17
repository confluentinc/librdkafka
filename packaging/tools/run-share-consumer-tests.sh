#!/bin/bash
set -e

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <kafka-version> <cp-version>"
    echo "Example: $0 4.2.0 8.0.0"
    exit 1
fi

KAFKA_VERSION=$1
CP_VERSION=$2

if [[ "$(uname)" == "Darwin" ]]; then
    CONFIGURE_ARGS="--install-deps --source-deps-only"
else
    source /home/user/venv/bin/activate
    CONFIGURE_ARGS="--install-deps --enable-werror"
fi
./configure ${CONFIGURE_ARGS}
make -j all
make -j -C tests build
(cd tests && python3 -m trivup.clusters.KafkaCluster --kraft \
 --version ${KAFKA_VERSION} \
 --cpversion ${CP_VERSION} --cmd 'TESTS_SKIP_BEFORE=0170 ./run-test.sh')
