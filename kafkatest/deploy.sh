#!/bin/bash
#
# Deploy librdkafka verifiable-client binaries on a Ducktape worker node.
#
# Assumes /librdkafka is the repo, mounted read-write into the container.
#
# The repo is a shared mount across all worker nodes. deploy.sh runs
# once per node (Ducktape calls it before first exec), and the build
# happens in the shared mount. We use two guards to stay safe:
#
#   1. A per-node sentinel (in /tmp, not on the shared mount) prevents
#      re-running deploy.sh on the same node.
#   2. A cross-node shared-mount sentinel (in the shared mount) lets
#      subsequent nodes skip the rebuild entirely if someone else already
#      produced the binaries. This also avoids concurrent `make clean`
#      races when multiple workers launch deploy.sh at the same time.
#
# Build dependencies (apt packages) still need to be installed on each
# node separately; those are per-container, not in the shared mount.

set -euo pipefail

REPO_DIR="${LIBRDKAFKA_DIR:-/librdkafka}"
NODE_SENTINEL="/tmp/librdkafka-deploy.done"
SHARED_SENTINEL="${REPO_DIR}/kafkatest/.built"
LOCK_FILE="${REPO_DIR}/kafkatest/.build-lock"

if [[ -f "${NODE_SENTINEL}" ]]; then
    echo "deploy.sh: node already deployed (${NODE_SENTINEL}); skipping"
    exit 0
fi

# Install build deps. Per-container state, safe to run concurrently.
if [[ "${DEPLOY_SKIP_APT:-0}" != "1" ]]; then
    sudo apt-get update
    sudo apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        libssl-dev \
        libsasl2-dev \
        libz-dev \
        liblz4-dev \
        libzstd-dev \
        libcurl4-openssl-dev \
        python3
fi

cd "${REPO_DIR}"

# Serialize builds across nodes with flock on the shared mount. The
# first node to grab the lock builds; everyone else waits and then
# short-circuits on the shared sentinel.
exec 9>"${LOCK_FILE}"
echo "deploy.sh: acquiring build lock..."
flock 9

if [[ -f "${SHARED_SENTINEL}" ]] && \
   [[ -x "${REPO_DIR}/kafkatest/verifiable_producer/verifiable_producer" ]] && \
   [[ -x "${REPO_DIR}/kafkatest/verifiable_share_consumer/verifiable_share_consumer" ]]
then
    echo "deploy.sh: binaries already built (shared sentinel); skipping build"
else
    # Clean first: a bind-mounted source tree may contain arch-mismatched
    # objects from a developer's macOS build. Incremental make on those
    # yields "ar: librdkafka.a: malformed archive".
    make clean 2>/dev/null || true
    ./configure
    make -j"$(nproc)" libs
    make -j"$(nproc)" -C kafkatest

    for bin in \
        kafkatest/verifiable_producer/verifiable_producer \
        kafkatest/verifiable_share_consumer/verifiable_share_consumer
    do
        if [[ ! -x "${REPO_DIR}/${bin}" ]]; then
            echo "deploy.sh: expected binary not found: ${REPO_DIR}/${bin}" >&2
            exit 1
        fi
    done

    touch "${SHARED_SENTINEL}"
fi

# Lock released when fd 9 closes at script exit.

touch "${NODE_SENTINEL}"
echo "deploy.sh: build complete"