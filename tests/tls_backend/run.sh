#!/usr/bin/env bash
# One entry point: builds OpenSSL and no-TLS libraries, checks policy and
# ownership, runs real Kafka, then runs the rustls host suite (rustls.sh).
# Usage: run.sh [--unit-only] [--rustls-fips]
set -euo pipefail
src=$(cd -- "$(dirname -- "$0")/../.." && pwd)
artifacts=${RDKAFKA_TLS_TEST_ARTIFACTS:-${TMPDIR:-/tmp}/librdkafka-tls-tests-$(id -u)}
mkdir -p "$artifacts"
unit_only=0
rustls_args=()
for arg in "$@"; do
  case $arg in
    --unit-only) unit_only=1; rustls_args+=(--unit-only) ;;
    --rustls-fips) rustls_args+=(--fips) ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done
if [[ -e "$src/config.h" ]]; then
  echo 'Remove configure-generated config.h from this checkout before CMake builds.' >&2
  exit 1
fi
python3 -m venv "$artifacts/venv"
"$artifacts/venv/bin/pip" install -r "$src/tests/tls_backend/requirements.txt"
policy_args=()
cert_args=()
clients=()
timeout_clients=()
for backend in openssl none; do
  build="$artifacts/$backend"
  cmake -S "$src" -B "$build" -DRDKAFKA_TLS_BACKEND="$backend" \
    -DWITH_CURL=OFF -DWITH_SASL=OFF -DRDKAFKA_BUILD_TESTS=OFF -DRDKAFKA_BUILD_EXAMPLES=OFF
  cmake --build "$build" -j "${CMAKE_BUILD_PARALLEL_LEVEL:-4}"
  common=(-Wall -Wextra -Wno-ignored-qualifiers -I "$src/src" -L "$build/src" "-Wl,-rpath,$build/src")
  "${CC:-cc}" "${common[@]}" "$src/tests/tls_policy.c" -lrdkafka -o "$artifacts/policy-$backend"
  policy_args+=(--policy "$artifacts/policy-$backend")
  if [[ $backend != none ]]; then
    "${CC:-cc}" "${common[@]}" "$src/tests/tls_backend/client.c" -lrdkafka -o "$artifacts/client-$backend"
    "${CC:-cc}" "${common[@]}" "$src/tests/tls_backend/cert_buffers.c" -lrdkafka -o "$artifacts/cert-$backend"
    clients+=(--client "$artifacts/client-$backend")
    "${CC:-cc}" "${common[@]}" "$src/tests/tls_backend/handshake_timeout.c" -lrdkafka -o "$artifacts/timeout-$backend"
    timeout_clients+=(--client "$artifacts/timeout-$backend")
    cert_args+=(--cert-buffers "$artifacts/cert-$backend")
  fi
done
"$artifacts/venv/bin/python" "$src/tests/tls_backend/focused.py" "${policy_args[@]}" "${cert_args[@]}"
"$artifacts/venv/bin/python" "$src/tests/tls_backend/timeout_check.py" "${timeout_clients[@]}"
if [[ -n ${RDKAFKA_TEST_FIPS_CONFIG:-} ]]; then
  "${CC:-cc}" -shared -fPIC "$src/tests/tls_fips_selftest_fail.c" -o "$artifacts/selftest-fail.so"
  "$artifacts/venv/bin/python" "$src/tests/tls_backend/fips_checks.py" \
    --policy "$artifacts/policy-openssl" --config "$RDKAFKA_TEST_FIPS_CONFIG" \
    --selftest-failure-library "$artifacts/selftest-fail.so"
fi
if ((!unit_only)); then
  "$artifacts/venv/bin/python" "$src/tests/tls_backend/acceptance.py" "${clients[@]}"
  if [[ -n ${RDKAFKA_TEST_FIPS_CONFIG:-} ]]; then
    "$artifacts/venv/bin/python" "$src/tests/tls_backend/acceptance.py" --fips \
      --client "$artifacts/client-openssl"
  fi
fi
RDKAFKA_TLS_TEST_ARTIFACTS="$artifacts" RDKAFKA_TLS_TEST_VENV="$artifacts/venv" \
  "$src/tests/tls_backend/rustls.sh" ${rustls_args[@]+"${rustls_args[@]}"}
if ((unit_only)); then
  echo 'PASS focused tests; Kafka acceptance intentionally not requested (--unit-only)'
fi
