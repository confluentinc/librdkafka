#!/usr/bin/env bash
# rustls backend: static librdkafka linked into a Rust test host that supplies
# rustls-ffi 0.15.7 and installs a stand-in process-default provider.
# Usage: rustls.sh [--fips] [--unit-only]
set -euo pipefail
src=$(cd -- "$(dirname -- "$0")/../.." && pwd)
host_src="$src/tests/tls_backend/rustls_host"
artifacts=${RDKAFKA_TLS_TEST_ARTIFACTS:-${TMPDIR:-/tmp}/librdkafka-tls-tests-$(id -u)}/rustls
fips=0
unit_only=0
for arg in "$@"; do
  case $arg in
    --fips) fips=1 ;;
    --unit-only) unit_only=1 ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done
if [[ -e "$src/config.h" ]]; then
  echo 'Remove configure-generated config.h from this checkout before CMake builds.' >&2
  exit 1
fi
mkdir -p "$artifacts"
venv=${RDKAFKA_TLS_TEST_VENV:-$artifacts/venv}
if [[ ! -x "$venv/bin/python" ]]; then
  python3 -m venv "$venv"
  "$venv/bin/pip" install -r "$src/tests/tls_backend/requirements.txt"
fi

# The header must come from the same rustls-ffi package that the host links.
crate=$(cargo metadata --locked --format-version 1 --manifest-path "$host_src/Cargo.toml" |
  python3 -c 'import json, sys; print(next(p["manifest_path"] for p in json.load(sys.stdin)["packages"] if p["name"] == "rustls-ffi"))')
cmake -S "$src" -B "$artifacts/build" -DRDKAFKA_TLS_BACKEND=rustls \
  -DRUSTLS_INCLUDE_DIR="$(dirname "$crate")/src" -DRDKAFKA_BUILD_STATIC=ON \
  -DWITH_CURL=OFF -DWITH_SASL=OFF -DCMAKE_INSTALL_PREFIX="$artifacts/install"
cmake --build "$artifacts/build" -j "${CMAKE_BUILD_PARALLEL_LEVEL:-4}"
cmake --install "$artifacts/build"
export PKG_CONFIG_PATH="$artifacts/install/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
RDKAFKA_STATIC_LIBS=$(pkg-config --static --libs rdkafka-static)
export RDKAFKA_STATIC_LIBS
export RDKAFKA_CONFIG_DIR="$artifacts/build/generated/dummy"

# librdkafka must not contain rustls-ffi, rustls or AWS-LC definitions.
if nm -g "$artifacts/install/lib/librdkafka.a" 2>/dev/null | grep -v ' U ' |
    grep -Ei ' _?(rustls_|aws_lc|awslc)'; then
  echo 'FAIL: static librdkafka defines rustls or AWS-LC symbols' >&2
  exit 1
fi

variants=(ordinary)
if ((fips)); then variants+=(fips); fi
for variant in "${variants[@]}"; do
  features=()
  if [[ $variant == fips ]]; then features=(--no-default-features --features fips); fi
  CARGO_TARGET_DIR="$artifacts/target-$variant" cargo build --locked --release \
    --manifest-path "$host_src/Cargo.toml" ${features[@]+"${features[@]}"}
  host="$artifacts/target-$variant/release/rdkafka-rustls-host"
  # One rustls-ffi copy: one rustls_version definition and no rustls library.
  count=$(nm "$host" | grep -cE ' [Tt] _?rustls_version$' || true)
  if command -v otool >/dev/null; then
    deps=$(otool -L "$host" | tail -n +2)
  else
    deps=$(ldd "$host")
  fi
  if [[ $count != 1 ]] || grep -i rustls <<<"$deps"; then
    echo "FAIL: $host has $count rustls_version definitions or links a rustls library" >&2
    exit 1
  fi
  # aws-lc-fips-sys builds the FIPS module as a shared library on macOS.
  fips_libs=$(find "$artifacts/target-$variant/release/build" -name 'libaws_lc_fips*' \
    -exec dirname {} \; | sort -u | paste -sd: -)
  for program in policy client cert_buffers fips_cert_buffers handshake_timeout no_provider transport_io; do
    printf '#!/bin/sh\nDYLD_LIBRARY_PATH="%s" LD_LIBRARY_PATH="%s" exec "%s" %s "$@"\n' \
      "$fips_libs" "$fips_libs" "$host" "$program" > "$artifacts/$program-$variant"
    chmod +x "$artifacts/$program-$variant"
  done
done
echo "PASS: one rustls-ffi copy in each host; none in static librdkafka"

focused=(--no-provider "$artifacts/no_provider-ordinary" --policy "$artifacts/policy-ordinary"
  --cert-buffers "$artifacts/cert_buffers-ordinary" --transport "$artifacts/transport_io-ordinary")
if ((fips)); then
  focused+=(--positive-policy "$artifacts/policy-fips" --fips-cert-buffers "$artifacts/fips_cert_buffers-fips")
fi
"$venv/bin/python" "$src/tests/tls_backend/focused.py" "${focused[@]}"
"$venv/bin/python" "$src/tests/tls_backend/timeout_check.py" --client "$artifacts/handshake_timeout-ordinary"
if ((unit_only)); then
  echo 'PASS focused rustls tests; Kafka acceptance intentionally not requested (--unit-only)'
  exit 0
fi
"$venv/bin/python" "$src/tests/tls_backend/acceptance.py" --client "$artifacts/client-ordinary"
if ((fips)); then
  "$venv/bin/python" "$src/tests/tls_backend/acceptance.py" --fips --client "$artifacts/client-fips"
fi
