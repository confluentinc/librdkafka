#!/bin/bash
set -euo pipefail

AWS_LC_FIPS_REF=${AWS_LC_FIPS_REF:-AWS-LC-FIPS-3.3.0}
AWS_LC_FIPS_REPOSITORY=${AWS_LC_FIPS_REPOSITORY:-https://github.com/aws/aws-lc.git}

work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT

git clone --depth 1 --branch "$AWS_LC_FIPS_REF" \
    "$AWS_LC_FIPS_REPOSITORY" "$work_dir/aws-lc"

# Warnings from the external dependency must not affect librdkafka's build.
cmake -S "$work_dir/aws-lc" -B "$work_dir/aws-lc-build" \
    -DCMAKE_BUILD_TYPE=Debug \
    "-DCMAKE_C_FLAGS=-ffunction-sections -fdata-sections -fPIC -w" \
    -DCMAKE_INSTALL_PREFIX="$work_dir/aws-lc-install" \
    -DBUILD_LIBSSL=ON \
    -DBUILD_SHARED_LIBS=OFF \
    -DBUILD_TESTING=OFF \
    -DBUILD_TOOL=OFF \
    -DFIPS=1
cmake --build "$work_dir/aws-lc-build" --parallel
cmake --install "$work_dir/aws-lc-build"

aws_lc_libcrypto=$(find "$work_dir/aws-lc-install" -name libcrypto.a -print -quit)
if [[ -z "$aws_lc_libcrypto" ]]; then
    echo "AWS-LC FIPS build did not produce libcrypto.a" >&2
    exit 1
fi
aws_lc_lib_dir=$(dirname "$aws_lc_libcrypto")
aws_lc_include_dir="$work_dir/aws-lc-install/include"

CPPFLAGS="-I$aws_lc_include_dir" \
CFLAGS="-Werror=implicit-function-declaration" \
LDFLAGS="-L$aws_lc_lib_dir" \
PKG_CONFIG_PATH="$aws_lc_lib_dir/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}" \
./configure --enable-ssl --disable-curl --disable-gssapi --disable-zlib \
    --disable-zstd --disable-lz4-ext --disable-regex-ext --enable-devel
make --jobs libs

cc -I"$aws_lc_include_dir" -I"$PWD/src" \
    "$PWD/packaging/tools/aws-lc-fips-check.c" \
    -L"$PWD/src" -Wl,-rpath,"$PWD/src" -lrdkafka \
    -L"$aws_lc_lib_dir" -Wl,-Bstatic -lcrypto -Wl,-Bdynamic -lpthread -ldl \
    -o "$work_dir/aws-lc-fips-check"
"$work_dir/aws-lc-fips-check"

if readelf -d src/librdkafka.so.1 | \
    grep -E 'Shared library: \[lib(ssl|crypto)\.so' >/dev/null; then
    echo "librdkafka unexpectedly links to a shared OpenSSL library" >&2
    exit 1
fi

for symbol in BORINGSSL_integrity_test FIPS_mode HMAC; do
    if ! nm src/librdkafka.so.1 | \
        grep -E "[[:space:]]${symbol}$" >/dev/null; then
        echo "librdkafka does not contain AWS-LC FIPS symbol $symbol" >&2
        exit 1
    fi
done
