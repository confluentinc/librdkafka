#!/bin/sh
#
# ^ NOTE: This needs to be sh, not bash, for alpine compatibility.
#
#
# Build dynamic and statically linked librdkafka libraries useful for
# release artifacts in high-level clients.
#
# Requires docker.
# Supported docker images:
#   alpine:3.16
#   quay.io/pypa/manylinux_2_28_aarch64 (centos8)
#   quay.io/pypa/manylinux_2_28_x86_64  (centos8)
#
# Usage:
# packaging/tools/build-release-artifacts.sh <docker-image> <relative-output-tarball-path.tgz>
#
# The output path must be a relative path and inside the librdkafka directory
# structure.
#

set -e

docker_image=""
extra_pkgs_rpm=""
extra_pkgs_apk=""
expected_features="gzip snappy ssl sasl regex lz4 sasl_gssapi sasl_plain sasl_scram plugins zstd sasl_oauthbearer http oidc"

# Check if we're running on the host or the (docker) build target.
if [ "$1" = "--in-docker" -a $# -eq 2 ]; then
    output="$2"
elif [ $# -eq 2 ]; then
    docker_image="$1"
    output="$2"
else
    echo "Usage: $0 <manylinux-docker-image> <output-path.tgz>"
    exit 1
fi

if [ -n "$docker_image" ]; then
    # Running on the host, spin up the docker builder.
    exec docker run -v "$PWD:/v" $docker_image /v/packaging/tools/build-release-artifacts.sh --in-docker "/v/$output"
    # Only reached on exec error
    exit $?
fi


########################################################################
# Running in the docker instance, this is where we perform the build.  #
########################################################################


# Packages required for building librdkafka (perl is for openssl).

if grep -q alpine /etc/os-release 2>/dev/null ; then
    # Alpine
    apk add \
        bash curl gcc g++ make musl-dev linux-headers bsd-compat-headers git \
        python3 perl patch $extra_pkgs_apk

else
    # CentOS
    yum install -y libstdc++-devel gcc gcc-c++ python3 git perl-IPC-Cmd perl-Pod-Html $extra_pkgs_rpm
fi


# Clone the repo so other builds are unaffected of what we're doing
# and we get a pristine build tree.
git config --system --add safe.directory '/v/.git'
git config --system --add safe.directory '/librdkafka/.git'
git clone /v /librdkafka

cd /librdkafka

# Build librdkafka
./configure \
    --install-deps --source-deps-only --disable-lz4-ext \
    --enable-static --enable-strip $extra_config_args

make -j

# Show library linkage (for troubleshooting) and checksums (for verification)
for lib in src/librdkafka.so.1 src-cpp/librdkafka++.so.1; do
    echo "$0: LINKAGE ${lib}:"
    ldd src/librdkafka.so.1
    echo "$0: SHA256 ${lib}:"
    sha256sum "$lib"
done

# Verify that expected features are indeed built.
features=$(examples/rdkafka_example -X builtin.features)
echo "$0: FEATURES: $features"

missing=""
for f in $expected_features; do
    if ! echo "$features" | grep -q "$f" ; then
        echo "$0: BUILD IS MISSING FEATURE $f"
        missing="${missing} $f"
    fi
done

if [ -n "$missing" ]; then
    exit 1
fi


# Run quick test suite, mark it as CI to avoid time/resource sensitive
# tests to fail in case the worker is under-powered.
CI=true make -C tests run_local_quick


# Install librdkafka and then make a tar ball of the installed files.
mkdir -p /destdir

DESTDIR=/destdir make install

cd /destdir
tar cvzf "$output" .

# Emit output hash so that build logs can be used to verify artifacts later.
echo "$0: SHA256 $output:"
sha256sum "$output"

