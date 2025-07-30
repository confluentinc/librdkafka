#!/bin/sh
set -e

build_tool="$1"
docker_image="$2"

if [ -n "$docker_image" ]; then
    echo "Running in docker image: $docker_image"
    # Running on the host, spin up the docker builder.
    exec docker run -v "$PWD:/v" -w /v $docker_image ./packaging/tools/build-configurations-checks.sh $build_tool
    # Only reached on exec error
    exit $?
fi

if [ -z "$build_tool" ]; then
    # Default to using make if no build tool is specified.
    build_tool="make"
fi

if grep -q alpine /etc/os-release 2>/dev/null ; then
    # Alpine
    apk add \
        bash gcc g++ make $build_tool git bsd-compat-headers
fi

# Clone the repo so other builds are unaffected of what we're doing
# and we get a pristine build tree.
git config --system --add safe.directory '/v/.git'
git config --system --add safe.directory '/librdkafka/.git'
git clone /v /librdkafka

cd /librdkafka

# Disable all flags to make sure it
# compiles correctly in all cases
if [ "$build_tool" = "make" ]; then
./configure --disable-ssl --disable-gssapi \
--disable-curl --disable-zlib \
--disable-zstd --disable-lz4-ext --disable-regex-ext \
--disable-c11threads --disable-syslog \
--enable-werror --enable-devel
cat ./config.h
else
cmake -DWITH_SSL=OFF -DWITH_SASL_CYRUS=OFF \
 -DWITH_CURL=OFF -DWITH_ZLIB=OFF \
 -DWITH_ZSTD=OFF -DHAVE_REGEX=OFF -DWITH_C11THREADS=OFF \
 -DWITH_LIBDL=OFF
cat ./generated/config.h
fi
make -j

export CI=true
if [ "$build_tool" = "make" ]; then
make -j -C tests run_local_quick
else
ctest -VV -R RdKafkaTestBrokerLessQuick --output-on-failure
fi
