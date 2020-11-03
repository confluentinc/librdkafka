#!/bin/bash

set -e

cmake \
    -G "MinGW Makefiles" \
    -D CMAKE_INSTALL_PREFIX="$PWD/dest/" \
    -D WITHOUT_WIN32_CONFIG=ON  \
    -D RDKAFKA_BUILD_EXAMPLES=ON \
    -D RDKAFKA_BUILD_TESTS=ON \
    -D WITH_LIBDL=OFF \
    -D WITH_PLUGINS=OFF \
    -D WITH_SASL=OFF \
    -D WITH_SSL=ON \
    -D WITH_ZLIB=OFF \
    -D RDKAFKA_BUILD_STATIC=ON \
    .

$mingw64 mingw32-make
$mingw64 mingw32-make install

# Bundle all the static dependencies with the static lib we just built
mkdir mergescratch
pushd mergescratch
cp /C/tools/msys64/mingw64/lib/libzstd.a ./
cp /C/tools/msys64/mingw64/lib/libcrypto.a ./
cp /C/tools/msys64/mingw64/lib/liblz4.a ./
cp /C/tools/msys64/mingw64/lib/libssl.a ./
cp ../src/librdkafka.a ./
cp ../src-cpp/librdkafka++.a ./librdkafkapp.a
ar -M << EOF
create librdkafka_merge.a
addlib librdkafka.a
addlib libzstd.a
addlib libcrypto.a
addlib liblz4.a
addlib libssl.a
save
end
EOF

ar -M << EOF
create librdkafkapp_merge.a
addlib librdkafka_merge.a
addlib librdkafkapp.a
save
end
EOF

cp ./librdkafka_merge.a ../dest/lib/librdkafka.a
cp ./librdkafkapp_merge.a ../dest/lib/librdkafka++.a
popd
rm -rf ./mergescratch

export PATH="$PWD/dest/bin:/mingw64/bin/:${PATH}"
cd tests
./test-runner.exe -l -Q -p1
