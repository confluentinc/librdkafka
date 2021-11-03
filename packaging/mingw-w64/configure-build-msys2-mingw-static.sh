#!/bin/bash

set -ex

echo building
cmake \
    -G "MinGW Makefiles" \
    -D CMAKE_INSTALL_PREFIX="$PWD/dest/" \
    -D RDKAFKA_BUILD_STATIC=ON \
    -D CMAKE_VERBOSE_MAKEFILE:BOOL=ON \
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
cp /C/tools/msys64/mingw64/lib/libz.a ./
cp ../src/librdkafka.a ./

echo "pwd? $PWD"
pwd

# Have to rename because ar won't work with + in the name
cp ../src-cpp/librdkafka++.a ./librdkafkacpp.a
ar -M << EOF
create librdkafka-static.a
addlib librdkafka.a
addlib libzstd.a
addlib libcrypto.a
addlib liblz4.a
addlib libssl.a
addlib libz.a
save
end
EOF

ar -M << EOF
create librdkafkacpp-static.a
addlib librdkafka-static.a
addlib librdkafkacpp.a
save
end
EOF

strip -g ./librdkafka-static.a
strip -g ./librdkafkacpp-static.a
cp ./librdkafka-static.a ../dest/lib/
cp ./librdkafkacpp-static.a ../dest/lib/librdkafka++-static.a
popd
rm -rf ./mergescratch

echo "I'm on $PWD or $(pwd)"
echo "Why is there no output? $PWD" >2
export PATH="$PWD/dest/bin:/mingw64/bin/:${PATH}"
pwd
find . -iname test-runner.exe
cd tests
ls -la
dumpbin /dependents test-runner.exe || echo "nope"
ldd test-runner.exe
./test-runner.exe -l -Q -p1 0000 || echo "failed to call test-runner"
./test-runner.exe --help 2>&1

file ./test-runner.exe
pacman -Qs mingw-w64

echo exit $?

