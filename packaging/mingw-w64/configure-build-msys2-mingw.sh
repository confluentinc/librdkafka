#!/bin/bash

set -e

$mingw64 cmake \
      -G "MinGW Makefiles" \
      -DCMAKE_INSTALL_PREFIX="$PWD/dest/" \
      -DWITHOUT_WIN32_CONFIG=ON  \
      -DRDKAFKA_BUILD_EXAMPLES=ON \
      -DRDKAFKA_BUILD_TESTS=ON \
      -DWITH_LIBDL=OFF \
      -DWITH_PLUGINS=OFF \
      -DWITH_SASL=ON \
      -DWITH_SSL=ON \
      -DWITH_ZLIB=OFF \
      -DRDKAFKA_BUILD_STATIC=OFF \
      -DCMAKE_WINDOWS_EXPORT_ALL_SYMBOLS=TRUE .

$mingw64 mingw32-make
$mingw64 mingw32-make install

export PATH="$PWD/dest/bin:/mingw64/bin/:${PATH}"
cd tests
./test-runner.exe -l -Q -p1
