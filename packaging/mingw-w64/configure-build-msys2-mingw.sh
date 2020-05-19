#!/bin/bash

set -e

$mingw64 cmake \
      -G "MinGW Makefiles" \
      -D'CMAKE_MAKE_PROGRAM=mingw32-make' \
      -D'CMAKE_INSTALL_PREFIX=$PWD/dest/' \
      -D'MINGW_BUILD=ON' \
      -D'WITHOUT_WIN32_CONFIG=ON' \
      -D'RDKAFKA_BUILD_EXAMPLES=ON' \
      -D'RDKAFKA_BUILD_TESTS=ON' \
      -D'WITH_LIBDL=OFF' \
      -D'WITH_PLUGINS=OFF' \
      -D'WITH_SASL=ON' \
      -D'WITH_SSL=ON' \
      -D'WITH_ZLIB=OFF' \
      -D'RDKAFKA_BUILD_STATIC=OFF' \
      -D'CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS=TRUE' .

$mingw64 mingw32-make
$mingw64 mingw32-make install

export PATH="$PWD/dest/bin:/mingw64/bin/:${PATH}"
cd tests
./test-runner.exe -l -Q -p1
