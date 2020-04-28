#!/bin/bash

set -e

$mingw64 cmake -DCMAKE_MAKE_PROGRAM=mingw32-make -G "MinGW Makefiles" \
      -DCMAKE_INSTALL_PREFIX=$PWD/dest/ \
      -DMINGW_BUILD:BOOL=ON \
      -DWITHOUT_WIN32_CONFIG:BOOL=ON \
      -DRDKAFKA_BUILD_EXAMPLES:BOOL=ON \
      -DRDKAFKA_BUILD_TESTS:BOOL=ON \
      -DWITH_LIBDL:BOOL=OFF \
      -DWITH_PLUGINS:BOOL=OFF \
      -DWITH_SASL:BOOL=ON \
      -DWITH_SSL:BOOL=ON \
      -DWITH_ZLIB:BOOL=OFF \
      -DRDKAFKA_BUILD_STATIC:BOOL=OFF \
      -DCMAKE_WINDOWS_EXPORT_ALL_SYMBOLS:BOOL=TRUE .

$mingw64 mingw32-make
$mingw64 mingw32-make install

export PATH="$PWD/dest/bin:/mingw64/bin/:${PATH}"
cd tests
./test-runner.exe -l -Q -p1
