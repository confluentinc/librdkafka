#!/bin/bash
#
# Configure librdkafka for development

set -e
./configure --clean
#export CFLAGS='-std=c99 -pedantic -Wshadow'
#export CXXFLAGS='-std=c++98 -pedantic'
./configure --enable-devel --enable-werror --disable-optimization
            #--enable-sharedptr-debug #--enable-refcnt-debug
