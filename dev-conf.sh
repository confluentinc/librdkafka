#!/bin/bash
#
# Configure librdkafka for development

set -e
./configure --clean
./configure --enable-werror --disable-optimization \
            --enable-sharedptr-debug #--enable-refcnt-debug
