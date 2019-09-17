#!/bin/bash
#

set -e

prefix=$1

tmpdir=$(mktemp -d)
pushd $tmpdir

ASTYLE_URL=https://github.com/edenhill/astyle/archive/master.tar.gz
curl -q -L $ASTYLE_URL | tar xz --strip-components=1 -f -

pushd build/gcc
make -j
make prefix=$prefix install
popd
popd
rm -rf "$tmpdir"

