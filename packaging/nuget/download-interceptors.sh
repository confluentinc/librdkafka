#!/bin/bash
#
#

set -e

CPVER=5.0
PLUGINVER=0.11.0

commondir=$PWD/common/p-common__plat-any__arch-any__bldtype-Release

mkdir -p $commondir

echo "Downloading confluent-librdkafka-plugins $PLUGINVER (CP $CPVER)"

# Download plugins zip file for osx and windows
pluginzip=confluent-librdkafka-plugins-${PLUGINVER}.zip

if [[ ! -f ${commondir}/$pluginzip ]]; then
    curl -ls -o ${commondir}/$pluginzip http://packages.confluent.io/archive/${CPVER}/${pluginzip}
    echo "Downloaded ${commondir}/$pluginzip"
else
    echo "${commondir}/$pluginzip already exists"
fi


# Download linux plugins (deb package), extract the library and create a new zip file.

linuxzip=confluent-librdkafka-plugins-linux-${PLUGINVER}.zip
if [[ ! -f ${commondir}/$linuxzip ]]; then
    tmpdir=$(mktemp -d)
    pushd $tmpdir
    curl -ls -o dl.deb https://packages.confluent.io/deb/${CPVER}/pool/main/c/confluent-librdkafka-plugins/confluent-librdkafka-plugins_${PLUGINVER}-1_amd64.deb
    dpkg -x dl.deb tmpdeb
    pushd $(dirname $(find tmpdeb -name monitoring-interceptor.so.1))
    # remove symlink and rename real library to not include SONAME
    rm monitoring-interceptor.so
    mv monitoring-interceptor.so{.1,}
    # Create zip file with just the library
    zip ${commondir}/$linuxzip monitoring-interceptor.so
    popd # tmpdeb/....
    popd # tmpdir
    rm -rf $tmpdir

    echo "Created ${commondir}/$linuxzip"
else
    echo "${commondir}/$linuxzip already exists"
fi
