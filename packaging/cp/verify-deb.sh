#!/bin/bash
#

set -e

base_url=$1
version=$2

if [[ -z $base_url ]]; then
    echo "Usage: $0 <base_url>"
    exit 1
fi

apt-get update
apt-get install -y apt-transport-https wget gnupg2 lsb-release

# apt-key is deprecated and removed in newer distros, use a signed-by keyring
keyring=/usr/share/keyrings/confluent-archive-keyring.gpg
wget -qO - ${base_url}/deb/archive.key | gpg --dearmor > $keyring

release=$(lsb_release -cs)
cat >/etc/apt/sources.list.d/Confluent.list <<EOF
deb [signed-by=$keyring] $base_url/deb ${release} main
EOF

apt-get update
# libc6-dev is explicit: since Ubuntu 26.04 the gcc metapackage no longer pulls it
apt-get install -y librdkafka-dev gcc libc6-dev

gcc /v/check_features.c -o /tmp/check_features -lrdkafka

/tmp/check_features $version

# FIXME: publish plugins in newer versions
# apt-get install -y confluent-librdkafka-plugins
#/tmp/check_features plugin.library.paths monitoring-interceptor
