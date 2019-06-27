#!/bin/bash
#

set -e

base_url=$1

if [[ -z $base_url ]]; then
    echo "Usage: $0 <base_url>"
    exit 1
fi

# FIXME: figure out how to extract the CP Version from the base url.
cat >/etc/yum.repos.d/Confluent.repo <<EOF
[Confluent.dist]
name=Confluent repository (dist)
baseurl=$base_url/rpm/5.3/7
gpgcheck=0
gpgkey=$base_url/rpm/5.3/archive.key
enabled=1
[Confluent]
name=Confluent repository
baseurl=$base_url/rpm/5.3
gpgcheck=0
gpgkey=$base_url/rpm/5.3/archive.key
enabled=1
EOF

yum install -y librdkafka-devel gcc

gcc /v/check_features.c -o /tmp/check_features -lrdkafka

/tmp/check_features

