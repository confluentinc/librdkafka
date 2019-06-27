#!/bin/bash
#

set -e

base_url=$1

if [[ -z $base_url ]]; then
    echo "Usage: $0 <base_url>"
    exit 1
fi

# FIXME: figure out how to extract the CP Version from the base url.
cat >/etc/apt/sources.list.d/Confluent.list <<EOF
deb [arch=amd64] $base_url/deb/5.3 stable main
EOF

apt-get update

apt-get install -y --allow-unauthenticated apt-transport-https librdkafka-dev gcc

gcc /v/check_features.c -o /tmp/check_features -lrdkafka

/tmp/check_features

