#!/bin/bash
#
#
# Generate ApiKey / protocol request defines and rd_kafka_ApiKey2str() fields.
# Cut'n'paste as needed to rdkafka_protocol.h and rdkafka_proto.h
#
#
# Usage:
#   src/generate_proto.sh /path/to/apache-kafka-source

set -e

KAFKA_DIR="$1"

if [[ ! -d $KAFKA_DIR ]]; then
    echo "Usage: $0 <path-to-kafka-source-directory>"
    exit 1
fi

cd "$KAFKA_DIR"

echo "################## Protocol defines (add to rdkafka_protocol.h) ###################"
grep apiKey clients/src/main/resources/common/message/*Request.json | \
    awk '{print $3, $1 }' | \
    sort -n | \
    sed -E -s 's/ cli.*\///' | \
    sed -E 's/\.json:$//' | \
    awk -F, '{print "#define RD_KAFKAP_" $2 " " $1}'
echo "!! Don't forget to update RD_KAFKAP__NUM !!"
echo
echo

echo "################## Protocol names (add to rdkafka_proto.h) ###################"
grep apiKey clients/src/main/resources/common/message/*Request.json | \
    awk '{print $3, $1 }' | \
    sort -n | \
    sed -E -s 's/ cli.*\///' | \
    sed -E 's/\.json:$//' | \
    awk -F, '{print "[RD_KAFKAP_" $2 "] = \"" $2 "\","}'

