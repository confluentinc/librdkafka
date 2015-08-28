#!/bin/bash
#

set -e

ZK=$1
KATOPS=$2
RE=$3

if [[ -z "$RE" ]]; then
    RE="^rdkafkatest_"
fi

if [[ -z "$KATOPS" ]]; then
    echo "Usage: $0 <zookeeper-address> <kafka-topics.sh> [<topic-name-regex>]"
    echo ""
    echo "Deletes all topics matching regex $RE"
    exit 1
fi


echo -n "Collecting list of matching topics... "
TOPICS=$($KATOPS --zookeeper $ZK --list 2>/dev/null | grep "$RE")
echo "${#TOPICS} topics found"

for t in $TOPICS; do
    echo -n "Deleting topic $t... "
    ($KATOPS --zookeeper $ZK --delete --topic "$t" 2>/dev/null && echo "deleted") || echo "failed"
done

echo "Done"

echo -n "Collecting list of matching topics... "
TOPICS=$($KATOPS --zookeeper $ZK --list 2>/dev/null | grep "$RE")
echo "${#TOPICS} topics found"

    
