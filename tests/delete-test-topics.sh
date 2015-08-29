#!/bin/bash
#

set -eu

if [[ "$1" == "-n" ]]; then
    DO_DELETE=0
    shift
else
    DO_DELETE=1
fi

ZK=$1
KATOPS=$2
RE=$3

if [[ -z "$RE" ]]; then
    RE="^rdkafkatest_"
fi

if [[ -z "$KATOPS" ]]; then
    echo "Usage: $0 [-n] <zookeeper-address> <kafka-topics.sh> [<topic-name-regex>]"
    echo ""
    echo "Deletes all topics matching regex $RE"
    echo ""
    echo "  -n  - Just collect, dont actually delete anything"
    exit 1
fi


echo -n "Collecting list of matching topics... "
TOPICS=$($KATOPS --zookeeper $ZK --list 2>/dev/null | grep "$RE") || true
N_TOPICS=$(echo "$TOPICS" | wc -w)
echo "$N_TOPICS topics found"

[[ $DO_DELETE != 1 ]] && exit 0

for t in $TOPICS; do
    echo -n "Deleting topic $t... "
    ($KATOPS --zookeeper $ZK --delete --topic "$t" 2>/dev/null && echo "deleted") || echo "failed"
done

echo "Done"

echo -n "Collecting list of matching topics... "
TOPICS=$($KATOPS --zookeeper $ZK --list 2>/dev/null | grep "$RE") || true
N_TOPICS=$(echo "$TOPICS" | wc -w)
echo "$N_TOPICS topics found"


    
