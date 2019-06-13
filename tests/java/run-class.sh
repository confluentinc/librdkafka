#!/bin/bash
#

if [[ -z $KAFKA_PATH ]]; then
    echo "$0: requires \$KAFKA_PATH to point to the kafka release top directory"
    exit 1
fi

CLASSPATH=. ${KAFKA_PATH}/bin/kafka-run-class.sh "$@"

