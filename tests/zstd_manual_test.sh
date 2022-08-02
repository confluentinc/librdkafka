#!/bin/bash
#

#
# Manual test (verification) of ZSTD decompression
#

set -e
# Debug what commands are being executed:
#set -x

TOPIC=zstd

# Create topic
${KAFKA_PATH}/bin/kafka-topics.sh --bootstrap-server $BROKERS --create \
	     --topic $TOPIC --partitions 1 --replication-factor 1

# Produce with Kafka
tmpfile=$(mktemp)
# Fill the file with repeated 'a' char (499kb) easy to compress
(printf 'a%.0s' {1..510976} ; printf '\n') > $tmpfile

echo "### Producing 6000 messages with Kafka: from ${tmpfile}"
(for i in {1..6000}; do cat $tmpfile ; done) | ${KAFKA_PATH}/bin/kafka-console-producer.sh \
			     --broker-list $BROKERS \
			     --topic $TOPIC \
           --max-memory-bytes 20971520 \
           --batch-size 10485760 \
           --compression-codec zstd \
           --producer-property max.request.size=512000 \
           --producer-property linger.ms=60000

# Consume with rdkafka
echo ""
echo "### Consuming with librdkafka: expect no decompression errors"
../examples/rdkafka_performance -C -b $BROKERS -t $TOPIC -p 0 -o beginning -e \
          -X message.max.bytes=10240000 \
			    $RDK_ARGS


echo ""
echo "### $TEST_KAFKA_VERSION: Did you see error message containing \"Consume error for topic \"zstd\" [0]\" ?"
