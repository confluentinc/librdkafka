#!/bin/bash

testresultConfluentProducerConsumer=$(mktemp)
testresultConfluentCtp=$(mktemp)
testresultKjsProducerConsumer=$(mktemp)
testresultKjsCtp=$(mktemp)

MODE=confluent MESSAGE_COUNT=500000 node performance-consolidated.js --create-topics --consumer --producer 2>&1 | tee "$testresultConfluentProducerConsumer"
MODE=kafkajs MESSAGE_COUNT=500000 node performance-consolidated.js --create-topics --consumer --producer 2>&1 | tee "$testresultKjsProducerConsumer"
MODE=confluent MESSAGE_COUNT=5000 node performance-consolidated.js --create-topics --ctp 2>&1 | tee "$testresultConfluentCtp"
MODE=kafkajs MESSAGE_COUNT=5000 node performance-consolidated.js --create-topics --ctp 2>&1 | tee "$testresultKjsCtp"

producerConfluent=$(grep "=== Producer Rate:" "$testresultConfluentProducerConsumer" | cut -d':' -f2 | tr -d ' ')
consumerConfluent=$(grep "=== Consumer Rate:" "$testresultConfluentProducerConsumer" | cut -d':' -f2 | tr -d ' ')
ctpConfluent=$(grep "=== Consume-Transform-Produce Rate:" "$testresultConfluentCtp" | cut -d':' -f2 | tr -d ' ')
producerKjs=$(grep "=== Producer Rate:" "$testresultKjsProducerConsumer" | cut -d':' -f2 | tr -d ' ')
consumerKjs=$(grep "=== Consumer Rate:" "$testresultKjsProducerConsumer" | cut -d':' -f2 | tr -d ' ')
ctpKjs=$(grep "=== Consume-Transform-Produce Rate:" "$testresultKjsCtp" | cut -d':' -f2 | tr -d ' ')

echo "Producer rates: confluent $producerConfluent, kafkajs $producerKjs"
echo "Consumer rates: confluent $consumerConfluent, kafkajs $consumerKjs"
echo "CTP rates: confluent $ctpConfluent, kafkajs $ctpKjs"

errcode=0

# Compare against KJS
if [[ $(echo "$producerConfluent < $producerKjs * 70 / 100" | bc -l) -eq 1 ]]; then
  echo "Producer rates differ by more than 30%: confluent $producerConfluent, kafkajs $producerKjs"
  errcode=1
fi

if [[ $(echo "$consumerConfluent < $consumerKjs * 70 / 100" | bc -l) -eq 1 ]]; then
  echo "Consumer rates differ by more than 30%: confluent $consumerConfluent, kafkajs $consumerKjs"
  errcode=1
fi

if [[ $(echo "$ctpConfluent < $ctpKjs * 70 / 100" | bc -l) -eq 1 ]]; then
  echo "CTP rates differ by more than 30%: confluent $ctpConfluent, kafkajs $ctpKjs"
  errcode=1
fi

# Compare against numbers set within semaphore config
TARGET_PRODUCE="${TARGET_PRODUCE_PERFORMANCE:-35}"
TARGET_CONSUME="${TARGET_CONSUME_PERFORMANCE:-18}"
TARGET_CTP="${TARGET_CTP_PERFORMANCE:-0.02}"

if [[ $(echo "$producerConfluent < $TARGET_PRODUCE" | bc -l) -eq 1 ]]; then
  echo "Confluent producer rate is below target: $producerConfluent"
  errcode=1
fi

if [[ $(echo "$consumerConfluent < $TARGET_CONSUME" | bc -l) -eq 1 ]]; then
  echo "Confluent consumer rate is below target: $consumerConfluent"
  errcode=1
fi

if [[ $(echo "$ctpConfluent < $TARGET_CTP" | bc -l) -eq 1 ]]; then
  echo "Confluent CTP rate is below target: $ctpConfluent"
  errcode=1
fi

exit $errcode

