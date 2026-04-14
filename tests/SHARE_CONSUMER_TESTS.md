# Share Consumer Integration Tests

## 0170-share_consumer_subscription.c

- Subscribe to a single topic, produce messages, consume and verify all received
- Subscribe then unsubscribe, verify no messages received after unsubscribe
- Call subscribe multiple times with same topics, verify no duplicate subscriptions
- Call unsubscribe multiple times, verify no error on repeated unsubscribe
- Incrementally add topics to existing subscription (subscribe to 2, then add 2 more)
- Subscribe to topic before it exists, create topic later, verify messages received
- Poll an empty topic, verify no messages and no errors
- Poll without any subscription, verify proper error handling
- Subscribe, consume some, unsubscribe, verify no more messages received
- Delete a topic while consumer is subscribed, verify graceful handling
- Rapidly update subscriptions (subscribe/unsubscribe in quick succession)
- Two consumers with overlapping subscriptions share messages from common topics
- Subscribe to 15 topics simultaneously to trigger multiple ShareFetch responses
- Verify share.auto.offset.reset=earliest receives messages produced before subscription
- Verify share.auto.offset.reset=latest (default) skips messages produced before subscription

## 0171-share_consumer_consume.c

- 1 consumer, 1 topic, 1 partition consuming 1000 messages
- 1 consumer, 1 topic, 3 partitions consuming 1500 messages
- 1 consumer, 2 topics, 1 partition each consuming 1000 messages
- 1 consumer, 3 topics, 2 partitions each consuming 3000 messages
- 2 consumers sharing 1 topic, 1 partition (cooperative consumption)
- 2 consumers sharing 1 topic, 4 partitions
- 3 consumers sharing 2 topics, 3 partitions each
- High volume: 10,000 messages on single partition
- High volume: 50,000 messages across 5 partitions
- 15 topics with 1 partition each (triggers multiple fetch responses)
- 10 topics with 2 partitions each
- 20 rapid produce-consume cycles (500 msgs each) testing state transitions
- Poll empty topic first, then produce and consume (empty-to-non-empty transition)
- Produce to only partitions 0,2,4 of 5-partition topic (sparse partition handling)
- Consumer closes without ack, verify redelivery after acquisition lock expiry (15s)

## 0172-share_consumer_acknowledge.c

- RELEASE causes records to be redelivered to same or other consumer
- REJECT permanently marks records as processed, no redelivery
- ACCEPT commits records, no redelivery
- Acknowledge with NULL message returns INVALID_ARG error
- Acknowledge with NULL rkshare returns appropriate error
- Acknowledge with invalid type (99 or GAP/0) returns INVALID_ARG error
- RELEASE then override with REJECT on same offset, final state is REJECT (no redelivery)
- RELEASE same record 5 times (max attempts), verify 6th delivery does not occur
- 5000 messages with random ACCEPT/RELEASE/REJECT on 1 topic, 1 partition
- 5000 messages with random acks across 4 topics, 1 partition each
- 5000 messages with random acks on 1 topic, 4 partitions
- 5000 messages with random acks across 2 topics, 2 partitions each
- 10,000 messages with random acks across 15 topics, 1 partition each
- 10,000 messages with random acks across 15 topics, 2 partitions each
- 8,000 messages with random acks across 8 topics, 4 partitions each
- 10,000 messages with random acks on 1 topic, 8 partitions
- 15,000 messages with random acks across 10 topics, 3 partitions each

## 0173-share_consumer_commit_async.c

- Implicit mode: Consumer 1 calls commit_async, Consumer 2 verifies no offset overlap
- Explicit mode: Consumer 1 ACCEPTs and calls commit_async, Consumer 2 verifies no overlap
- Mixed acks (~50% ACCEPT, ~40% RELEASE, ~10% REJECT) with commit_async after each batch
- commit_async across 10 topics x 5 partitions over 20 rounds
- 5 rounds alternating between commit_async and letting consume_batch piggyback acks
- 3 rounds of produce-consume with mixed acks and inline redelivery handling
- commit_async with no pending acks returns NULL (no error)
- Multiple consecutive commit_async calls, verify no ack duplication or loss
- commit_async between two produce batches
- All RELEASE acks, verify second consumer receives all as redeliveries
- All REJECT acks, verify second consumer receives nothing
- commit_async after each individual record acknowledgement (fine-grained)
- Mock broker test for inflight acknowledgement caching behavior

## 0174-share_consumer_concurrency.c

**Threading model:** Each producer runs in its own thread (`thrd_create`), producing messages in round-robin across topics/partitions. Consumers run in the main thread, polling in round-robin. Shared state is protected by mutex. Producer threads update `total_produced` atomically, main thread tracks `total_consumed`.

- 1 producer thread, 1 consumer thread, 1 topic, 1 partition (baseline)
- 1 producer, 4 consumers competing for 4 partitions (fan-out)
- 4 producers, 1 consumer aggregating from 4 partitions (fan-in)
- 4 producers, 4 consumers on 4 partitions (symmetric)
- 2 producers, 2 consumers across 3 topics, 2 partitions each
- 2 producers, 2 consumers across 8 topics, 1 partition each
- 1 producer, 8 consumers on single partition (high contention)
- 2 producers, 6 consumers on 2 partitions (3:1 consumer-to-partition ratio)
- 4 producers, 4 consumers with explicit acknowledgement mode
- Consumers start polling before producers start producing (staggered start)
- 4 producers, 4 consumers, 20,000 messages (high volume stress)
- 4 producers, 4 consumers, 8 partitions (high volume many partitions)
- 8 producers, 2 consumers (producer-heavy asymmetric)
- 2 producers, 8 consumers (consumer-heavy asymmetric)
- 4 topics with varied partition counts (1,2,3,4 partitions)
- 10 producers, 10 consumers, 4 topics (maximum concurrency)
- 4 producers (fast), 2 consumers with 50ms delay (slow consumers)
- 2 producers with 50ms delay (slow), 4 consumers (fast consumers)

## 0175-share_consumer_groups.c

- 2 share groups consuming same topic independently, each receives all 100 messages
- 3 share groups with 2 consumers each, all independently consume same topic
- 5 share groups each with 1 consumer, all independently consume same topic
- Group A starts first, Group B joins later, both eventually receive all messages
