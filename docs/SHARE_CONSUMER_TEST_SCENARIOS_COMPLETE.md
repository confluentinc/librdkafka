# Share Consumer Integration Test Scenarios - Complete Reference

This document contains all integration test scenarios for Kafka Share Consumer functionality from the clients module. Use this as a reference for implementing similar test coverage.

**Source Files:**
- `clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/ShareConsumerTest.java`
- `clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/ShareConsumerRackAwareTest.java`

---

## Table of Contents

1. [Basic Subscription Tests](#1-basic-subscription-tests)
2. [Single Consumer Single Partition Tests](#2-single-consumer-single-partition-tests)
3. [Multiple Consumers Same Group Tests](#3-multiple-consumers-same-group-tests)
4. [Multiple Groups Tests](#4-multiple-groups-tests)
5. [Share Auto Offset Reset Tests](#5-share-auto-offset-reset-tests)
6. [Isolation Level Tests](#6-isolation-level-tests)
7. [Topic Lifecycle Tests](#7-topic-lifecycle-tests)
8. [Coordinator Movement Tests](#8-coordinator-movement-tests)
9. [Acquire Mode Tests](#9-acquire-mode-tests)
10. [Throttled Delivery Tests](#10-throttled-delivery-tests)
11. [Share Partition Lag Tests](#11-share-partition-lag-tests)
12. [Group Configuration Tests](#12-group-configuration-tests)
13. [Rack Aware Assignment Tests](#13-rack-aware-assignment-tests)
14. [Renew Acknowledgement Tests](#14-renew-acknowledgement-tests)
15. [Test Combinations Matrix](#15-test-combinations-matrix)

---

## 1. Basic Subscription Tests

Tests that verify the fundamental subscription and poll lifecycle for share consumers.

### testPollNoSubscribeFails
Tests that calling poll() without subscribing to any topics throws an IllegalStateException.

### testSubscribeAndPollNoRecords
Tests that subscribing and polling with no records in the topic returns 0 records successfully.

### testSubscribePollUnsubscribe
Tests the complete lifecycle of subscribe -> poll -> unsubscribe, verifying subscription state changes correctly.

### testSubscribePollSubscribe
Tests subscribing to a topic, polling, and then subscribing again to the same topic works without errors.

### testSubscribeUnsubscribePollFails
Tests that unsubscribing and then calling poll() throws an IllegalStateException.

### testSubscribeSubscribeEmptyPollFails
Tests that subscribing with an empty topic set and then polling throws an IllegalStateException.

### testAcknowledgementSentOnSubscriptionChange
Tests that when subscription changes (e.g., from one topic to another), pending acknowledgements are correctly sent.

---

## 2. Single Consumer Single Partition Tests

Tests that verify basic consumption behavior with a single share consumer and a single partition.

### testSubscriptionAndPoll
Tests basic subscription and polling with a single consumer consuming from one partition. Verifies that one record produced is consumed successfully.

### testSubscriptionAndPollMultiple
Tests subscription and polling with a single consumer consuming multiple records produced sequentially to one partition.

### testPollRecordsGreaterThanMaxBytes
Tests that records larger than FETCH_MAX_BYTES_CONFIG are still fetched correctly when the record size exceeds the configured max bytes.

### testHeaders
Tests that record headers are preserved correctly when producing and consuming messages via share consumer. Verifies header order is maintained.

### testHeadersSerializerDeserializer
Tests custom serializers and deserializers work correctly with share consumer for headers.

### testMaxPollRecords
Tests that the max.poll.records configuration is respected, limiting records per poll call (in implicit acknowledgement mode).

### testControlRecordsSkipped
Tests that control records (transaction markers, etc.) are automatically skipped and not returned to the share consumer.

### testFetchRecordLargerThanMaxPartitionFetchBytes
Tests consuming records that are larger than max.partition.fetch.bytes configuration.

### testAcquisitionLockTimeoutOnConsumer
Tests the behavior when acquisition lock timeout occurs on a consumer, verifying records are released for redelivery.

### testWakeupWithFetchedRecordsAvailable
Tests that wakeup() throws WakeupException and subsequent poll() can successfully retrieve available records.

### testPollThrowsInterruptExceptionIfInterrupted
Tests that poll() throws InterruptException when the consumer thread is interrupted.

### testSubscribeOnInvalidTopicThrowsInvalidTopicException
Tests that subscribing to an invalid topic name throws InvalidTopicException.

---

## 3. Multiple Consumers Same Group Tests

Tests that verify consumption behavior when multiple share consumers are part of the same share group.

### testMultipleConsumersInGroupSequentialConsumption
Tests two consumers in the same group consuming 2000 messages sequentially. Verifies that all messages are consumed exactly once across both consumers polling alternately.

### testMultipleConsumersInGroupConcurrentConsumption
Tests 4 consumers in the same group consuming 20000 messages (4 producers x 5000 messages) concurrently. Uses CompletableFuture for parallel execution. Verifies total consumed equals total produced.

### testConsumerCloseInGroupSequential
Tests that when one consumer closes, its unacknowledged records are released and picked up by another consumer in the same group. Consumer1 consumes all records, closes (releasing last poll batch), Consumer2 consumes the released records.

### testMultipleConsumersInGroupFailureConcurrentConsumption
Tests failure recovery with 4 consumers where one "failing" consumer polls and immediately closes. Verifies the remaining consumers successfully consume all 20000 messages.

### testSharePartitionLagForMultipleShareConsumers
Tests share partition lag calculation with two consumers in the same group. Verifies lag is correctly computed as records are consumed and acknowledged by different consumers.

### testFetchWithThrottledDeliveryBatchesMultipleConsumers
Tests throttled delivery behavior with two consumers in the same group with different max.poll.records settings. Verifies records are distributed and throttled correctly across consumers.

---

## 4. Multiple Groups Tests

Tests that verify consumption behavior when multiple share groups consume from the same topic independently.

### testMultipleConsumersWithDifferentGroupIds
Tests two consumers in different share groups (group1 and group2) consuming from the same topic. Both consumers should receive all records independently since they are in separate groups.

### testMultipleConsumersInMultipleGroupsConcurrentConsumption
Tests 3 share groups (group1, group2, group3) each with 2 consumers, concurrently consuming 8000 messages. Each group should independently consume all messages. Uses CompletableFuture for parallel execution.

### testShareAutoOffsetResetMultipleGroupsWithDifferentValue
Tests two share groups with different share.auto.offset.reset values (group1=earliest, group2=latest). Verifies each group starts consuming from its configured offset reset position.

---

## 5. Share Auto Offset Reset Tests

Tests that verify the share.auto.offset.reset configuration behavior for share consumers.

### testShareAutoOffsetResetDefaultValue
Tests the default value of share.auto.offset.reset which is "latest". Records produced before share partition initialization are not consumed; only records produced after are consumed.

### testShareAutoOffsetResetEarliest
Tests share.auto.offset.reset set to "earliest". Consumer should consume all messages present on the partition from the beginning.

### testShareAutoOffsetResetEarliestAfterLsoMovement
Tests that with share.auto.offset.reset=earliest, after log start offset (LSO) movement due to record deletion, consumers start from the new LSO.

### testShareAutoOffsetResetByDuration
Tests share.auto.offset.reset with duration-based configuration (e.g., "by_duration:PT1H"). Consumer should only receive messages from the last hour based on message timestamps.

### testShareAutoOffsetResetByDurationInvalidFormat
Tests that invalid duration formats (e.g., "by_duration:1h" instead of ISO-8601 format) or negative durations throw InvalidConfigurationException.

---

## 6. Isolation Level Tests

Tests that verify share.isolation.level configuration behavior with transactional producers.

### testReadCommittedIsolationLevel
Tests share.isolation.level set to "read_committed". Consumer should only receive records from committed transactions; aborted transaction records are filtered out.

### testReadUncommittedIsolationLevel
Tests share.isolation.level set to "read_uncommitted". Consumer receives all records including those from aborted transactions.

### testAlterReadUncommittedToReadCommittedIsolationLevel
Tests dynamically altering isolation level from read_uncommitted to read_committed while consuming. Records consumed before the change include aborted transactions; records consumed after only include committed transactions.

### testAlterReadCommittedToReadUncommittedIsolationLevelWithReleaseAck
Tests altering isolation level from read_committed to read_uncommitted with RELEASE acknowledgement. Released records are re-delivered according to the new isolation level setting.

### testAlterReadCommittedToReadUncommittedIsolationLevelWithRejectAck
Tests altering isolation level from read_committed to read_uncommitted with REJECT acknowledgement. Rejected records are not re-delivered; subsequent records follow the new isolation level.

---

## 7. Topic Lifecycle Tests

Tests that verify share consumer behavior when topics are created or deleted during consumption.

### testSubscriptionFollowedByTopicCreation
Tests subscribing to a non-existent topic and then creating it. Consumer should automatically start consuming from the newly created topic once metadata is synced.

### testSubscriptionAndPollFollowedByTopicDeletion
Tests consuming from multiple topics where one topic is deleted during consumption. Consumer should handle topic deletion gracefully and continue consuming from remaining subscribed topics.

### testLsoMovementByRecordsDeletion
Tests that when records are deleted (LSO moves forward), the share consumer correctly starts from the new LSO and only consumes available records.

### testCommitSyncFailsForDeletedTopic
Tests that commitSync() fails appropriately when the topic has been deleted, returning proper exception for the deleted topic partition.

---

## 8. Coordinator Movement Tests

Tests that verify share consumer behavior during coordinator (group coordinator or share coordinator) failover.

### testShareConsumerAfterCoordinatorMovement
Tests share consumer resilience during share coordinator failover. With 3 brokers and 3 partitions, the test:
- Produces records continuously
- Consumes records concurrently
- Shuts down the broker hosting the share coordinator
- Verifies coordinator moves to another broker
- Verifies all produced records are eventually consumed (at-least-once delivery)

### testConsumerCloseOnBrokerShutdown
Tests that consumer.close() completes within a reasonable time (5 seconds) when the broker is already shutdown, rather than waiting for the full configured timeout.

### testSharePartitionLagOnGroupCoordinatorMovement
Tests that share partition lag is correctly computed after group coordinator movement. With 3 brokers and replicated topics, verifies lag calculation remains accurate after coordinator failover.

### testSharePartitionLagOnShareCoordinatorMovement
Tests that share partition lag is correctly computed after share coordinator movement. Similar to group coordinator test but specifically for share coordinator failover.

---

## 9. Acquire Mode Tests

Tests that verify different share acquire modes (BATCH_OPTIMIZED vs RECORD_LIMIT).

### testPollInBatchOptimizedMode
Tests share.acquire.mode set to "batch_optimized". In this mode, max.poll.records is ignored and all available records in the batch are returned (10 records even when max.poll.records=5).

### testPollInRecordLimitMode
Tests share.acquire.mode set to "record_limit". In this mode, max.poll.records is respected and poll returns at most the configured number of records (5 records when max.poll.records=5).

### testPollAndExplicitAcknowledgeSingleMessageInRecordLimitMode
Tests record_limit mode with max.poll.records=1 and explicit acknowledgement. Each poll returns exactly 1 record which must be explicitly acknowledged.

### testExplicitAcknowledgeSuccessInRecordLimitMode
Tests explicit acknowledgement flow in record_limit mode. Verifies that after acknowledging one batch, the next batch of records is correctly delivered.

### testExplicitAcknowledgeReleaseAcceptInRecordLimitMode
Tests mixed RELEASE and ACCEPT acknowledgements in record_limit mode. Released records are re-delivered while accepted records are not.

---

## 10. Throttled Delivery Tests

Tests that verify the throttled delivery behavior for records approaching the delivery count limit.

### testFetchWithThrottledDelivery
Tests throttled delivery with default delivery limit of 5. As records approach the delivery limit, fewer records are returned per poll. Pattern: full batch for first half of attempts, then exponentially decreasing batches.

### testFetchWithThrottledDeliveryBatchesWithIncreasedDeliveryLimit
Tests throttled delivery with delivery count limit of 10 and 512 messages. Verifies the throttling algorithm: full batches for first 5 deliveries, then halving for subsequent deliveries (256, 128, 64, 32, 1).

### testFetchWithThrottledDeliveryValidateDeliveryCount
Tests that delivery count is correctly incremented with each redelivery and validates the throttling behavior with 500 messages and delivery limit of 10.

### testFetchWithThrottledDeliveryBatchesWithDecreasedDeliveryLimit
Tests throttled delivery behavior when the delivery count limit is decreased dynamically.

### testDeliveryCountDifferentBehaviorWhenClosingSessionWithExplicitAcknowledgement
Tests that delivery count is incremented when consumer closes without acknowledging records. Records released due to session close have their delivery count incremented.

### testBehaviorOnDeliveryCountBoundary
Tests behavior when records reach the delivery count boundary (limit=3). Verifies records are archived (not re-delivered) after reaching the limit.

---

## 11. Share Partition Lag Tests

Tests that verify share partition lag calculation and monitoring.

### testDescribeShareGroupOffsetsForEmptySharePartition
Tests that describing share group offsets for an empty partition (no records consumed) returns null as the share partition start offset is -1.

### testSharePartitionLagForSingleShareConsumer
Tests lag calculation for a single consumer:
- After consuming and acknowledging: lag = 0
- After producing new record without consuming: lag = 1

### testSharePartitionLagForMultipleShareConsumers
Tests lag calculation with two consumers in the same group:
- Verifies lag changes as different consumers consume and acknowledge records
- Lag reflects unconsumed + unacknowledged records

### testSharePartitionLagWithReleaseAcknowledgement
Tests lag calculation with RELEASE acknowledgement:
- Released records are counted in lag (available for redelivery)
- After re-consuming and accepting: lag = 0

### testSharePartitionLagWithRejectAcknowledgement
Tests lag calculation with REJECT acknowledgement:
- Rejected records are NOT counted in lag (permanently rejected)
- Offset moves forward after rejection

### testSharePartitionLagAfterAlterShareGroupOffsets
Tests lag calculation after altering share group offsets using admin API.

### testSharePartitionLagAfterDeleteShareGroupOffsets
Tests lag calculation after deleting share group offsets using admin API.

---

## 12. Group Configuration Tests

Tests that verify share group configuration limits and constraints.

### testShareGroupMaxSizeConfigExceeded
Tests the group.share.max.size configuration limit:
- Creates 3 consumers in a group with max size = 3
- 4th consumer attempting to join throws GroupMaxSizeReachedException
- Verifies the configuration properly limits group membership

### testShareGroupShareSessionCacheIsFull
Tests the group.share.max.share.sessions configuration limit:
- With max.share.sessions = 1 and max.group.size = 1
- First consumer from group1 successfully polls and receives records
- Second consumer from group2 cannot establish session, receives 0 records
- Verifies share session cache limit is enforced across groups

### testComplexShareConsumer
Tests complex consumption scenario with explicit acknowledgement:
- Single topic with 3 partitions
- Producer continuously producing records
- Consumer with explicit acknowledgement mode
- Some records released for redelivery (approximately 2x records read vs produced)
- Verifies at-least-once semantics

---

## 13. Rack Aware Assignment Tests

Tests that verify rack-aware partition assignment for share consumers.

### testShareConsumerWithRackAwareAssignor
Tests rack-aware assignment with 3 brokers on different racks (rack0, rack1, rack2) and 3 consumers with corresponding client.rack configurations:

1. **Initial State**: Single partition on broker 0
   - Consumer 0 (rack0) assigned to partition 0
   - Consumer 1 (rack1) and Consumer 2 (rack2) have no assignments

2. **Add Partitions 1 & 2 to broker 1**:
   - Consumer 0: partition 0
   - Consumer 1: partitions 1 and 2
   - Consumer 2: no assignment

3. **Add Partitions 3, 4, 5 to broker 2**:
   - Consumer 0: partition 0
   - Consumer 1: partitions 1 and 2
   - Consumer 2: partitions 3, 4, and 5

4. **Reassign Partitions to Different Brokers**:
   - Partition 0,1,2 -> broker 2; Partition 3,4 -> broker 1; Partition 5 -> broker 0
   - Consumer 0: partition 5 (now on rack0)
   - Consumer 1: partitions 3 and 4 (now on rack1)
   - Consumer 2: partitions 0, 1, and 2 (now on rack2)

Verifies that partitions are assigned to consumers on the same rack as the partition leader for optimal data locality.

---

## 14. Renew Acknowledgement Tests

Tests that verify the renew acknowledgement feature for extending record lock duration.

### testRenewAcknowledgementOnPoll
Tests that calling poll() automatically renews acknowledgement for previously fetched records, extending the acquisition lock timeout.

### testRenewAcknowledgementOnCommitSync
Tests that calling commitSync() renews acknowledgement for records in the current batch without requiring another poll().

### testRenewAcknowledgementInvalidStateRecord
Tests renew acknowledgement behavior when records are in an invalid state (e.g., already acknowledged). Verifies proper error handling.

### testRenewAcknowledgementNoResultInPoll
Tests scenarios where renew acknowledgement returns no result in poll, handling edge cases in the renewal flow.

---

## 15. Test Combinations Matrix

### Consumer Count x Partition Count

| Scenario | Single Partition | Multiple Partitions |
|----------|------------------|---------------------|
| **Single Consumer** | testSubscriptionAndPoll, testMaxPollRecords, testHeaders | testComplexShareConsumer (3 partitions) |
| **Multiple Consumers (Same Group)** | testMultipleConsumersInGroupSequentialConsumption | testShareConsumerAfterCoordinatorMovement |
| **Multiple Consumers (Different Groups)** | testMultipleConsumersWithDifferentGroupIds | testMultipleConsumersInMultipleGroupsConcurrentConsumption |

### Configuration Variations

| Configuration | Tests |
|---------------|-------|
| share.auto.offset.reset=earliest | Most tests use this |
| share.auto.offset.reset=latest | testShareAutoOffsetResetDefaultValue |
| share.auto.offset.reset=by_duration | testShareAutoOffsetResetByDuration |
| share.isolation.level=read_committed | testReadCommittedIsolationLevel |
| share.isolation.level=read_uncommitted | testReadUncommittedIsolationLevel |
| share.acquire.mode=batch_optimized | testPollInBatchOptimizedMode |
| share.acquire.mode=record_limit | testPollInRecordLimitMode |
| group.share.max.size | testShareGroupMaxSizeConfigExceeded |
| group.share.delivery.count.limit | testBehaviorOnDeliveryCountBoundary |

### Key Test Scenarios Summary

1. **Basic Flow**: Subscribe -> Poll -> Process -> (Implicit/Explicit) Acknowledge
2. **Error Recovery**: Consumer failure -> Records released -> Redelivery to other consumers
3. **Coordinator Failover**: Group/Share coordinator moves -> Consumption continues
4. **Throttled Delivery**: Records nearing delivery limit get throttled batches
5. **Rack Awareness**: Partitions assigned to consumers on same rack as leader

---

## Complete Test List (72 Tests Total)

| # | Test Name | Category |
|---|-----------|----------|
| 1 | testPollNoSubscribeFails | Basic Subscription |
| 2 | testSubscribeAndPollNoRecords | Basic Subscription |
| 3 | testSubscribePollUnsubscribe | Basic Subscription |
| 4 | testSubscribePollSubscribe | Basic Subscription |
| 5 | testSubscribeUnsubscribePollFails | Basic Subscription |
| 6 | testSubscribeSubscribeEmptyPollFails | Basic Subscription |
| 7 | testAcknowledgementSentOnSubscriptionChange | Basic Subscription |
| 8 | testSubscriptionAndPoll | Single Consumer |
| 9 | testSubscriptionAndPollMultiple | Single Consumer |
| 10 | testPollRecordsGreaterThanMaxBytes | Single Consumer |
| 11 | testHeaders | Single Consumer |
| 12 | testHeadersSerializerDeserializer | Single Consumer |
| 13 | testMaxPollRecords | Single Consumer |
| 14 | testControlRecordsSkipped | Single Consumer |
| 15 | testFetchRecordLargerThanMaxPartitionFetchBytes | Single Consumer |
| 16 | testAcquisitionLockTimeoutOnConsumer | Single Consumer |
| 17 | testWakeupWithFetchedRecordsAvailable | Single Consumer |
| 18 | testPollThrowsInterruptExceptionIfInterrupted | Single Consumer |
| 19 | testSubscribeOnInvalidTopicThrowsInvalidTopicException | Single Consumer |
| 20 | testMultipleConsumersInGroupSequentialConsumption | Multiple Consumers Same Group |
| 21 | testMultipleConsumersInGroupConcurrentConsumption | Multiple Consumers Same Group |
| 22 | testConsumerCloseInGroupSequential | Multiple Consumers Same Group |
| 23 | testMultipleConsumersInGroupFailureConcurrentConsumption | Multiple Consumers Same Group |
| 24 | testSharePartitionLagForMultipleShareConsumers | Multiple Consumers Same Group |
| 25 | testFetchWithThrottledDeliveryBatchesMultipleConsumers | Multiple Consumers Same Group |
| 26 | testMultipleConsumersWithDifferentGroupIds | Multiple Groups |
| 27 | testMultipleConsumersInMultipleGroupsConcurrentConsumption | Multiple Groups |
| 28 | testShareAutoOffsetResetMultipleGroupsWithDifferentValue | Multiple Groups |
| 29 | testShareAutoOffsetResetDefaultValue | Auto Offset Reset |
| 30 | testShareAutoOffsetResetEarliest | Auto Offset Reset |
| 31 | testShareAutoOffsetResetEarliestAfterLsoMovement | Auto Offset Reset |
| 32 | testShareAutoOffsetResetByDuration | Auto Offset Reset |
| 33 | testShareAutoOffsetResetByDurationInvalidFormat | Auto Offset Reset |
| 34 | testReadCommittedIsolationLevel | Isolation Level |
| 35 | testReadUncommittedIsolationLevel | Isolation Level |
| 36 | testAlterReadUncommittedToReadCommittedIsolationLevel | Isolation Level |
| 37 | testAlterReadCommittedToReadUncommittedIsolationLevelWithReleaseAck | Isolation Level |
| 38 | testAlterReadCommittedToReadUncommittedIsolationLevelWithRejectAck | Isolation Level |
| 39 | testSubscriptionFollowedByTopicCreation | Topic Lifecycle |
| 40 | testSubscriptionAndPollFollowedByTopicDeletion | Topic Lifecycle |
| 41 | testLsoMovementByRecordsDeletion | Topic Lifecycle |
| 42 | testCommitSyncFailsForDeletedTopic | Topic Lifecycle |
| 43 | testShareConsumerAfterCoordinatorMovement | Coordinator Movement |
| 44 | testConsumerCloseOnBrokerShutdown | Coordinator Movement |
| 45 | testSharePartitionLagOnGroupCoordinatorMovement | Coordinator Movement |
| 46 | testSharePartitionLagOnShareCoordinatorMovement | Coordinator Movement |
| 47 | testPollInBatchOptimizedMode | Acquire Mode |
| 48 | testPollInRecordLimitMode | Acquire Mode |
| 49 | testPollAndExplicitAcknowledgeSingleMessageInRecordLimitMode | Acquire Mode |
| 50 | testExplicitAcknowledgeSuccessInRecordLimitMode | Acquire Mode |
| 51 | testExplicitAcknowledgeReleaseAcceptInRecordLimitMode | Acquire Mode |
| 52 | testFetchWithThrottledDelivery | Throttled Delivery |
| 53 | testFetchWithThrottledDeliveryBatchesWithIncreasedDeliveryLimit | Throttled Delivery |
| 54 | testFetchWithThrottledDeliveryValidateDeliveryCount | Throttled Delivery |
| 55 | testFetchWithThrottledDeliveryBatchesWithDecreasedDeliveryLimit | Throttled Delivery |
| 56 | testDeliveryCountDifferentBehaviorWhenClosingSessionWithExplicitAcknowledgement | Throttled Delivery |
| 57 | testBehaviorOnDeliveryCountBoundary | Throttled Delivery |
| 58 | testDescribeShareGroupOffsetsForEmptySharePartition | Share Partition Lag |
| 59 | testSharePartitionLagForSingleShareConsumer | Share Partition Lag |
| 60 | testSharePartitionLagForMultipleShareConsumers | Share Partition Lag |
| 61 | testSharePartitionLagWithReleaseAcknowledgement | Share Partition Lag |
| 62 | testSharePartitionLagWithRejectAcknowledgement | Share Partition Lag |
| 63 | testSharePartitionLagAfterAlterShareGroupOffsets | Share Partition Lag |
| 64 | testSharePartitionLagAfterDeleteShareGroupOffsets | Share Partition Lag |
| 65 | testShareGroupMaxSizeConfigExceeded | Group Configuration |
| 66 | testShareGroupShareSessionCacheIsFull | Group Configuration |
| 67 | testComplexShareConsumer | Group Configuration |
| 68 | testShareConsumerWithRackAwareAssignor | Rack Aware Assignment |
| 69 | testRenewAcknowledgementOnPoll | Renew Acknowledgement |
| 70 | testRenewAcknowledgementOnCommitSync | Renew Acknowledgement |
| 71 | testRenewAcknowledgementInvalidStateRecord | Renew Acknowledgement |
| 72 | testRenewAcknowledgementNoResultInPoll | Renew Acknowledgement |
