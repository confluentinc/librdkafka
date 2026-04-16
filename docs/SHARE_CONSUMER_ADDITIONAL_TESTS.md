# Share Consumer Additional Test Recommendations

This document identifies additional integration tests that can be implemented in librdkafka based on the Kafka Java client test scenarios.

**Reference:** `docs/SHARE_CONSUMER_TEST_SCENARIOS_COMPLETE.md`

**Last Updated:** March 2026

---

## Exclusions (Not Ready for Implementation)

The following test categories are **excluded** because the required functionality is not yet implemented in librdkafka:

| Category | Reason |
|----------|--------|
| **Acquire Mode Tests** (batch_mode/record_mode) | `share.acquire.mode` and `max.poll.records` not configured |
| **Wakeup API Tests** | `rd_kafka_share_wakeup()` not implemented |
| **Consumer Close Tests** (full) | `rd_kafka_share_consumer_close()` not fully implemented |
| **Coordinator Movement Tests** | Requires mock broker with coordinator failover support |
| **Leader Change Tests** | Requires mock broker with leader election support |
| **Rack Aware Assignment Tests** | Complex multi-broker rack-aware setup |

---

## Already Implemented

### 0170-share_consumer_subscription.c

Subscription lifecycle tests.

| Test | Java Equivalent |
|------|-----------------|
| `test_single_subscribe` | `testSubscriptionAndPoll` |
| `test_single_unsubscribe` | `testSubscribePollUnsubscribe` |
| `test_repeated_subscribe` | `testSubscribePollSubscribe` |
| `test_repeated_unsubscribe` | - |
| `test_incremental_subscription` | - |
| `test_subscribe_before_topic_exists` | `testSubscriptionFollowedByTopicCreation` |
| `test_poll_empty_topic` | `testSubscribeAndPollNoRecords` |
| `test_poll_no_subscription` | `testPollNoSubscribeFails` |
| `test_poll_after_unsubscribe` | `testSubscribeUnsubscribePollFails` |
| `test_topic_deletion` | `testSubscriptionAndPollFollowedByTopicDeletion` |
| `test_rapid_updates` | - |
| `do_test_multi_consumer_overlap` | - |
| `do_test_subscribe_15_topics` | - |
| `test_auto_offset_reset_earliest` | - |
| `test_auto_offset_reset_default_latest` | - |

### 0171-share_consumer_consume.c

Basic consumption tests.

| Test | Java Equivalent |
|------|-----------------|
| `test_single_consumer_single_topic_single_partition` | `testSubscriptionAndPoll` |
| `test_single_consumer_single_topic_multiple_partitions` | - |
| `test_single_consumer_multiple_topic_single_partition` | - |
| `test_single_consumer_multiple_topic_multiple_partitions` | `testComplexShareConsumer` (partial) |
| `test_multiple_consumers_single_topic_single_partition` | `testMultipleConsumersInGroupSequentialConsumption` |
| `test_multiple_consumers_single_topic_multiple_partitions` | - |
| `test_multiple_consumers_multiple_topics_multiple_partitions` | - |
| `test_high_volume_10k_messages` | - |
| `test_high_volume_50k_multi_partition` | - |
| `test_many_topics_15` | - |
| `test_many_topics_10_multi_partition` | - |
| `test_rapid_produce_consume_cycles` | - |
| `test_empty_then_produce` | - |
| `test_sparse_partitions` | - |
| `test_acquisition_lock_expiry_redelivery` | - |

### 0172-share_consumer_acknowledge.c

Acknowledgement API tests (ACCEPT, REJECT, RELEASE).

| Test | Java Equivalent |
|------|-----------------|
| `test_release_redelivery` | `testExplicitAcknowledgeReleasePollAccept` |
| `test_reject_no_redelivery` | `testExplicitAcknowledgeReject` |
| `test_accept_no_redelivery` | `testExplicitAcknowledgeCommitSync` |
| `test_mixed_ack_types` | - |
| `test_release_multiple_partitions` | - |
| `test_mixed_ack_across_partitions` | - |
| `test_release_multiple_topics` | - |
| `test_mixed_ack_across_topics` | - |
| `test_ack_null_message` | - |
| `test_ack_null_rkshare` | - |
| `test_ack_invalid_type` | - |
| `test_release_then_reject_no_redelivery` | - |
| `test_max_delivery_attempts` | `testExplicitAcknowledgeReleaseMaxDeliveries` |
| `test_mixed_per_offset_*` (4 tests) | - |
| `test_alternating_ack_pattern` | - |
| `test_random_ack_*` (4 tests) | - |
| `test_scale_*` (5 tests) | - |

### 0173-share_consumer_commit_async.c

Commit async API tests.

| Test | Java Equivalent |
|------|-----------------|
| `do_test_implicit_second_consumer` | `testImplicitAcknowledgeCommitAsync` (partial) |
| `do_test_explicit_second_consumer` | `testExplicitAcknowledgeCommitAsync` (partial) |
| `do_test_mixed_acks_second_consumer` | - |
| `do_test_multi_topic_partition` | - |
| `do_test_produce_consume_loop` | - |
| `do_test_multi_round_mixed_second_consumer` | - |
| `do_test_no_pending_acks` | - |
| `do_test_multiple_commit_async_calls` | - |
| `do_test_commit_between_produces` | - |
| `do_test_all_release_second_consumer` | - |
| `do_test_all_reject_second_consumer` | - |
| `do_test_per_record_commit_async` | - |
| `do_test_mock_inflight_caching` | - |

### 0174-share_consumer_concurrency.c

Concurrent producer/consumer stress tests with threaded execution.

| Test | Description |
|------|-------------|
| `test_1p_1c_1t_1part` | Baseline: 1 producer, 1 consumer |
| `test_1p_4c_1t_4part` | Fan out: 1 producer, 4 consumers |
| `test_4p_1c_1t_4part` | Fan in: 4 producers, 1 consumer |
| `test_4p_4c_1t_4part` | Symmetric: 4 producers, 4 consumers |
| `test_2p_2c_3t_2part` | Multiple topics concurrent |
| `test_2p_2c_8t_1part` | Many topics (8) concurrent |
| `test_1p_8c_1t_1part` | High contention: 8 consumers, 1 partition |
| `test_2p_6c_1t_2part` | More consumers than partitions |
| `test_explicit_ack_4p_4c` | Concurrent with explicit ack |
| `test_staggered_start` | Consumers start before producers |
| `test_high_volume_10k` | 10k messages stress test |
| `test_high_volume_many_partitions` | 8 partitions x 1k messages |
| `test_8p_2c` | Producer heavy (8 producers, 2 consumers) |
| `test_2p_8c` | Consumer heavy (2 producers, 8 consumers) |
| `test_complex_varied_partitions` | 4 topics with 1,2,3,4 partitions |
| `test_max_concurrent` | Maximum: 10 producers, 10 consumers |
| `test_slow_consumers` | Fast producers, slow consumers |
| `test_slow_producers` | Slow producers, fast consumers |

### 0175-share_consumer_groups.c

Multiple share groups tests - different groups consuming same topic independently.

| Test | Description |
|------|-------------|
| `test_two_groups_same_topic` | Two groups receive all messages independently |
| `test_three_groups_concurrent` | Three groups with 2 consumers each |
| `test_groups_different_offset_reset` | Groups with earliest vs latest offset reset |
| `test_five_groups_same_topic` | Five groups consuming same topic |
| `test_groups_staggered_join` | Groups joining at different times |

---

## Recommended Additional Tests

### 1. Multiple Groups Tests - IMPLEMENTED in 0175-share_consumer_groups.c

Tests for multiple share groups consuming from the same topic independently.

#### `test_two_groups_same_topic`
```
Java: testMultipleConsumersWithDifferentGroupIds

Setup:
- 1 topic, 1 partition
- Group A: 1 consumer
- Group B: 1 consumer
- Produce 100 messages

Expected:
- Both groups receive all 100 messages independently
- Total consumed = 200 (100 per group)
```

#### `test_three_groups_concurrent`
```
Java: testMultipleConsumersInMultipleGroupsConcurrentConsumption

Setup:
- 1 topic, 3 partitions
- Group A: 2 consumers
- Group B: 2 consumers
- Group C: 2 consumers
- Produce 1000 messages

Expected:
- Each group consumes all 1000 messages
- Total consumed across all groups = 3000
```

#### `test_groups_different_offset_reset`
```
Java: testShareAutoOffsetResetMultipleGroupsWithDifferentValue

Setup:
- 1 topic, 1 partition
- Produce 50 messages BEFORE consumers start
- Group A: share.auto.offset.reset = earliest
- Group B: share.auto.offset.reset = latest
- Produce 50 more messages AFTER consumers subscribe

Expected:
- Group A consumes 100 messages (all)
- Group B consumes ~50 messages (only after subscription)
```

---

### 2. Share Auto Offset Reset Tests (Add to 0170)

#### `test_auto_offset_reset_default_latest`
```
Java: testShareAutoOffsetResetDefaultValue

Setup:
- 1 topic, 1 partition
- Produce 100 messages BEFORE consumer subscribes
- Create consumer with default config (no share.auto.offset.reset set)
- Subscribe and poll

Expected:
- Consumer receives 0 messages (default is "latest")
- Produce 50 more messages
- Consumer receives 50 messages
```

#### `test_auto_offset_reset_earliest`
```
Java: testShareAutoOffsetResetEarliest

Setup:
- 1 topic, 1 partition
- Produce 100 messages BEFORE consumer subscribes
- Set share.auto.offset.reset = earliest via IncrementalAlterConfigs
- Subscribe and poll

Expected:
- Consumer receives all 100 messages
```

---

### 3. Message Header Tests (Add to 0171)

#### `test_message_headers_preserved`
```
Java: testHeaders

Setup:
- 1 topic, 1 partition
- Produce 10 messages with headers:
  - Header "key1" = "value1"
  - Header "key2" = "value2"
- Consume messages

Expected:
- All consumed messages have correct headers
- Header order is preserved
```

---

### 4. Large Message Tests (Add to 0171)

#### `test_large_message_consumption`
```
Java: testPollRecordsGreaterThanMaxBytes / testFetchRecordLargerThanMaxPartitionFetchBytes

Setup:
- 1 topic, 1 partition
- Produce 5 messages of 1MB each (larger than typical fetch.max.bytes)
- Consume messages

Expected:
- All 5 large messages consumed successfully
```

---

### 5. Transaction and Isolation Tests (NEW FILE: 0176-share_consumer_transactions.c)

#### `test_control_records_skipped`
```
Java: testControlRecordsSkipped

Setup:
- 1 topic, 1 partition
- Use transactional producer:
  - Begin transaction
  - Produce 5 messages
  - Commit transaction (creates control record)
  - Begin another transaction
  - Produce 5 messages
  - Abort transaction (creates control record)
- Consume with share consumer

Expected:
- Receive only 5 messages (from committed transaction)
- Control records (commit/abort markers) are NOT delivered
- Aborted transaction messages are NOT delivered
```

#### `test_read_committed_isolation`
```
Java: testReadCommittedIsolationLevel

Setup:
- 1 topic, 1 partition
- Use transactional producer:
  - Begin transaction, produce 5 messages, commit
  - Begin transaction, produce 5 messages, abort
- Set share.isolation.level = read_committed
- Consume

Expected:
- Only 5 messages received (from committed transaction)
```

#### `test_read_uncommitted_isolation`
```
Java: testReadUncommittedIsolationLevel

Setup:
- Same as above but share.isolation.level = read_uncommitted

Expected:
- All 10 messages received (including aborted)
```

---

### 6. Concurrent Consumer Tests (Add to 0171)

#### `test_concurrent_consumers_4_threads`
```
Java: testMultipleConsumersInGroupConcurrentConsumption

Setup:
- 1 topic, 4 partitions
- 4 consumers in same group (running in parallel threads/processes)
- Produce 10000 messages

Expected:
- All 10000 messages consumed exactly once across all consumers
- No duplicate consumption
```

#### `test_consumer_failure_recovery`
```
Java: testMultipleConsumersInGroupFailureConcurrentConsumption

Setup:
- 1 topic, 2 partitions
- 3 consumers in same group
- Produce 5000 messages
- Consumer 1 polls once then closes immediately (simulates failure)
- Consumers 2 and 3 continue consuming

Expected:
- All 5000 messages eventually consumed
- Messages held by Consumer 1 are released and redelivered
```

---

### 7. Subscription Edge Cases (Add to 0170)

#### `test_subscribe_empty_list_fails`
```
Java: testSubscribeSubscribeEmptyPollFails

Setup:
- Create consumer
- Subscribe with empty topic list
- Poll

Expected:
- Error returned (illegal state)
```

#### `test_invalid_topic_name_fails`
```
Java: testSubscribeOnInvalidTopicThrowsInvalidTopicException

Setup:
- Create consumer
- Subscribe to invalid topic name (e.g., contains illegal characters)

Expected:
- Error returned (invalid topic)
```

---

### 8. High Concurrency Stress Tests (Add to 0171)

#### `test_many_consumers_same_partition`
```
Setup:
- 1 topic, 1 partition
- 10 consumers in same group
- Produce 10000 messages

Expected:
- All messages consumed exactly once
- Work distributed across consumers
```

#### `test_subscription_churn`
```
Setup:
- 5 topics, 1 partition each
- 1 consumer
- Loop 20 times:
  - Subscribe to random subset of topics
  - Produce 100 messages to each subscribed topic
  - Consume all
  - Unsubscribe

Expected:
- All messages consumed correctly in each iteration
- No state corruption between subscription changes
```

---

## Implementation Priority

### High Priority (Core Functionality)
1. ~~`test_two_groups_same_topic`~~ - DONE (0175)
2. ~~`test_auto_offset_reset_earliest`~~ - DONE (0170)
3. ~~`test_auto_offset_reset_default_latest`~~ - DONE (0170)
4. `test_message_headers_preserved` - Header support verification (add to 0171)

### Medium Priority (Robustness)
5. `test_control_records_skipped` - Transaction support (requires 0176)
6. `test_read_committed_isolation` - Isolation level (requires 0176)
7. ~~`test_concurrent_consumers_4_threads`~~ - DONE (0174)
8. `test_large_message_consumption` - Edge case (add to 0171)

### Lower Priority (Stress/Edge Cases)
9. `test_consumer_failure_recovery` - Requires careful timing (add to 0171)
10. ~~`test_many_consumers_same_partition`~~ - DONE (0174 - test_1p_8c_1t_1part)
11. `test_subscription_churn` - Stress test (add to 0170)
12. `test_invalid_topic_name_fails` - Error handling (add to 0170)

---

## File Organization

### Current Structure
```
tests/
  0170-share_consumer_subscription.c  (subscription lifecycle + auto offset reset)
  0171-share_consumer_consume.c       (basic consumption)
  0172-share_consumer_acknowledge.c   (acknowledge APIs: ACCEPT/REJECT/RELEASE)
  0173-share_consumer_commit_async.c  (commit_async API)
  0174-share_consumer_concurrency.c   (concurrent producer/consumer stress tests)
  0175-share_consumer_groups.c        (multiple share groups tests)
```

### Recommended Additions
```
tests/
  0176-share_consumer_transactions.c  (NEW - isolation + control records)
```

### Tests to Add to Existing Files

**Add to 0170-share_consumer_subscription.c:**
- `test_subscribe_empty_list_fails`
- `test_invalid_topic_name_fails`
- `test_subscription_churn`

**Add to 0171-share_consumer_consume.c:**
- `test_message_headers_preserved`
- `test_large_message_consumption`
- `test_consumer_failure_recovery`

**Already implemented:**
- Auto offset reset tests (earliest, default/latest) - added to 0170
- Concurrent consumer tests - implemented in 0174
- Multiple groups tests - implemented in 0175

---

## Summary of Changes (This PR)

This PR added the following test files:

### 0172-share_consumer_acknowledge.c (NEW)
- 25+ tests covering explicit acknowledgement APIs
- Tests ACCEPT, REJECT, RELEASE behavior
- Tests redelivery semantics
- Tests error handling (null params, invalid types)
- Tests max delivery attempts
- Scale tests with multiple topics/partitions

### 0173-share_consumer_commit_async.c (NEW)
- 13 tests covering `rd_kafka_share_commit_async()` API
- Tests implicit and explicit ack modes
- Tests mixed ack types with commit_async
- Tests multi-topic/partition scenarios
- Tests interaction with consume_batch piggybacking
- Mock broker test for inflight caching behavior

### 0174-share_consumer_concurrency.c (NEW)
- 18 concurrent stress tests with threaded producers and consumers
- Config-based test structure for flexibility
- Scenarios covered:
  - Basic: 1p/1c, 1p/4c, 4p/1c, 4p/4c configurations
  - Multi-topic: 3 topics, 8 topics scenarios
  - High contention: 8 consumers on 1 partition, more consumers than partitions
  - Explicit acknowledgement with concurrent access
  - Staggered start (consumers before producers)
  - High volume: 10k messages, 8 partitions
  - Asymmetric: producer-heavy (8p/2c), consumer-heavy (2p/8c)
  - Complex: varied partition counts (1,2,3,4), maximum concurrent (10p/10c)
  - Slow consumer/producer scenarios

### 0175-share_consumer_groups.c (NEW)
- 5 tests for multiple share groups consuming same topic independently
- Tests covered:
  - `test_two_groups_same_topic` - Two groups receive all messages independently
  - `test_three_groups_concurrent` - Three groups with 2 consumers each
  - `test_groups_different_offset_reset` - Groups with earliest vs latest offset reset
  - `test_five_groups_same_topic` - Five groups consuming same topic
  - `test_groups_staggered_join` - Groups joining at different times

### Tests added to existing files

**0170-share_consumer_subscription.c:**
- `test_auto_offset_reset_earliest` - Verify earliest offset behavior
- `test_auto_offset_reset_default_latest` - Verify default (latest) offset behavior

---

## Notes

1. All tests should use `test_IncrementalAlterConfigs_simple()` to set `share.auto.offset.reset = earliest` unless testing default/latest behavior.

2. For concurrent tests, use C threads (`pthread`) or run multiple consumer instances sequentially with interleaved polling.

3. Transactional producer tests require creating a producer with `transactional.id` config and using `rd_kafka_init_transactions()`, `rd_kafka_begin_transaction()`, etc.

4. Consider increasing `max_attempts` for tests with many topics/partitions to account for share group join time.
