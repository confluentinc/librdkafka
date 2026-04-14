# KIP-932 Share Consumer: UNKNOWN Error During Topic Deletion

## Root Cause (2-3 lines)
When a topic is deleted while a share consumer is subscribed, the broker's share partition state in `__share_group_state` is not properly cleaned up. Subsequent ShareFetch requests fail with "Write operation on uninitialized share partition not allowed" but the broker returns a generic UNKNOWN error instead of UNKNOWN_TOPIC_OR_PARTITION, causing the client to retry indefinitely.

---

## Client Logs (librdkafka)

**Timestamp: 1775657149.993 (19:00:49 UTC)**

Test deletes topic while share consumer is still subscribed:
```
%7|1775657149.484|RECV|...: Received DeleteTopicsResponse (v4, 50 bytes, CorrId 34, rtt 139.46ms)
[0170_share_consumer_subscription/ 19.497s] DeleteTopics: duration 144.706ms
```

Immediately after, ShareFetch starts failing with UNKNOWN error:
```
%7|1775657149.993|RECV|...: Received ShareFetchResponse (v1, 15 bytes, CorrId 35, rtt 549.06ms)
%3|1775657149.993|SHAREFETCH|...: localhost:9092/1: ShareFetch response error UNKNOWN: ''
%7|1775657149.993|SHAREFETCH|...: localhost:9092/1: ShareFetch reply error: Unknown broker error
```

This UNKNOWN error repeats continuously:
```
%3|1775657150.498|SHAREFETCH|...: localhost:9092/1: ShareFetch response error UNKNOWN: ''
%3|1775657151.002|SHAREFETCH|...: localhost:9092/1: ShareFetch response error UNKNOWN: ''
%3|1775657151.507|SHAREFETCH|...: localhost:9092/1: ShareFetch response error UNKNOWN: ''
... (continues indefinitely)
```

Client is stuck in retry loop because UNKNOWN error is not properly handled for deleted topics.

---

## Broker Logs (Kafka 4.2.0)

**Group state shows active subscription to deleted topic:**

Topic deletion completed:
```
[2026-04-08 19:00:55,372] INFO Log for partition rdkafkatest_rnd2546039a17968694_0170-t1-0 
is renamed to /tmp/kraft-combined-logs/rdkafkatest_rnd2546039a17968694_0170-t1-0.380e6e7edd564825be961215354e5f50-delete 
and is scheduled for deletion (kafka.log.LogManager)
```

But share group metadata still has assignments:
```
[2026-04-08 19:00:58,245] INFO [GroupCoordinator id=1 topic=__consumer_offsets partition=33] 
[GroupId share-topic-deletion-while-subscribed] Member fmE3hU69Qjyrmz21aKCNPg new assignment state: 
epoch=7, previousEpoch=6, state=STABLE, assignedPartitions=[aR9iBS_7SyKidYbzhwJf_g-0].
```

**Critical Error - Share partition state write failure:**
```
[2026-04-08 19:01:28,379] ERROR Unable to perform write state RPC for key 
SharePartitionKey{groupId=share-topic-deletion-while-subscribed, topicIdPartition=aR9iBS_7SyKidYbzhwJf_g:null-0}: 
Write operation on uninitialized share partition not allowed. 
(org.apache.kafka.server.share.persister.PersisterStateManager$WriteStateHandler)

[2026-04-08 19:01:28,379] ERROR Failed to write the share group state for share partition: 
share-topic-deletion-while-subscribed-aR9iBS_7SyKidYbzhwJf_g:rdkafkatest_rnd5918de636fb06347_0170-t0-0 
due to exception (kafka.server.share.SharePartition)
org.apache.kafka.common.errors.UnknownServerException: 
Error in write state RPC. Write operation on uninitialized share partition not allowed.
```

**Note the topicIdPartition:** `aR9iBS_7SyKidYbzhwJf_g:null-0` 
- Topic ID exists: `aR9iBS_7SyKidYbzhwJf_g`
- Partition is NULL: `null-0`
- This indicates topic metadata was deleted but share partition state still references it

---

## Expected vs Actual Behavior

**Expected:**
1. When topic is deleted, share partition state in `__share_group_state` should be cleaned up
2. ShareFetch for deleted topic should return `UNKNOWN_TOPIC_OR_PARTITION` error
3. Client should handle this error by unsubscribing/rebalancing

**Actual:**
1. Topic deleted but share partition state remains in `__share_group_state`
2. ShareFetch attempts to write state for non-existent partition, fails with internal error
3. Broker returns generic `UNKNOWN` error instead of proper error code
4. Client cannot distinguish this from transient errors and retries forever

---

## Reproduction

Test: `tests/0170-share_consumer_subscription.c` - function `do_test_topic_deletion_while_subscribed()`

Steps:
1. Create share consumer subscribed to 2 topics
2. Delete 1 topic while consumer is still subscribed
3. Attempt to consume from remaining topic
4. Observe UNKNOWN errors and test timeout

---

## Affected Components

- **Share Group Coordinator:** Share partition state cleanup on topic deletion
- **PersisterStateManager:** Error handling for deleted topics
- **ShareFetch RPC:** Error code mapping (should return UNKNOWN_TOPIC_OR_PARTITION, not UNKNOWN)

## Test Environment

- Kafka Version: 4.2.0
- KIP-932 Share Groups enabled
- librdkafka version: latest dev branch
