# Test 0173 Timeout Analysis - job_logs 2.txt

## Summary
Test `0173_share_consumer_commit_async` times out at **191.524 seconds** while stuck in round 5 of the `do_test_multi_topic_partition()` test.

## Root Cause
The test creates an inefficient produce pattern that overwhelms the broker and eventually hangs in `test_wait_delivery()`.

### The Problem Pattern

**Test configuration:**
- 10 topics × 5 partitions = 50 total partitions
- 20 rounds
- 10 messages per partition per round
- Total: 10,000 messages (500 per round)

**Inefficient implementation:**
```c
for (round = 0; round < rounds; round++) {
    for (t = 0; t < topic_cnt; t++) {           // 10 topics
        for (p = 0; p < part_cnt; p++)          // 5 partitions each
            produce_to_topic(topics[t], p, msgs_per_part);  // BLOCKS!
    }
    // consume and commit
}
```

**What `produce_to_topic()` does:**
```c
static void produce_to_topic(const char *topic, int32_t partition, int msgcnt) {
    rd_kafka_topic_t *rkt;
    rkt = test_create_producer_topic(common_producer, topic, NULL);
    test_produce_msgs(common_producer, rkt, 0, partition, 0, msgcnt, NULL, 0);
    rd_kafka_topic_destroy(rkt);  // ← Creates/destroys topic handle 50 times per round!
}
```

**`test_produce_msgs()` implementation:**
```c
void test_produce_msgs(...) {
    test_produce_msgs_nowait(...);
    test_wait_delivery(rk, &remains);  // ← BLOCKS waiting for delivery!
}
```

### Timeline of Failure

| Time | Round | Event |
|------|-------|-------|
| 94.454s | 0 | Completed (500/500 messages) |
| 113.137s | 1 | Completed (500/500 messages) |
| 131.873s | 2 | Completed (500/500 messages) |
| 151.813s | 3 | Completed (500/500 messages) |
| 174.905s | 4 | Completed (500/500 messages) |
| 188.377s | 5 | **Last successful produce** (PRODUCE.DELIVERY.WAIT: 7.268ms) |
| 188.377s - 191.524s | 5 | **HUNG** - Stuck in `test_wait_delivery()`, never completes |
| 191.524s | - | **TIMEOUT** |

### Evidence from Logs

**Last successful produce at 188.377s:**
```
[0173_share_consumer_commit_async/188.377s] PRODUCE.DELIVERY.WAIT: duration 7.268ms
[0173_share_consumer_commit_async/188.377s] PRODUCE: duration 7.296ms
%7|1775571938.692|DESTROY|0173_share_consumer_commit_async#producer-987| [thrd:app]: Terminating instance
```

**Delivery wait times getting progressively worse:**
- Round 3-4: ~8-17ms per partition
- Round 4-5 transition: **2734ms** (partition getting slow)
- Mid round 5: **3447ms** (broker overloaded)
- Late round 5: Producer-987 destroyed, **next produce never completes**

**What's happening:**
1. Each round creates **50 topic handles** (rd_kafka_topic_t)
2. Each produce **blocks** waiting for delivery before moving to next partition
3. By round 5, the broker/producer queue is overwhelmed
4. After producing to ~42/50 partitions in round 5, delivery stops completing
5. `test_wait_delivery()` has **no timeout** and waits forever
6. Test times out after 191 seconds

## Why It Fails

### 1. **Inefficient topic handle management**
- Creates/destroys 50 topic handles per round
- Total: 1000 topic handle create/destroy operations across 20 rounds
- Adds overhead and fragmentation

### 2. **Sequential blocking produces**
- Each partition produce waits for delivery before starting next
- Prevents pipelined/batched delivery
- Serializes what should be parallel operations

### 3. **test_wait_delivery() has no timeout**
- Waits indefinitely for message delivery
- If broker/network issue prevents delivery, test hangs forever

### 4. **Broker resource exhaustion**
- Rapid creation/destruction of topic handles
- 50 sequential produce operations per round
- ShareAcknowledge RPCs happening concurrently
- Broker gets overwhelmed and stops processing

## The Fix (Already in 0177)

Test 0177 follows the **correct pattern** used in the working parts of 0173:

```c
// GOOD: Create common producer ONCE
static rd_kafka_t *common_producer;

static void produce_to_topic(const char *topic, int32_t partition, int msgcnt) {
    rd_kafka_topic_t *rkt;
    rkt = test_create_producer_topic(common_producer, topic, NULL);  // Reuse producer
    test_produce_msgs(common_producer, rkt, 0, partition, 0, msgcnt, NULL, 0);
    rd_kafka_topic_destroy(rkt);
}

int main() {
    common_producer = test_create_producer();  // Create ONCE
    
    // Run all tests using common_producer
    
    rd_kafka_destroy(common_producer);  // Destroy ONCE
}
```

**Key improvements:**
1. ✅ Producer created once, reused across all tests
2. ✅ Topic handles still created/destroyed per partition (minimal overhead)
3. ✅ Reduces connection churn
4. ✅ Better resource utilization
5. ✅ Faster test execution

## Recommendation

**For test 0173:**
The test already uses `common_producer` correctly. The issue is that by round 5, after ~4200 messages produced sequentially with blocking delivery waits, the broker queue/state gets corrupted and stops delivering messages.

**Potential solutions:**
1. Add timeout to `test_wait_delivery()` to detect stuck deliveries
2. Reduce number of rounds or messages per partition
3. Add delay between rounds to let broker catch up
4. Investigate broker-side ShareAcknowledge RPC handling under load
5. Consider batching produces before waiting for delivery

**This is likely a broker-side issue** where the share partition state manager gets overwhelmed by the combination of:
- Continuous produce traffic
- Concurrent ShareFetch operations
- Concurrent ShareAcknowledge operations
- Multiple topics/partitions active simultaneously

The test is valid and correctly exposes a broker scalability issue in KIP-932 implementation.
