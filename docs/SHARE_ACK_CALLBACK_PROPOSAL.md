# Share Acknowledgement Callback - Implementation Proposal

## Decision 1: Callback Result Structure

### Alternative A: New Dedicated Structure

```c
typedef struct rd_kafka_share_ack_result_s {
    char *topic;
    int32_t partition;
    int64_t start_offset;
    int64_t end_offset;
    rd_kafka_share_AcknowledgeType_t type;
    rd_kafka_resp_err_t err;
} rd_kafka_share_ack_result_t;

typedef struct rd_kafka_share_ack_result_list_s {
    int cnt;
    rd_kafka_share_ack_result_t *results;
} rd_kafka_share_ack_result_list_t;

void (*share_acknowledgement_cb)(
    rd_kafka_t *rk,
    rd_kafka_resp_err_t err,
    rd_kafka_share_ack_result_list_t *results,
    void *opaque);
```

### Alternative B: Reuse topic_partition_list + Accessor APIs

```c
void (*share_acknowledgement_cb)(
    rd_kafka_t *rk,
    rd_kafka_resp_err_t err,
    rd_kafka_topic_partition_list_t *acks,
    void *opaque);

// Store extended data in _private, expose via APIs
RD_EXPORT int64_t rd_kafka_share_ack_result_start_offset(const rd_kafka_topic_partition_t *rktpar);
RD_EXPORT int64_t rd_kafka_share_ack_result_end_offset(const rd_kafka_topic_partition_t *rktpar);
RD_EXPORT rd_kafka_share_AcknowledgeType_t rd_kafka_share_ack_result_type(const rd_kafka_topic_partition_t *rktpar);
```

---

## Decision 2: Callback Trigger Points

All callbacks are created in [rd_kafka_broker_share_fetch_reply()](../src/rdkafka_fetcher.c#L1808) (broker thread):

| Trigger | Error Code |
|---------|------------|
| Broker response (success) | `NO_ERROR` |
| Broker response (error) | Per-partition error |
| Request timeout | `REQUEST_TIMED_OUT` |
| Session invalidation | `SHARE_SESSION_NOT_FOUND` |
| Leader change | `NOT_LEADER_OR_FOLLOWER` |

---

## Decision 3: Callback Granularity

**Option A**: Per-broker callbacks (multiple callbacks per commit_async if partitions span brokers)
**Option B**: Aggregated callback (single callback per commit_async, wait for all brokers)

### Current Implementation Status

Acks are **already segregated per-broker** in the current code:

```
commit_async()  // rd_kafka_share_commit_async() - src/rdkafka.c:3746
  → SHARE_COMMIT_ASYNC_FANOUT op (enqueued to main thread)
  → rd_kafka_share_commit_async_fanout_op()  // src/rdkafka.c:3709
  → rd_kafka_share_segregate_acks_by_leader()  // src/rdkafka.c:3344
      - distributes acks to per-broker rkb_share_async_ack_details
  → Separate SHARE_FETCH op created per broker
  → Each broker thread sends request independently
  → Each broker receives response independently
```

Code links:
- [rd_kafka_share_commit_async()](../src/rdkafka.c#L3746)
- [rd_kafka_share_commit_async_fanout_op()](../src/rdkafka.c#L3709)
- [rd_kafka_share_segregate_acks_by_leader()](../src/rdkafka.c#L3344)

**Implication**:
- **Option A (per-broker)**: Natural fit - create callback op in each broker's response handler
- **Option B (aggregated)**: Requires new tracking mechanism to correlate responses and wait for all brokers

---

## Files to Modify

| File | Changes |
|------|---------|
| [rdkafka.h](../src/rdkafka.h) | Add callback registration API, result struct/accessor APIs |
| [rdkafka_conf.h](../src/rdkafka_conf.h) | Add `share_acknowledgement_cb` field |
| [rdkafka_conf.c](../src/rdkafka_conf.c) | Add `rd_kafka_conf_set_share_acknowledgement_cb()` |
| [rdkafka_op.h](../src/rdkafka_op.h) | Add `RD_KAFKA_OP_SHARE_ACK_REPLY`, union member |
| [rdkafka_op.c](../src/rdkafka_op.c) | Add op type name, destroy handler |
| [rdkafka_fetcher.c](../src/rdkafka_fetcher.c) | Create callback op on response/timeout |
| [rdkafka.c](../src/rdkafka.c) | Add poll handler for `RD_KAFKA_OP_SHARE_ACK_REPLY` |

---

## New Op Type

```c
// rdkafka_op.h
RD_KAFKA_OP_SHARE_ACK_REPLY

// Union member
struct {
    rd_kafka_share_ack_result_list_t *results;  // Alt A
    // OR
    rd_kafka_topic_partition_list_t *acks;      // Alt B

    void (*cb)(...);
    void *opaque;
} share_ack_reply;
```

---

## Callback Creation Flow

```
Broker Thread:
  ShareFetch response received (success or error)
    -> ack_details available in rko_orig->rko_u.share_fetch.ack_details
    -> Create RD_KAFKA_OP_SHARE_ACK_REPLY with results
    -> Enqueue to rk->rk_rep

Application Thread:
  poll() / consume_batch()
    -> Dequeue RD_KAFKA_OP_SHARE_ACK_REPLY
    -> Invoke callback
    -> Destroy op
```

**Note**: No separate pending acks tracking needed. All error scenarios (timeout, session invalidation, leader change) come through the broker response handler where `ack_details` is already available in the original op.

---

## Exact Code Location for Callback Creation

### Broker Response Handler

**File**: [rdkafka_fetcher.c](../src/rdkafka_fetcher.c)
**Function**: [rd_kafka_broker_share_fetch_reply()](../src/rdkafka_fetcher.c#L1808)
**Thread**: Broker thread

```c
// src/rdkafka_fetcher.c:1808
static void rd_kafka_broker_share_fetch_reply(rd_kafka_t *rk,
                                              rd_kafka_broker_t *rkb,
                                              rd_kafka_resp_err_t err,
                                              rd_kafka_buf_t *reply,
                                              rd_kafka_buf_t *request,
                                              void *opaque) {
    rd_kafka_op_t *rko_orig = opaque;

    // ... parse response (line 1837-1839) ...
    if (!err && reply)
        err = rd_kafka_share_fetch_reply_handle(rkb, reply, request, &response_rko);
        // rd_kafka_share_fetch_reply_handle() defined at src/rdkafka_fetcher.c:1415

    // ... error handling ...

    // LINE 1887-1891: Current code - ack_details destroyed without callback
    // TODO KIP-932: When acknowledgement callbacks are implemented,
    // report failed acks to the application via the callback.
    if (rko_orig->rko_u.share_fetch.ack_details) {
        rd_list_destroy(rko_orig->rko_u.share_fetch.ack_details);
        rko_orig->rko_u.share_fetch.ack_details = NULL;
    }

    rd_kafka_op_reply(rko_orig, err);
}
```

**Key locations**:
- Function start: [line 1808](../src/rdkafka_fetcher.c#L1808)
- Response parsing: [line 1837-1839](../src/rdkafka_fetcher.c#L1837-L1839) calls [rd_kafka_share_fetch_reply_handle()](../src/rdkafka_fetcher.c#L1415)
- TODO comment (where callback should be added): [line 1887](../src/rdkafka_fetcher.c#L1887)
- ack_details destruction: [line 1889-1891](../src/rdkafka_fetcher.c#L1889-L1891)

### Required Change

```c
// BEFORE destroying ack_details:
if (rk->rk_conf.share_acknowledgement_cb &&
    rko_orig->rko_u.share_fetch.ack_details) {

    rd_kafka_op_t *cb_rko = rd_kafka_op_new(RD_KAFKA_OP_SHARE_ACK_REPLY);
    cb_rko->rko_err = err;
    cb_rko->rko_u.share_ack_reply.cb = rk->rk_conf.share_acknowledgement_cb;
    cb_rko->rko_u.share_ack_reply.opaque = rk->rk_conf.opaque;

    // Build results from rko_orig->rko_u.share_fetch.ack_details
    cb_rko->rko_u.share_ack_reply.results =
        rd_kafka_share_build_ack_callback_results(
            rko_orig->rko_u.share_fetch.ack_details, err);

    rd_kafka_q_enq(rk->rk_rep, cb_rko);
}

// Then destroy
if (rko_orig->rko_u.share_fetch.ack_details) {
    rd_list_destroy(rko_orig->rko_u.share_fetch.ack_details);
    rko_orig->rko_u.share_fetch.ack_details = NULL;
}
```

### Data Available at This Location

`rko_orig->rko_u.share_fetch.ack_details` contains:
- `rd_list_t*` of [rd_kafka_share_ack_batches_t*](../src/rdkafka_share_acknowledgement.h#L90)
- Each batch has:
  - `rktpar` (topic, partition)
  - `entries` list of [rd_kafka_share_ack_batch_entry_t*](../src/rdkafka_share_acknowledgement.h#L63)
    - `start_offset`, `end_offset`
    - `types[]` (ACCEPT/RELEASE/REJECT)

`err` contains:
- `RD_KAFKA_RESP_ERR_NO_ERROR` on success
- `RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT` on timeout
- Other errors from broker response
