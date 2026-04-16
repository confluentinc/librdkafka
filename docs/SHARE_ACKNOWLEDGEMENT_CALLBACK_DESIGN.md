# Share Consumer Acknowledgement Callback Design

This document describes the design for implementing acknowledgement callbacks in the Share Consumer (KIP-932). The design is based on the offset commit callback pattern used in regular consumers.

## Overview

Acknowledgement callbacks provide a mechanism for applications to receive notification when acknowledgements are processed by the broker or invalidated on the client side. This is essential for:

1. Knowing when acks have been successfully committed to the broker
2. Handling ack failures with appropriate retry/error handling logic
3. Implementing at-least-once or exactly-once semantics in share group consumers

### Registration

The application can optionally register an acknowledgement callback when creating the share consumer. If registered, the callback is invoked for each acknowledgement batch that completes or fails.

### When Callbacks are Triggered

Acknowledgement callbacks are triggered in the following scenarios:

| Trigger | Description | Error Code |
|---------|-------------|------------|
| **Successful acknowledgement** | Broker successfully processes the acknowledgement request | `RD_KAFKA_RESP_ERR_NO_ERROR` |
| **Broker error** | Broker returns an error for the acknowledgement request | Per-partition error |
| **Client-side invalidation** | Acknowledgements invalidated due to leader change or session reset | `NOT_LEADER_OR_FOLLOWER`, `SHARE_SESSION_NOT_FOUND` |
| **Timeout** | Acknowledgements fail due to request timeout | `REQUEST_TIMED_OUT` |

### Callback Op Flow

1. When acknowledgements complete or are invalidated, an acknowledgement callback op is created
2. The callback op is enqueued to the `rk_rep` queue
3. The callback op contains per-partition error information
4. The op is processed in the application thread during `poll()`, `commitAsync()`, or `commitSync()` calls

### Processing Location

| Stage | Thread |
|-------|--------|
| **Creation** | Main thread or broker thread (depending on where the acknowledgement result is determined) |
| **Execution** | Application thread (or background thread if `background_event_cb` is enabled) when processing the `rk_rep` queue |

### Callback Information

The callback receives the following information:
- **Partition information** (topic and partition)
- **Offset range** of the acknowledged records
- **Error code** (success or specific error)

### Relationship with Commit APIs

| API | Callback Behavior |
|-----|-------------------|
| **`commitAsync()`** | Callback is invoked asynchronously when the acknowledgement completes. The application continues execution and receives the result via the callback. |
| **`commitSync()`** | The API blocks and returns the result directly. If a callback is registered, it is also invoked asynchronously, generally before the API returns. |

---

## Current State Analysis

### Existing Share Consumer Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                          Application Thread                          │
├──────────────────────────────────────────────────────────────────────┤
│  rd_kafka_share_consume_batch()   rd_kafka_share_commit_async()      │
│           │                                │                         │
│           ▼                                ▼                         │
│  ┌─────────────────┐              ┌────────────────────┐            │
│  │ Extract acks    │              │ Extract acks from  │            │
│  │ from inflight   │              │ inflight map       │            │
│  │ map (implicit)  │              │ (explicit mode)    │            │
│  └────────┬────────┘              └──────────┬─────────┘            │
│           │                                  │                       │
│           ▼                                  ▼                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │              RD_KAFKA_OP_SHARE_FETCH_FANOUT                 │    │
│  │              RD_KAFKA_OP_SHARE_COMMIT_ASYNC_FANOUT          │    │
│  │              (contains ack_batches list)                     │    │
│  └──────────────────────────┬──────────────────────────────────┘    │
└─────────────────────────────┼────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│                           Main Thread                                │
├──────────────────────────────────────────────────────────────────────┤
│  Fanout handler:                                                     │
│  - Segregate ack_batches by leader broker                            │
│  - Create RD_KAFKA_OP_SHARE_FETCH ops per broker                     │
│  - Each broker op gets its partition acks                            │
└──────────────────────────┬───────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│                          Broker Thread(s)                            │
├──────────────────────────────────────────────────────────────────────┤
│  - Build ShareFetch/ShareAcknowledge request                         │
│  - Include acknowledgements in request                               │
│  - Send to broker, receive response                                  │
│  - Parse response with per-partition errors                          │
│  - Currently: NO callback mechanism exists                           │
└──────────────────────────────────────────────────────────────────────┘
```

### Key Existing Structures

```c
// rdkafka_int.h:790-824
struct rd_kafka_share_s {
    rd_kafka_t *rkshare_rk;
    rd_kafka_share_inflight_map_t rkshare_inflight_acks;
    int64_t rkshare_unacked_cnt;
    rd_bool_t rkshare_fetch_more_records_requested;
    rd_bool_t rkshare_consumer_closing;
};

// rdkafka_share_acknowledgement.h:63-71
typedef struct rd_kafka_share_ack_batch_entry_s {
    int64_t start_offset;
    int64_t end_offset;
    int64_t size;
    int32_t types_cnt;
    int16_t delivery_count;
    rd_kafka_share_internal_acknowledgement_type *types;
} rd_kafka_share_ack_batch_entry_t;

// rdkafka_share_acknowledgement.h:90-100
typedef struct rd_kafka_share_ack_batches_s {
    rd_kafka_topic_partition_t *rktpar;
    int32_t response_leader_id;
    int32_t response_leader_epoch;
    int64_t response_acquired_offsets_count;
    rd_list_t entries;  // rd_kafka_share_ack_batch_entry_t*
} rd_kafka_share_ack_batches_t;
```

---

## Proposed Design

### 1. Public API

#### Callback Registration

```c
// rdkafka.h - New API
/**
 * @brief Set share acknowledgement callback.
 *
 * The acknowledgement callback is triggered when acknowledgements
 * sent via rd_kafka_share_commit_async() or piggybacked on
 * rd_kafka_share_consume_batch() complete (successfully or with error).
 *
 * This callback is served by rd_kafka_share_consume_batch() or
 * rd_kafka_poll() / rd_kafka_consumer_poll().
 *
 * @param conf Configuration object.
 * @param share_acknowledgement_cb Callback function.
 *
 * The callback receives:
 * - rk: The Kafka handle (use rd_kafka_share_get_consumer(rkshare)
 *       to get the share consumer if needed)
 * - err: Overall error code (RD_KAFKA_RESP_ERR_NO_ERROR on success)
 * - acknowledgements: List of topic-partitions with per-partition results.
 *       Each partition contains:
 *       - topic, partition: The topic-partition
 *       - offset: The start_offset of the acknowledged range
 *       - metadata: Pointer to rd_kafka_share_ack_result_t with details
 *       - err: Per-partition error code
 * - opaque: Application opaque from rd_kafka_conf_set_opaque()
 *
 * @remark The acknowledgements list and its contents are only valid
 *         during the callback invocation.
 */
RD_EXPORT
void rd_kafka_conf_set_share_acknowledgement_cb(
    rd_kafka_conf_t *conf,
    void (*share_acknowledgement_cb)(
        rd_kafka_t *rk,
        rd_kafka_resp_err_t err,
        rd_kafka_topic_partition_list_t *acknowledgements,
        void *opaque));
```

#### Acknowledgement Result Structure

```c
// rdkafka.h - New type for detailed ack results
/**
 * @brief Share acknowledgement result details.
 *
 * Passed via the metadata field of topic_partition in the callback.
 */
typedef struct rd_kafka_share_ack_result_s {
    int64_t start_offset;        /**< First offset in range */
    int64_t end_offset;          /**< Last offset in range (inclusive) */
    rd_kafka_share_AcknowledgeType_t type;  /**< ACCEPT/RELEASE/REJECT */
} rd_kafka_share_ack_result_t;
```

### 2. Configuration Storage

```c
// rdkafka_conf.h - Add to rd_kafka_conf_s
struct rd_kafka_conf_s {
    // ... existing fields ...

    /* Share consumer acknowledgement callback */
    void (*share_acknowledgement_cb)(
        rd_kafka_t *rk,
        rd_kafka_resp_err_t err,
        rd_kafka_topic_partition_list_t *acknowledgements,
        void *opaque);
};
```

### 3. New Op Type

```c
// rdkafka_op.h - Add new op type
typedef enum {
    // ... existing types ...
    RD_KAFKA_OP_SHARE_ACK_REPLY,  /**< Share acknowledgement reply
                                   *   for callback delivery */
    // ...
} rd_kafka_op_type_t;

// rdkafka_op.h - Add new union member
struct rd_kafka_op_s {
    // ... base fields ...
    union {
        // ... existing members ...

        struct {
            /** List of acknowledgement results.
             *  Type: rd_kafka_topic_partition_list_t*
             *  Each entry has per-partition error and metadata
             *  pointing to rd_kafka_share_ack_result_t. */
            rd_kafka_topic_partition_list_t *ack_results;

            /** Callback function reference */
            void (*cb)(rd_kafka_t *rk,
                       rd_kafka_resp_err_t err,
                       rd_kafka_topic_partition_list_t *acknowledgements,
                       void *opaque);

            /** User opaque */
            void *opaque;

            /** Request identifier for correlation */
            int64_t request_id;
        } share_ack_reply;
    } rko_u;
};
```

### 4. Flow Diagram

```
┌────────────────────────────────────────────────────────────────────────────┐
│                            APPLICATION THREAD                              │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │ Step 1: User calls rd_kafka_share_commit_async() or consume_batch()  │ │
│  │         with acks (implicit/explicit mode)                           │ │
│  └───────────────────────────────┬──────────────────────────────────────┘ │
│                                  │                                         │
│                                  ▼                                         │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │ Step 2: Build ack_batches list from inflight map                     │ │
│  │         rd_kafka_share_build_ack_details()                           │ │
│  └───────────────────────────────┬──────────────────────────────────────┘ │
│                                  │                                         │
│                                  ▼                                         │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │ Step 3: Create FANOUT op with:                                       │ │
│  │   - ack_batches                                                      │ │
│  │   - callback reference (from rk->rk_conf.share_acknowledgement_cb)   │ │
│  │   - request_id (for correlation)                                     │ │
│  └───────────────────────────────┬──────────────────────────────────────┘ │
│                                  │                                         │
└──────────────────────────────────┼─────────────────────────────────────────┘
                                   │ Enqueue to rk_ops
                                   ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                              MAIN THREAD                                   │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │ Step 4: Fanout handler                                               │ │
│  │   - Segregate ack_batches by leader broker                           │ │
│  │   - Track which partitions need ack results                          │ │
│  │   - Create per-broker SHARE_FETCH ops with ack_details               │ │
│  │   - Each broker op carries callback reference for reply              │ │
│  └───────────────────────────────┬──────────────────────────────────────┘ │
│                                  │                                         │
└──────────────────────────────────┼─────────────────────────────────────────┘
                                   │ Per-broker ops to broker threads
                                   ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                           BROKER THREAD(S)                                 │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │ Step 5: Build and send ShareFetch/ShareAcknowledge request           │ │
│  │         with acknowledgements                                        │ │
│  └───────────────────────────────┬──────────────────────────────────────┘ │
│                                  │                                         │
│                                  ▼                                         │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │ Step 6: Receive response, parse:                                     │ │
│  │   - Top-level error (timeout, network, fatal)                        │ │
│  │   - Per-partition errors                                             │ │
│  │   - Per-acknowledgement errors                                       │ │
│  └───────────────────────────────┬──────────────────────────────────────┘ │
│                                  │                                         │
│                                  ▼                                         │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │ Step 7: Create RD_KAFKA_OP_SHARE_ACK_REPLY op                        │ │
│  │   - If callback registered                                           │ │
│  │   - Populate ack_results with per-partition/ack errors               │ │
│  │   - Enqueue to rk->rk_rep (reply queue)                              │ │
│  └───────────────────────────────┬──────────────────────────────────────┘ │
│                                  │                                         │
└──────────────────────────────────┼─────────────────────────────────────────┘
                                   │ Enqueue to rk_rep
                                   ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                           APPLICATION THREAD                               │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │ Step 8: rd_kafka_share_consume_batch() / rd_kafka_poll() serves      │ │
│  │         the reply queue                                              │ │
│  └───────────────────────────────┬──────────────────────────────────────┘ │
│                                  │                                         │
│                                  ▼                                         │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │ Step 9: rd_kafka_poll_cb() routes RD_KAFKA_OP_SHARE_ACK_REPLY        │ │
│  │   case RD_KAFKA_OP_SHARE_ACK_REPLY:                                  │ │
│  │       rko->rko_u.share_ack_reply.cb(                                 │ │
│  │           rk,                                                        │ │
│  │           rko->rko_err,                                              │ │
│  │           rko->rko_u.share_ack_reply.ack_results,                    │ │
│  │           rko->rko_u.share_ack_reply.opaque                          │ │
│  │       );                                                             │ │
│  └───────────────────────────────┬──────────────────────────────────────┘ │
│                                  │                                         │
│                                  ▼                                         │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │ Step 10: Op destroyed, resources freed                               │ │
│  └──────────────────────────────────────────────────────────────────────┘ │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## Error Handling

### Error Hierarchy (From Documentation)

Based on the provided documentation, errors should be reported in this priority:

```
1. Request timeout / Network error
   └─► Return this error in callback (REQUEST_TIMED_OUT)

2. Top-level error (Fatal, Session invalidation)
   └─► Return this error, ignore partition-level errors

3. Client-side invalidation
   └─► NOT_LEADER_OR_FOLLOWER (leader change)
   └─► SHARE_SESSION_NOT_FOUND (session reset)

4. Partition not present in response
   └─► Return INVALID_RECORD_STATE for that partition

5. Acknowledgement-level error
   └─► Return per-ack error for each affected range

6. Success
   └─► Return RD_KAFKA_RESP_ERR_NO_ERROR
```

### Error Scenarios and Callback Behavior

| Scenario | Top Error | Per-Partition | Callback Behavior |
|----------|-----------|---------------|-------------------|
| Success | NO_ERROR | NO_ERROR | Single callback with all acked partitions |
| Network error | REQUEST_TIMED_OUT | N/A | Single callback with timeout error |
| Fatal error | FATAL | N/A | Single callback, consumer not usable |
| Leader change | NO_ERROR | NOT_LEADER_OR_FOLLOWER | Callback with invalidation error |
| Session reset | NO_ERROR | SHARE_SESSION_NOT_FOUND | Callback with session error |
| Some partitions failed | NO_ERROR | Mixed | Single callback, check each partition's err |
| Retriable (after retries) | NO_ERROR | Last error | Callback after retry exhaustion |
| Non-retriable | NO_ERROR | Error | Immediate callback with error |

### Client-Side Invalidation

Acknowledgements can be invalidated on the client side in these scenarios:

1. **Leader Change (`NOT_LEADER_OR_FOLLOWER`)**:
   - Partition leadership changes while acks are pending
   - Acks are invalidated and callback is triggered with this error
   - Application should re-acknowledge after consuming from new leader

2. **Session Reset (`SHARE_SESSION_NOT_FOUND`)**:
   - Share session is invalidated (e.g., due to reconnection)
   - All pending acks for that session are invalidated
   - Callback triggered immediately with session error

### Retry Handling

From the documentation:
- **ShareFetch requests**: Never retried. All acks fail with received error.
- **ShareAcknowledge requests**:
  - Top-level errors: Not retriable
  - Partition-level errors: Retriable until absolute timeout
  - Callback sent after all retries or on non-retriable error

```c
// Example: Retry tracking in broker thread
struct share_ack_retry_state {
    rd_ts_t absolute_timeout;
    rd_list_t pending_partitions;     // Partitions still being retried
    rd_list_t completed_partitions;   // Partitions with final result
    rd_kafka_resp_err_t last_error;   // Most recent error for display
};
```

### commitSync vs commitAsync Callback Timing

```
┌────────────────────────────────────────────────────────────────┐
│                     commitAsync() Flow                         │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  Application Thread              Broker Thread                 │
│  ─────────────────              ─────────────                  │
│       │                              │                         │
│       │ commitAsync()                │                         │
│       │────────────────────────────► │                         │
│       │                              │                         │
│       │ Returns immediately          │ Process acks            │
│       │ ◄─────────────────           │─────────────►           │
│       │                              │                         │
│       │                              │ Create reply op         │
│       │                              │─────────────────┐       │
│       │                              │                 │       │
│       │ poll() / consume_batch()     │ Enqueue to rk_rep       │
│       │◄─────────────────────────────│◄────────────────┘       │
│       │                              │                         │
│       │ Callback invoked             │                         │
│       │ (asynchronously)             │                         │
│                                                                │
└────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────┐
│                      commitSync() Flow                         │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  Application Thread              Broker Thread                 │
│  ─────────────────              ─────────────                  │
│       │                              │                         │
│       │ commitSync()                 │                         │
│       │────────────────────────────► │                         │
│       │                              │                         │
│       │ (blocks waiting)             │ Process acks            │
│       │ . . . . . . .                │─────────────►           │
│       │ . . . . . . .                │                         │
│       │ . . . . . . .                │ Create reply op         │
│       │ . . . . . . .                │─────────────────┐       │
│       │                              │                 │       │
│       │ Callback invoked ◄───────────│◄────────────────┘       │
│       │ (during commitSync)          │                         │
│       │                              │                         │
│       │ Returns with result          │                         │
│       │ ◄─────────────────           │                         │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

---

## Implementation Plan

### Phase 1: API and Configuration

1. **Add callback type and registration API** (`rdkafka.h`)
   - `rd_kafka_conf_set_share_acknowledgement_cb()`
   - `rd_kafka_share_ack_result_t` struct

2. **Add callback storage** (`rdkafka_conf.h`)
   - `share_acknowledgement_cb` field

3. **Add registration function** (`rdkafka_conf.c`)
   - Implement `rd_kafka_conf_set_share_acknowledgement_cb()`

### Phase 2: Op Infrastructure

4. **Add new op type** (`rdkafka_op.h`)
   - `RD_KAFKA_OP_SHARE_ACK_REPLY`
   - `share_ack_reply` union member

5. **Add op handlers** (`rdkafka_op.c`)
   - Add to `rd_kafka_op_type_names[]`
   - Add destroy handler for `share_ack_reply`
   - Add to `rd_kafka_op_destroy_flags[]` if needed

### Phase 3: Pending Acks Tracking

6. **Add pending acks structure** (`rdkafka_broker.h`)
   - `rd_kafka_share_pending_ack_t` struct
   - Add `rkb_share_pending_acks` list to broker struct
   - Add mutex for thread safety

7. **Implement pending acks management** (`rdkafka_broker.c`)
   - `rd_kafka_share_pending_ack_add()` - before sending request
   - `rd_kafka_share_pending_ack_remove()` - on response
   - `rd_kafka_share_pending_ack_invalidate_all()` - on session/leader change

### Phase 4: Request Flow Modification

8. **Modify FANOUT ops** (`rdkafka.c`, `rdkafka_fetcher.c`)
   - Carry callback reference in fanout
   - Pass through to per-broker ops

9. **Modify broker ops** (`rdkafka_fetcher.c`, `rdkafka_broker.c`)
   - Store callback reference in `share_fetch` op
   - Add to pending acks before sending request
   - Track acknowledgements for result correlation

### Phase 5: Response Handling (Broker Thread)

10. **Modify response handler** (`rdkafka_fetcher.c`)
    - Parse acknowledgement results from response
    - Handle error hierarchy (top-level > partition > ack-level)
    - Remove from pending acks
    - Create reply op if callback registered

11. **Create reply op and enqueue** (`rdkafka_fetcher.c`)
    ```c
    if (rk->rk_conf.share_acknowledgement_cb) {
        rd_kafka_op_t *reply = rd_kafka_op_new(
            RD_KAFKA_OP_SHARE_ACK_REPLY | RD_KAFKA_OP_REPLY);
        reply->rko_err = top_level_err;
        reply->rko_u.share_ack_reply.ack_results = build_ack_results(...);
        reply->rko_u.share_ack_reply.cb = rk->rk_conf.share_acknowledgement_cb;
        reply->rko_u.share_ack_reply.opaque = rk->rk_conf.opaque;
        rd_kafka_q_enq(rk->rk_rep, reply);
    }
    ```

### Phase 6: Invalidation Handling (Main Thread)

12. **Handle session reset** (`rdkafka_share.c` or relevant session code)
    - On `SHARE_SESSION_NOT_FOUND`: invalidate pending acks
    - Create reply op with session error

13. **Handle leader change** (`rdkafka_metadata.c` or relevant code)
    - On `NOT_LEADER_OR_FOLLOWER`: invalidate pending acks for partition
    - Create reply op with leader error

### Phase 7: Callback Delivery

14. **Add poll handler** (`rdkafka.c`)
    ```c
    case RD_KAFKA_OP_SHARE_ACK_REPLY:
        if (rko->rko_u.share_ack_reply.cb)
            rko->rko_u.share_ack_reply.cb(
                rk,
                rko->rko_err,
                rko->rko_u.share_ack_reply.ack_results,
                rko->rko_u.share_ack_reply.opaque);
        break;
    ```

15. **Ensure callback during commitSync** (`rdkafka.c`)
    - Process reply queue before commitSync returns
    - Callback invoked during the blocking wait

### Phase 8: Testing

16. **Unit tests** (`tests/`)
    - Callback registration
    - Callback invocation on success
    - Callback with various error scenarios
    - Pending acks tracking correctness

17. **Integration tests** (`tests/`)
    - Real broker callback delivery
    - Error injection scenarios
    - Session invalidation callbacks
    - Leader change callbacks
    - commitSync vs commitAsync callback timing

18. **Mock broker tests** (`tests/`)
    - Inject NOT_LEADER_OR_FOLLOWER
    - Inject SHARE_SESSION_NOT_FOUND
    - Inject REQUEST_TIMED_OUT
    - Verify callback content accuracy

---

## Callback Aggregation Strategy

Since acknowledgements can span multiple brokers, we need a strategy for aggregating results:

### Option A: Per-Broker Callbacks (Simpler)
- Each broker response triggers its own callback
- Application sees multiple callbacks per `commit_async()` call
- Pros: Simpler implementation, faster feedback
- Cons: Application must correlate results

### Option B: Aggregated Callback (More Complex)
- Wait for all broker responses
- Single callback per `commit_async()` call
- Pros: Simpler for application
- Cons: Delayed feedback, need tracking mechanism

### Recommended: Option A (Per-Broker)
Match the offset commit callback behavior which also fires per-response.

---

## Pending Acks Tracking

To support client-side invalidation, we need to track acknowledgements that are in-flight (sent but not yet confirmed). This enables triggering callbacks when acks are invalidated.

### Tracking Structure

```c
// Per-broker pending acks tracking
typedef struct rd_kafka_share_pending_ack_s {
    rd_kafka_topic_partition_t *rktpar;  /* Topic-partition */
    int64_t start_offset;                /* First offset */
    int64_t end_offset;                  /* Last offset (inclusive) */
    rd_kafka_share_AcknowledgeType_t type;  /* Ack type */
    rd_ts_t ts_sent;                     /* Timestamp when sent */
    TAILQ_ENTRY(rd_kafka_share_pending_ack_s) link;
} rd_kafka_share_pending_ack_t;

// In broker structure (rdkafka_broker.h)
struct rd_kafka_broker_s {
    // ... existing fields ...

    /** Pending share acks awaiting broker response.
     *  Used for invalidation callbacks on leader change/session reset. */
    TAILQ_HEAD(, rd_kafka_share_pending_ack_s) rkb_share_pending_acks;
    mtx_t rkb_share_pending_acks_lock;
};
```

### Tracking Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                   Pending Acks Lifecycle                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. Acks extracted from inflight map                            │
│     │                                                           │
│     ▼                                                           │
│  2. Acks added to rkb_share_pending_acks (before request sent)  │
│     │                                                           │
│     ├──────────────────────────────────┐                        │
│     │                                  │                        │
│     ▼                                  ▼                        │
│  3a. Response received              3b. Invalidation occurs     │
│     │                                  │                        │
│     ▼                                  ▼                        │
│  4a. Remove from pending,           4b. Remove from pending,    │
│      create callback op with           create callback op with  │
│      broker response results           invalidation error       │
│     │                                  │                        │
│     ▼                                  ▼                        │
│  5. Callback delivered to application                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Memory Management

| Resource | Allocation | Ownership | Freed By |
|----------|------------|-----------|----------|
| `ack_results` list | Main/Broker thread | Reply op | `rd_kafka_op_destroy()` |
| `rd_kafka_share_ack_result_t` | Main/Broker thread | List element | `rd_kafka_op_destroy()` |
| `rd_kafka_share_pending_ack_t` | Broker thread | Broker | On response/invalidation |
| `opaque` | Application | Application | Never by system |
| Reply op | Main/Broker thread | Queue | After callback |

---

## Thread Safety Considerations

1. **Callback invocation**: Always in application thread (via poll)
2. **Reply op creation**: In broker thread, enqueued atomically
3. **ack_results list**: Created in broker thread, consumed in app thread, no concurrent access
4. **Configuration callback pointer**: Set at startup, read-only during operation

---

## Example Usage

### Callback Function

```c
// Callback function - invoked for both commitAsync() and commitSync()
void my_ack_cb(rd_kafka_t *rk,
               rd_kafka_resp_err_t err,
               rd_kafka_topic_partition_list_t *acks,
               void *opaque) {
    int *ack_count = (int *)opaque;

    if (err) {
        fprintf(stderr, "Ack batch failed: %s\n", rd_kafka_err2str(err));
        return;
    }

    for (int i = 0; i < acks->cnt; i++) {
        rd_kafka_topic_partition_t *p = &acks->elems[i];
        rd_kafka_share_ack_result_t *result = p->metadata;

        if (p->err) {
            // Handle specific errors
            switch (p->err) {
                case RD_KAFKA_RESP_ERR_NOT_LEADER_OR_FOLLOWER:
                    fprintf(stderr, "Leader changed for %s[%d], "
                            "will re-ack after next consume\n",
                            p->topic, p->partition);
                    break;
                case RD_KAFKA_RESP_ERR_SHARE_SESSION_NOT_FOUND:
                    fprintf(stderr, "Session reset for %s[%d], "
                            "records will be redelivered\n",
                            p->topic, p->partition);
                    break;
                default:
                    fprintf(stderr, "Ack failed for %s[%d] offsets %ld-%ld: %s\n",
                            p->topic, p->partition,
                            result->start_offset, result->end_offset,
                            rd_kafka_err2str(p->err));
            }
        } else {
            printf("Ack success for %s[%d] offsets %ld-%ld (type=%d)\n",
                   p->topic, p->partition,
                   result->start_offset, result->end_offset,
                   result->type);
            (*ack_count)++;
        }
    }
}
```

### Configuration and Setup

```c
int ack_count = 0;

rd_kafka_conf_t *conf = rd_kafka_conf_new();
rd_kafka_conf_set_share_acknowledgement_cb(conf, my_ack_cb);
rd_kafka_conf_set_opaque(conf, &ack_count);

rd_kafka_share_t *rkshare = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
```

### Usage with commitAsync()

```c
// commitAsync() - non-blocking, callback invoked later
while (running) {
    rd_kafka_message_t *msgs[100];
    size_t cnt;

    error = rd_kafka_share_consume_batch(rkshare, 1000, msgs, &cnt);

    for (size_t i = 0; i < cnt; i++) {
        process_message(msgs[i]);
        rd_kafka_share_acknowledge(rkshare, msgs[i]);
        rd_kafka_message_destroy(msgs[i]);
    }

    // Non-blocking commit - returns immediately
    // Callback will be invoked during a future consume_batch() or poll()
    error = rd_kafka_share_commit_async(rkshare);
    if (error) {
        fprintf(stderr, "commit_async failed: %s\n",
                rd_kafka_error_string(error));
        rd_kafka_error_destroy(error);
    }

    // Callback is delivered here (during consume_batch)
    // ...next iteration of loop...
}
```

### Usage with commitSync()

```c
// commitSync() - blocking, callback may be invoked before return
rd_kafka_message_t *msgs[100];
size_t cnt;

error = rd_kafka_share_consume_batch(rkshare, 1000, msgs, &cnt);

for (size_t i = 0; i < cnt; i++) {
    process_message(msgs[i]);
    rd_kafka_share_acknowledge(rkshare, msgs[i]);
    rd_kafka_message_destroy(msgs[i]);
}

// Blocking commit - waits for result
// Callback is typically invoked before this returns
error = rd_kafka_share_commit_sync(rkshare);
if (error) {
    fprintf(stderr, "commit_sync failed: %s\n",
            rd_kafka_error_string(error));
    rd_kafka_error_destroy(error);
} else {
    printf("Sync commit successful, ack_count now: %d\n", ack_count);
}
```

---

## Callback Op Creation Location

The callback op can be created in different threads depending on where the acknowledgement result is determined:

### Broker Thread Creation (Normal Path)

When acknowledgements are successfully sent to and processed by the broker:

```c
// In broker response handler (rdkafka_fetcher.c)
static void rd_kafka_share_fetch_response_handle(...) {
    // Parse response...

    if (rk->rk_conf.share_acknowledgement_cb && has_ack_results) {
        rd_kafka_op_t *reply = rd_kafka_op_new(
            RD_KAFKA_OP_SHARE_ACK_REPLY | RD_KAFKA_OP_REPLY);
        // ... populate reply ...
        rd_kafka_q_enq(rk->rk_rep, reply);  // Enqueue to reply queue
    }
}
```

### Main Thread Creation (Invalidation Path)

When acknowledgements are invalidated due to session reset or leader change:

```c
// In main thread during session handling
static void rd_kafka_share_session_invalidate(...) {
    // For each pending ack batch...
    if (rk->rk_conf.share_acknowledgement_cb) {
        rd_kafka_op_t *reply = rd_kafka_op_new(
            RD_KAFKA_OP_SHARE_ACK_REPLY | RD_KAFKA_OP_REPLY);
        reply->rko_err = RD_KAFKA_RESP_ERR_SHARE_SESSION_NOT_FOUND;
        // ... populate with invalidated acks ...
        rd_kafka_q_enq(rk->rk_rep, reply);
    }
}

// In main thread during metadata update (leader change)
static void rd_kafka_share_handle_leader_change(...) {
    if (rk->rk_conf.share_acknowledgement_cb && pending_acks) {
        rd_kafka_op_t *reply = rd_kafka_op_new(
            RD_KAFKA_OP_SHARE_ACK_REPLY | RD_KAFKA_OP_REPLY);
        reply->rko_err = RD_KAFKA_RESP_ERR_NOT_LEADER_OR_FOLLOWER;
        // ... populate with invalidated acks ...
        rd_kafka_q_enq(rk->rk_rep, reply);
    }
}
```

---

## Open Questions

1. **Should we support per-call callbacks?** (like `rd_kafka_commit_queue`)
   - Add optional callback parameter to `rd_kafka_share_commit_async()`?
   - This would allow different callbacks for different commit operations

2. **Aggregation timeout**: If aggregating across brokers, how long to wait?
   - Current recommendation: Per-broker callbacks (no aggregation)

3. **Event API support**: Should we support `RD_KAFKA_EVENT_SHARE_ACK` for event-based consumption?
   - Would enable rd_kafka_queue_poll() based result retrieval

4. **Background thread support**: The documentation mentions "background thread if enabled" - should we support `background_event_cb` for share ack callbacks?

---

## References

- [OFFSET_COMMIT_CALLBACK_DESIGN.md](OFFSET_COMMIT_CALLBACK_DESIGN.md) - Reference implementation
- [KIP-932 - Fetch Flow Design and Internal Architecture](https://confluentinc.atlassian.net/wiki/spaces/~62f24383432ef494c8cb9d81/pages/4994008108) - Original specification
- `src/rdkafka_share_acknowledgement.c` - Current acknowledgement implementation
- `src/rdkafka_fetcher.c` - Share fetch/ack request handling
