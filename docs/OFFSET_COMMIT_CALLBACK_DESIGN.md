# Offset Commit Callback Design (Reference Implementation)

This document describes how the offset commit callback is implemented in librdkafka's regular consumer. This serves as the reference design for implementing the Share Consumer Acknowledgement Callback.

## Overview

The offset commit callback provides users a way to receive notification when offset commits complete (either successfully or with an error). It is triggered for both automatic commits (auto.commit) and manual commits (rd_kafka_commit/rd_kafka_commit_queue).

## Key Components

### 1. Callback Registration

#### Public API (rdkafka.h:2162-2167)

```c
void rd_kafka_conf_set_offset_commit_cb(
    rd_kafka_conf_t *conf,
    void (*offset_commit_cb)(rd_kafka_t *rk,
                             rd_kafka_resp_err_t err,
                             rd_kafka_topic_partition_list_t *offsets,
                             void *opaque));
```

**Parameters:**
- `rk`: Kafka handle
- `err`: Overall error code (RD_KAFKA_RESP_ERR_NO_ERROR on success)
- `offsets`: List of topic-partitions with per-partition errors
- `opaque`: User-provided opaque pointer from configuration

#### Storage (rdkafka_conf.h:460-463)

```c
struct rd_kafka_conf_s {
    // ... other fields ...
    void (*offset_commit_cb)(rd_kafka_t *rk,
                             rd_kafka_resp_err_t err,
                             rd_kafka_topic_partition_list_t *offsets,
                             void *opaque);
};
```

The callback is stored in the global configuration structure and is accessible via `rk->rk_conf.offset_commit_cb`.

---

### 2. Operation Type

#### Op Type Definition (rdkafka_op.h:84-150)

```c
typedef enum {
    // ... other types ...
    RD_KAFKA_OP_OFFSET_COMMIT,  /* any -> toppar's Broker thread */
    // ... other types ...
} rd_kafka_op_type_t;
```

#### Op Union Member (rdkafka_op.h:352-363)

```c
struct rd_kafka_op_s {
    // ... base fields ...
    union {
        // ... other members ...
        struct {
            rd_kafka_topic_partition_list_t *partitions;
            void (*cb)(rd_kafka_t *rk,
                       rd_kafka_resp_err_t err,
                       rd_kafka_topic_partition_list_t *offsets,
                       void *opaque);
            void *opaque;
            int silent_empty;       /* Don't log empty commits */
            rd_ts_t ts_timeout;     /* Request timeout timestamp */
            char *reason;           /* Descriptive reason string */
        } offset_commit;
    } rko_u;
};
```

---

### 3. Queue System

#### Reply Queue Pattern

librdkafka uses a reply queue pattern for callback delivery:

```c
struct rd_kafka_s {
    rd_kafka_q_t *rk_rep;   /* kafka -> application reply queue */
    rd_kafka_q_t *rk_ops;   /* any -> rdkafka main thread ops */
};
```

- `rk_rep`: The reply queue where callback ops are enqueued for application consumption
- `rk_ops`: The operations queue for main thread processing

#### Queue Enqueue (rdkafka_queue.h:428-486)

```c
static RD_INLINE int rd_kafka_q_enq(rd_kafka_q_t *rkq, rd_kafka_op_t *rko) {
    return rd_kafka_q_enq1(rkq, rko, rkq, 0, 1);
}
```

Key characteristics:
- Thread-safe with mutex protection
- Supports queue forwarding (ops can be redirected)
- Priority-based insertion (callback ops use `RD_KAFKA_PRIO_HIGH`)
- Condition variable signaling for waiting threads

---

### 4. Complete Flow

#### Step 1: Offset Commit Request Initiated

When the application calls `rd_kafka_commit()` or auto-commit triggers:

```c
// rdkafka_cgrp.c:4681-4696
rko = rd_kafka_op_new(RD_KAFKA_OP_OFFSET_COMMIT);
rko->rko_u.offset_commit.reason = rd_strdup(reason);
rko->rko_u.offset_commit.cb = rkcg->rkcg_rk->rk_conf.offset_commit_cb;
rko->rko_u.offset_commit.opaque = rkcg->rkcg_rk->rk_conf.opaque;
rko->rko_u.offset_commit.partitions = rd_kafka_topic_partition_list_copy(offsets);
```

#### Step 2: Request Sent to Broker

The op is sent to the broker thread which:
1. Builds the OffsetCommitRequest
2. Sends it to the coordinator broker
3. Waits for the response

#### Step 3: Broker Response Handler

```c
// rdkafka_cgrp.c:4357
rd_kafka_cgrp_op_handle_OffsetCommit(...)
    -> Parses response
    -> Fills partition-level errors
    -> Calls rd_kafka_cgrp_offset_commit_reply_enq()
```

#### Step 4: Reply Op Created and Enqueued

```c
// rdkafka_cgrp.c:4285-4299
if (rk->rk_conf.offset_commit_cb) {
    rko_reply = rd_kafka_op_new_reply(rko_orig, err);
    rko_reply->rko_u.offset_commit.cb = rk->rk_conf.offset_commit_cb;
    rko_reply->rko_u.offset_commit.opaque = rk->rk_conf.opaque;
    rko_reply->rko_u.offset_commit.partitions =
        rd_kafka_topic_partition_list_copy(offsets_with_errors);

    // Enqueue to application reply queue
    rd_kafka_q_enq(rk->rk_rep, rko_reply);
}
```

The reply op is marked with `RD_KAFKA_OP_REPLY` flag:
```c
#define RD_KAFKA_OP_REPLY    (int)(1 << 30)  /* Reply op */
```

#### Step 5: Application Polls

When the application calls `rd_kafka_poll()` or `rd_kafka_consumer_poll()`:

```c
// rdkafka_queue.c:558-650
int rd_kafka_q_serve0(rd_kafka_q_t *rkq, ...) {
    // Wait for ops
    while (!(rko = TAILQ_FIRST(&rkq->rkq_q)))
        cnd_timedwait(...);

    // Process each op
    while ((rko = TAILQ_FIRST(&localq.rkq_q))) {
        rd_kafka_q_deq0(&localq, rko);
        res = rd_kafka_op_handle(rk, &localq, rko, cb_type, opaque, callback);
    }
}
```

#### Step 6: Op Routing to Handler

```c
// rdkafka_op.c:991-1021
rd_kafka_op_res_t rd_kafka_op_handle(...) {
    res = rd_kafka_op_handle_std(rk, rkq, rko, cb_type);
    // Calls rd_kafka_poll_cb() for routing
}
```

#### Step 7: Callback Invocation

```c
// rdkafka.c:5057-5063
case RD_KAFKA_OP_OFFSET_COMMIT | RD_KAFKA_OP_REPLY:
    if (!rko->rko_u.offset_commit.cb)
        return RD_KAFKA_OP_RES_PASS;

    // INVOKE THE CALLBACK
    rko->rko_u.offset_commit.cb(
        rk,
        rko->rko_err,
        rko->rko_u.offset_commit.partitions,
        rko->rko_u.offset_commit.opaque
    );
    break;
```

#### Step 8: Cleanup

After callback execution, the op is destroyed:
```c
rd_kafka_op_destroy(rko);
// Frees: partitions list, reason string, op itself
```

---

### 5. Thread Model

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Application    │     │   Broker Thread  │     │  Main Thread    │
│     Thread      │     │                  │     │                 │
└────────┬────────┘     └────────┬─────────┘     └────────┬────────┘
         │                       │                        │
         │ rd_kafka_commit()     │                        │
         │──────────────────────►│                        │
         │                       │                        │
         │                       │ OffsetCommitRequest    │
         │                       │───────────────────────►│
         │                       │                        │ (to broker)
         │                       │◄───────────────────────│
         │                       │ OffsetCommitResponse   │
         │                       │                        │
         │                       │ Create reply op        │
         │                       │──────────────────┐     │
         │                       │                  │     │
         │                       │ Enqueue to rk_rep      │
         │                       │──────────────────┘     │
         │                       │                        │
         │ rd_kafka_poll()       │                        │
         │ (serves rk_rep)       │                        │
         │                       │                        │
         │ Callback invoked      │                        │
         │ in app thread         │                        │
         │                       │                        │
```

**Critical Points:**
1. Callbacks are **NEVER** invoked from broker threads
2. Callbacks are invoked from the application thread calling `rd_kafka_poll()`
3. Reply ops are enqueued with high priority for prompt delivery

---

### 6. Error Handling

#### Error Levels

1. **Top-level error** (`rko->rko_err`):
   - Request timeout
   - Network error
   - Coordinator not available
   - Fatal errors

2. **Partition-level errors** (`offsets[i].err`):
   - UNKNOWN_TOPIC_OR_PARTITION
   - OFFSET_METADATA_TOO_LARGE
   - GROUP_AUTHORIZATION_FAILED
   - etc.

#### Error Propagation

```c
// If no callback registered, errors are logged
if (!offset_commit_cb_served && errcnt > 0) {
    rd_kafka_log(rk, LOG_WARNING, "COMMITFAIL",
                 "Offset commit failed for %d/%d partition(s): %s",
                 errcnt, total_cnt, rd_kafka_err2str(err));
}
```

---

### 7. Memory Management

| Resource | Ownership | Freed By |
|----------|-----------|----------|
| `rko` (op) | System | `rd_kafka_op_destroy()` after callback |
| `partitions` list | Op | Op destroy callback |
| `reason` string | Op | Op destroy callback |
| `opaque` | Application | Application (never freed by system) |

---

### 8. Key Design Patterns

1. **Op-based Communication**: All inter-thread communication uses ops
2. **Reply Queue Pattern**: Request ops spawn reply ops for async results
3. **Priority Queuing**: Callback ops get high priority
4. **Copy-on-Send**: Partition lists are copied when creating reply ops
5. **Thread Isolation**: Callbacks run in application thread context
6. **Graceful Degradation**: Missing callbacks result in logging, not crashes

---

### 9. File References

| File | Content |
|------|---------|
| `src/rdkafka.h:2162-2167` | Public callback registration API |
| `src/rdkafka_conf.h:460-463` | Callback storage in config |
| `src/rdkafka_op.h:352-363` | Op union member definition |
| `src/rdkafka_cgrp.c:4274-4346` | Reply op creation and enqueue |
| `src/rdkafka.c:5057-5063` | Callback invocation in poll |
| `src/rdkafka_queue.c:558-650` | Queue serving implementation |
| `src/rdkafka_op.c:991-1021` | Op handling and routing |

---

## Summary

The offset commit callback follows a well-established pattern:
1. **Registration**: Callback stored in configuration
2. **Request**: Commit op created with callback reference
3. **Response**: Reply op created and enqueued to reply queue
4. **Delivery**: Application poll dequeues and invokes callback
5. **Cleanup**: Op and resources freed after callback returns

This pattern ensures thread safety, predictable execution context (application thread), and clean separation of concerns between broker threads and application code.
