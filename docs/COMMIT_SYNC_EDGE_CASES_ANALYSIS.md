# rd_kafka_share_commit_sync() Edge Cases Analysis

This document analyzes all possible scenarios, edge cases, and potential failure modes for the `rd_kafka_share_commit_sync()` API implementation.

---

## Table of Contents

1. [Normal Flow Scenarios](#1-normal-flow-scenarios)
2. [Timeout Scenarios](#2-timeout-scenarios)
3. [Broker Failure Scenarios](#3-broker-failure-scenarios)
4. [Concurrent Operations](#4-concurrent-operations)
5. [Consumer Lifecycle Edge Cases](#5-consumer-lifecycle-edge-cases)
6. [Multi-Broker Scenarios](#6-multi-broker-scenarios)
7. [Race Conditions](#7-race-conditions)
8. [Resource Management](#8-resource-management)
9. [Protocol-Level Errors](#9-protocol-level-errors)
10. [Summary of Potential Issues](#10-summary-of-potential-issues)

---

## 1. Normal Flow Scenarios

### 1.1 Basic Success - Single Partition
```
App: commit_sync(timeout=30s) with 1 partition ack pending
  → Main thread: segregate by leader → dispatch to broker
  → Broker: responds with NO_ERROR
  → App: receives partitions[0].err = NO_ERROR
```
**Status:** HANDLED CORRECTLY

### 1.2 Basic Success - Multiple Partitions, Single Broker
```
App: commit_sync() with 5 partitions, all on same broker
  → Single ShareAcknowledge request sent
  → All 5 partitions return NO_ERROR
```
**Status:** HANDLED CORRECTLY

### 1.3 Basic Success - Multiple Partitions, Multiple Brokers
```
App: commit_sync() with partitions on 3 different brokers
  → 3 parallel ShareAcknowledge requests
  → wait_broker_result_count = 3
  → Each broker responds, count decrements
  → Response sent when count = 0
```
**Status:** HANDLED CORRECTLY

### 1.4 No Pending Acks
```
App: commit_sync() with no pending acknowledgements
  → ack_batches = NULL
  → Returns NULL error, partitions = NULL
```
**Status:** HANDLED CORRECTLY (line 4304-4308)

---

## 2. Timeout Scenarios

### 2.1 Full Timeout - No Broker Responds

```
Timeline:
  t=0s:   commit_sync(timeout=5s) called, 2 brokers
  t=5s:   Timeout fires
          → Pending acks moved to async_ack_details
          → All partitions set to _TIMED_OUT
          → Response sent to app
```

**Code path:** `rd_kafka_share_commit_sync_timeout_cb()` (line 3904)

**Status:** HANDLED CORRECTLY
- Pending sync acks are moved to async queue (will be sent later)
- All `_IN_PROGRESS` partitions become `_TIMED_OUT`

### 2.2 Partial Timeout - Some Brokers Respond

```
Timeline:
  t=0s:   commit_sync(timeout=5s), brokers A and B
  t=2s:   Broker A responds (partitions 0,1 = NO_ERROR)
  t=5s:   Timeout fires, broker B hasn't responded
          → Broker B's pending acks → async queue
          → Partitions 2,3 (from B) = _TIMED_OUT
          → Partitions 0,1 = NO_ERROR (already set)
```

**Status:** HANDLED CORRECTLY
- Successfully responded partitions retain their errors
- Only `_IN_PROGRESS` partitions become `_TIMED_OUT`

### 2.3 Timeout = 0 or Negative

```
App: commit_sync(timeout_ms=0)
  → abs_timeout = rd_timeout_init(0) = current time
  → timeout_us = abs_timeout - rd_clock() ≈ 0 or negative
  → Code sets timeout_us = 1 if <= 0 (line 4192-4193)
  → Timer fires in 1 microsecond = immediate timeout
```

**Status:** HANDLED CORRECTLY
- Code handles `timeout_us <= 0` by setting to 1 microsecond
- Results in immediate timeout (expected behavior)
- Same applies to negative timeout values

### 2.4 Very Long Timeout

```
App: commit_sync(timeout_ms=INT_MAX)
  → Should work, but excessive timeout
```

**Status:** HANDLED (no upper bound check, but not harmful)

---

## 3. Broker Failure Scenarios

### 3.1 Broker Disconnects During Request

```
Timeline:
  t=0s:   ShareAcknowledge sent to broker
  t=1s:   Broker disconnects
          → err = RD_KAFKA_RESP_ERR__TRANSPORT
          → Session reset triggered
          → rko_orig->rko_err set
          → Reply handler processes error
```

**Code path:** `rd_kafka_broker_share_acknowledge_reply()` (line 1930)

**Status:** PARTIALLY HANDLED (Known TODO)
- Error is propagated via `rd_kafka_op_reply(rko_orig, err)`
- **Known TODO (line 3241-3247):** When broker returns top-level error, `ack_results` is NULL. Partitions for that broker remain `_IN_PROGRESS`. Will be addressed in follow-up work.

### 3.2 Broker Not Available at Dispatch Time

```
Scenario: Leader for partition is not available when segregating acks
```

**Code path:** Line 4027-4043

```c
if (!rktp || !rktp->rktp_leader) {
    result_rktpar->err = RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE;
    rd_kafka_share_ack_batches_destroy(batch);
    continue;
}
```

**Status:** HANDLED CORRECTLY
- Partition error set to `LEADER_NOT_AVAILABLE`
- Batch destroyed, continues to next

### 3.3 All Brokers Unavailable

```
Scenario: commit_sync() called but no broker connections
  → All acks fail leader lookup
  → wait_broker_result_count = 0
  → Immediate response with all partitions = LEADER_NOT_AVAILABLE
```

**Code path:** Line 4182-4188

**Status:** HANDLED CORRECTLY

### 3.4 Broker Returns Top-Level Error

```
Errors: SHARE_SESSION_NOT_FOUND, INVALID_SHARE_SESSION_EPOCH, etc.
  → ack_results = NULL (no per-partition results)
  → Session reset triggered
```

**Status:** PARTIALLY HANDLED (Known TODO)
- Per-partition results not populated, partitions remain `_IN_PROGRESS`
- Known TODO at line 3241-3247 - will be addressed in follow-up work

---

## 4. Concurrent Operations

### 4.1 commit_sync While consume_batch In Progress

```
Thread 1: consume_batch() → SHARE_FETCH in flight
Thread 2: commit_sync() called
  → Builds ack_batches from inflight map
  → Creates SYNC_FANOUT op
  → Broker has request in flight (rkb_share_fetch_enqueued = true)
  → Sync acks stored in pending_commit_sync
  → When SHARE_FETCH completes, sync acks dispatched with priority
```

**Code path:** Line 3266-3272 (dispatch with priority)

**Status:** HANDLED CORRECTLY
- Sync acks have priority over async acks
- Stored in `pending_commit_sync` until broker is free

### 4.2 commit_async Immediately Before commit_sync

```
Thread: commit_async() → acks sent asynchronously
Thread: commit_sync() immediately after
  → If acks already extracted by commit_async, commit_sync has empty ack_batches
  → Returns NULL/NULL (no pending acks)
```

**Status:** HANDLED CORRECTLY
- `rd_kafka_share_build_ack_details()` extracts from inflight map
- Each call gets whatever is pending at that moment

### 4.3 commit_sync During Rebalance

```
Scenario: Partitions being revoked while commit_sync in progress
  → Partition leader may change
  → Pending acks may fail with NOT_LEADER_FOR_PARTITION
```

**Status:** PARTIALLY HANDLED
- Individual partition errors returned
- But no specific rebalance handling in commit_sync

### 4.4 Multiple commit_sync Calls (Sequential)

```
App thread is blocked, so can't call commit_sync again while one is in progress.
```

**Status:** N/A - Not possible (app thread blocks)

### 4.5 commit_sync From Multiple Threads

```
Thread A: commit_sync()
Thread B: commit_sync() on same rkshare handle
```

**Status:** CONSISTENT WITH API PATTERN
- Same as `rd_kafka_consume_batch()` which is documented as not thread-safe when called concurrently
- From INTRODUCTION.md: "Using multiple instances of rd_kafka_consume_batch() APIs concurrently is not thread safe"
- Expected to be called from a single thread at a time on the same handle

---

## 5. Consumer Lifecycle Edge Cases

### 5.1 commit_sync After share_consumer_close

```
App: rd_kafka_share_consumer_close(rkshare)
App: rd_kafka_share_commit_sync(rkshare, ...)  // After close
```

**Status:** NOT HANDLED
- No check for consumer state at API entry
- **POTENTIAL ISSUE:** Undefined behavior, possible crash

### 5.2 commit_sync During Consumer Shutdown

```
Scenario: rd_kafka_share_destroy() called while commit_sync blocked
  → Instance terminating
  → Broker threads see terminating flag
  → Ops receive ERR__DESTROY
```

**Code path:** Line 1938-1946 (DESTROY handling)

**Status:** HANDLED
- Reply handler returns immediately on `ERR__DESTROY`
- App thread unblocked with error

### 5.3 commit_sync Before Subscribe

```
App: Creates share consumer
App: commit_sync() without subscribing first
  → No partitions assigned
  → No pending acks
  → Returns NULL/NULL
```

**Status:** HANDLED (trivially - no acks to commit)

### 5.4 commit_sync With NULL Handle

```
App: rd_kafka_share_commit_sync(NULL, 5000, &partitions)
```

**Status:** CONSISTENT WITH API PATTERN
- No NULL check at function entry (same as other share consumer APIs)
- `rd_kafka_share_consume_batch()` and `rd_kafka_share_commit_async()` also don't check for NULL
- Expected that user passes correct handle

---

## 6. Multi-Broker Scenarios

### 6.1 Leader Change During commit_sync

```
Timeline:
  t=0s:   commit_sync() dispatched to broker A (leader for partition 0)
  t=1s:   Leadership changes, broker B is new leader
  t=2s:   Broker A responds with NOT_LEADER_FOR_PARTITION
```

**Status:** HANDLED
- Error returned to app via per-partition results
- Metadata refresh triggered (line 1982-1983)
- Acks lost (not retried) - app must re-acknowledge

### 6.2 Broker Returns NOT_LEADER_FOR_PARTITION

```c
case RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION:
    rd_kafka_metadata_refresh_known_topics(...);
    break;
```

**Status:** HANDLED
- Metadata refresh triggered
- Error propagated to app
- BUT: Acks are destroyed, not retried

### 6.3 One Broker Fast, One Broker Slow

```
Timeline:
  t=0s:   commit_sync(timeout=10s), brokers A and B
  t=1s:   Broker A responds quickly
  t=9s:   Broker B responds just before timeout
  → Both successful, no timeout
```

**Status:** HANDLED CORRECTLY
- Timer only fires if not all brokers respond
- Timer stopped when `wait_broker_result_count = 0`

### 6.4 Broker Responds After Timeout (Stale Reply)

```
Timeline:
  t=0s:   commit_sync(ID=1, timeout=5s)
  t=5s:   Timeout fires, response sent, ID reset to 0
  t=7s:   Broker responds with commit_sync_request_id=1
  → Check: reply.commit_sync_request_id != rkcg.id (0)
  → Stale reply ignored
```

**Code path:** Line 3221-3223

```c
if (rko_orig->rko_u.share_fetch.commit_sync_request_id != 0 &&
    rko_orig->rko_u.share_fetch.commit_sync_request_id ==
        rkcg->rkcg_commit_sync_request.id) {
```

**Status:** HANDLED CORRECTLY
- `commit_sync_id_counter` ensures stale replies are ignored

---

## 7. Race Conditions

### 7.1 Timeout Fires Exactly When Last Broker Responds

```
Timeline:
  t=5000ms:  Timeout callback fires
  t=5000ms:  Broker reply arrives (race!)
```

**Analysis:**
- Both execute on main thread
- One will execute first
- If timeout first: broker reply processed but request.id already 0 → ignored
- If reply first: timer stopped, timeout never fires

**Status:** HANDLED CORRECTLY (single-threaded main loop)

### 7.2 Timer Stop vs Timer Fire Race

```c
// In maybe_complete():
rd_kafka_timer_stop(&rk->rk_timers, &rkcg->rkcg_commit_sync_request.tmr, 1);

// vs timer callback firing
```

**Status:** HANDLED
- Timer stop is thread-safe
- If timer already fired, stop is no-op

### 7.3 Broker Reply After Instance Termination Started

```
Scenario: rd_kafka_destroy() called, broker reply in flight
```

**Code path:** Line 3266-3267

```c
if (!reply_rkb->rkb_share_fetch_enqueued &&
    !rd_kafka_broker_or_instance_terminating(reply_rkb) &&
```

**Status:** HANDLED
- Terminating check prevents new ops from being enqueued
- Existing ops receive `ERR__DESTROY`

---

## 8. Resource Management

### 8.1 Memory Leak on Error Path

**Scenario:** Error occurs after resources allocated

**Check points:**
1. `ack_batches` ownership transferred to op (line 4326) → destroyed by op destroy
2. `results` ownership transferred to op (line 4328) → destroyed by op destroy
3. `tmpq` destroyed after pop (line 4337)

**Status:** HANDLED CORRECTLY
- Op destroy functions handle cleanup (rdkafka_op.c:556-565)

### 8.2 Early Return Without Cleanup

```c
if (!ack_batches) {
    return NULL;  // Early return
}
```

**Analysis:** At this point, only `*partitions = NULL` was set. No resources allocated yet.

**Status:** HANDLED CORRECTLY

### 8.3 Reply Op Cleanup

```c
*partitions = rko_reply->rko_u.share_commit_sync_fanout_reply.results;
rko_reply->rko_u.share_commit_sync_fanout_reply.results = NULL;  // Transfer ownership
rd_kafka_op_destroy(rko_reply);  // Safe - results already extracted
```

**Status:** HANDLED CORRECTLY

---

## 9. Protocol-Level Errors

### 9.1 ShareAcknowledge Specific Errors

| Error | Handling | Status |
|-------|----------|--------|
| `SHARE_SESSION_NOT_FOUND` | Session reset | HANDLED |
| `INVALID_SHARE_SESSION_EPOCH` | Session reset | HANDLED |
| `SHARE_SESSION_LIMIT_REACHED` | Session reset | HANDLED |
| `UNKNOWN_TOPIC_OR_PART` | Metadata refresh | HANDLED |
| `LEADER_NOT_AVAILABLE` | Metadata refresh | HANDLED |
| `NOT_LEADER_FOR_PARTITION` | Metadata refresh | HANDLED |
| `BROKER_NOT_AVAILABLE` | Metadata refresh | HANDLED |
| `REQUEST_TIMED_OUT` (broker-side) | Session reset | HANDLED |

### 9.2 Transport Errors

```c
case RD_KAFKA_RESP_ERR__TRANSPORT:
    rd_kafka_broker_session_reset(rkb, rko_orig);
    break;
```

**Status:** HANDLED
- Session reset triggered
- Error propagated

### 9.3 Parse Error

```c
err_parse:
    rd_kafka_topic_partition_list_destroy(ack_results);
    return rkbuf->rkbuf_err;
```

**Status:** HANDLED
- Parse error returned
- Results destroyed

---

## 10. Summary of Potential Issues

### Known TODOs (Will be addressed separately)

| # | Issue | Location | Status |
|---|-------|----------|--------|
| 1 | **Top-level broker error leaves partitions as `_IN_PROGRESS`** | Line 3241-3247 | TODO in code |

### Medium Issues

| # | Issue | Location | Impact |
|---|-------|----------|--------|
| 1 | **No check for closed consumer** | Line 4278 | Undefined behavior after close |
| 2 | **Function always returns NULL** | Line 4351 | No top-level error reporting |

### Minor Issues (Style/Defensive Code)

| # | Issue | Location | Impact |
|---|-------|----------|--------|
| 1 | **`result_rktpar` NULL check is defensive only** | Line 4031 | Defensive code, not a bug |

---

## Test Coverage Recommendations

Based on this analysis, the following test scenarios should be added:

### Must Have
1. Partial timeout (some brokers respond, some don't)
2. Broker disconnect during commit_sync
3. Leader change during commit_sync
4. commit_sync with zero timeout

### Should Have
5. Stale broker reply after timeout
6. commit_sync during consumer shutdown
7. Very large number of partitions across many brokers

### Nice to Have
8. Network partition simulation
9. Broker returning parse error
10. Session epoch mismatch

---

*Analysis completed: 2026-04-07*
