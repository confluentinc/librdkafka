# Share Consumer Per-Offset Error Handling Plan

## Executive Summary

**Issue:** Share consumers receive MessageSet-level errors (CRC, decompression, unsupported MagicByte) as a single error op, but need per-offset error ops to properly track acknowledgements for all affected offsets.

**Solution:** Create `rd_kafka_share_consumer_err_range()` function that generates one error op per offset in the affected range, with explicit `ack_type` control:
- **CRC errors → REJECT** (permanent data corruption)
- **Decompression errors → RELEASE** (consumer-specific capability issue)
- **Unsupported MagicByte → REJECT** (permanent protocol version mismatch)
- **Error ops in message stream** (not separate queue)

**Impact:** 
- 4 files modified (~150 lines)
- Share consumer-specific changes only (zero impact on regular consumers)
- Enables proper offset tracking and re-delivery for share groups

**Status:** Ready for implementation

---

## Visual Summary

### Current Behavior (BROKEN for Share Consumers)
```
MessageSet v2: BaseOffset=100, LastOffsetDelta=4 → Contains offsets [100, 101, 102, 103, 104]
CRC check fails ❌

Current implementation:
  rd_kafka_consumer_err(..., offset=100, ...)  → Creates 1 error op
  
Share consumer acknowledgement tracking:
  Offset 100: REJECT ✅
  Offset 101: ❌ STUCK (no acknowledgement sent)
  Offset 102: ❌ STUCK (no acknowledgement sent)
  Offset 103: ❌ STUCK (no acknowledgement sent)
  Offset 104: ❌ STUCK (no acknowledgement sent)
  
Result: 4 offsets stuck, consumer group cannot make progress
```

### New Behavior (CORRECT for Share Consumers)
```
MessageSet v2: BaseOffset=100, LastOffsetDelta=4 → Contains offsets [100, 101, 102, 103, 104]
CRC check fails ❌

New implementation:
  rd_kafka_share_consumer_err_range(..., start=100, end=104, ack_type=REJECT)
  → Creates 5 error ops (one per offset)
  → All 5 added to message_rkos list (delivered to application in message stream)
  
Application receives message list:
  Message 1: RD_KAFKA_OP_CONSUMER_ERR, offset=100, error="CRC check failed"
  Message 2: RD_KAFKA_OP_CONSUMER_ERR, offset=101, error="CRC check failed"
  Message 3: RD_KAFKA_OP_CONSUMER_ERR, offset=102, error="CRC check failed"
  Message 4: RD_KAFKA_OP_CONSUMER_ERR, offset=103, error="CRC check failed"
  Message 5: RD_KAFKA_OP_CONSUMER_ERR, offset=104, error="CRC check failed"
  
Share consumer acknowledgement tracking:
  Offset 100: REJECT ✅ (error op with ack_type=REJECT)
  Offset 101: REJECT ✅ (error op with ack_type=REJECT)
  Offset 102: REJECT ✅ (error op with ack_type=REJECT)
  Offset 103: REJECT ✅ (error op with ack_type=REJECT)
  Offset 104: REJECT ✅ (error op with ack_type=REJECT)
  
ShareAcknowledge RPC → Sends REJECT for all 5 offsets
Broker → Marks as permanently failed 

Result: All offsets properly acknowledged, consumer group makes progress
```

### Decompression Error Flow (RELEASE for Re-delivery)
```
MessageSet v2: BaseOffset=200, LastOffsetDelta=2 → Contains offsets [200, 201, 202]
ZSTD decompression fails ❌ (Consumer A lacks ZSTD support)

New implementation:
  rd_kafka_share_consumer_err_range(..., start=200, end=202, ack_type=RELEASE)
  → Creates 3 error ops with ack_type=RELEASE
  → All 3 added to message_rkos list (delivered to application)
  
Consumer A receives message list:
  Message 1: RD_KAFKA_OP_CONSUMER_ERR, offset=200, error="ZSTD decompression failed"
  Message 2: RD_KAFKA_OP_CONSUMER_ERR, offset=201, error="ZSTD decompression failed"
  Message 3: RD_KAFKA_OP_CONSUMER_ERR, offset=202, error="ZSTD decompression failed"
  
Consumer A acknowledgements:
  Offset 200: RELEASE ✅ (cannot decompress)
  Offset 201: RELEASE ✅ (cannot decompress)
  Offset 202: RELEASE ✅ (cannot decompress)
  
ShareAcknowledge RPC → Sends RELEASE for all 3 offsets
Broker → Returns offsets to available pool

Consumer B (has ZSTD support):
  Receives same offsets 200-202
  Successfully decompresses ✅
  Processes messages ✅
  Sends ACCEPT ✅

Result: Messages successfully processed by capable consumer
```

---

## Problem Statement

Currently, when MessageSet-level errors occur (CRC failures, decompression failures), librdkafka enqueues a **single error op** for the MessageSet's BaseOffset. However, a MessageSet v2 contains multiple records spanning from `BaseOffset` to `LastOffset` (calculated as `BaseOffset + LastOffsetDelta`).

For **share consumers**, this creates a critical issue:
- Share consumers track acknowledgements **per-offset** (not per-MessageSet)
- Each offset in an acquired range must receive an explicit acknowledgement type (ACCEPT/REJECT/RELEASE)
- When a MessageSet fails (CRC/decompression), ALL offsets in that MessageSet need individual error ops
- Each error op must be marked appropriately based on error type (see below)

**Current behavior:** One error op → one offset rejected → remaining offsets in MessageSet stuck
**Required behavior:** One error → N error ops (one per offset) → all offsets properly acknowledged

### Acknowledgement Type Strategy

Different error types require different acknowledgement strategies:

#### CRC Errors → REJECT
- **Rationale:** Data corruption at the storage/network level
- **Behavior:** Will NOT change between consumers - corrupted data is corrupted for everyone
- **Action:** REJECT to mark as permanently failed (broker may send to DLQ or drop based on config)

#### Decompression Errors → RELEASE
- **Rationale:** Decompression capability varies by consumer implementation
- **Examples:**
  - Consumer missing codec library (e.g., no ZSTD support)
  - Different codec versions (e.g., LZ4 framing differences)
  - Consumer-side memory constraints (different `message.max.bytes` settings)
- **Behavior:** Another consumer might successfully decompress the same data
- **Action:** RELEASE to return records to available pool for re-delivery to a different consumer

---

## Affected Error Scenarios

### 1. CRC32C Verification Failure (MessageSet v2)
**Location:** `src/rdkafka_msgset_reader.c:1091-1102` in `rd_kafka_msgset_reader_v2()`

**Current code:**
```c
if (unlikely((uint32_t)hdr.Crc != calc_crc)) {
    rd_kafka_consumer_err(
        &msetr->msetr_rkq, msetr->msetr_broker_id,
        RD_KAFKA_RESP_ERR__BAD_MSG,
        msetr->msetr_tver->version, NULL, rktp,
        hdr.BaseOffset,  // ⚠️ Only BaseOffset reported
        "MessageSet at offset %" PRId64 " (%" PRId32 " bytes) "
        "failed CRC32C check...",
        hdr.BaseOffset, hdr.Length, hdr.Crc, calc_crc);
    // ... skip and return
}
```

**Affected range:** `[BaseOffset, BaseOffset + LastOffsetDelta]` (calculated as `LastOffset`)
**Record count:** `hdr.RecordCount` records

### 2. Decompression Failures (All Codecs)
**Location:** `src/rdkafka_msgset_reader.c:517-528` in `rd_kafka_msgset_reader_decompress()`

**Current code:**
```c
err:
    rd_kafka_consumer_err(
        &msetr->msetr_rkq, msetr->msetr_broker_id, err,
        msetr->msetr_tver->version, NULL, rktp, Offset,  // ⚠️ Only first offset
        "Decompression (codec 0x%x) of message at %" PRIu64 "...",
        codec, Offset, compressed_size, rd_kafka_err2str(err));
```

**Affected range:** Same as CRC failure - entire MessageSet `[BaseOffset, LastOffset]`

**Error Codes by Codec:**

| Codec | Error Code | Cause | Should RELEASE? |
|-------|-----------|-------|-----------------|
| **GZIP** | RD_KAFKA_RESP_ERR__BAD_COMPRESSION | zlib decompression failure | ✅ Yes |
| **Snappy** | RD_KAFKA_RESP_ERR__BAD_COMPRESSION | Snappy/snappy-java decompression failure | ✅ Yes |
| **Snappy** | RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE | Memory allocation failure | ✅ Yes (consumer memory constraint) |
| **LZ4** | RD_KAFKA_RESP_ERR__BAD_COMPRESSION | LZ4 framing or decompression error | ✅ Yes |
| **LZ4** | RD_KAFKA_RESP_ERR__BAD_MSG | Bytes remaining after decompression | ✅ Yes |
| **LZ4** | RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE | Memory allocation or context failure | ✅ Yes (consumer memory constraint) |
| **ZSTD** | RD_KAFKA_RESP_ERR__BAD_COMPRESSION | ZSTD decompression error | ✅ Yes |
| **ZSTD** | RD_KAFKA_RESP_ERR__BAD_COMPRESSION | Output exceeds message.max.bytes | ✅ Yes (consumer config limit) |
| **ZSTD** | RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE | Memory allocation failure | ✅ Yes (consumer memory constraint) |
| **Any** | RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED | Unsupported/unknown codec | ✅ Yes (consumer doesn't support codec) |

**Key Insight:** ALL decompression error codes represent consumer-specific limitations or capabilities → **ALL should use RELEASE**

### 3. Unsupported MagicByte (Unknown MessageSet Version)
**Location:** `src/rdkafka_msgset_reader.c:1224-1267` in `rd_kafka_msgset_reader_peek_msg_version()`

**Current code:**
```c
rd_kafka_buf_peek_i8(rkbuf, read_offset + 8 + 4 + 4, MagicBytep);

if (unlikely(*MagicBytep < 0 || *MagicBytep > 2)) {
    int64_t Offset;
    int32_t Length;
    
    rd_kafka_buf_read_i64(rkbuf, &Offset);  // Only reads BaseOffset
    
    // Regular consumer: single error op
    if (!RD_KAFKA_IS_SHARE_CONSUMER(...)) {
        rd_kafka_consumer_err(..., Offset, "Unsupported MagicByte...");
    }
    // Share consumer: NO ERROR GENERATED ❌ (silently dropped)
    
    rd_kafka_buf_read_i32(rkbuf, &Length);
    rd_kafka_buf_skip(rkbuf, Length);
    return RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED;
}
```

**Problem:** 
- Currently for regular consumers: generates error for `BaseOffset` only
- For share consumers: NO error ops generated at all (silently dropped at `rdkafka_fetcher.c:1287`)
- Affected range: Entire MessageSet `[BaseOffset, BaseOffset + LastOffsetDelta]`

**Acknowledgement Type:** REJECT
- **Rationale:** Unsupported protocol version is permanent - format won't change
- **Behavior:** Same as CRC error - data format issue that won't resolve on retry
- **Action:** REJECT to mark as permanently failed

**New implementation approach:**
```c
if (unlikely(*MagicBytep < 0 || *MagicBytep > 2)) {
    int64_t BaseOffset;
    int32_t Length;
    int32_t LastOffsetDelta;
    
    // Read BaseOffset and Length
    rd_kafka_buf_read_i64(rkbuf, &BaseOffset);
    rd_kafka_buf_read_i32(rkbuf, &Length);
    size_t len_start = rd_slice_offset(&rkbuf->rkbuf_reader);
    
    if (!RD_KAFKA_IS_SHARE_CONSUMER(msetr->msetr_rkb->rkb_rk)) {
        // Regular consumer: single error (existing behavior)
        if (BaseOffset >= msetr->msetr_rktp->rktp_offsets.fetch_pos.offset) {
            rd_kafka_consumer_err(..., BaseOffset, ...);
            msetr->msetr_rktp->rktp_offsets.fetch_pos.offset = BaseOffset + 1;
        }
    } else {
        // Share consumer: read LastOffsetDelta to get full range
        int32_t PartitionLeaderEpoch;
        int8_t MagicByteActual;
        int32_t Crc;
        int16_t Attributes;
        
        rd_kafka_buf_read_i32(rkbuf, &PartitionLeaderEpoch);  // +4
        rd_kafka_buf_read_i8(rkbuf, &MagicByteActual);        // +1
        rd_kafka_buf_read_i32(rkbuf, &Crc);                   // +4
        rd_kafka_buf_read_i16(rkbuf, &Attributes);            // +2
        rd_kafka_buf_read_i32(rkbuf, &LastOffsetDelta);       // +4
        
        int64_t LastOffset = BaseOffset + LastOffsetDelta;
        
        // Generate per-offset errors with REJECT ack_type
        rd_kafka_share_consumer_err_range(
            &msetr->msetr_rkq, msetr->msetr_broker_id,
            RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED,
            msetr->msetr_tver->version, rktp,
            BaseOffset, LastOffset,
            RD_KAFKA_SHARE_INTERNAL_ACK_REJECT,
            "Unsupported Message(Set) MagicByte %d at offsets %" PRId64 
            "-%" PRId64, (int)MagicByteActual, BaseOffset, LastOffset);
        
        // Calculate remaining bytes to skip (same pattern as successful v2 read)
        size_t bytes_read = rd_slice_offset(&rkbuf->rkbuf_reader) - len_start;
        size_t remaining = Length - bytes_read;
        rd_kafka_buf_skip(rkbuf, remaining);
        return RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED;
    }
    
    // Regular consumer path: skip MessageSet
    rd_kafka_buf_skip(rkbuf, Length - (rd_slice_offset(&rkbuf->rkbuf_reader) - len_start));
    return RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED;
}
```

**Note:** We speculatively read `LastOffsetDelta` assuming future MessageSet versions maintain similar header structure. Even if this assumption is wrong and we read garbage, the worst case is we REJECT more offsets than necessary (safe, just inefficient).

### 4. CRC32 Verification Failure (MessageSet v0/v1)
**Location:** `src/rdkafka_msgset_reader.c:614-625` in `rd_kafka_msgset_reader_msg_v0_1()`

**Note:** v0/v1 messages are NOT batched in the same way - each message has individual CRC. This already reports errors per-message, so **no changes needed** for v0/v1 path.

---

## Proposed Solutions

### Option A: Loop-Based Multi-Call Approach
**Concept:** When MessageSet-level error occurs, loop through the offset range and call `rd_kafka_consumer_err()` for each offset.

**Pros:**
- ✅ Minimal code changes
- ✅ Reuses existing `rd_kafka_consumer_err()` function
- ✅ No new functions to maintain
- ✅ Clear and explicit per-offset reporting

**Cons:**
- ❌ N separate function calls (overhead for large MessageSets)
- ❌ N separate op allocations
- ❌ String formatting repeated N times
- ❌ Clutters the critical path in `rd_kafka_msgset_reader_v2()`

**Implementation sketch:**
```c
// For share consumers only
if (RD_KAFKA_IS_SHARE_CONSUMER(msetr->msetr_rkb->rkb_rk)) {
    for (int64_t offset = hdr.BaseOffset; 
         offset <= LastOffset; 
         offset++) {
        rd_kafka_consumer_err(..., offset, ...);
    }
} else {
    // Existing single-call for regular consumers
    rd_kafka_consumer_err(..., hdr.BaseOffset, ...);
}
```

---

### Option B: Dedicated Range-Based Error Function with ack_type Control (RECOMMENDED)
**Concept:** Create a new function `rd_kafka_share_consumer_err_range()` that handles per-offset error op creation for share consumers internally, with explicit `ack_type` parameter to control REJECT vs RELEASE behavior.

**Pros:**
- ✅ Encapsulates share consumer logic in one place
- ✅ Minimal changes to critical `rd_kafka_msgset_reader_v2()` function
- ✅ Can optimize internal implementation (batch allocations, shared error string)
- ✅ Cleaner separation of concerns
- ✅ Future-proof for other range-based error scenarios
- ✅ **Explicit control over REJECT vs RELEASE** via ack_type parameter
- ✅ Error ops are created with ack_type already set (avoids hardcoded REJECT in fetcher)

**Cons:**
- ❌ New function to maintain
- ❌ Slightly more complex initial implementation

**Implementation sketch:**

```c
// New function in rdkafka_op.c
void rd_kafka_share_consumer_err_range(
    rd_kafka_q_t *rkq,
    int32_t broker_id,
    rd_kafka_resp_err_t err,
    int32_t version,
    rd_kafka_toppar_t *rktp,
    int64_t start_offset,
    int64_t end_offset,  // inclusive
    rd_kafka_share_internal_acknowledgement_type ack_type,  // NEW: REJECT or RELEASE
    const char *fmt,
    ...) {
    // Format error message once
    // Loop through offset range
    // For each offset:
    //   - Create RD_KAFKA_OP_CONSUMER_ERR op
    //   - Set rko_u.err.rkm.rkm_u.consumer.ack_type = ack_type (CRITICAL!)
    //   - Enqueue op
}
```

**Usage in msgset_reader:**
```c
// CRC error: REJECT (data corruption won't change)
if (RD_KAFKA_IS_SHARE_CONSUMER(msetr->msetr_rkb->rkb_rk)) {
    rd_kafka_share_consumer_err_range(
        &msetr->msetr_rkq, msetr->msetr_broker_id,
        RD_KAFKA_RESP_ERR__BAD_MSG,
        msetr->msetr_tver->version, rktp,
        hdr.BaseOffset, LastOffset,
        RD_KAFKA_SHARE_INTERNAL_ACK_REJECT,  // REJECT for CRC errors
        "MessageSet at offsets %" PRId64 "-%" PRId64 " failed CRC32C check...",
        hdr.BaseOffset, LastOffset, hdr.Crc, calc_crc);
} else {
    rd_kafka_consumer_err(...);  // existing single-call
}

// Decompression error: RELEASE (consumer-specific capability)
if (RD_KAFKA_IS_SHARE_CONSUMER(msetr->msetr_rkb->rkb_rk)) {
    rd_kafka_share_consumer_err_range(
        &msetr->msetr_rkq, msetr->msetr_broker_id, err,
        msetr->msetr_tver->version, rktp,
        BaseOffset, LastOffset,
        RD_KAFKA_SHARE_INTERNAL_ACK_RELEASE,  // RELEASE for decompression
        "Decompression (codec 0x%x) failed...", codec);
} else {
    rd_kafka_consumer_err(...);
}
```

---

## Recommended Approach: Option B

**Rationale:**
1. **Minimal edits to critical function:** Replace single `rd_kafka_consumer_err()` call with `rd_kafka_share_consumer_err_range()` only for share consumers
2. **Performance:** Can optimize internally (shared error string, efficient op creation)
3. **Maintainability:** Share consumer complexity isolated in one function
4. **Safety:** Doesn't add loops/complexity to the hot path of `rd_kafka_msgset_reader_v2()`
5. **Extensibility:** Can handle future range-based error scenarios

---

## Implementation Plan

### Phase 1: Create Core Infrastructure

#### 1.1 New Function: `rd_kafka_share_consumer_err_range()`
**File:** `src/rdkafka_op.c`
**Location:** After existing `rd_kafka_consumer_err()` (after line 654)

**Function signature:**
```c
void rd_kafka_share_consumer_err_range(
    rd_kafka_q_t *rkq,
    int32_t broker_id,
    rd_kafka_resp_err_t err,
    int32_t version,
    rd_kafka_toppar_t *rktp,
    int64_t start_offset,
    int64_t end_offset,
    rd_kafka_share_internal_acknowledgement_type ack_type,
    const char *fmt,
    ...);
```

**Full implementation:**
```c
void rd_kafka_share_consumer_err_range(
    rd_kafka_q_t *rkq,
    int32_t broker_id,
    rd_kafka_resp_err_t err,
    int32_t version,
    rd_kafka_toppar_t *rktp,
    int64_t start_offset,
    int64_t end_offset,
    rd_kafka_share_internal_acknowledgement_type ack_type,
    const char *fmt,
    ...) {
    va_list ap;
    char base_buf[2048];
    char offset_buf[2560];  /* Larger to accommodate prefix */
    rd_kafka_op_t *rko;
    int64_t offset;
    int64_t count = end_offset - start_offset + 1;

    /* Format base error message once */
    va_start(ap, fmt);
    rd_vsnprintf(base_buf, sizeof(base_buf), fmt, ap);
    va_end(ap);

    /* Create one error op per offset in range */
    for (offset = start_offset; offset <= end_offset; offset++) {
        /* Create offset-specific error message for clarity */
        rd_snprintf(offset_buf, sizeof(offset_buf),
                    "[Offset %" PRId64 " of range %" PRId64 "-%" PRId64 
                    " (%" PRId64 " records)] %s",
                    offset, start_offset, end_offset, count, base_buf);

        /* Create error op (same structure as rd_kafka_consumer_err) */
        rko = rd_kafka_op_new(RD_KAFKA_OP_CONSUMER_ERR);
        rko->rko_version = version;
        rko->rko_err = err;
        rko->rko_u.err.offset = offset;
        rko->rko_u.err.errstr = rd_strdup(offset_buf);
        rko->rko_u.err.rkm.rkm_broker_id = broker_id;

        /* CRITICAL: Set ack_type for share consumer acknowledgement
         * Structure path: rko_u.err.rkm.rkm_u.consumer.ack_type
         * This is set BEFORE enqueueing to allow fetcher to respect it */
        rko->rko_u.err.rkm.rkm_u.consumer.ack_type = ack_type;

        /* Keep toppar reference */
        if (rktp)
            rko->rko_rktp = rd_kafka_toppar_keep(rktp);

        /* Enqueue error op */
        rd_kafka_q_enq(rkq, rko);
    }
}
```

**Structure path for ack_type:**
```
rd_kafka_op_t  [rdkafka_op.h]
└─ rko_u (union)
    └─ err (struct) [lines 382-396]
        └─ rkm (rd_kafka_msg_t) [line 391]
            └─ rkm_u (union)  [rdkafka_msg.h]
                └─ consumer (struct) [lines 152-165]
                    └─ ack_type (rd_kafka_share_internal_acknowledgement_type) [lines 158-162]
```

**Key implementation notes:**
1. **Error string:** Each offset gets a unique error string with offset-specific prefix for clear debugging
2. **ack_type:** Set directly in the op structure via `rko->rko_u.err.rkm.rkm_u.consumer.ack_type`
3. **Memory:** Each op allocates its own error string (worth it for clarity)
4. **Offset count:** Calculated as `end_offset - start_offset + 1` (NOT using MessageSet's RecordCount header)

#### 1.2 Function Declaration
**File:** `src/rdkafka_op.h`
**Location:** After `rd_kafka_consumer_err()` declaration (after line 884)

```c
void rd_kafka_share_consumer_err_range(
    rd_kafka_q_t *rkq,
    int32_t broker_id,
    rd_kafka_resp_err_t err,
    int32_t version,
    rd_kafka_toppar_t *rktp,
    int64_t start_offset,
    int64_t end_offset,
    rd_kafka_share_internal_acknowledgement_type ack_type,
    const char *fmt,
    ...);
```

#### 1.3 Modify Fetcher to Respect Pre-Set ack_type
**File:** `src/rdkafka_fetcher.c`
**Function:** `rd_kafka_share_fetch_response_handle_one_partition()`
**Location:** Lines 933-937

**CRITICAL CHANGE:** The fetcher currently hardcodes REJECT for all error ops. We need to modify it to respect pre-set ack_type values from our range function.

**Current code:**
```c
} else if (rko->rko_type == RD_KAFKA_OP_CONSUMER_ERR) {
    rkm = &rko->rko_u.err.rkm;
    rkm->rkm_u.consumer.ack_type =
        RD_KAFKA_SHARE_INTERNAL_ACK_REJECT;
}
```

**Modified code:**
```c
} else if (rko->rko_type == RD_KAFKA_OP_CONSUMER_ERR) {
    rkm = &rko->rko_u.err.rkm;
    /* Only set ack_type if not already set by range-based error handler.
     * GAP (0) indicates uninitialized. Error ops from
     * rd_kafka_share_consumer_err_range() will have ack_type pre-set to
     * REJECT or RELEASE based on error type. */
    if (rkm->rkm_u.consumer.ack_type == RD_KAFKA_SHARE_INTERNAL_ACK_GAP) {
        rkm->rkm_u.consumer.ack_type =
            RD_KAFKA_SHARE_INTERNAL_ACK_REJECT;
    }
    /* Otherwise, respect the pre-set ack_type (RELEASE for decompression,
     * REJECT for CRC errors, etc.) */
}
```

**Rationale:**
- `RD_KAFKA_SHARE_INTERNAL_ACK_GAP` (value 0) is the default/uninitialized state
- Error ops from `rd_kafka_share_consumer_err_range()` will have ack_type explicitly set
- Regular error ops (from `rd_kafka_consumer_err()`) will have GAP and get defaulted to REJECT
- This preserves backward compatibility while allowing explicit control

---

#### 1.4 Modify Fetcher to Include Error Ops in Message List
**File:** `src/rdkafka_fetcher.c`
**Function:** `rd_kafka_share_fetch_response_handle_one_partition()`
**Location:** Lines 1097-1108

**CRITICAL CHANGE:** Error ops should be added to the message list (like FETCH ops), NOT sent separately to consumer group queue.

**Current code:**
```c
if (msg_rko->rko_type == RD_KAFKA_OP_FETCH) {
    rd_list_add(
        response_rko->rko_u.share_fetch_response.message_rkos,
        msg_rko);
    msg_cnt++;
} else {
    /* RD_KAFKA_OP_CONSUMER_ERR: forward to consumer group queue */
    rd_kafka_q_enq(rkb->rkb_rk->rk_cgrp->rkcg_q,
                   msg_rko);
}
```

**Modified code:**
```c
if (msg_rko->rko_type == RD_KAFKA_OP_FETCH) {
    rd_list_add(
        response_rko->rko_u.share_fetch_response.message_rkos,
        msg_rko);
    msg_cnt++;
} else if (msg_rko->rko_type == RD_KAFKA_OP_CONSUMER_ERR) {
    /* Add error ops to message list along with fetch ops.
     * Error ops are already tracked in acknowledgement system and
     * should be delivered to application in the same message stream. */
    rd_list_add(
        response_rko->rko_u.share_fetch_response.message_rkos,
        msg_rko);
    msg_cnt++;
}
```

**Rationale:**
- Error ops should be part of the message stream delivered to the application
- They're already tracked in the acknowledgement system (ack_type is set)
- Application should receive errors in the message flow, not as separate callbacks
- This ensures errors and messages are delivered together in the correct order
- Both will be acknowledged together via ShareAcknowledge RPC

**Impact:**
- Error messages will appear in the message list returned to the application
- Application can iterate through messages and see both successful fetches and errors
- All messages (fetch + error) are acknowledged together in the same batch

---

### Phase 2: Update CRC Error Handling (MessageSet v2)

#### 2.1 CRC32C Failure Path
**File:** `src/rdkafka_msgset_reader.c`
**Function:** `rd_kafka_msgset_reader_v2()`
**Location:** Lines 1091-1102

**Current code:**
```c
if (unlikely((uint32_t)hdr.Crc != calc_crc)) {
    /* Propagate CRC error to application and
     * continue with next message. */
    rd_kafka_consumer_err(
        &msetr->msetr_rkq, msetr->msetr_broker_id,
        RD_KAFKA_RESP_ERR__BAD_MSG,
        msetr->msetr_tver->version, NULL, rktp,
        hdr.BaseOffset,
        "MessageSet at offset %" PRId64 " (%" PRId32 " bytes) "
        "failed CRC32C check (original 0x%" PRIx32 " != "
        "calculated 0x%" PRIx32 ")",
        hdr.BaseOffset, hdr.Length, hdr.Crc, calc_crc);
    rd_kafka_buf_skip_to(rkbuf, crc_len);
    rd_atomic64_add(&msetr->msetr_rkb->rkb_c.rx_err, 1);
    return RD_KAFKA_RESP_ERR_NO_ERROR;
}
```

**Modified code:**
```c
if (unlikely((uint32_t)hdr.Crc != calc_crc)) {
    /* Propagate CRC error to application.
     * For share consumers, report error for ALL offsets in MessageSet
     * using REJECT (data corruption won't change between consumers). */
    if (RD_KAFKA_IS_SHARE_CONSUMER(msetr->msetr_rkb->rkb_rk)) {
        int64_t record_count = LastOffset - hdr.BaseOffset + 1;
        rd_kafka_share_consumer_err_range(
            &msetr->msetr_rkq, msetr->msetr_broker_id,
            RD_KAFKA_RESP_ERR__BAD_MSG,
            msetr->msetr_tver->version, rktp,
            hdr.BaseOffset, LastOffset,  // Full range
            RD_KAFKA_SHARE_INTERNAL_ACK_REJECT,  // REJECT for CRC errors
            "MessageSet at offsets %" PRId64 "-%" PRId64 
            " (%" PRId64 " records) failed CRC32C check "
            "(original 0x%" PRIx32 " != calculated 0x%" PRIx32 ")",
            hdr.BaseOffset, LastOffset, record_count,
            hdr.Crc, calc_crc);
    } else {
        rd_kafka_consumer_err(
            &msetr->msetr_rkq, msetr->msetr_broker_id,
            RD_KAFKA_RESP_ERR__BAD_MSG,
            msetr->msetr_tver->version, NULL, rktp,
            hdr.BaseOffset,
            "MessageSet at offset %" PRId64 " (%" PRId32 " bytes) "
            "failed CRC32C check (original 0x%" PRIx32 " != "
            "calculated 0x%" PRIx32 ")",
            hdr.BaseOffset, hdr.Length, hdr.Crc, calc_crc);
    }
    rd_kafka_buf_skip_to(rkbuf, crc_len);
    rd_atomic64_add(&msetr->msetr_rkb->rkb_c.rx_err, 1);
    return RD_KAFKA_RESP_ERR_NO_ERROR;
}
```

**Impact:** 
- Single if-else block, minimal disruption to function flow
- Calculate record_count as `LastOffset - BaseOffset + 1` (don't trust header's RecordCount)
- Use `RD_KAFKA_SHARE_INTERNAL_ACK_REJECT` for CRC errors (data corruption is permanent)

---

### Phase 3: Update Decompression Error Handling

#### 3.1 Decompression Failure Path
**File:** `src/rdkafka_msgset_reader.c`
**Function:** `rd_kafka_msgset_reader_decompress()`
**Location:** Lines 517-528

**Challenge:** This function doesn't have direct access to MessageSet header information needed for range calculation.

**Current function parameters:**
```c
static rd_kafka_resp_err_t
rd_kafka_msgset_reader_decompress(rd_kafka_msgset_reader_t *msetr,
                                  int MsgVersion,
                                  int Attributes,
                                  int64_t Timestamp,
                                  int64_t BaseOffset,
                                  const void *compressed,
                                  size_t compressed_size)
```

**Solution:** Pass additional parameters for range-based error reporting

**Modified function signature:**
```c
static rd_kafka_resp_err_t
rd_kafka_msgset_reader_decompress(rd_kafka_msgset_reader_t *msetr,
                                  int MsgVersion,
                                  int Attributes,
                                  int64_t Timestamp,
                                  int64_t BaseOffset,
                                  int64_t LastOffset,     // NEW: for range calculation
                                  const void *compressed,
                                  size_t compressed_size)
```

**Modified error handling (lines 517-528):**
```c
err:
    /* Enqueue error message for decompression failure.
     * For share consumers, report error for ALL offsets in MessageSet
     * using RELEASE (another consumer might be able to decompress). */
    if (RD_KAFKA_IS_SHARE_CONSUMER(msetr->msetr_rkb->rkb_rk)) {
        int64_t record_count = LastOffset - BaseOffset + 1;
        rd_kafka_share_consumer_err_range(
            &msetr->msetr_rkq, msetr->msetr_broker_id, err,
            msetr->msetr_tver->version, rktp,
            BaseOffset, LastOffset,  // Full range
            RD_KAFKA_SHARE_INTERNAL_ACK_RELEASE,  // RELEASE for decompression
            "Decompression (codec 0x%x) of MessageSet at offsets "
            "%" PRId64 "-%" PRId64 " (%" PRId64 " records, %" PRIusz " bytes) "
            "failed: %s",
            codec, BaseOffset, LastOffset, record_count,
            compressed_size, rd_kafka_err2str(err));
    } else {
        rd_kafka_consumer_err(
            &msetr->msetr_rkq, msetr->msetr_broker_id, err,
            msetr->msetr_tver->version, NULL, rktp, BaseOffset,
            "Decompression (codec 0x%x) of message at %" PRId64 
            " of %" PRIusz " bytes failed: %s",
            codec, BaseOffset, compressed_size, rd_kafka_err2str(err));
    }
    return err;
```

**Key changes:**
- Remove `RecordCount` parameter - calculate it as `LastOffset - BaseOffset + 1`
- Add `RD_KAFKA_SHARE_INTERNAL_ACK_RELEASE` to use RELEASE for decompression errors
- Calculate record_count locally from offset range (don't trust header)

#### 3.2 Update Decompression Call Sites

**Call site 1: MessageSet v2 compressed path**
**Location:** `src/rdkafka_msgset_reader.c:1147-1154`

**Current:**
```c
err = rd_kafka_msgset_reader_decompress(
    msetr, 2 /*MsgVersion v2*/, hdr.Attributes,
    hdr.BaseTimestamp, hdr.BaseOffset, compressed,
    payload_size);
```

**Modified:**
```c
err = rd_kafka_msgset_reader_decompress(
    msetr, 2 /*MsgVersion v2*/, hdr.Attributes,
    hdr.BaseTimestamp, hdr.BaseOffset,
    LastOffset,  // NEW: pass LastOffset for range calculation
    compressed, payload_size);
```

**Note:** `LastOffset` is already calculated earlier in the function as `hdr.BaseOffset + hdr.LastOffsetDelta`

**Call site 2: MessageSet v0/v1 compressed path**
**Location:** `src/rdkafka_msgset_reader.c` (in `rd_kafka_msgset_reader_msg_v0_1()`)

**Note:** v0/v1 messages have different structure. Each message is independent, so decompression failure affects only that single message offset. Range-based handling is NOT needed for v0/v1.

**Action:** 
- Search for decompression call in v0/v1 path
- Update function signature to match (may need to pass same offset for BaseOffset and LastOffset)
- OR: Keep v0/v1 using single-offset error reporting (existing behavior is correct)

---

### Phase 4: Testing & Verification

#### 4.1 Unit Test Scenarios

**Test 1: CRC32C failure with share consumer**
- Create MessageSet v2 with corrupted CRC
- BaseOffset=100, LastOffsetDelta=9 (10 records)
- Verify 10 separate error ops created (offsets 100-109)
- **Verify all 10 error ops are in the message_rkos list (not sent to separate queue)**
- Verify each error op has correct offset and error message
- **Verify each error op has ack_type = RD_KAFKA_SHARE_INTERNAL_ACK_REJECT**
- Verify application receives all 10 errors in message iteration

**Test 2: Decompression failure with share consumer**
- Create compressed MessageSet v2 with invalid compression
- BaseOffset=200, LastOffsetDelta=4 (5 records: offsets 200-204)
- Verify 5 separate error ops created
- **Verify all 5 error ops are in the message_rkos list (not sent to separate queue)**
- Verify error message includes compression codec and full range
- **Verify each error op has ack_type = RD_KAFKA_SHARE_INTERNAL_ACK_RELEASE**
- Verify application receives all 5 errors in message iteration

**Test 3: Regular consumer behavior unchanged**
- Same scenarios as above but with regular consumer
- Verify only 1 error op enqueued
- Verify existing behavior maintained

**Test 4: Acknowledgement type verification for CRC errors**
- Verify CRC error ops for share consumers flow through acknowledgement system
- Verify offsets are marked as REJECT
- Verify ShareAcknowledge RPC sends REJECT for these offsets
- Verify broker handles REJECT appropriately (DLQ or drop based on config)

**Test 5: Acknowledgement type verification for decompression errors**
- Verify decompression error ops flow through acknowledgement system  
- Verify offsets are marked as RELEASE
- Verify ShareAcknowledge RPC sends RELEASE for these offsets
- Verify broker returns messages to available pool for re-delivery

**Test 5: Mixed good messages and error ops in same batch**
- Receive batch with multiple MessageSets:
  - MessageSet 1: Offsets 100-104 (good, decompressed successfully)
  - MessageSet 2: Offsets 105-109 (CRC error)
  - MessageSet 3: Offsets 110-114 (good, decompressed successfully)
- **Verify message_rkos list contains all ops in order:**
  - 5 x RD_KAFKA_OP_FETCH (offsets 100-104)
  - 5 x RD_KAFKA_OP_CONSUMER_ERR (offsets 105-109, ack_type=REJECT)
  - 5 x RD_KAFKA_OP_FETCH (offsets 110-114)
- Verify application iterates through all 15 messages
- Verify acknowledgements sent correctly (ACCEPT for good, REJECT for errors)

**Test 6: Large MessageSet**
- MessageSet with 1000+ records
- Verify performance acceptable
- Verify all error ops created correctly

#### 4.2 Integration Test Scenarios

**Test 7: End-to-end CRC error flow with REJECT**
- Share consumer receives MessageSet with CRC error (BaseOffset=100, LastOffsetDelta=4)
- Verify application receives 5 error callbacks (offsets 100-104)
- Verify each error has ack_type = REJECT
- Verify ShareAcknowledge RPC sends REJECT for offsets 100-104
- Verify broker does NOT re-deliver these offsets to any consumer
- Verify offsets move to DLQ or are dropped (based on broker config)

**Test 8: End-to-end decompression error flow with RELEASE**
- Share consumer A receives compressed MessageSet (ZSTD, offsets 200-204)
- Consumer A lacks ZSTD support → decompression fails
- Verify application receives 5 error messages in message stream (offsets 200-204)
- Verify each error has ack_type = RELEASE
- Verify ShareAcknowledge RPC sends RELEASE for offsets 200-204
- Verify broker returns offsets to available pool
- Share consumer B (with ZSTD support) receives same offsets
- Verify consumer B successfully decompresses and processes messages

**Test 9: Mixed consumer capabilities**
- Publish ZSTD-compressed messages
- Consumer A: No ZSTD support → RELEASE all offsets
- Consumer B: Has ZSTD support → Successfully processes
- Verify load naturally shifts to capable consumers

---

## Error Type to Acknowledgement Type Mapping

### Current Behavior (from exploration)
**File:** `src/rdkafka_fetcher.c:933-937`

```c
} else if (rko->rko_type == RD_KAFKA_OP_CONSUMER_ERR) {
    rkm = &rko->rko_u.err.rkm;
    rkm->rkm_u.consumer.ack_type =
        RD_KAFKA_SHARE_INTERNAL_ACK_REJECT;  // ⚠️ Hardcoded - we need to change this
}
```

**Problem:** Error ops are **hardcoded to REJECT** regardless of error type. This is incorrect for decompression errors.

### Release vs. Reject Semantics

**REJECT (value 3):** Record was processed but cannot be accepted (permanent failure)
- Broker typically moves to DLQ or drops based on configuration
- Will NOT be re-delivered to any consumer
- Use for: data corruption, schema violations, permanent processing failures

**RELEASE (value 2):** Record could not be processed by this consumer, return to available pool (transient/consumer-specific failure)
- Broker returns record to available pool
- Can be re-delivered to a different consumer
- Use for: consumer capability issues, transient errors, resource constraints

### Correct Mapping for Our Error Scenarios

| Error Type | Error Code | ack_type | Rationale |
|-----------|-----------|----------|-----------|
| **CRC32C Check Failure** | RD_KAFKA_RESP_ERR__BAD_MSG | **REJECT** | Data corruption at storage/network level. Will NOT change between consumers. Corrupted data is corrupted for everyone. |
| **GZIP Decompression** | RD_KAFKA_RESP_ERR__BAD_COMPRESSION | **RELEASE** | Consumer may lack GZIP support or have different zlib version |
| **Snappy Decompression** | RD_KAFKA_RESP_ERR__BAD_COMPRESSION | **RELEASE** | Consumer may lack Snappy library or use incompatible version (snappy vs snappy-java) |
| **Snappy Memory** | RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE | **RELEASE** | Consumer-specific memory constraint. Another consumer may have more memory. |
| **LZ4 Decompression** | RD_KAFKA_RESP_ERR__BAD_COMPRESSION | **RELEASE** | LZ4 framing differences between consumer implementations |
| **LZ4 Memory** | RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE | **RELEASE** | Consumer-specific memory constraint |
| **ZSTD Decompression** | RD_KAFKA_RESP_ERR__BAD_COMPRESSION | **RELEASE** | Consumer may lack ZSTD support or have different ZSTD version |
| **ZSTD Size Limit** | RD_KAFKA_RESP_ERR__BAD_COMPRESSION | **RELEASE** | Consumer's `message.max.bytes` setting exceeded. Another consumer may have higher limit. |
| **ZSTD Memory** | RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE | **RELEASE** | Consumer-specific memory constraint |
| **Unsupported Codec** | RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED | **RELEASE** | Consumer doesn't support this codec. Another consumer might. |

### Implementation Strategy

**Modified fetcher code** (Phase 1.3):
```c
} else if (rko->rko_type == RD_KAFKA_OP_CONSUMER_ERR) {
    rkm = &rko->rko_u.err.rkm;
    /* Only set ack_type if not already set.
     * GAP (0) indicates uninitialized. */
    if (rkm->rkm_u.consumer.ack_type == RD_KAFKA_SHARE_INTERNAL_ACK_GAP) {
        rkm->rkm_u.consumer.ack_type = RD_KAFKA_SHARE_INTERNAL_ACK_REJECT;
    }
    /* Otherwise, respect pre-set value from rd_kafka_share_consumer_err_range() */
}
```

**Error op creation** (Phase 1.1):
- CRC errors: Call `rd_kafka_share_consumer_err_range()` with `RD_KAFKA_SHARE_INTERNAL_ACK_REJECT`
- Decompression errors: Call `rd_kafka_share_consumer_err_range()` with `RD_KAFKA_SHARE_INTERNAL_ACK_RELEASE`
- Regular errors: Call `rd_kafka_consumer_err()` (ack_type defaults to REJECT via fetcher)

**Key insight:** ALL decompression errors should use RELEASE because they represent consumer-specific capabilities or constraints, not permanent data corruption.

---

## Risk Assessment & Mitigation

### Risk 1: Performance Impact
**Concern:** Creating N error ops for large MessageSets could impact performance

**Mitigation:**
- Error scenarios are **exceptional** cases (CRC/decompression failures are rare)
- Performance impact only affects error path, not happy path
- Share consumers need per-offset tracking anyway for correctness
- Alternative (not tracking per-offset) would cause stuck offsets and consumer hangs

**Assessment:** Acceptable trade-off for correctness

### Risk 2: Memory Consumption
**Concern:** Large MessageSet errors could allocate many error ops

**Mitigation:**
- Error strings can be shared or templated to reduce allocation
- Ops are queued and processed/freed promptly
- Again, error scenarios are exceptional
- Can add safeguard: if RecordCount > threshold (e.g., 10000), truncate to first N and log warning

**Assessment:** Low risk, can add safeguards if needed

### Risk 3: Breaking Regular Consumer Behavior
**Concern:** Changes might affect non-share consumers

**Mitigation:**
- All changes gated by `RD_KAFKA_IS_SHARE_CONSUMER()` check
- Regular consumers use existing code path (minimal/no changes)
- Comprehensive testing of both consumer types

**Assessment:** Low risk with proper gating

### Risk 4: Incomplete Coverage
**Concern:** Missing some error scenarios that need per-offset handling

**Mitigation:**
- Systematic review of all `rd_kafka_consumer_err()` calls in msgset_reader.c
- Focus on MessageSet-level errors (CRC, decompression)
- Message-level errors (v0/v1 CRC) already per-message

**Additional scenarios to review:**
- Parse errors in v2 message reading
- Buffer underflow errors
- Other MessageSet validation failures

**Assessment:** Medium risk - requires thorough code review

---

## Code Locations Summary

### Files to Modify

| File | Function | Lines | Change Description |
|------|----------|-------|-------------------|
| `src/rdkafka_op.h` | (declarations) | After 884 | Add `rd_kafka_share_consumer_err_range()` declaration with `ack_type` parameter |
| `src/rdkafka_op.c` | (new function) | After 654 | Implement `rd_kafka_share_consumer_err_range()` with per-offset error op creation and ack_type setting |
| `src/rdkafka_fetcher.c` | `rd_kafka_share_fetch_response_handle_one_partition()` | 933-937 | Modify to respect pre-set ack_type (don't override if already set) |
| `src/rdkafka_fetcher.c` | `rd_kafka_share_fetch_response_handle_one_partition()` | 1097-1108 | **Add error ops to message list** (not separate queue) - deliver errors in message stream |
| `src/rdkafka_msgset_reader.c` | `rd_kafka_msgset_reader_v2()` | 1091-1102 | Update CRC error handling - call err_range with REJECT for share consumers |
| `src/rdkafka_msgset_reader.c` | `rd_kafka_msgset_reader_decompress()` | Function signature | Add `LastOffset` parameter (NOT RecordCount) |
| `src/rdkafka_msgset_reader.c` | `rd_kafka_msgset_reader_decompress()` | 517-528 | Update error handling - call err_range with RELEASE for share consumers |
| `src/rdkafka_msgset_reader.c` | `rd_kafka_msgset_reader_v2()` | ~1150 | Update decompress call to pass `LastOffset` parameter |

### Total Estimated Changes
- **New code:** ~80 lines (new `rd_kafka_share_consumer_err_range()` function)
- **Modified code:** ~70 lines (fetcher changes + call sites + error handling)
- **Total impact:** ~150 lines across 4 files

### Complexity Assessment
- **Low complexity:** Gated changes, well-defined scope
- **Isolated impact:** Share consumer-specific paths only (regular consumers unchanged)
- **Testing required:** Moderate (unit + integration tests needed)
- **Risk level:** Low (all changes behind share consumer checks)

---

## Decisions Made

### ✅ Decision 1: Error String Formatting
**Decision:** Use offset-specific messages for each error op

**Format:** `"[Offset X of range START-END (N records)] {base_error_message}"`

**Rationale:** Clearer for debugging when tracking specific offset failures. Worth the minor overhead of per-offset string allocation.

---

### ✅ Decision 2: Record Count Calculation
**Decision:** Calculate record count as `LastOffset - BaseOffset + 1`

**Do NOT use:** `hdr.RecordCount` from MessageSet header

**Rationale:** 
- MessageSet header may be corrupted (especially with CRC errors)
- `RecordCount` header field may be inaccurate
- Offset range is the authoritative source of truth
- Ensures correct per-offset error reporting even with corrupted headers

---

### ✅ Decision 3: Acknowledgement Type Strategy
**Decision:** Differentiate based on error cause

**CRC errors → REJECT:**
- Data corruption is permanent (won't change between consumers)
- Should not be re-delivered to any consumer

**Decompression errors → RELEASE:**
- Consumer-specific capability issue (codec support, memory, config)
- Another consumer might successfully decompress
- Return to pool for re-delivery

**Implementation:** Pass `ack_type` parameter to `rd_kafka_share_consumer_err_range()`

---

### ✅ Decision 4: Safeguard for Large MessageSets
**Decision:** No cap initially (Option A)

**Rationale:**
- Error scenarios are exceptional (rare in production)
- Correctness is paramount for share consumers
- Extremely large MessageSets (>10k records) with errors are theoretical
- Can add cap later if needed based on real-world usage

**Future consideration:** Add optional cap with warning log if needed

---

### ✅ Decision 5: v0/v1 Decompression Handling
**Decision:** Keep existing single-offset error reporting for v0/v1

**Rationale:**
- v0/v1 messages are NOT batched (one message = one offset)
- Decompression failure already affects only that single offset
- No range-based handling needed
- Existing behavior is correct

**Action required:** May need to update function signature for consistency, but behavior unchanged

---

## Implementation Checklist

### Phase 1: Infrastructure
- [ ] **1.1** Implement `rd_kafka_share_consumer_err_range()` in `rdkafka_op.c` (after line 654)
  - Accept `ack_type` parameter
  - Calculate record count as `LastOffset - BaseOffset + 1`
  - Format offset-specific error strings
  - Set `rko->rko_u.err.rkm.rkm_u.consumer.ack_type = ack_type`
  - Create and enqueue one op per offset
- [ ] **1.2** Add declaration to `rdkafka_op.h` (after line 884)
  - Include `rd_kafka_share_internal_acknowledgement_type` parameter
- [ ] **1.3** Modify fetcher to respect pre-set ack_type (`rdkafka_fetcher.c:933-937`)
  - Check if `ack_type == GAP` before setting to REJECT
  - Preserve pre-set ack_type from `rd_kafka_share_consumer_err_range()`
- [ ] **1.4** Modify fetcher to include error ops in message list (`rdkafka_fetcher.c:1097-1108`)
  - Add `RD_KAFKA_OP_CONSUMER_ERR` ops to `message_rkos` list
  - Remove separate enqueue to consumer group queue
  - Error ops delivered to application in same message stream
- [ ] **1.5** Compile and verify no build errors

### Phase 2: CRC Error Handling
- [ ] **2.1** Modify CRC32C check in `rd_kafka_msgset_reader_v2()` (lines 1091-1102)
  - Add share consumer conditional check
  - Calculate `record_count = LastOffset - BaseOffset + 1`
  - Call `rd_kafka_share_consumer_err_range()` with `RD_KAFKA_SHARE_INTERNAL_ACK_REJECT`
  - Keep existing `rd_kafka_consumer_err()` call for regular consumers
- [ ] **2.2** Test with corrupted CRC MessageSet
  - Verify N error ops created for share consumer
  - Verify each has ack_type = REJECT
  - Verify single error op for regular consumer

### Phase 3: Decompression Error Handling
- [ ] **3.1** Update `rd_kafka_msgset_reader_decompress()` function signature
  - Add `int64_t LastOffset` parameter (remove RecordCount param)
  - Update function comment
- [ ] **3.2** Modify error handling in decompress function (lines 517-528)
  - Add share consumer conditional check
  - Calculate `record_count = LastOffset - BaseOffset + 1`
  - Call `rd_kafka_share_consumer_err_range()` with `RD_KAFKA_SHARE_INTERNAL_ACK_RELEASE`
  - Keep existing `rd_kafka_consumer_err()` for regular consumers
- [ ] **3.3** Update call site in `rd_kafka_msgset_reader_v2()` (~line 1150)
  - Pass `LastOffset` to decompress function
- [ ] **3.4** Search for v0/v1 decompression call sites
  - Update signatures for consistency (if needed)
  - Verify behavior unchanged (single-offset errors already correct)
- [ ] **3.5** Compile and verify all call sites updated

### Phase 4: Testing
- [ ] **4.1** Unit test: CRC error with share consumer
  - MessageSet with corrupted CRC (offsets 100-109)
  - Verify 10 error ops with ack_type = REJECT
- [ ] **4.2** Unit test: Decompression error with share consumer
  - Compressed MessageSet with invalid compression (offsets 200-204)
  - Verify 5 error ops with ack_type = RELEASE
- [ ] **4.3** Unit test: Regular consumer unchanged
  - Same scenarios, verify single error op each
- [ ] **4.4** Unit test: ack_type verification
  - Verify CRC errors → REJECT
  - Verify decompression errors → RELEASE
  - Verify fetcher respects pre-set ack_type
- [ ] **4.5** Integration test: CRC error REJECT flow
  - Verify ShareAcknowledge sends REJECT
  - Verify broker does not re-deliver
- [ ] **4.6** Integration test: Decompression error RELEASE flow
  - Consumer A fails decompression → RELEASE
  - Consumer B receives same offsets → succeeds
- [ ] **4.7** Performance test: Large MessageSet with errors
  - 1000+ record MessageSet with CRC error
  - Verify acceptable performance

### Phase 5: Code Review
- [ ] **5.1** Review all `rd_kafka_consumer_err()` calls in msgset_reader.c
- [ ] **5.2** Identify other MessageSet-level errors needing range handling
  - Buffer underflow errors
  - Parse errors
  - Validation failures
- [ ] **5.3** Verify regular consumer path unchanged
- [ ] **5.4** Verify share consumer offset tracking correct
- [ ] **5.5** Code review with team

### Phase 6: Documentation
- [ ] **6.1** Add function comment for `rd_kafka_share_consumer_err_range()`
- [ ] **6.2** Document ack_type behavior in code comments
- [ ] **6.3** Update CHANGELOG.md with bug fix description
- [ ] **6.4** Update any relevant developer documentation

---

## Success Criteria

1. **Correctness:** All offsets in failed MessageSet receive error ops for share consumers
   - One error op per offset in range `[BaseOffset, LastOffset]`
   - Each op has correct offset, error code, and error message

2. **Proper Acknowledgement Types:**
   - CRC errors → ack_type = REJECT (permanent data corruption)
   - Decompression errors → ack_type = RELEASE (consumer-specific capability)
   - Fetcher respects pre-set ack_type values

3. **Broker Interaction:**
   - REJECT errors: ShareAcknowledge sends REJECT, broker does not re-deliver
   - RELEASE errors: ShareAcknowledge sends RELEASE, broker returns to pool for re-delivery
   - Different consumers can receive released messages

4. **Isolation:** Regular consumers completely unaffected
   - Single error op per MessageSet (existing behavior)
   - No performance degradation
   - All changes gated by `RD_KAFKA_IS_SHARE_CONSUMER()` check

5. **Performance:** No measurable impact on happy path
   - Error path overhead acceptable (exceptional case)
   - Large MessageSets with errors handled without blocking

6. **Testing:** Comprehensive coverage
   - Unit tests: CRC errors, decompression errors, regular consumers
   - Integration tests: End-to-end REJECT flow, end-to-end RELEASE flow
   - Performance tests: Large MessageSets with errors

7. **Code Quality:** Clean implementation
   - Minimal changes to critical `rd_kafka_msgset_reader_v2()` function
   - Share consumer complexity isolated in `rd_kafka_share_consumer_err_range()`
   - Clear separation of concerns

---

## Timeline Estimate

- **Phase 1:** 2-3 hours (implement core function)
- **Phase 2:** 1-2 hours (CRC error handling)
- **Phase 3:** 2-3 hours (decompression error handling + call sites)
- **Phase 4:** 4-6 hours (comprehensive testing)
- **Phase 5:** 1-2 hours (code review and validation)
- **Phase 6:** 1 hour (documentation)

**Total:** 11-17 hours of focused development and testing

---

## Conclusion

### Recommended Approach
**Option B:** Dedicated `rd_kafka_share_consumer_err_range()` function with `ack_type` parameter

### Key Design Decisions

1. **Per-Offset Error Ops:** Create one error op per offset in range `[BaseOffset, LastOffset]`
   - Ensures share consumer acknowledgement system can track each offset independently
   - Prevents stuck offsets in share groups

2. **Differentiated Acknowledgement Types:**
   - **CRC errors → REJECT:** Data corruption is permanent, won't change between consumers
   - **Decompression errors → RELEASE:** Consumer-specific capability, another consumer might succeed

3. **Record Count Calculation:** Use `LastOffset - BaseOffset + 1`
   - Don't trust MessageSet header's `RecordCount` (may be corrupted)
   - Offset range is authoritative source of truth

4. **Fetcher Modification - Respect Pre-Set ack_type:** 
   - Check if `ack_type == GAP` before overriding to REJECT
   - Allows explicit control over REJECT vs RELEASE behavior

5. **Error Ops in Message Stream (NOT Separate Queue):**
   - Error ops added to `message_rkos` list along with fetch ops
   - Application receives errors in the same message iteration
   - Ensures errors and successful messages are delivered together
   - Both acknowledged together via ShareAcknowledge RPC
   - **Critical:** Do NOT send error ops separately to consumer group queue

### Key Benefits

- ✅ **Minimal edits** to critical `rd_kafka_msgset_reader_v2()` function
- ✅ **Clean separation** of share consumer complexity
- ✅ **Explicit control** over REJECT vs RELEASE semantics
- ✅ **Extensible** for future range-based error scenarios
- ✅ **Zero impact** on regular consumers (all changes gated)
- ✅ **Correct behavior** for share consumer offset tracking and re-delivery

### Implementation Scope

- **4 files** to modify (rdkafka_op.h/c, rdkafka_fetcher.c, rdkafka_msgset_reader.c)
- **~140 lines** of code changes
- **Low complexity:** Well-defined scope, isolated changes
- **Low risk:** All changes behind share consumer checks

### Next Steps - Part A (Critical Bug Fix)

1. ✅ **Get approval on this plan** ← YOU ARE HERE
2. ⏭️ Implement Phase 1 (infrastructure: new function + fetcher modifications)
3. ⏭️ Implement Phase 2 (CRC error handling with REJECT)
4. ⏭️ Implement Phase 3 (decompression error handling with RELEASE)
5. ⏭️ Comprehensive testing (Phase 4: unit + integration tests)
6. ⏭️ Code review and validation (Phase 5)
7. ⏭️ Documentation (Phase 6)

### Next Steps - Part B (Optional Optimization)

**If implementing together with Part A:**
- Add to Phase 1 or create new phase before testing
- Refactor while fetcher code is fresh in mind
- Single comprehensive PR

**If implementing separately after Part A:**
1. Wait for Part A to merge
2. Create separate optimization PR
3. Focus on performance testing and validation
4. Lower risk, easier to review independently

---

## Approval Checklist

Before proceeding with implementation, please confirm:

### Part A: Per-Offset Error Handling (Critical Bug Fix)
- [ ] **Approach approved:** Use `rd_kafka_share_consumer_err_range()` with `ack_type` parameter
- [ ] **CRC errors → REJECT:** Confirmed appropriate for data corruption scenarios
- [ ] **Decompression errors → RELEASE:** Confirmed appropriate for consumer capability issues
- [ ] **Record count calculation:** Use `LastOffset - BaseOffset + 1` (not header RecordCount)
- [ ] **Fetcher modification - ack_type:** Approved to modify hardcoded REJECT behavior
- [ ] **Fetcher modification - message list:** Approved to add error ops to message_rkos (not separate queue)
- [ ] **Scope acceptable:** 4 files, ~150 lines, low risk
- [ ] **Ready to proceed:** Begin implementation of Part A

### Part B: Array Allocation Optimization (Performance Improvement - Optional)
- [ ] **Include Part B:** Yes / No / Defer to later PR
- [ ] **Timing:** Together with Part A / Separate PR after Part A
- [ ] **Approach approved:** Eliminate FirstOffsets/LastOffsets/DeliveryCounts arrays, use batches list
- [ ] **Refactor filter function:** Change signature to use batches instead of arrays
- [ ] **Scope acceptable:** ~80 lines refactoring, pure optimization (no functional changes)

---

**Plan created:** 2026-04-14  
**Status:** Awaiting approval  
**Estimated effort:** 
- Part A (Per-Offset Error Handling): 11-17 hours
- Part B (Optimization - Optional): 4-6 hours

---

# Part B: Optimization - Eliminate Temporary Array Allocations (Optional)

## Background

**Location:** `src/rdkafka_fetcher.c:1320-1384`

**Current Implementation:**
```c
// Allocate temporary arrays (lines 1321-1326)
FirstOffsets = rd_malloc(sizeof(*FirstOffsets) * AcquiredRecordsArrayCnt);
LastOffsets = rd_malloc(sizeof(*LastOffsets) * AcquiredRecordsArrayCnt);
DeliveryCounts = rd_malloc(sizeof(*DeliveryCounts) * AcquiredRecordsArrayCnt);

// Read acquired ranges and create batch entries (lines 1334-1371)
for (i = 0; i < AcquiredRecordsArrayCnt; i++) {
    rd_kafka_buf_read_i64(rkbuf, &FirstOffsets[i]);
    rd_kafka_buf_read_i64(rkbuf, &LastOffsets[i]);
    rd_kafka_buf_read_i16(rkbuf, &DeliveryCounts[i]);
    
    entry = rd_kafka_share_ack_batch_entry_new(...);
    rd_list_add(&batches_out->entries, entry);
}

// Filter messages using arrays (lines 1378-1380)
rd_kafka_share_filter_acquired_records_and_update_ack_type(
    temp_fetchq, filtered_msgs, 
    FirstOffsets, LastOffsets, DeliveryCounts,  // ← Arrays needed here
    AcquiredRecordsArrayCnt);

// Free temporary arrays (lines 1382-1384)
rd_free(FirstOffsets);
rd_free(LastOffsets);
rd_free(DeliveryCounts);
```

**TODO Comment (line 1329):**
```c
/**
 * TODO KIP-932: There could be an improvement where we
 * segregate the records while reading the FirstOffsets and
 * LastOffsets, so that we can avoid mallocs above and directly
 * create the batches and fill the entries.
 */
```

## Problem

**Inefficiency:**
1. Allocate 3 temporary arrays
2. Read data into arrays
3. Create batch entries from arrays
4. Pass arrays to filter function
5. Free arrays

**Overhead:**
- 3 malloc calls
- 3 free calls
- Temporary memory consumption (3 * AcquiredRecordsArrayCnt * sizeof(type))
- Cache misses from separate array accesses

## Proposed Solution

### Refactor to Create Batch Entries Directly

**Step 1:** Create batch entries while reading (no intermediate arrays)
```c
// Lines 1334-1371: Read and create entries directly
for (i = 0; i < AcquiredRecordsArrayCnt; i++) {
    int64_t FirstOffset, LastOffset;
    int16_t DeliveryCount;
    
    rd_kafka_buf_read_i64(rkbuf, &FirstOffset);
    rd_kafka_buf_read_i64(rkbuf, &LastOffset);
    rd_kafka_buf_read_i16(rkbuf, &DeliveryCount);
    rd_kafka_buf_skip_tags(rkbuf);
    
    size = LastOffset - FirstOffset + 1;
    
    entry = rd_kafka_share_ack_batch_entry_new(
        FirstOffset, LastOffset, (int32_t)size, DeliveryCount);
    
    // Initialize types to GAP
    for (j = 0; j < size; j++) {
        entry->types[j] = RD_KAFKA_SHARE_INTERNAL_ACK_GAP;
    }
    
    rd_list_add(&batches_out->entries, entry);
    batches_out->response_acquired_offsets_count += (int32_t)size;
    
    // Track sortedness
    if (is_sorted && FirstOffset <= prev_end_offset)
        is_sorted = rd_false;
    prev_end_offset = LastOffset;
}

if (is_sorted)
    batches_out->entries.rl_flags |= RD_LIST_F_SORTED;
```

**Step 2:** Refactor filter function to use batches list instead of arrays

**Current signature:**
```c
void rd_kafka_share_filter_acquired_records_and_update_ack_type(
    rd_kafka_q_t *temp_fetchq,
    rd_list_t *filtered_msgs,
    const int64_t *FirstOffsets,      // ← Remove
    const int64_t *LastOffsets,       // ← Remove
    const int16_t *DeliveryCounts,    // ← Remove
    int32_t AcquiredRecordsArrayCnt); // ← Remove
```

**New signature:**
```c
void rd_kafka_share_filter_acquired_records_and_update_ack_type(
    rd_kafka_q_t *temp_fetchq,
    rd_list_t *filtered_msgs,
    rd_kafka_share_ack_batches_t *batches);  // ← Use batches instead
```

**New implementation:**
```c
void rd_kafka_share_filter_acquired_records_and_update_ack_type(
    rd_kafka_q_t *temp_fetchq,
    rd_list_t *filtered_msgs,
    rd_kafka_share_ack_batches_t *batches) {
    
    rd_kafka_op_t *rko;
    
    while ((rko = rd_kafka_q_pop(temp_fetchq, RD_POLL_NOWAIT, 0)) != NULL) {
        int64_t rko_offset = rd_kafka_op_get_offset(rko);
        
        // Use existing rd_kafka_share_find_entry_for_offset() function
        rd_kafka_share_ack_batch_entry_t *entry =
            rd_kafka_share_find_entry_for_offset(batches, rko_offset);
        
        if (entry) {
            // In acquired range
            rd_kafka_msg_t *rkm = NULL;
            
            if (unlikely(rd_kafka_op_is_ctrl_msg(rko)))
                continue;
            
            if (rko->rko_type == RD_KAFKA_OP_FETCH) {
                rkm = &rko->rko_u.fetch.rkm;
                rkm->rkm_u.consumer.ack_type =
                    RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED;
                rkm->rkm_u.consumer.delivery_count = entry->delivery_count;
            } else if (rko->rko_type == RD_KAFKA_OP_CONSUMER_ERR) {
                rkm = &rko->rko_u.err.rkm;
                rkm->rkm_u.consumer.ack_type =
                    RD_KAFKA_SHARE_INTERNAL_ACK_REJECT;
                // Note: Part A will modify this to respect pre-set ack_type
            }
            
            rd_list_add(filtered_msgs, rko);
        } else {
            // Not in acquired range, discard
            rd_kafka_op_destroy(rko);
        }
    }
}
```

**Step 3:** Update call site
```c
// Old call (line 1378)
rd_kafka_share_filter_acquired_records_and_update_ack_type(
    temp_fetchq, filtered_msgs, FirstOffsets, LastOffsets,
    DeliveryCounts, AcquiredRecordsArrayCnt);

// New call
rd_kafka_share_filter_acquired_records_and_update_ack_type(
    temp_fetchq, filtered_msgs, batches_out);
```

## Benefits

### Memory Efficiency
- ✅ Eliminate 3 malloc/free calls per ShareFetch response
- ✅ Reduce temporary memory usage
- ✅ Better cache locality (data accessed once during reading)

### Performance
- ✅ Fewer allocations in hot path
- ✅ One-pass reading and batch creation
- ✅ Reuse existing `rd_kafka_share_find_entry_for_offset()` function

### Code Quality
- ✅ Simpler flow (no array management)
- ✅ Less error-prone (no manual malloc/free)
- ✅ Resolves TODO comment

## Performance Impact

**Scenario:** ShareFetch response with 100 acquired ranges

**Before:**
- 3 mallocs (3 × 100 × 8 bytes = 2.4KB)
- 300 array writes
- 3 frees
- Linear search through arrays for each message

**After:**
- 0 extra mallocs (batch entries already allocated)
- Direct batch creation
- Binary search through sorted batches list (if sorted flag set)

**Expected improvement:** ~5-10% reduction in ShareFetch response processing time for responses with many acquired ranges.

## Implementation Checklist

### Step 1: Refactor Batch Creation
- [ ] Remove FirstOffsets, LastOffsets, DeliveryCounts array allocations
- [ ] Create batch entries directly in read loop
- [ ] Verify sortedness tracking still works
- [ ] Test with various AcquiredRecordsArrayCnt values

### Step 2: Refactor Filter Function
- [ ] Change function signature to take batches instead of arrays
- [ ] Use `rd_kafka_share_find_entry_for_offset()` for range lookup
- [ ] Extract delivery_count from entry instead of array
- [ ] Update function comment/documentation

### Step 3: Update Call Sites
- [ ] Update call in `rd_kafka_share_fetch_response_handle_one_partition()`
- [ ] Verify any other call sites (if any)
- [ ] Update function declaration in header

### Step 4: Testing
- [ ] Unit test: Small acquired range count (1-10)
- [ ] Unit test: Large acquired range count (1000+)
- [ ] Unit test: Unsorted ranges
- [ ] Performance test: Compare before/after for 100 ranges
- [ ] Integration test: End-to-end ShareFetch flow

## Compatibility Note

This is a **pure refactoring** - no functional changes:
- Same behavior
- Same outputs
- Just eliminates temporary allocations

Can be implemented **independently** of Part A, or **combined** with Part A in the same PR.

## Recommendation

**Priority:** Medium (optimization, not a bug fix)

**Timing:**
- **Option 1 (Recommended):** Implement Part B **after** Part A is merged
  - Keep changes separate and focused
  - Part A fixes critical bug (stuck offsets)
  - Part B is performance optimization
  - Easier to review and validate independently

- **Option 2:** Implement Part B **together** with Part A
  - Single PR with both changes
  - More churn but complete solution
  - Requires careful testing to separate concerns

**Estimated effort:** 4-6 hours (refactoring + testing)

---

## Files Modified for Part B

| File | Function | Change |
|------|----------|--------|
| `src/rdkafka_fetcher.c` | `rd_kafka_share_fetch_response_handle_one_partition()` | Lines 1320-1384: Remove array allocations, create batches directly |
| `src/rdkafka_fetcher.c` | `rd_kafka_share_filter_acquired_records_and_update_ack_type()` | Lines 888-946: Change signature, use batches list instead of arrays |
| Header file | (declarations) | Update function signature |

**Total lines changed:** ~80 lines (refactoring, no new functionality)
