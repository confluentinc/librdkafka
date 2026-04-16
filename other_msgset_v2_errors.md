# Other MessageSet v2 Errors Analysis

## Summary of Findings

After searching all `rd_kafka_consumer_err()` calls in msgset_reader.c, here are ALL error scenarios:

### ✅ Already Covered in Plan

1. **CRC32C check failure** (line 1091-1102)
   - MessageSet v2 specific
   - Affects entire MessageSet range
   - ✅ Plan includes per-offset error handling with REJECT

2. **Decompression failures** (line 520-525)
   - All codecs: GZIP, Snappy, LZ4, ZSTD
   - Affects entire MessageSet range
   - ✅ Plan includes per-offset error handling with RELEASE

---

### ⚠️ NEWLY DISCOVERED: Unsupported MagicByte (Share Consumer Bug!)

**Location:** `src/rdkafka_msgset_reader.c:1246-1259`

**The Issue:**
```c
if (Offset >= msetr->msetr_rktp->rktp_offsets.fetch_pos.offset &&
    !RD_KAFKA_IS_SHARE_CONSUMER(msetr->msetr_rkb->rkb_rk)) {
    rd_kafka_consumer_err(
        &msetr->msetr_rkq, msetr->msetr_broker_id,
        RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED,
        msetr->msetr_tver->version, NULL, rktp, Offset,
        "Unsupported Message(Set) MagicByte %d at offset %" PRId64,
        (int)*MagicBytep, Offset);
}
```

**The Problem:**
- **Regular consumers:** Get error op for the unsupported MessageSet
- **Share consumers:** Do NOT get any error op! (`!RD_KAFKA_IS_SHARE_CONSUMER` check skips them)
- The MessageSet is skipped (lines 1264-1265)
- The loop continues to next MessageSet (line 1310)

**What Happens:**
```
Share consumer receives MessageSet with MagicByte=5 (unsupported future version)
BaseOffset=300, Length indicates offsets 300-309

Current code:
1. Peek detects MagicByte=5
2. Error check is FALSE for share consumers (line 1248)
3. No error op created
4. MessageSet is skipped
5. Continue to next MessageSet

Result: Offsets 300-309 are STUCK!
- No error ops created
- No acknowledgements sent
- Broker thinks consumer still has them acquired
- Consumer has no record of them
```

**Should This Be Fixed?**

**YES!** This is the same bug as CRC/decompression errors. Share consumers need per-offset error ops for all MessageSet-level failures.

**Recommended ack_type:**

Option 1: **REJECT** (like CRC errors)
- Rationale: If broker is sending unsupported message versions, it's a broker/producer configuration issue
- The data won't become "supported" by sending to a different consumer
- Should be marked as permanently failed

Option 2: **RELEASE** (like decompression errors)
- Rationale: Maybe a newer consumer version could handle it
- Different consumer versions in the group might have different capabilities

**I recommend Option 1 (REJECT)** because:
- MagicByte versions are protocol-level, not codec-level
- If a message has MagicByte=5, no current consumer can read it
- This is a fundamental incompatibility, not a consumer capability difference
- Similar to CRC errors (data corruption at protocol level)

---

### ❌ NOT MessageSet-Level Errors (No Changes Needed)

3. **CRC32 check failure (v0/v1)** (line 614-625)
   - Message v0/v1 specific (NOT v2)
   - Each v0/v1 message is independent (one message = one offset)
   - Already per-message error reporting
   - ❌ No change needed

4. **MSG_SIZE_TOO_LARGE** (line 1417-1425)
   - Fetch-level error when entire fetch buffer couldn't contain even one message
   - Reports error at `fetch_pos.offset` (single offset)
   - Not a MessageSet-level error affecting multiple offsets
   - ❌ No change needed
   - **Note:** Has TODO comment for share consumer handling (KIP-932)

5. **Parse failures / Buffer underflows** (lines 1060, 1124, etc.)
   - Use `rd_kafka_buf_parse_fail()` or `rd_kafka_buf_underflow_fail()`
   - Do NOT call `rd_kafka_consumer_err()`
   - Jump to `err_parse:` label and return error code
   - Typically suppressed as partial messages (acceptable)
   - ❌ No change needed

---

## Recommendation

**Add unsupported MagicByte error to the plan:**

### Location
`src/rdkafka_msgset_reader.c:1246-1259` in `rd_kafka_msgset_reader_peek_msg_version()`

### Current Code
```c
if (Offset >= msetr->msetr_rktp->rktp_offsets.fetch_pos.offset &&
    !RD_KAFKA_IS_SHARE_CONSUMER(msetr->msetr_rkb->rkb_rk)) {
    rd_kafka_consumer_err(
        &msetr->msetr_rkq, msetr->msetr_broker_id,
        RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED,
        msetr->msetr_tver->version, NULL, rktp, Offset,
        "Unsupported Message(Set) MagicByte %d at offset %" PRId64,
        (int)*MagicBytep, Offset);
    /* Skip message(set) */
    msetr->msetr_rktp->rktp_offsets.fetch_pos.offset = Offset + 1;
}
```

### Problem
- For share consumers, this reports NO error and skips the MessageSet
- We don't know LastOffset at this point (we're just peeking)
- We need to read the Length field to calculate the offset range

### Proposed Solution

**Step 1:** Read Length to calculate offset range
```c
int64_t Offset;
int32_t Length;
int64_t LastOffset;

rd_kafka_buf_read_i64(rkbuf, &Offset);  // Already done at line 1230
rd_kafka_buf_read_i32(rkbuf, &Length);  // Need to add this

// For v2, calculate LastOffset
// For v0/v1, we can't know without parsing, so just use Offset
// But we're skipping the whole MessageSet anyway
```

**Problem:** We can't determine if it's v0, v1, or v2 at this point - that's the whole problem! We don't know the message format to calculate the offset range.

**Alternative Solution:** Skip this error for now, mark as future work
- This is an edge case (unsupported future MagicByte versions)
- Unlikely in production (broker should only send supported versions)
- Would require reading Length and making assumptions about format
- Could be addressed in a separate fix

**OR Alternative:** Create single error op at Offset
- Better than no error op at all
- At least one offset gets acknowledged (REJECT)
- Remaining offsets in MessageSet still stuck (but better than nothing)

---

## Final Summary

### Errors That Need Per-Offset Handling for Share Consumers

| Error Type | Location | ack_type | Status |
|-----------|----------|----------|--------|
| CRC32C check failure | msgset_reader_v2:1091 | REJECT | ✅ In plan |
| Decompression failures (all codecs) | msgset_reader_decompress:520 | RELEASE | ✅ In plan |
| Unsupported MagicByte | msgset_reader_peek_msg_version:1249 | REJECT | ⚠️ **NEWLY FOUND** |

### Errors That Don't Need Changes

| Error Type | Reason |
|-----------|--------|
| CRC32 (v0/v1) | Already per-message |
| MSG_SIZE_TOO_LARGE | Single-offset fetch error |
| Parse/underflow errors | Not reported as error ops |

---

## Recommendation for Plan Update

**Option A (Recommended):** Add unsupported MagicByte to plan, but mark as "deferred"
- Document the issue
- Note that it requires additional complexity (reading Length without knowing format)
- Can be addressed in follow-up work
- Priority: Low (edge case, unlikely in production)

**Option B:** Add to plan now with simple single-offset error
- Create error op for the Offset we read
- Better than nothing
- Doesn't solve the full problem but prevents complete silence

**Option C:** Add to plan now with full per-offset handling
- Read Length field
- Make reasonable assumptions about offset range
- Full solution but higher risk

I recommend **Option A** for the initial implementation - focus on CRC and decompression (common cases), defer unsupported MagicByte (rare edge case).
