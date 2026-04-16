# Decompression Error Handling Bug - Fix Plan

**Date:** 2026-04-16  
**Status:** Ready to implement  
**Priority:** High - Parser stops processing valid MessageSets after decompression error

---

## Bug Summary

When a decompression error occurs in a MessageSet, the parser **stops processing** instead of continuing to parse subsequent MessageSets in the same Fetch response. This is inconsistent with how CRC errors and unsupported MagicByte errors are handled.

### Impact
- Valid messages after a corrupted compressed batch are **not delivered**
- Share consumer doesn't get a chance to acknowledge/release messages it could process
- Defeats the purpose of per-offset error handling

---

## Current Behavior Analysis

### ✅ Working: CRC Errors
**Location:** `rdkafka_msgset_reader.c:1158-1161`

```c
/* CRC error handling */
rd_kafka_share_consumer_err_range(...);  // Create error ops
rd_kafka_buf_skip_to(rkbuf, len_start + hdr.Length);  // Skip to end
return RD_KAFKA_RESP_ERR_NO_ERROR;  // ← Continue parsing
```

**Result:** Parser loop continues ✓

---

### ✅ Working: Unsupported MagicByte
**Location:** `rdkafka_msgset_reader.c:1403-1407`

```c
/* Unsupported MagicByte handling */
if (unlikely(err)) {
    if (err == RD_KAFKA_RESP_ERR__BAD_MSG)
        return RD_KAFKA_RESP_ERR_NO_ERROR;
    
    err = RD_KAFKA_RESP_ERR_NO_ERROR;  // ← Reset error
    continue;  // Continue parsing
}
```

**Result:** Parser loop continues ✓

---

### ❌ BROKEN: Decompression Errors
**Location:** `rdkafka_msgset_reader.c:1210-1215` and `line 550`

```c
// In rd_kafka_msgset_reader_v2()
err = rd_kafka_msgset_reader_decompress(...);
if (err)
    goto err;  // ← Goes to line 1254

// In rd_kafka_msgset_reader_decompress() at line 517-550
err:
    rd_kafka_share_consumer_err_range(...);  // Create error ops ✓
    return err;  // ← Returns RD_KAFKA_RESP_ERR__BAD_COMPRESSION ✗

// Back in rd_kafka_msgset_reader_v2() at line 1254-1256
err:
    return err;  // ← Propagates error to outer loop

// In rd_kafka_msgset_reader() at line 1411-1413
err = reader[(int)MagicByte](msetr);
} while (!err && rd_slice_remains(&rkbuf->rkbuf_reader) > 0);  // ← EXITS!
```

**Result:** Parser loop **STOPS**, subsequent MessageSets are **NOT parsed** ✗

---

## The Fix

### Recommended Approach: Match CRC Error Pattern

**File:** `src/rdkafka_msgset_reader.c`  
**Function:** `rd_kafka_msgset_reader_v2()`  
**Lines:** Around 1210-1215

**Change from:**
```c
/* Handle compressed MessageSet */
if (hdr.Attributes & RD_KAFKA_MSG_ATTR_COMPRESSION_MASK) {
        const void *compressed;

        compressed =
            rd_slice_ensure_contig(&rkbuf->rkbuf_reader, payload_size);
        rd_assert(compressed);

        err = rd_kafka_msgset_reader_decompress(
            msetr, 2 /*MsgVersion v2*/, hdr.Attributes,
            hdr.BaseTimestamp, hdr.BaseOffset, compressed,
            payload_size);
        if (err)
                goto err;  // ← PROBLEM: Exits to outer loop
```

**Change to:**
```c
/* Handle compressed MessageSet */
if (hdr.Attributes & RD_KAFKA_MSG_ATTR_COMPRESSION_MASK) {
        const void *compressed;

        compressed =
            rd_slice_ensure_contig(&rkbuf->rkbuf_reader, payload_size);
        rd_assert(compressed);

        err = rd_kafka_msgset_reader_decompress(
            msetr, 2 /*MsgVersion v2*/, hdr.Attributes,
            hdr.BaseTimestamp, hdr.BaseOffset, compressed,
            payload_size);
        if (err) {
                /* Decompression failed, error ops already created.
                 * Skip this MessageSet and continue parsing. */
                rd_kafka_buf_skip(rkbuf, payload_size);
                goto done;  // ← FIX: Skip to line 1238, returns NO_ERROR
        }
```

### Why This Fix?

1. **Error ops already created:** `rd_kafka_msgset_reader_decompress()` already calls `rd_kafka_share_consumer_err_range()` for share consumers (line 531-539)

2. **Skip corrupted data:** `rd_kafka_buf_skip(rkbuf, payload_size)` skips the compressed data we can't decompress

3. **Continue parsing:** `goto done` jumps to line 1238 which returns `RD_KAFKA_RESP_ERR_NO_ERROR`, allowing the outer loop to continue

4. **Consistent with CRC errors:** Matches the exact pattern used for CRC errors

---

## Testing Plan

### Step 1: Verify Bug Exists

**Test already added:** `unittest_msgset_decomp_then_success()` in `rdunittest_msgset_errors.c`

**Test scenario:**
- MessageSet 1: Decompression error (offsets 200-202)
- MessageSet 2: Valid messages (offsets 203-204) ← **MUST** be parsed

**Expected with current code:** Test **FAILS** - MessageSet 2 not parsed  
**Expected after fix:** Test **PASSES** - MessageSet 2 is parsed

### Step 2: Apply Fix

Apply the code change described above in `rdkafka_msgset_reader.c`

### Step 3: Run All Tests

```bash
cd /Users/pratyushranjan/Desktop/librdkafka
make
cd tests
make build
DYLD_LIBRARY_PATH=../src:../src-cpp:$DYLD_LIBRARY_PATH ./test-runner 0000
```

**Expected results:**
- ✅ `unittest_msgset_decomp_then_success` - NEW test passes
- ✅ `unittest_msgset_decompression_error_share_consumer` - Existing test still passes
- ✅ `unittest_msgset_mixed_crc_success_decomp` - Mixed scenario still passes
- ✅ All other msgset tests pass
- ✅ All unit tests pass

### Step 4: Verify Consistent Behavior

After fix, all three error types should have identical behavior:

| Error Type | Create Ops | Skip MessageSet | Continue Parsing |
|------------|------------|-----------------|------------------|
| CRC Error | ✅ | ✅ | ✅ |
| Unsupported Magic | ✅ | ✅ | ✅ |
| Decompression Error | ✅ | ✅ (after fix) | ✅ (after fix) |

---

## Alternative Approaches Considered

### Alternative 1: Modify decompress function to return NO_ERROR

**Rejected because:**
- `rd_kafka_msgset_reader_decompress()` is also called from v0/v1 message readers
- Changing return value could affect v0/v1 behavior
- Less clear intent - error occurred but returns success?

### Alternative 2: Check error type in outer loop

**Rejected because:**
- Adds complexity to outer loop
- Less maintainable
- Doesn't match existing pattern

---

## Related Bugs Fixed

This is the **third bug** found in MessageSet error handling:

### Bug 1: CRC Error Skip Position (Fixed)
**File:** `rdkafka_msgset_reader.c:1158`  
**Issue:** Used length instead of absolute position  
**Fix:** `rd_kafka_buf_skip_to(rkbuf, len_start + hdr.Length)`

### Bug 2: Unsupported MagicByte Loop Exit (Fixed)
**File:** `rdkafka_msgset_reader.c:1406`  
**Issue:** Didn't reset error before continue  
**Fix:** `err = RD_KAFKA_RESP_ERR_NO_ERROR; continue;`

### Bug 3: Decompression Error Blocks Parsing (This Fix)
**File:** `rdkafka_msgset_reader.c:1214-1215`  
**Issue:** Returns error instead of continuing  
**Fix:** Skip and goto done

---

## Verification Checklist

Before marking as complete:

- [ ] Code compiles without warnings
- [ ] `unittest_msgset_decomp_then_success` passes
- [ ] All existing decompression tests pass
- [ ] All mixed scenario tests pass
- [ ] All unit tests pass (0000-unittests)
- [ ] Manual testing with real broker (optional but recommended)
- [ ] Code review completed
- [ ] Documentation updated (if needed)

---

## References

- **Main parser loop:** `rdkafka_msgset_reader.c:1376-1414` (`rd_kafka_msgset_reader()`)
- **V2 MessageSet parser:** `rdkafka_msgset_reader.c:1063-1257` (`rd_kafka_msgset_reader_v2()`)
- **Decompress function:** `rdkafka_msgset_reader.c:194-551` (`rd_kafka_msgset_reader_decompress()`)
- **Share consumer error range:** `rdkafka_fetcher.c` (`rd_kafka_share_consumer_err_range()`)

---

## Notes

- This fix ensures share consumers can continue processing valid messages even when some batches fail decompression
- The RELEASE ack_type for decompression errors is correct - other consumers with different decompression capabilities might succeed
- After this fix, librdkafka's error handling will be consistent across all error types
