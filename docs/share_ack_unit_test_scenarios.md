# Share Consumer Acknowledgement Flow - Unit Test Scenarios

## Call Flow Overview

```
BROKER THREAD (ShareFetch Response Parsing):
    rd_kafka_share_filter_acquired_records_and_update_ack_type()   ─┐
                    ↓                               ├─ Group A (Response Building)
    rd_kafka_share_build_response_rko()            ─┘
                    ↓
    Enqueue RKO to consumer queue

APPLICATION THREAD (Poll):
    rd_kafka_q_serve_share_rkmessages()            ─┐
                    ↓                               │
        rd_kafka_share_process_fetch_response()     ├─ Group C (Queue Serving + Ack Mapping)
                    ↓                               │
            rd_kafka_share_build_inflight_acks_map()     ─┘
                    ↓
        Return messages to app

APPLICATION THREAD (Before next poll / commit):
    rd_kafka_share_build_ack_details()             ─── Group B (Ack Details Extraction)
```

## Functions Under Test

| Group | Functions | File |
|-------|-----------|------|
| A | `rd_kafka_share_filter_acquired_records_and_update_ack_type()` + `rd_kafka_share_build_response_rko()` | rdkafka_fetcher.c |
| B | `rd_kafka_share_build_ack_details()` | rdkafka_share_acknowledgement.c |
| C | `rd_kafka_q_serve_share_rkmessages()` (internally calls `rd_kafka_share_build_inflight_acks_map()`) | rdkafka_queue.c |

## Helper Functions Used (from rdkafka_share_acknowledgement.h)

- `rd_kafka_share_ack_batch_entry_new(start_offset, end_offset, types_cnt, delivery_count)`
- `rd_kafka_share_ack_batch_entry_destroy(entry)`
- `rd_kafka_share_ack_batch_entry_copy(src)`
- `rd_kafka_share_ack_batches_new(rktpar, leader_id, leader_epoch, msgs_count)`
- `rd_kafka_share_ack_batches_new_empty()`
- `rd_kafka_share_ack_batches_destroy(batches)`
- `rd_kafka_share_ack_batches_copy(src)`

---

## Group A: Response Building Flow

**Functions:** `rd_kafka_share_filter_acquired_records_and_update_ack_type()` → `rd_kafka_share_build_response_rko()`

**Purpose:** Tests the broker-thread flow of filtering messages by acquired ranges and building the SHARE_FETCH_RESPONSE RKO structure.

### Test A.1: All messages acquired - contiguous range

**Significance:** Validates the happy path where all fetched messages fall within a single acquired range and all become ACQUIRED in the response RKO.

**Setup:**
- Create temp_fetchq with messages at offsets 0-4
- Acquired range: `FirstOffsets=[0], LastOffsets=[4]`

**Flow:**
1. Call `rd_kafka_share_filter_acquired_records_and_update_ack_type()` → produces filtered_msgs with 5 messages
2. Create inflight_acks using helpers with all ACQUIRED types
3. Call `rd_kafka_share_build_response_rko()` → produces response RKO

**Expected:**
- filtered_msgs contains 5 messages, all with `ack_type = ACQUIRED`
- response_rko->message_rkos has 5 messages
- response_rko->inflight_acks has 1 batches entry with 5 ACQUIRED types

---

### Test A.2: Partial range - some messages filtered out

**Significance:** Validates that messages outside acquired ranges are discarded and don't appear in the response RKO.

**Setup:**
- Create temp_fetchq with messages at offsets 0-9
- Acquired range: `FirstOffsets=[2], LastOffsets=[5]` (only middle section)

**Flow:**
1. Call `rd_kafka_share_filter_acquired_records_and_update_ack_type()` → produces filtered_msgs with 4 messages (offsets 2-5)
2. Create inflight_acks for offsets 2-5
3. Call `rd_kafka_share_build_response_rko()`

**Expected:**
- filtered_msgs contains only 4 messages (offsets 2,3,4,5)
- Messages at offsets 0,1,6,7,8,9 are discarded
- response_rko->message_rkos has 4 messages

---

### Test A.3: Multiple disjoint acquired ranges

**Significance:** Validates handling of non-contiguous acquired ranges (gaps where records were acquired by another consumer).

**Setup:**
- Create temp_fetchq with messages at offsets 0-9
- Acquired ranges: `FirstOffsets=[1,5,9], LastOffsets=[2,6,9]`

**Flow:**
1. Call `rd_kafka_share_filter_acquired_records_and_update_ack_type()` → produces 5 messages
2. Create inflight_acks with entries for each range
3. Call `rd_kafka_share_build_response_rko()`

**Expected:**
- filtered_msgs contains 5 messages (offsets 1,2,5,6,9)
- response_rko->message_rkos has 5 messages
- inflight_acks correctly represents the disjoint ranges

---

### Test A.4: Messages with GAPs in inflight_acks

**Significance:** Validates that GAPs (offsets without messages) are correctly tracked in inflight_acks but don't create placeholder messages.

**Setup using helpers:**
```c
// Create entry with GAPs
rd_kafka_share_ack_batch_entry_t *entry =
    rd_kafka_share_ack_batch_entry_new(start=0, end=5, types_cnt=6, delivery_count=1);
// Types: [ACQUIRED, GAP, GAP, ACQUIRED, ACQUIRED, GAP]
entry->types[0] = RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED;
entry->types[1] = RD_KAFKA_SHARE_INTERNAL_ACK_GAP;
entry->types[2] = RD_KAFKA_SHARE_INTERNAL_ACK_GAP;
entry->types[3] = RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED;
entry->types[4] = RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED;
entry->types[5] = RD_KAFKA_SHARE_INTERNAL_ACK_GAP;
```

**Input:**
- filtered_msgs with 3 messages at offsets 0, 3, 4 (no messages at GAP positions)

**Expected:**
- response_rko->message_rkos has 3 messages (no GAP placeholders)
- response_rko->inflight_acks preserves GAP types for offsets 1, 2, 5

---

### Test A.5: Multiple partitions in single response

**Significance:** Validates correct handling of multi-partition responses with different leader metadata per partition.

**Setup using helpers:**
```c
// Partition 0
rd_kafka_share_ack_batches_t *batches0 = rd_kafka_share_ack_batches_new(
    rktpar0, leader_id=1, leader_epoch=5, msgs_count=3);

// Partition 1
rd_kafka_share_ack_batches_t *batches1 = rd_kafka_share_ack_batches_new(
    rktpar1, leader_id=2, leader_epoch=3, msgs_count=4);
```

**Expected:**
- response_rko->message_rkos has 7 messages total
- response_rko->inflight_acks has 2 batches entries
- Each batches preserves its leader_id and leader_epoch

---

### Test A.6: Non-sequential offsets in queue
**Significance:** Queue messages may have gaps in offsets (sparse offsets)

**Setup:**
- Create temp_fetchq with messages at offsets 0, 10, 20, 30
- Acquired ranges: `FirstOffsets=[5], LastOffsets=[25]`

**Expected:**
- filtered_msgs contains messages at offsets 10, 20 only

---

### Test A.7: Range starts after all queue messages
**Significance:** All messages filtered when range is beyond queue offsets

**Setup:**
- Create temp_fetchq with messages at offsets 0-4
- Acquired range: `FirstOffsets=[100], LastOffsets=[200]`

**Expected:**
- filtered_msgs is empty (all messages outside range)

---

## Group B: Ack Details Extraction

**Function:** `rd_kafka_share_build_ack_details()`

**Purpose:** Extracts acknowledged (non-ACQUIRED) offsets from inflight map for sending to broker. Collates consecutive same-type offsets.

### Test B.1: All ACCEPT - extracts everything

**Significance:** Validates the happy path where all records are accepted. The entire entry should be extracted for sending, and the partition should be removed from the inflight map.

**Setup using helpers:**
```c
rd_kafka_share_ack_batch_entry_t *entry =
    rd_kafka_share_ack_batch_entry_new(start=100, end=104, types_cnt=5, delivery_count=1);
for (int i = 0; i < 5; i++)
    entry->types[i] = RD_KAFKA_SHARE_INTERNAL_ACK_ACCEPT;
// Add to rkshare->rkshare_inflight_acks map
```

**Action:**
- Call `rd_kafka_share_build_ack_details(rkshare)`

**Expected:**
- Returns list with 1 batch, 1 collated entry: start=100, end=104, types[0]=ACCEPT
- Partition removed from inflight map (no ACQUIRED remaining)
- `rkshare_unacked_cnt == 0`

---

### Test B.2: All ACQUIRED - returns NULL

**Significance:** Validates that when no records have been acknowledged yet, the function returns NULL indicating nothing to send.

**Setup using helpers:**
```c
rd_kafka_share_ack_batch_entry_t *entry =
    rd_kafka_share_ack_batch_entry_new(start=100, end=104, types_cnt=5, delivery_count=1);
for (int i = 0; i < 5; i++)
    entry->types[i] = RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED;
```

**Action:**
- Call `rd_kafka_share_build_ack_details(rkshare)`

**Expected:**
- Returns NULL (nothing to send)
- Inflight map unchanged
- `rkshare_unacked_cnt == 5`

---

### Test B.3: Mixed types - collation

**Significance:** Tests collation of consecutive same-type offsets and separation of ACQUIRED offsets to remain in inflight map. This is the most complex scenario.

**Setup using helpers:**
```c
rd_kafka_share_ack_batch_entry_t *entry =
    rd_kafka_share_ack_batch_entry_new(start=100, end=109, types_cnt=10, delivery_count=1);
// [ACCEPT, ACCEPT, REJECT, REJECT, REJECT, ACQUIRED, ACQUIRED, RELEASE, RELEASE, ACCEPT]
entry->types[0] = RD_KAFKA_SHARE_INTERNAL_ACK_ACCEPT;
entry->types[1] = RD_KAFKA_SHARE_INTERNAL_ACK_ACCEPT;
entry->types[2] = RD_KAFKA_SHARE_INTERNAL_ACK_REJECT;
entry->types[3] = RD_KAFKA_SHARE_INTERNAL_ACK_REJECT;
entry->types[4] = RD_KAFKA_SHARE_INTERNAL_ACK_REJECT;
entry->types[5] = RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED;
entry->types[6] = RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED;
entry->types[7] = RD_KAFKA_SHARE_INTERNAL_ACK_RELEASE;
entry->types[8] = RD_KAFKA_SHARE_INTERNAL_ACK_RELEASE;
entry->types[9] = RD_KAFKA_SHARE_INTERNAL_ACK_ACCEPT;
```

**Expected:**
- ack_details has 1 batch with 4 collated entries:
  - 100-101: ACCEPT
  - 102-104: REJECT
  - 107-108: RELEASE
  - 109-109: ACCEPT
- Inflight map retains: 105-106: ACQUIRED
- `rkshare_unacked_cnt == 2`

---

### Test B.4: Alternating types - no collation possible

**Significance:** Validates correct behavior when collation is impossible due to alternating types.

**Setup using helpers:**
```c
rd_kafka_share_ack_batch_entry_t *entry =
    rd_kafka_share_ack_batch_entry_new(start=100, end=104, types_cnt=5, delivery_count=1);
// [ACCEPT, REJECT, ACCEPT, REJECT, ACCEPT]
entry->types[0] = RD_KAFKA_SHARE_INTERNAL_ACK_ACCEPT;
entry->types[1] = RD_KAFKA_SHARE_INTERNAL_ACK_REJECT;
entry->types[2] = RD_KAFKA_SHARE_INTERNAL_ACK_ACCEPT;
entry->types[3] = RD_KAFKA_SHARE_INTERNAL_ACK_REJECT;
entry->types[4] = RD_KAFKA_SHARE_INTERNAL_ACK_ACCEPT;
```

**Expected:**
- ack_details has 5 separate collated entries (each single offset)
- Inflight map: partition removed
- `rkshare_unacked_cnt == 0`

---

### Test B.5: Multiple partitions with mixed states

**Significance:** Validates that multiple partitions are processed independently with correct partial extraction.

**Setup using helpers:**
```c
// Partition 0: all ACCEPT (fully extracted)
rd_kafka_share_ack_batch_entry_t *entry0 =
    rd_kafka_share_ack_batch_entry_new(start=100, end=102, types_cnt=3, delivery_count=1);
for (int i = 0; i < 3; i++)
    entry0->types[i] = RD_KAFKA_SHARE_INTERNAL_ACK_ACCEPT;

// Partition 1: mixed with ACQUIRED (partial extraction)
rd_kafka_share_ack_batch_entry_t *entry1 =
    rd_kafka_share_ack_batch_entry_new(start=200, end=204, types_cnt=5, delivery_count=2);
// [ACQUIRED, REJECT, REJECT, ACQUIRED, ACCEPT]
entry1->types[0] = RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED;
entry1->types[1] = RD_KAFKA_SHARE_INTERNAL_ACK_REJECT;
entry1->types[2] = RD_KAFKA_SHARE_INTERNAL_ACK_REJECT;
entry1->types[3] = RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED;
entry1->types[4] = RD_KAFKA_SHARE_INTERNAL_ACK_ACCEPT;
```

**Expected:**
- ack_details has 2 batches:
  - Partition 0: 1 entry (100-102: ACCEPT)
  - Partition 1: 2 entries (201-202: REJECT, 204: ACCEPT)
- Inflight map:
  - Partition 0: removed
  - Partition 1: remains with ACQUIRED at offsets 200, 203
- `rkshare_unacked_cnt == 2`

---

### Test B.6: Empty inflight map

**Significance:** Validates graceful handling when there's nothing to process.

**Setup:**
- Empty `rkshare_inflight_acks` map

**Expected:**
- Returns NULL
- `rkshare_unacked_cnt == 0`

---

### Test B.7: Preserves delivery_count and leader metadata

**Significance:** Validates that metadata from the original fetch response is preserved for correct broker routing and redelivery tracking.

**Setup using helpers:**
```c
rd_kafka_share_ack_batches_t *batches = rd_kafka_share_ack_batches_new(
    rktpar, leader_id=5, leader_epoch=10, msgs_count=2);
rd_kafka_share_ack_batch_entry_t *entry =
    rd_kafka_share_ack_batch_entry_new(start=100, end=101, types_cnt=2, delivery_count=3);
entry->types[0] = RD_KAFKA_SHARE_INTERNAL_ACK_ACCEPT;
entry->types[1] = RD_KAFKA_SHARE_INTERNAL_ACK_ACCEPT;
```

**Expected:**
- Output batch has `response_leader_id == 5`, `response_leader_epoch == 10`
- Collated entry has `delivery_count == 3`

---

### Test B.8: GAP + ACQUIRED only (no ack types)
**Significance:** Only GAPs extracted, ACQUIREDs remain

**Setup:**
- Partition 0: offsets 100-104, types `[GAP, ACQUIRED, GAP, ACQUIRED, GAP]`

**Expected:**
- ack_details has 1 batch with 3 entries (GAPs extracted separately due to non-contiguous)
- Inflight map retains ACQUIRED at 101, 103
- `rkshare_unacked_cnt == 2`

---

### Test B.9: Multiple batch entries within same partition
**Significance:** Each entry in batches should be processed independently

**Setup:**
- Partition 0:
  - Entry 1: offsets 100-102, types `[ACCEPT x3]`
  - Entry 2: offsets 200-202, types `[REJECT x3]`

**Expected:**
- ack_details: 2 collated entries (100-102 ACCEPT, 200-202 REJECT)
- Partition removed from map (all entries extracted)

---

### Test B.10: Interleaved ACQUIRED with ack types
**Significance:** Complex pattern where ACQUIRED breaks up extractable ranges

**Setup:**
- Partition 0: offsets 100-109, types `[ACCEPT, ACQUIRED, REJECT, REJECT, ACQUIRED, RELEASE, RELEASE, ACQUIRED, ACCEPT, ACCEPT]`

**Expected:**
- ack_details: 5 collated entries (offset 100 ACCEPT, 102-103 REJECT, 105-106 RELEASE, 108-109 ACCEPT)
- Map retains ACQUIRED at 101, 104, 107
- `rkshare_unacked_cnt == 3`

---

## Group C: Queue Serving + Ack Mapping

**Functions:** `rd_kafka_q_serve_share_rkmessages()` (internally calls `rd_kafka_share_build_inflight_acks_map()`)

**Purpose:** Tests the application-thread flow of dequeuing ops and returning messages, including verification that ack mapping is correctly built.

### Test C.1: Single SHARE_FETCH_RESPONSE - messages and ack mapping

**Significance:** Validates that all messages come out together AND that `rd_kafka_share_build_inflight_acks_map()` correctly transfers inflight_acks to rkshare's map.

**Queue Structure:**
```
[SHARE_FETCH_RESPONSE(5 messages, 5 ACQUIRED types)]
```

**Action:**
- Call `rd_kafka_q_serve_share_rkmessages()` once

**Expected:**
- Returns NULL (no error)
- `out_size == 5`
- All 5 messages in rkmessages array
- **Ack mapping verification:**
  - `rkshare->rkshare_inflight_acks` has 1 entry
  - `rkshare->rkshare_unacked_cnt == 5`

---

### Test C.2: Single Error op

**Significance:** Validates that error ops are correctly surfaced to the application.

**Queue Structure:**
```
[CONSUMER_ERR op]
```

**Action:**
- Call `rd_kafka_q_serve_share_rkmessages()` once

**Expected:**
- Returns error (non-NULL)
- `out_size == 0`
- Queue is now empty

---

### Test C.3: Error op followed by SHARE_FETCH_RESPONSE

**Significance:** Validates FIFO ordering - error comes first, then messages on next call. Also verifies ack mapping only happens for the fetch response.

**Queue Structure:**
```
[CONSUMER_ERR op] -> [SHARE_FETCH_RESPONSE(3 messages)]
```

**Action & Expected:**

| Call | Returns | out_size | rkshare_unacked_cnt |
|------|---------|----------|---------------------|
| 1 | error | 0 | 0 (no ack mapping yet) |
| 2 | NULL | 3 | 3 (ack mapping done) |

---

### Test C.4: SHARE_FETCH_RESPONSE followed by Error op

**Significance:** Validates messages are returned before errors when they come first in queue.

**Queue Structure:**
```
[SHARE_FETCH_RESPONSE(4 messages)] -> [CONSUMER_ERR op]
```

**Action & Expected:**

| Call | Returns | out_size | rkshare_unacked_cnt |
|------|---------|----------|---------------------|
| 1 | NULL | 4 | 4 |
| 2 | error | 0 | 4 (unchanged) |

---

### Test C.5: Multiple SHARE_FETCH_RESPONSE ops - cumulative ack mapping

**Significance:** Validates that multiple fetch responses accumulate in the ack mapping (multiple partitions or sequential fetches).

**Queue Structure:**
```
[SHARE_FETCH_RESPONSE(3 msgs, partition 0)] -> [SHARE_FETCH_RESPONSE(2 msgs, partition 1)]
```

**Action & Expected:**

| Call | Returns | out_size | rkshare_unacked_cnt | Map entries |
|------|---------|----------|---------------------|-------------|
| 1 | NULL | 3 | 3 | 1 (partition 0) |
| 2 | NULL | 2 | 5 | 2 (partition 0 + 1) |

---

### Test C.6: Ack mapping counts only ACQUIRED (not GAPs)

**Significance:** Validates that GAPs in inflight_acks don't contribute to unacked_cnt.

**Queue Structure:**
```
[SHARE_FETCH_RESPONSE with inflight_acks: [ACQUIRED, GAP, ACQUIRED, GAP, ACQUIRED]]
```
(5 types total, but only 3 are ACQUIRED)

**Action:**
- Call `rd_kafka_q_serve_share_rkmessages()`

**Expected:**
- `rkshare->rkshare_unacked_cnt == 3` (not 5)

---

### Test C.7: Empty queue

**Significance:** Validates graceful handling of empty queue.

**Queue Structure:**
```
[empty]
```

**Action:**
- Call with `timeout_ms = 0`

**Expected:**
- Returns NULL
- `out_size == 0`

---

### Test C.8: SHARE_FETCH_RESPONSE with zero messages

**Significance:** Validates edge case where response has no messages but still has inflight_acks (all GAPs).

**Queue Structure:**
```
[SHARE_FETCH_RESPONSE(0 messages, inflight_acks with all GAPs)]
```

**Action:**
- Call `rd_kafka_q_serve_share_rkmessages()`

**Expected:**
- Returns NULL
- `out_size == 0`
- `rkshare_unacked_cnt == 0` (no ACQUIRED types)

---

## Summary

| Group | Functions | Test Count |
|-------|-----------|------------|
| A | `filter_msg_from_acq_records` + `build_response_rko` | 7 |
| B | `build_ack_details` | 10 |
| C | `q_serve_share_rkmessages` + `build_ack_mapping` | 8 |
| **Total** | | **25** |
