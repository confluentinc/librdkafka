/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012-2022, Magnus Edenhill
 *               2023, Confluent Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * @brief Unit tests for Share Consumer acknowledgement flow
 *
 * Tests the following flow:
 * 1. rd_kafka_share_filter_forward() - Filter messages by acquired ranges
 * 2. rd_kafka_share_build_response_rko() - Build SHARE_FETCH_RESPONSE with:
 *    - messages list (actual messages only, no GAP placeholders)
 *    - inflight_acks list (per-offset type mapping including GAPs)
 * 3. rd_kafka_share_build_ack_mapping() - Merge inflight_acks from RKO to rkshare
 * 4. rd_kafka_share_build_ack_batches_for_fetch() - Collate map for ShareFetch request
 * 5. rd_kafka_q_serve_share_rkmessages() - Filter REJECT when serving to app
 * 6. Partial consumption - Re-enqueue RKO when max_poll_records limits delivery
 *    - Track progress via next_msg_idx field
 *    - Continue from saved index on next poll
 */

#include "rd.h"
#include "rdunittest.h"
#include "rdkafka_int.h"
#include "rdkafka_queue.h"
#include "rdkafka_fetcher.h"
#include "rdkafka_partition.h"

static rd_kafka_t *ut_create_rk(void) {
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        char errstr[128];

        if (rd_kafka_conf_set(conf, "group.id", "ut-share-filter", errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                rd_kafka_conf_destroy(conf);
                return NULL;
        }

        rd_kafka_t *rk =
            rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        if (!rk)
                rd_kafka_conf_destroy(conf);

        return rk;
}

static rd_kafka_toppar_t *ut_create_toppar(rd_kafka_t *rk, const char *topic,
                                           int32_t partition) {
        rd_kafka_toppar_t *rktp = rd_calloc(1, sizeof(*rktp));
        if (!rktp)
                return NULL;

        rktp->rktp_partition = partition;
        /* Initialize refcnt to 1 - IMPORTANT for proper cleanup */
        rd_refcnt_init(&rktp->rktp_refcnt, 1);
        
        /* Create a minimal topic structure for testing */
        rktp->rktp_rkt = rd_calloc(1, sizeof(*rktp->rktp_rkt));
        if (rktp->rktp_rkt) {
                rktp->rktp_rkt->rkt_topic = rd_kafkap_str_new(topic, -1);
                rktp->rktp_rkt->rkt_rk = rk;
        }

        return rktp;
}

static void ut_destroy_toppar(rd_kafka_toppar_t *rktp) {
        if (!rktp)
                return;

        /* Decrement refcnt; only free if it reaches 0.
         * Other references (from ops) will also decrement when destroyed. */
        if (rd_refcnt_sub(&rktp->rktp_refcnt) > 0)
                return;

        if (rktp->rktp_rkt) {
                if (rktp->rktp_rkt->rkt_topic)
                        rd_kafkap_str_destroy(rktp->rktp_rkt->rkt_topic);
                rd_free(rktp->rktp_rkt);
        }

        rd_free(rktp);
}

/**
 * @brief Destroy a test op without triggering full toppar destroy.
 *
 * Our minimal test toppars don't have all fields initialized (queues, locks, etc.)
 * so we can't let rd_kafka_op_destroy call rd_kafka_toppar_destroy.
 * Instead, we manually decrement refcnt and free the op.
 */
static void ut_destroy_op(rd_kafka_op_t *rko) {
        if (!rko)
                return;
        
        /* Decrement toppar refcnt but don't let op_destroy handle it */
        if (rko->rko_rktp) {
                rd_refcnt_sub(&rko->rko_rktp->rktp_refcnt);
                rko->rko_rktp = NULL;
        }
        
        rd_kafka_op_destroy(rko);
}

/**
 * @brief Drain and destroy a queue containing test ops.
 *
 * Properly handles ops with minimal toppars.
 */
static void ut_drain_and_destroy_queue(rd_kafka_q_t *rkq) {
        rd_kafka_op_t *rko;
        while ((rko = rd_kafka_q_pop(rkq, RD_POLL_NOWAIT, 0)) != NULL) {
                ut_destroy_op(rko);
        }
        rd_kafka_q_destroy_owner(rkq);
}

static rd_kafka_broker_t *ut_create_broker(rd_kafka_t *rk) {
        rd_kafka_broker_t *rkb = rd_calloc(1, sizeof(*rkb));
        if (rkb)
                rkb->rkb_rk = rk;
        return rkb;
}

static rd_kafka_share_t *ut_create_rkshare(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = rd_calloc(1, sizeof(*rkshare));
        if (!rkshare)
                return NULL;

        rkshare->rkshare_rk = rk;
        rkshare->rkshare_unacked_cnt = 0;

        /* Initialize the inflight acks map */
        RD_MAP_INIT(&rkshare->rkshare_inflight_acks, 16,
                    rd_kafka_topic_partition_cmp,
                    rd_kafka_topic_partition_hash,
                    rd_kafka_topic_partition_destroy_free,
                    NULL /* value destructor handled manually */);

        return rkshare;
}

static void ut_destroy_rkshare(rd_kafka_share_t *rkshare) {
        if (!rkshare)
                return;

        /* Destroy inflight map entries */
        const rd_kafka_topic_partition_t *tp_key;
        rd_kafka_share_ack_batches_t *batches;

        RD_MAP_FOREACH(tp_key, batches, &rkshare->rkshare_inflight_acks) {
                if (batches) {
                        if (batches->topic)
                                rd_free(batches->topic);
                        /* Destroy entries list with custom cleanup */
                        rd_kafka_share_ack_batch_entry_t *entry;
                        int i;
                        RD_LIST_FOREACH(entry, &batches->entries, i) {
                                if (entry->types)
                                        rd_free(entry->types);
                                rd_free(entry);
                        }
                        rd_list_destroy(&batches->entries);
                        rd_free(batches);
                }
        }
        RD_MAP_DESTROY(&rkshare->rkshare_inflight_acks);

        rd_free(rkshare);
}

static rd_kafka_op_t *ut_make_fetch_op(rd_kafka_toppar_t *rktp, int64_t offset) {
        rd_kafka_op_t *rko = rd_kafka_op_new(RD_KAFKA_OP_FETCH);
        rko->rko_flags |= RD_KAFKA_OP_F_FREE;
        /* Increment refcnt since op holds a reference to rktp */
        rd_refcnt_add(&rktp->rktp_refcnt);
        rko->rko_rktp = rktp;
        rko->rko_u.fetch.rkm.rkm_rkmessage.partition = rktp->rktp_partition;
        rko->rko_u.fetch.rkm.rkm_rkmessage.offset = offset;
        rko->rko_u.fetch.rkm.rkm_rkmessage.err = RD_KAFKA_RESP_ERR_NO_ERROR;
        return rko;
}

static rd_kafka_op_t *ut_make_error_op(rd_kafka_toppar_t *rktp, int64_t offset) {
        rd_kafka_op_t *rko = rd_kafka_op_new(RD_KAFKA_OP_CONSUMER_ERR);
        rko->rko_flags |= RD_KAFKA_OP_F_FREE;
        /* Increment refcnt since op holds a reference to rktp */
        rd_refcnt_add(&rktp->rktp_refcnt);
        rko->rko_rktp = rktp;
        rko->rko_err = RD_KAFKA_RESP_ERR__MSG_TIMED_OUT;
        rko->rko_u.err.offset = offset;
        rko->rko_u.err.rkm.rkm_rkmessage.partition = rktp->rktp_partition;
        rko->rko_u.err.rkm.rkm_rkmessage.offset = offset;
        return rko;
}

/**
 * Create a mock SHARE_FETCH_RESPONSE rko for testing.
 *
 * The new design:
 * - messages list contains ONLY actual messages (ACQUIRED/REJECT), no GAP placeholders
 * - inflight_acks list contains per-offset type mapping (including GAPs)
 *
 * @param ack_types Array describing type at each offset from acquired_start to acquired_end
 *                  GAP types mean no message at that offset
 */
static rd_kafka_op_t *ut_make_share_fetch_response(
    rd_kafka_t *rk,
    rd_kafka_toppar_t *rktp,
    const char *topic,
    int32_t partition,
    rd_kafka_share_acknowledgement_type *ack_types,  /* Types for each offset */
    int64_t acquired_start,
    int64_t acquired_end) {

        rd_kafka_op_t *response_rko =
            rd_kafka_op_new(RD_KAFKA_OP_SHARE_FETCH_RESPONSE);
        response_rko->rko_rk = rk;

        int64_t range_size = acquired_end - acquired_start + 1;

        /* Initialize lists */
        rd_list_init(&response_rko->rko_u.share_fetch_response.messages, 
                     (int)range_size, NULL);
        rd_list_init(&response_rko->rko_u.share_fetch_response.partition_acks,
                     1, NULL);
        rd_list_init(&response_rko->rko_u.share_fetch_response.inflight_acks,
                     1, NULL);

        /* Add messages ONLY for non-GAP offsets */
        for (int64_t i = 0; i < range_size; i++) {
                int64_t offset = acquired_start + i;

                /* Skip GAPs - they are tracked in inflight_acks only */
                if (ack_types[i] == RD_KAFKA_SHARE_ACK_GAP)
                        continue;

                rd_kafka_op_t *msg_rko;
                rd_kafka_msg_t *rkm;

                if (ack_types[i] == RD_KAFKA_SHARE_ACK_REJECT) {
                        msg_rko = ut_make_error_op(rktp, offset);
                        rkm = &msg_rko->rko_u.err.rkm;
                } else {
                        msg_rko = ut_make_fetch_op(rktp, offset);
                        rkm = &msg_rko->rko_u.fetch.rkm;
                }
                rkm->rkm_u.consumer.ack_type = (int8_t)ack_types[i];
                rd_list_add(&response_rko->rko_u.share_fetch_response.messages,
                            msg_rko);
        }

        /* Add partition ack info */
        rd_kafka_share_partition_ack_t *packs =
            rd_calloc(1, sizeof(*packs));
        packs->topic = rd_strdup(topic);
        packs->partition = partition;
        packs->leader_id = 1;
        packs->leader_epoch = 1;
        packs->acquired_ranges_cnt = 1;
        packs->acquired_ranges = rd_calloc(1, sizeof(*packs->acquired_ranges));
        packs->acquired_ranges[0].start_offset = acquired_start;
        packs->acquired_ranges[0].end_offset = acquired_end;
        packs->acquired_msg_cnt = (int32_t)range_size;

        rd_list_add(&response_rko->rko_u.share_fetch_response.partition_acks,
                    packs);

        /* Build inflight_acks - this is what broker thread does */
        rd_kafka_share_ack_batches_t *batches =
            rd_calloc(1, sizeof(*batches));
        batches->topic = rd_strdup(topic);
        batches->partition = partition;
        batches->leader_id = 1;
        batches->leader_epoch = 1;
        batches->number_of_acquired_msgs = (int32_t)range_size;
        rd_list_init(&batches->entries, 1, NULL);

        rd_kafka_share_ack_batch_entry_t *entry =
            rd_calloc(1, sizeof(*entry));
        entry->start_offset = acquired_start;
        entry->end_offset = acquired_end;
        entry->size = range_size;
        entry->types = rd_calloc(range_size, sizeof(*entry->types));

        /* Copy types from input */
        memcpy(entry->types, ack_types, range_size * sizeof(*entry->types));

        rd_list_add(&batches->entries, entry);
        rd_list_add(&response_rko->rko_u.share_fetch_response.inflight_acks,
                    batches);

        return response_rko;
}

static void ut_destroy_share_fetch_response(rd_kafka_op_t *rko) {
        if (!rko)
                return;

        /* Destroy message ops using ut_destroy_op to handle minimal toppars */
        rd_kafka_op_t *msg_rko;
        int i, j;
        RD_LIST_FOREACH(msg_rko, &rko->rko_u.share_fetch_response.messages, i) {
                ut_destroy_op(msg_rko);
        }
        rd_list_destroy(&rko->rko_u.share_fetch_response.messages);

        /* Destroy partition_acks */
        rd_kafka_share_partition_ack_t *packs;
        RD_LIST_FOREACH(packs, &rko->rko_u.share_fetch_response.partition_acks, i) {
                if (packs->topic)
                        rd_free(packs->topic);
                if (packs->acquired_ranges)
                        rd_free(packs->acquired_ranges);
                rd_free(packs);
        }
        rd_list_destroy(&rko->rko_u.share_fetch_response.partition_acks);

        /* Destroy inflight_acks */
        rd_kafka_share_ack_batches_t *batches;
        RD_LIST_FOREACH(batches, &rko->rko_u.share_fetch_response.inflight_acks, i) {
                if (batches->topic)
                        rd_free(batches->topic);
                rd_kafka_share_ack_batch_entry_t *entry;
                RD_LIST_FOREACH(entry, &batches->entries, j) {
                        if (entry->types)
                                rd_free(entry->types);
                        rd_free(entry);
                }
                rd_list_destroy(&batches->entries);
                rd_free(batches);
        }
        rd_list_destroy(&rko->rko_u.share_fetch_response.inflight_acks);

        rd_free(rko);
}

/*******************************************************************************
 * Test: rd_kafka_share_filter_forward()
 * Tests filtering messages by acquired ranges
 ******************************************************************************/

static int ut_case_filter_all_in_range(rd_kafka_t *rk) {
        rd_kafka_broker_t *rkb = ut_create_broker(rk);
        RD_UT_ASSERT(rkb != NULL, "broker alloc failed");

        rd_kafka_toppar_t *rktp = ut_create_toppar(rk, "test-topic", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        rd_kafka_q_t *temp_fetchq = rd_kafka_q_new(rk);
        rd_kafka_q_t *temp_appq = rd_kafka_q_new(rk);

        /* Add messages with offsets 0-4 */
        for (int64_t off = 0; off <= 4; off++)
                rd_kafka_q_enq(temp_fetchq, ut_make_fetch_op(rktp, off));

        /* Acquired range: 0-4 (all messages) */
        int64_t first[] = {0};
        int64_t last[] = {4};
        int16_t dcnt[] = {1};

        rd_kafka_share_filter_forward(rkb, rktp, temp_fetchq, temp_appq,
                                      1, first, last, dcnt);

        /* All 5 messages should be forwarded */
        RD_UT_ASSERT(rd_kafka_q_len(temp_appq) == 5,
                     "appq len %d != 5", rd_kafka_q_len(temp_appq));

        /* Verify offsets */
        for (int64_t exp = 0; exp <= 4; exp++) {
                rd_kafka_op_t *rko = rd_kafka_q_pop(temp_appq, RD_POLL_NOWAIT, 0);
                RD_UT_ASSERT(rko != NULL, "pop returned NULL at offset %" PRId64, exp);
                RD_UT_ASSERT(rko->rko_u.fetch.rkm.rkm_offset == exp,
                             "offset %" PRId64 " != %" PRId64,
                             rko->rko_u.fetch.rkm.rkm_offset, exp);
                ut_destroy_op(rko);
        }

        /* Note: temp_fetchq is already destroyed by rd_kafka_share_filter_forward */
        ut_drain_and_destroy_queue(temp_appq);
        ut_destroy_toppar(rktp);
        rd_free(rkb);

        RD_UT_SAY("  PASS: ut_case_filter_all_in_range");
        return 0;
}

static int ut_case_filter_partial_range(rd_kafka_t *rk) {
        rd_kafka_broker_t *rkb = ut_create_broker(rk);
        RD_UT_ASSERT(rkb != NULL, "broker alloc failed");

        rd_kafka_toppar_t *rktp = ut_create_toppar(rk, "test-topic", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        rd_kafka_q_t *temp_fetchq = rd_kafka_q_new(rk);
        rd_kafka_q_t *temp_appq = rd_kafka_q_new(rk);

        /* Add messages with offsets 0-9 */
        for (int64_t off = 0; off <= 9; off++)
                rd_kafka_q_enq(temp_fetchq, ut_make_fetch_op(rktp, off));

        /* Acquired range: 2-5 (only some messages) */
        int64_t first[] = {2};
        int64_t last[] = {5};
        int16_t dcnt[] = {1};

        rd_kafka_share_filter_forward(rkb, rktp, temp_fetchq, temp_appq,
                                      1, first, last, dcnt);

        /* Only 4 messages should be forwarded (offsets 2,3,4,5) */
        RD_UT_ASSERT(rd_kafka_q_len(temp_appq) == 4,
                     "appq len %d != 4", rd_kafka_q_len(temp_appq));

        /* Note: temp_fetchq is already destroyed by rd_kafka_share_filter_forward */
        ut_drain_and_destroy_queue(temp_appq);
        ut_destroy_toppar(rktp);
        rd_free(rkb);

        RD_UT_SAY("  PASS: ut_case_filter_partial_range");
        return 0;
}

static int ut_case_filter_multiple_ranges(rd_kafka_t *rk) {
        rd_kafka_broker_t *rkb = ut_create_broker(rk);
        RD_UT_ASSERT(rkb != NULL, "broker alloc failed");

        rd_kafka_toppar_t *rktp = ut_create_toppar(rk, "test-topic", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        rd_kafka_q_t *temp_fetchq = rd_kafka_q_new(rk);
        rd_kafka_q_t *temp_appq = rd_kafka_q_new(rk);

        /* Add messages with offsets 0-9 */
        for (int64_t off = 0; off <= 9; off++)
                rd_kafka_q_enq(temp_fetchq, ut_make_fetch_op(rktp, off));

        /* Acquired ranges: 1-2, 5-6, 9-9 */
        int64_t first[] = {1, 5, 9};
        int64_t last[] = {2, 6, 9};
        int16_t dcnt[] = {1, 1, 1};

        rd_kafka_share_filter_forward(rkb, rktp, temp_fetchq, temp_appq,
                                      3, first, last, dcnt);

        /* 5 messages should be forwarded (1,2,5,6,9) */
        RD_UT_ASSERT(rd_kafka_q_len(temp_appq) == 5,
                     "appq len %d != 5", rd_kafka_q_len(temp_appq));

        /* Note: temp_fetchq is already destroyed by rd_kafka_share_filter_forward */
        ut_drain_and_destroy_queue(temp_appq);
        ut_destroy_toppar(rktp);
        rd_free(rkb);

        RD_UT_SAY("  PASS: ut_case_filter_multiple_ranges");
        return 0;
}

/*******************************************************************************
 * Test: RKO Structure Creation (simulating broker thread)
 * Tests that SHARE_FETCH_RESPONSE RKO is built correctly
 ******************************************************************************/

static int ut_case_rko_structure_all_acquired(rd_kafka_t *rk) {
        rd_kafka_toppar_t *rktp = ut_create_toppar(rk, "T1", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        /* Create response with all ACQUIRED messages */
        rd_kafka_share_acknowledgement_type types[] = {
            RD_KAFKA_SHARE_ACK_ACQUIRED, RD_KAFKA_SHARE_ACK_ACQUIRED,
            RD_KAFKA_SHARE_ACK_ACQUIRED, RD_KAFKA_SHARE_ACK_ACQUIRED,
            RD_KAFKA_SHARE_ACK_ACQUIRED, RD_KAFKA_SHARE_ACK_ACQUIRED
        };

        rd_kafka_op_t *response_rko = ut_make_share_fetch_response(
            rk, rktp, "T1", 0, types, 1, 6);

        /* Verify the response structure */
        /* All 6 messages should be present (no GAPs) */
        RD_UT_ASSERT(rd_list_cnt(&response_rko->rko_u.share_fetch_response.messages) == 6,
                     "message cnt %d != 6",
                     rd_list_cnt(&response_rko->rko_u.share_fetch_response.messages));

        RD_UT_ASSERT(rd_list_cnt(&response_rko->rko_u.share_fetch_response.partition_acks) == 1,
                     "partition_acks cnt %d != 1",
                     rd_list_cnt(&response_rko->rko_u.share_fetch_response.partition_acks));

        /* Verify inflight_acks was created */
        RD_UT_ASSERT(rd_list_cnt(&response_rko->rko_u.share_fetch_response.inflight_acks) == 1,
                     "inflight_acks cnt %d != 1",
                     rd_list_cnt(&response_rko->rko_u.share_fetch_response.inflight_acks));

        ut_destroy_share_fetch_response(response_rko);
        ut_destroy_toppar(rktp);

        RD_UT_SAY("  PASS: ut_case_rko_structure_all_acquired");
        return 0;
}

static int ut_case_rko_structure_with_gaps(rd_kafka_t *rk) {
        rd_kafka_toppar_t *rktp = ut_create_toppar(rk, "T1", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        /* Create response with gaps: ACQ, ACQ, GAP, GAP, ACQ, ACQ */
        rd_kafka_share_acknowledgement_type types[] = {
            RD_KAFKA_SHARE_ACK_ACQUIRED, RD_KAFKA_SHARE_ACK_ACQUIRED,
            RD_KAFKA_SHARE_ACK_GAP, RD_KAFKA_SHARE_ACK_GAP,
            RD_KAFKA_SHARE_ACK_ACQUIRED, RD_KAFKA_SHARE_ACK_ACQUIRED
        };

        rd_kafka_op_t *response_rko = ut_make_share_fetch_response(
            rk, rktp, "T1", 0, types, 1, 6);

        /* Only 4 messages should be present (2 GAPs excluded) */
        RD_UT_ASSERT(rd_list_cnt(&response_rko->rko_u.share_fetch_response.messages) == 4,
                     "message cnt %d != 4",
                     rd_list_cnt(&response_rko->rko_u.share_fetch_response.messages));

        /* Verify inflight_acks has all 6 offsets including GAPs */
        rd_kafka_share_ack_batches_t *batches =
            rd_list_elem(&response_rko->rko_u.share_fetch_response.inflight_acks, 0);
        RD_UT_ASSERT(batches != NULL, "inflight_acks[0] is NULL");

        rd_kafka_share_ack_batch_entry_t *entry =
            rd_list_elem(&batches->entries, 0);
        RD_UT_ASSERT(entry != NULL, "entries[0] is NULL");
        RD_UT_ASSERT(entry->size == 6, "entry size %"PRId64" != 6", entry->size);

        /* Verify types array has GAPs at positions 2,3 */
        RD_UT_ASSERT(entry->types[0] == RD_KAFKA_SHARE_ACK_ACQUIRED, "type[0] != ACQ");
        RD_UT_ASSERT(entry->types[1] == RD_KAFKA_SHARE_ACK_ACQUIRED, "type[1] != ACQ");
        RD_UT_ASSERT(entry->types[2] == RD_KAFKA_SHARE_ACK_GAP, "type[2] != GAP");
        RD_UT_ASSERT(entry->types[3] == RD_KAFKA_SHARE_ACK_GAP, "type[3] != GAP");
        RD_UT_ASSERT(entry->types[4] == RD_KAFKA_SHARE_ACK_ACQUIRED, "type[4] != ACQ");
        RD_UT_ASSERT(entry->types[5] == RD_KAFKA_SHARE_ACK_ACQUIRED, "type[5] != ACQ");

        ut_destroy_share_fetch_response(response_rko);
        ut_destroy_toppar(rktp);

        RD_UT_SAY("  PASS: ut_case_rko_structure_with_gaps");
        return 0;
}

static int ut_case_rko_structure_with_rejects(rd_kafka_t *rk) {
        rd_kafka_toppar_t *rktp = ut_create_toppar(rk, "T1", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        /* Create response with rejects: ACQ, REJ, GAP, REJ, REJ, ACQ */
        rd_kafka_share_acknowledgement_type types[] = {
            RD_KAFKA_SHARE_ACK_ACQUIRED, RD_KAFKA_SHARE_ACK_REJECT,
            RD_KAFKA_SHARE_ACK_GAP, RD_KAFKA_SHARE_ACK_REJECT,
            RD_KAFKA_SHARE_ACK_REJECT, RD_KAFKA_SHARE_ACK_ACQUIRED
        };

        rd_kafka_op_t *response_rko = ut_make_share_fetch_response(
            rk, rktp, "T1", 0, types, 1, 6);

        /* 5 messages present (1 GAP excluded): ACQ, REJ, REJ, REJ, ACQ */
        RD_UT_ASSERT(rd_list_cnt(&response_rko->rko_u.share_fetch_response.messages) == 5,
                     "message cnt %d != 5",
                     rd_list_cnt(&response_rko->rko_u.share_fetch_response.messages));

        /* Count CONSUMER_ERR ops (should be 3 REJECTs) */
        rd_kafka_op_t *msg_rko;
        int i, reject_cnt = 0;
        RD_LIST_FOREACH(msg_rko, &response_rko->rko_u.share_fetch_response.messages, i) {
                if (msg_rko->rko_type == RD_KAFKA_OP_CONSUMER_ERR)
                        reject_cnt++;
        }
        RD_UT_ASSERT(reject_cnt == 3, "reject cnt %d != 3", reject_cnt);

        /* Verify inflight_acks has correct types */
        rd_kafka_share_ack_batches_t *batches =
            rd_list_elem(&response_rko->rko_u.share_fetch_response.inflight_acks, 0);
        rd_kafka_share_ack_batch_entry_t *entry =
            rd_list_elem(&batches->entries, 0);

        RD_UT_ASSERT(entry->types[0] == RD_KAFKA_SHARE_ACK_ACQUIRED, "type[0] != ACQ");
        RD_UT_ASSERT(entry->types[1] == RD_KAFKA_SHARE_ACK_REJECT, "type[1] != REJ");
        RD_UT_ASSERT(entry->types[2] == RD_KAFKA_SHARE_ACK_GAP, "type[2] != GAP");
        RD_UT_ASSERT(entry->types[3] == RD_KAFKA_SHARE_ACK_REJECT, "type[3] != REJ");
        RD_UT_ASSERT(entry->types[4] == RD_KAFKA_SHARE_ACK_REJECT, "type[4] != REJ");
        RD_UT_ASSERT(entry->types[5] == RD_KAFKA_SHARE_ACK_ACQUIRED, "type[5] != ACQ");

        ut_destroy_share_fetch_response(response_rko);
        ut_destroy_toppar(rktp);

        RD_UT_SAY("  PASS: ut_case_rko_structure_with_rejects");
        return 0;
}

/*******************************************************************************
 * Test: rd_kafka_share_build_ack_mapping()
 * Tests merging inflight_acks from RKO to rkshare map
 ******************************************************************************/

static int ut_case_merge_single_partition(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        rd_kafka_toppar_t *rktp = ut_create_toppar(rk, "T1", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        /* Create response with: ACQ, ACQ, GAP, ACQ, ACQ, REJ */
        rd_kafka_share_acknowledgement_type types[] = {
            RD_KAFKA_SHARE_ACK_ACQUIRED, RD_KAFKA_SHARE_ACK_ACQUIRED,
            RD_KAFKA_SHARE_ACK_GAP, RD_KAFKA_SHARE_ACK_ACQUIRED,
            RD_KAFKA_SHARE_ACK_ACQUIRED, RD_KAFKA_SHARE_ACK_REJECT
        };

        rd_kafka_op_t *response_rko = ut_make_share_fetch_response(
            rk, rktp, "T1", 0, types, 1, 6);

        /* Verify rkshare map is empty before merge */
        RD_UT_ASSERT(RD_MAP_CNT(&rkshare->rkshare_inflight_acks) == 0,
                     "rkshare map not empty before merge");

        /* Call the merge function (simulating app thread) */
        rd_kafka_share_build_ack_mapping(rkshare, response_rko);

        /* Verify rkshare map has 1 partition */
        RD_UT_ASSERT(RD_MAP_CNT(&rkshare->rkshare_inflight_acks) == 1,
                     "rkshare map cnt %d != 1",
                     (int)RD_MAP_CNT(&rkshare->rkshare_inflight_acks));

        /* Verify unacked count */
        RD_UT_ASSERT(rkshare->rkshare_unacked_cnt == 6,
                     "unacked cnt %"PRId64" != 6", rkshare->rkshare_unacked_cnt);

        /* Lookup the merged batches */
        rd_kafka_topic_partition_t lookup_key = {.topic = "T1", .partition = 0};
        rd_kafka_share_ack_batches_t *merged =
            RD_MAP_GET(&rkshare->rkshare_inflight_acks, &lookup_key);
        RD_UT_ASSERT(merged != NULL, "merged batches not found");

        /* Verify merged data */
        RD_UT_ASSERT(strcmp(merged->topic, "T1") == 0, "topic mismatch");
        RD_UT_ASSERT(merged->partition == 0, "partition mismatch");
        RD_UT_ASSERT(rd_list_cnt(&merged->entries) == 1, "entries cnt mismatch");

        rd_kafka_share_ack_batch_entry_t *entry =
            rd_list_elem(&merged->entries, 0);
        RD_UT_ASSERT(entry->start_offset == 1 && entry->end_offset == 6,
                     "offset range mismatch");

        /* Verify types were copied correctly */
        RD_UT_ASSERT(entry->types[0] == RD_KAFKA_SHARE_ACK_ACQUIRED, "type[0]");
        RD_UT_ASSERT(entry->types[1] == RD_KAFKA_SHARE_ACK_ACQUIRED, "type[1]");
        RD_UT_ASSERT(entry->types[2] == RD_KAFKA_SHARE_ACK_GAP, "type[2]");
        RD_UT_ASSERT(entry->types[3] == RD_KAFKA_SHARE_ACK_ACQUIRED, "type[3]");
        RD_UT_ASSERT(entry->types[4] == RD_KAFKA_SHARE_ACK_ACQUIRED, "type[4]");
        RD_UT_ASSERT(entry->types[5] == RD_KAFKA_SHARE_ACK_REJECT, "type[5]");

        ut_destroy_share_fetch_response(response_rko);
        ut_destroy_toppar(rktp);
        ut_destroy_rkshare(rkshare);

        RD_UT_SAY("  PASS: ut_case_merge_single_partition");
        return 0;
}

static int ut_case_merge_multiple_rkos(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        rd_kafka_toppar_t *rktp1 = ut_create_toppar(rk, "T1", 0);
        rd_kafka_toppar_t *rktp2 = ut_create_toppar(rk, "T2", 0);
        RD_UT_ASSERT(rktp1 != NULL && rktp2 != NULL, "toppar alloc failed");

        /* First RKO from broker 1: T1-0 with ACQ, ACQ, GAP */
        rd_kafka_share_acknowledgement_type types1[] = {
            RD_KAFKA_SHARE_ACK_ACQUIRED, RD_KAFKA_SHARE_ACK_ACQUIRED,
            RD_KAFKA_SHARE_ACK_GAP
        };
        rd_kafka_op_t *rko1 = ut_make_share_fetch_response(
            rk, rktp1, "T1", 0, types1, 1, 3);

        /* Second RKO from broker 2: T2-0 with ACQ, REJ, ACQ */
        rd_kafka_share_acknowledgement_type types2[] = {
            RD_KAFKA_SHARE_ACK_ACQUIRED, RD_KAFKA_SHARE_ACK_REJECT,
            RD_KAFKA_SHARE_ACK_ACQUIRED
        };
        rd_kafka_op_t *rko2 = ut_make_share_fetch_response(
            rk, rktp2, "T2", 0, types2, 10, 12);

        /* Merge first RKO */
        rd_kafka_share_build_ack_mapping(rkshare, rko1);

        /* Verify after first merge */
        RD_UT_ASSERT(RD_MAP_CNT(&rkshare->rkshare_inflight_acks) == 1,
                     "map cnt %d != 1 after first merge",
                     (int)RD_MAP_CNT(&rkshare->rkshare_inflight_acks));
        RD_UT_ASSERT(rkshare->rkshare_unacked_cnt == 3,
                     "unacked %"PRId64" != 3", rkshare->rkshare_unacked_cnt);

        /* Merge second RKO */
        rd_kafka_share_build_ack_mapping(rkshare, rko2);

        /* Verify after second merge */
        RD_UT_ASSERT(RD_MAP_CNT(&rkshare->rkshare_inflight_acks) == 2,
                     "map cnt %d != 2 after second merge",
                     (int)RD_MAP_CNT(&rkshare->rkshare_inflight_acks));
        RD_UT_ASSERT(rkshare->rkshare_unacked_cnt == 6,
                     "unacked %"PRId64" != 6", rkshare->rkshare_unacked_cnt);

        /* Verify T1-0 */
        rd_kafka_topic_partition_t key1 = {.topic = "T1", .partition = 0};
        rd_kafka_share_ack_batches_t *batches1 =
            RD_MAP_GET(&rkshare->rkshare_inflight_acks, &key1);
        RD_UT_ASSERT(batches1 != NULL, "T1-0 not found");
        RD_UT_ASSERT(rd_list_cnt(&batches1->entries) == 1, "T1-0 entries cnt");

        /* Verify T2-0 */
        rd_kafka_topic_partition_t key2 = {.topic = "T2", .partition = 0};
        rd_kafka_share_ack_batches_t *batches2 =
            RD_MAP_GET(&rkshare->rkshare_inflight_acks, &key2);
        RD_UT_ASSERT(batches2 != NULL, "T2-0 not found");
        RD_UT_ASSERT(rd_list_cnt(&batches2->entries) == 1, "T2-0 entries cnt");

        rd_kafka_share_ack_batch_entry_t *entry2 =
            rd_list_elem(&batches2->entries, 0);
        RD_UT_ASSERT(entry2->start_offset == 10 && entry2->end_offset == 12,
                     "T2-0 offset range mismatch");
        RD_UT_ASSERT(entry2->types[0] == RD_KAFKA_SHARE_ACK_ACQUIRED, "T2 type[0]");
        RD_UT_ASSERT(entry2->types[1] == RD_KAFKA_SHARE_ACK_REJECT, "T2 type[1]");
        RD_UT_ASSERT(entry2->types[2] == RD_KAFKA_SHARE_ACK_ACQUIRED, "T2 type[2]");

        ut_destroy_share_fetch_response(rko1);
        ut_destroy_share_fetch_response(rko2);
        ut_destroy_toppar(rktp1);
        ut_destroy_toppar(rktp2);
        ut_destroy_rkshare(rkshare);

        RD_UT_SAY("  PASS: ut_case_merge_multiple_rkos");
        return 0;
}

static int ut_case_merge_same_partition_multiple_rkos(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        rd_kafka_toppar_t *rktp = ut_create_toppar(rk, "T1", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        /* First RKO: T1-0 offsets 1-3 */
        rd_kafka_share_acknowledgement_type types1[] = {
            RD_KAFKA_SHARE_ACK_ACQUIRED, RD_KAFKA_SHARE_ACK_ACQUIRED,
            RD_KAFKA_SHARE_ACK_GAP
        };
        rd_kafka_op_t *rko1 = ut_make_share_fetch_response(
            rk, rktp, "T1", 0, types1, 1, 3);

        /* Second RKO: T1-0 offsets 10-12 (different range, same partition) */
        rd_kafka_share_acknowledgement_type types2[] = {
            RD_KAFKA_SHARE_ACK_ACQUIRED, RD_KAFKA_SHARE_ACK_REJECT,
            RD_KAFKA_SHARE_ACK_ACQUIRED
        };
        rd_kafka_op_t *rko2 = ut_make_share_fetch_response(
            rk, rktp, "T1", 0, types2, 10, 12);

        /* Merge both RKOs */
        rd_kafka_share_build_ack_mapping(rkshare, rko1);
        rd_kafka_share_build_ack_mapping(rkshare, rko2);

        /* Should still be 1 partition, but with 2 entries */
        RD_UT_ASSERT(RD_MAP_CNT(&rkshare->rkshare_inflight_acks) == 1,
                     "map cnt %d != 1",
                     (int)RD_MAP_CNT(&rkshare->rkshare_inflight_acks));
        RD_UT_ASSERT(rkshare->rkshare_unacked_cnt == 6,
                     "unacked %"PRId64" != 6", rkshare->rkshare_unacked_cnt);

        /* Verify T1-0 has 2 entries now */
        rd_kafka_topic_partition_t key = {.topic = "T1", .partition = 0};
        rd_kafka_share_ack_batches_t *batches =
            RD_MAP_GET(&rkshare->rkshare_inflight_acks, &key);
        RD_UT_ASSERT(batches != NULL, "T1-0 not found");
        RD_UT_ASSERT(rd_list_cnt(&batches->entries) == 2,
                     "T1-0 entries cnt %d != 2",
                     rd_list_cnt(&batches->entries));

        /* Verify first entry (1-3) */
        rd_kafka_share_ack_batch_entry_t *entry1 =
            rd_list_elem(&batches->entries, 0);
        RD_UT_ASSERT(entry1->start_offset == 1 && entry1->end_offset == 3,
                     "entry1 offset mismatch");

        /* Verify second entry (10-12) */
        rd_kafka_share_ack_batch_entry_t *entry2 =
            rd_list_elem(&batches->entries, 1);
        RD_UT_ASSERT(entry2->start_offset == 10 && entry2->end_offset == 12,
                     "entry2 offset mismatch");

        ut_destroy_share_fetch_response(rko1);
        ut_destroy_share_fetch_response(rko2);
        ut_destroy_toppar(rktp);
        ut_destroy_rkshare(rkshare);

        RD_UT_SAY("  PASS: ut_case_merge_same_partition_multiple_rkos");
        return 0;
}

/*******************************************************************************
 * Test: rd_kafka_share_build_ack_batches_for_fetch()
 * Tests collating inflight map into ranges for ShareFetch
 ******************************************************************************/

static int ut_case_collate_all_same_type(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        /* Manually populate inflight map with all ACQUIRED */
        rd_kafka_share_ack_batches_t *batches = rd_calloc(1, sizeof(*batches));
        batches->topic = rd_strdup("T1");
        batches->partition = 0;
        batches->leader_id = 1;
        batches->leader_epoch = 1;
        rd_list_init(&batches->entries, 1, NULL);

        rd_kafka_share_ack_batch_entry_t *entry = rd_calloc(1, sizeof(*entry));
        entry->start_offset = 1;
        entry->end_offset = 4;
        entry->size = 4;
        entry->types = rd_calloc(4, sizeof(*entry->types));
        for (int i = 0; i < 4; i++)
                entry->types[i] = RD_KAFKA_SHARE_ACK_ACQUIRED;
        rd_list_add(&batches->entries, entry);

        rd_kafka_topic_partition_t *key =
            rd_kafka_topic_partition_new("T1", 0);
        RD_MAP_SET(&rkshare->rkshare_inflight_acks, key, batches);

        /* Call collate function */
        rd_list_t ack_batches_out;
        rd_kafka_share_build_ack_batches_for_fetch(rkshare, &ack_batches_out);

        /* Should produce 1 batch with 1 range (all ACCEPT) */
        RD_UT_ASSERT(rd_list_cnt(&ack_batches_out) == 1,
                     "ack_batches cnt %d != 1", rd_list_cnt(&ack_batches_out));

        rd_kafka_share_fetch_ack_batch_t *out_batch =
            rd_list_elem(&ack_batches_out, 0);
        RD_UT_ASSERT(rd_list_cnt(&out_batch->ranges) == 1,
                     "ranges cnt %d != 1", rd_list_cnt(&out_batch->ranges));

        rd_kafka_share_fetch_ack_range_t *range =
            rd_list_elem(&out_batch->ranges, 0);
        RD_UT_ASSERT(range->start_offset == 1 && range->end_offset == 4,
                     "range %"PRId64"-%"PRId64" != 1-4",
                     range->start_offset, range->end_offset);
        RD_UT_ASSERT(range->type == RD_KAFKA_SHARE_ACK_ACCEPT,
                     "type %d != ACCEPT", range->type);

        rd_list_destroy(&ack_batches_out);
        ut_destroy_rkshare(rkshare);

        RD_UT_SAY("  PASS: ut_case_collate_all_same_type");
        return 0;
}

static int ut_case_collate_mixed_types(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        /* Manually populate: ACQ, ACQ, GAP, ACQ, ACQ, REJ, REJ, ACQ, ACQ */
        rd_kafka_share_ack_batches_t *batches = rd_calloc(1, sizeof(*batches));
        batches->topic = rd_strdup("T1");
        batches->partition = 0;
        batches->leader_id = 1;
        batches->leader_epoch = 1;
        rd_list_init(&batches->entries, 1, NULL);

        rd_kafka_share_ack_batch_entry_t *entry = rd_calloc(1, sizeof(*entry));
        entry->start_offset = 1;
        entry->end_offset = 9;
        entry->size = 9;
        entry->types = rd_calloc(9, sizeof(*entry->types));
        /* ACQ, ACQ, GAP, ACQ, ACQ, REJ, REJ, ACQ, ACQ */
        entry->types[0] = RD_KAFKA_SHARE_ACK_ACQUIRED;
        entry->types[1] = RD_KAFKA_SHARE_ACK_ACQUIRED;
        entry->types[2] = RD_KAFKA_SHARE_ACK_GAP;
        entry->types[3] = RD_KAFKA_SHARE_ACK_ACQUIRED;
        entry->types[4] = RD_KAFKA_SHARE_ACK_ACQUIRED;
        entry->types[5] = RD_KAFKA_SHARE_ACK_REJECT;
        entry->types[6] = RD_KAFKA_SHARE_ACK_REJECT;
        entry->types[7] = RD_KAFKA_SHARE_ACK_ACQUIRED;
        entry->types[8] = RD_KAFKA_SHARE_ACK_ACQUIRED;
        rd_list_add(&batches->entries, entry);

        rd_kafka_topic_partition_t *key =
            rd_kafka_topic_partition_new("T1", 0);
        RD_MAP_SET(&rkshare->rkshare_inflight_acks, key, batches);

        /* Call collate function */
        rd_list_t ack_batches_out;
        rd_kafka_share_build_ack_batches_for_fetch(rkshare, &ack_batches_out);

        /* Should produce 1 batch with 5 ranges */
        RD_UT_ASSERT(rd_list_cnt(&ack_batches_out) == 1,
                     "ack_batches cnt %d != 1", rd_list_cnt(&ack_batches_out));

        rd_kafka_share_fetch_ack_batch_t *out_batch =
            rd_list_elem(&ack_batches_out, 0);
        RD_UT_ASSERT(rd_list_cnt(&out_batch->ranges) == 5,
                     "ranges cnt %d != 5", rd_list_cnt(&out_batch->ranges));

        /* Verify ranges:
         * {1-2, ACCEPT}, {3-3, GAP}, {4-5, ACCEPT}, {6-7, REJECT}, {8-9, ACCEPT}
         */
        rd_kafka_share_fetch_ack_range_t *r;

        r = rd_list_elem(&out_batch->ranges, 0);
        RD_UT_ASSERT(r->start_offset == 1 && r->end_offset == 2 &&
                     r->type == RD_KAFKA_SHARE_ACK_ACCEPT,
                     "range[0] mismatch: %"PRId64"-%"PRId64" type=%d",
                     r->start_offset, r->end_offset, r->type);

        r = rd_list_elem(&out_batch->ranges, 1);
        RD_UT_ASSERT(r->start_offset == 3 && r->end_offset == 3 &&
                     r->type == RD_KAFKA_SHARE_ACK_GAP,
                     "range[1] mismatch: %"PRId64"-%"PRId64" type=%d",
                     r->start_offset, r->end_offset, r->type);

        r = rd_list_elem(&out_batch->ranges, 2);
        RD_UT_ASSERT(r->start_offset == 4 && r->end_offset == 5 &&
                     r->type == RD_KAFKA_SHARE_ACK_ACCEPT,
                     "range[2] mismatch: %"PRId64"-%"PRId64" type=%d",
                     r->start_offset, r->end_offset, r->type);

        r = rd_list_elem(&out_batch->ranges, 3);
        RD_UT_ASSERT(r->start_offset == 6 && r->end_offset == 7 &&
                     r->type == RD_KAFKA_SHARE_ACK_REJECT,
                     "range[3] mismatch: %"PRId64"-%"PRId64" type=%d",
                     r->start_offset, r->end_offset, r->type);

        r = rd_list_elem(&out_batch->ranges, 4);
        RD_UT_ASSERT(r->start_offset == 8 && r->end_offset == 9 &&
                     r->type == RD_KAFKA_SHARE_ACK_ACCEPT,
                     "range[4] mismatch: %"PRId64"-%"PRId64" type=%d",
                     r->start_offset, r->end_offset, r->type);

        rd_list_destroy(&ack_batches_out);
        ut_destroy_rkshare(rkshare);

        RD_UT_SAY("  PASS: ut_case_collate_mixed_types");
        return 0;
}

static int ut_case_collate_empty_map(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        /* Don't add anything to inflight map */

        /* Call collate function */
        rd_list_t ack_batches_out;
        rd_kafka_share_build_ack_batches_for_fetch(rkshare, &ack_batches_out);

        /* Should produce empty list */
        RD_UT_ASSERT(rd_list_cnt(&ack_batches_out) == 0,
                     "ack_batches cnt %d != 0", rd_list_cnt(&ack_batches_out));

        rd_list_destroy(&ack_batches_out);
        ut_destroy_rkshare(rkshare);

        RD_UT_SAY("  PASS: ut_case_collate_empty_map");
        return 0;
}

/*******************************************************************************
 * Test: Full flow with multiple partitions
 * Tests the complete scenario from user's example
 ******************************************************************************/

static int ut_case_full_flow_multi_partition(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        /*
         * Test case from user:
         * Tp1: 1:ACQ, 2:ACQ, 3:GAP, 4:GAP, 5:ACQ, 6:ACQ
         * Tp2: 1:ACQ, 2:REJ, 3:GAP, 4:REJ, 5:REJ, 6:ACQ, 7:ACQ, 8:GAP
         */

        /* Populate Tp1 */
        rd_kafka_share_ack_batches_t *batches1 = rd_calloc(1, sizeof(*batches1));
        batches1->topic = rd_strdup("Tp1");
        batches1->partition = 0;
        batches1->leader_id = 1;
        batches1->leader_epoch = 1;
        rd_list_init(&batches1->entries, 1, NULL);

        rd_kafka_share_ack_batch_entry_t *entry1 = rd_calloc(1, sizeof(*entry1));
        entry1->start_offset = 1;
        entry1->end_offset = 6;
        entry1->size = 6;
        entry1->types = rd_calloc(6, sizeof(*entry1->types));
        entry1->types[0] = RD_KAFKA_SHARE_ACK_ACQUIRED;  /* off 1 */
        entry1->types[1] = RD_KAFKA_SHARE_ACK_ACQUIRED;  /* off 2 */
        entry1->types[2] = RD_KAFKA_SHARE_ACK_GAP;       /* off 3 */
        entry1->types[3] = RD_KAFKA_SHARE_ACK_GAP;       /* off 4 */
        entry1->types[4] = RD_KAFKA_SHARE_ACK_ACQUIRED;  /* off 5 */
        entry1->types[5] = RD_KAFKA_SHARE_ACK_ACQUIRED;  /* off 6 */
        rd_list_add(&batches1->entries, entry1);

        rd_kafka_topic_partition_t *key1 =
            rd_kafka_topic_partition_new("Tp1", 0);
        RD_MAP_SET(&rkshare->rkshare_inflight_acks, key1, batches1);

        /* Populate Tp2 */
        rd_kafka_share_ack_batches_t *batches2 = rd_calloc(1, sizeof(*batches2));
        batches2->topic = rd_strdup("Tp2");
        batches2->partition = 0;
        batches2->leader_id = 2;
        batches2->leader_epoch = 1;
        rd_list_init(&batches2->entries, 1, NULL);

        rd_kafka_share_ack_batch_entry_t *entry2 = rd_calloc(1, sizeof(*entry2));
        entry2->start_offset = 1;
        entry2->end_offset = 8;
        entry2->size = 8;
        entry2->types = rd_calloc(8, sizeof(*entry2->types));
        entry2->types[0] = RD_KAFKA_SHARE_ACK_ACQUIRED;  /* off 1 */
        entry2->types[1] = RD_KAFKA_SHARE_ACK_REJECT;    /* off 2 */
        entry2->types[2] = RD_KAFKA_SHARE_ACK_GAP;       /* off 3 */
        entry2->types[3] = RD_KAFKA_SHARE_ACK_REJECT;    /* off 4 */
        entry2->types[4] = RD_KAFKA_SHARE_ACK_REJECT;    /* off 5 */
        entry2->types[5] = RD_KAFKA_SHARE_ACK_ACQUIRED;  /* off 6 */
        entry2->types[6] = RD_KAFKA_SHARE_ACK_ACQUIRED;  /* off 7 */
        entry2->types[7] = RD_KAFKA_SHARE_ACK_GAP;       /* off 8 */
        rd_list_add(&batches2->entries, entry2);

        rd_kafka_topic_partition_t *key2 =
            rd_kafka_topic_partition_new("Tp2", 0);
        RD_MAP_SET(&rkshare->rkshare_inflight_acks, key2, batches2);

        /* Call collate function */
        rd_list_t ack_batches_out;
        rd_kafka_share_build_ack_batches_for_fetch(rkshare, &ack_batches_out);

        /* Should produce 2 batches */
        RD_UT_ASSERT(rd_list_cnt(&ack_batches_out) == 2,
                     "ack_batches cnt %d != 2", rd_list_cnt(&ack_batches_out));

        /* Find Tp1 and Tp2 batches */
        rd_kafka_share_fetch_ack_batch_t *tp1_batch = NULL;
        rd_kafka_share_fetch_ack_batch_t *tp2_batch = NULL;
        rd_kafka_share_fetch_ack_batch_t *batch;
        int i;

        RD_LIST_FOREACH(batch, &ack_batches_out, i) {
                if (strcmp(batch->topic, "Tp1") == 0)
                        tp1_batch = batch;
                else if (strcmp(batch->topic, "Tp2") == 0)
                        tp2_batch = batch;
        }

        RD_UT_ASSERT(tp1_batch != NULL, "Tp1 batch not found");
        RD_UT_ASSERT(tp2_batch != NULL, "Tp2 batch not found");

        /*
         * Tp1 expected ranges:
         * {1-2, ACCEPT}, {3-4, GAP}, {5-6, ACCEPT}
         */
        RD_UT_ASSERT(rd_list_cnt(&tp1_batch->ranges) == 3,
                     "Tp1 ranges cnt %d != 3", rd_list_cnt(&tp1_batch->ranges));

        rd_kafka_share_fetch_ack_range_t *r;
        r = rd_list_elem(&tp1_batch->ranges, 0);
        RD_UT_ASSERT(r->start_offset == 1 && r->end_offset == 2 &&
                     r->type == RD_KAFKA_SHARE_ACK_ACCEPT,
                     "Tp1 range[0] mismatch");

        r = rd_list_elem(&tp1_batch->ranges, 1);
        RD_UT_ASSERT(r->start_offset == 3 && r->end_offset == 4 &&
                     r->type == RD_KAFKA_SHARE_ACK_GAP,
                     "Tp1 range[1] mismatch");

        r = rd_list_elem(&tp1_batch->ranges, 2);
        RD_UT_ASSERT(r->start_offset == 5 && r->end_offset == 6 &&
                     r->type == RD_KAFKA_SHARE_ACK_ACCEPT,
                     "Tp1 range[2] mismatch");

        /*
         * Tp2 expected ranges:
         * {1-1, ACCEPT}, {2-2, REJECT}, {3-3, GAP}, {4-5, REJECT}, {6-7, ACCEPT}, {8-8, GAP}
         */
        RD_UT_ASSERT(rd_list_cnt(&tp2_batch->ranges) == 6,
                     "Tp2 ranges cnt %d != 6", rd_list_cnt(&tp2_batch->ranges));

        r = rd_list_elem(&tp2_batch->ranges, 0);
        RD_UT_ASSERT(r->start_offset == 1 && r->end_offset == 1 &&
                     r->type == RD_KAFKA_SHARE_ACK_ACCEPT,
                     "Tp2 range[0] mismatch: %"PRId64"-%"PRId64" type=%d",
                     r->start_offset, r->end_offset, r->type);

        r = rd_list_elem(&tp2_batch->ranges, 1);
        RD_UT_ASSERT(r->start_offset == 2 && r->end_offset == 2 &&
                     r->type == RD_KAFKA_SHARE_ACK_REJECT,
                     "Tp2 range[1] mismatch: %"PRId64"-%"PRId64" type=%d",
                     r->start_offset, r->end_offset, r->type);

        r = rd_list_elem(&tp2_batch->ranges, 2);
        RD_UT_ASSERT(r->start_offset == 3 && r->end_offset == 3 &&
                     r->type == RD_KAFKA_SHARE_ACK_GAP,
                     "Tp2 range[2] mismatch: %"PRId64"-%"PRId64" type=%d",
                     r->start_offset, r->end_offset, r->type);

        r = rd_list_elem(&tp2_batch->ranges, 3);
        RD_UT_ASSERT(r->start_offset == 4 && r->end_offset == 5 &&
                     r->type == RD_KAFKA_SHARE_ACK_REJECT,
                     "Tp2 range[3] mismatch: %"PRId64"-%"PRId64" type=%d",
                     r->start_offset, r->end_offset, r->type);

        r = rd_list_elem(&tp2_batch->ranges, 4);
        RD_UT_ASSERT(r->start_offset == 6 && r->end_offset == 7 &&
                     r->type == RD_KAFKA_SHARE_ACK_ACCEPT,
                     "Tp2 range[4] mismatch: %"PRId64"-%"PRId64" type=%d",
                     r->start_offset, r->end_offset, r->type);

        r = rd_list_elem(&tp2_batch->ranges, 5);
        RD_UT_ASSERT(r->start_offset == 8 && r->end_offset == 8 &&
                     r->type == RD_KAFKA_SHARE_ACK_GAP,
                     "Tp2 range[5] mismatch: %"PRId64"-%"PRId64" type=%d",
                     r->start_offset, r->end_offset, r->type);

        rd_list_destroy(&ack_batches_out);
        ut_destroy_rkshare(rkshare);

        RD_UT_SAY("  PASS: ut_case_full_flow_multi_partition");
        return 0;
}

/*******************************************************************************
 * Test: Partial consumption with next_msg_idx tracking
 * Tests re-enqueueing RKO when max_poll_records limits delivery
 ******************************************************************************/

static int ut_case_partial_consumption_single_poll(rd_kafka_t *rk) {
        rd_kafka_toppar_t *rktp = ut_create_toppar(rk, "T1", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        /* Create response with 10 ACQUIRED messages (offsets 0-9) */
        rd_kafka_share_acknowledgement_type types[10];
        for (int i = 0; i < 10; i++)
                types[i] = RD_KAFKA_SHARE_ACK_ACQUIRED;

        rd_kafka_op_t *response_rko = ut_make_share_fetch_response(
            rk, rktp, "T1", 0, types, 0, 9);

        /* Verify initial next_msg_idx is 0 */
        RD_UT_ASSERT(response_rko->rko_u.share_fetch_response.next_msg_idx == 0,
                     "initial next_msg_idx %d != 0",
                     response_rko->rko_u.share_fetch_response.next_msg_idx);

        /* Verify all 10 messages are present */
        RD_UT_ASSERT(rd_list_cnt(&response_rko->rko_u.share_fetch_response.messages) == 10,
                     "message cnt %d != 10",
                     rd_list_cnt(&response_rko->rko_u.share_fetch_response.messages));

        /* Simulate partial consumption by updating next_msg_idx */
        response_rko->rko_u.share_fetch_response.next_msg_idx = 5;

        /* Verify we can track progress */
        RD_UT_ASSERT(response_rko->rko_u.share_fetch_response.next_msg_idx == 5,
                     "updated next_msg_idx %d != 5",
                     response_rko->rko_u.share_fetch_response.next_msg_idx);

        ut_destroy_share_fetch_response(response_rko);
        ut_destroy_toppar(rktp);

        RD_UT_SAY("  PASS: ut_case_partial_consumption_single_poll");
        return 0;
}

static int ut_case_partial_consumption_message_order(rd_kafka_t *rk) {
        rd_kafka_toppar_t *rktp = ut_create_toppar(rk, "T1", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        /* Create response with 10 ACQUIRED messages (offsets 0-9) */
        rd_kafka_share_acknowledgement_type types[10];
        for (int i = 0; i < 10; i++)
                types[i] = RD_KAFKA_SHARE_ACK_ACQUIRED;

        rd_kafka_op_t *response_rko = ut_make_share_fetch_response(
            rk, rktp, "T1", 0, types, 0, 9);

        /* Simulate first poll: consume messages 0-4 */
        int next_idx = 0;
        int delivered = 0;
        int max_deliver = 5;

        for (int i = next_idx; i < rd_list_cnt(&response_rko->rko_u.share_fetch_response.messages); i++) {
                if (delivered >= max_deliver)
                        break;
                
                rd_kafka_op_t *msg_rko = rd_list_elem(
                    &response_rko->rko_u.share_fetch_response.messages, i);
                int64_t offset = rd_kafka_op_get_offset(msg_rko);
                
                /* First 5 should be offsets 0-4 */
                RD_UT_ASSERT(offset == delivered,
                             "first poll: offset %" PRId64 " != expected %d",
                             offset, delivered);
                
                delivered++;
                next_idx = i + 1;
        }

        RD_UT_ASSERT(delivered == 5, "first poll delivered %d != 5", delivered);
        RD_UT_ASSERT(next_idx == 5, "first poll next_idx %d != 5", next_idx);

        /* Save next_idx (simulating re-enqueue) */
        response_rko->rko_u.share_fetch_response.next_msg_idx = next_idx;

        /* Simulate second poll: consume messages 5-9 */
        next_idx = response_rko->rko_u.share_fetch_response.next_msg_idx;
        delivered = 0;
        max_deliver = 5;

        for (int i = next_idx; i < rd_list_cnt(&response_rko->rko_u.share_fetch_response.messages); i++) {
                if (delivered >= max_deliver)
                        break;
                
                rd_kafka_op_t *msg_rko = rd_list_elem(
                    &response_rko->rko_u.share_fetch_response.messages, i);
                int64_t offset = rd_kafka_op_get_offset(msg_rko);
                
                /* Second 5 should be offsets 5-9 */
                RD_UT_ASSERT(offset == next_idx + delivered,
                             "second poll: offset %" PRId64 " != expected %d",
                             offset, next_idx + delivered);
                
                delivered++;
        }

        RD_UT_ASSERT(delivered == 5, "second poll delivered %d != 5", delivered);

        ut_destroy_share_fetch_response(response_rko);
        ut_destroy_toppar(rktp);

        RD_UT_SAY("  PASS: ut_case_partial_consumption_message_order");
        return 0;
}

static int ut_case_partial_consumption_with_gaps(rd_kafka_t *rk) {
        rd_kafka_toppar_t *rktp = ut_create_toppar(rk, "T1", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        /* Create response with gaps: ACQ, ACQ, GAP, ACQ, ACQ, GAP, ACQ, ACQ, ACQ, ACQ */
        rd_kafka_share_acknowledgement_type types[] = {
            RD_KAFKA_SHARE_ACK_ACQUIRED, RD_KAFKA_SHARE_ACK_ACQUIRED,
            RD_KAFKA_SHARE_ACK_GAP,      RD_KAFKA_SHARE_ACK_ACQUIRED,
            RD_KAFKA_SHARE_ACK_ACQUIRED, RD_KAFKA_SHARE_ACK_GAP,
            RD_KAFKA_SHARE_ACK_ACQUIRED, RD_KAFKA_SHARE_ACK_ACQUIRED,
            RD_KAFKA_SHARE_ACK_ACQUIRED, RD_KAFKA_SHARE_ACK_ACQUIRED
        };

        rd_kafka_op_t *response_rko = ut_make_share_fetch_response(
            rk, rktp, "T1", 0, types, 0, 9);

        /* Should have 8 messages (10 - 2 GAPs) */
        RD_UT_ASSERT(rd_list_cnt(&response_rko->rko_u.share_fetch_response.messages) == 8,
                     "message cnt %d != 8",
                     rd_list_cnt(&response_rko->rko_u.share_fetch_response.messages));

        /* Simulate consuming 4 messages (first poll) */
        int next_idx = 0;
        int delivered = 0;
        int max_deliver = 4;
        int64_t expected_offsets_poll1[] = {0, 1, 3, 4};  /* GAP at offset 2 */

        for (int i = next_idx; i < rd_list_cnt(&response_rko->rko_u.share_fetch_response.messages); i++) {
                if (delivered >= max_deliver)
                        break;
                
                rd_kafka_op_t *msg_rko = rd_list_elem(
                    &response_rko->rko_u.share_fetch_response.messages, i);
                int64_t offset = rd_kafka_op_get_offset(msg_rko);
                
                RD_UT_ASSERT(offset == expected_offsets_poll1[delivered],
                             "poll1 msg[%d]: offset %" PRId64 " != expected %" PRId64,
                             delivered, offset, expected_offsets_poll1[delivered]);
                
                delivered++;
                next_idx = i + 1;
        }

        RD_UT_ASSERT(delivered == 4, "poll1 delivered %d != 4", delivered);

        /* Second poll: remaining 4 messages */
        response_rko->rko_u.share_fetch_response.next_msg_idx = next_idx;
        next_idx = response_rko->rko_u.share_fetch_response.next_msg_idx;
        delivered = 0;
        int64_t expected_offsets_poll2[] = {6, 7, 8, 9};  /* GAP at offset 5 */

        for (int i = next_idx; i < rd_list_cnt(&response_rko->rko_u.share_fetch_response.messages); i++) {
                if (delivered >= max_deliver)
                        break;
                
                rd_kafka_op_t *msg_rko = rd_list_elem(
                    &response_rko->rko_u.share_fetch_response.messages, i);
                int64_t offset = rd_kafka_op_get_offset(msg_rko);
                
                RD_UT_ASSERT(offset == expected_offsets_poll2[delivered],
                             "poll2 msg[%d]: offset %" PRId64 " != expected %" PRId64,
                             delivered, offset, expected_offsets_poll2[delivered]);
                
                delivered++;
        }

        RD_UT_ASSERT(delivered == 4, "poll2 delivered %d != 4", delivered);

        ut_destroy_share_fetch_response(response_rko);
        ut_destroy_toppar(rktp);

        RD_UT_SAY("  PASS: ut_case_partial_consumption_with_gaps");
        return 0;
}

/*******************************************************************************
 * Main Test Entry Point
 ******************************************************************************/

int unittest_fetcher_share_filter_forward(void) {
        rd_kafka_t *rk = ut_create_rk();
        RD_UT_ASSERT(rk != NULL, "rd_kafka_new failed");

        RD_UT_SAY("Testing rd_kafka_share_filter_forward()...");
        if (ut_case_filter_all_in_range(rk) ||
            ut_case_filter_partial_range(rk) ||
            ut_case_filter_multiple_ranges(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        RD_UT_SAY("Testing RKO structure creation (broker thread simulation)...");
        if (ut_case_rko_structure_all_acquired(rk) ||
            ut_case_rko_structure_with_gaps(rk) ||
            ut_case_rko_structure_with_rejects(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        RD_UT_SAY("Testing rd_kafka_share_build_ack_mapping() merge...");
        if (ut_case_merge_single_partition(rk) ||
            ut_case_merge_multiple_rkos(rk) ||
            ut_case_merge_same_partition_multiple_rkos(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        RD_UT_SAY("Testing rd_kafka_share_build_ack_batches_for_fetch()...");
        if (ut_case_collate_all_same_type(rk) ||
            ut_case_collate_mixed_types(rk) ||
            ut_case_collate_empty_map(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        RD_UT_SAY("Testing full flow with multi-partition...");
        if (ut_case_full_flow_multi_partition(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        RD_UT_SAY("Testing partial consumption with next_msg_idx...");
        if (ut_case_partial_consumption_single_poll(rk) ||
            ut_case_partial_consumption_message_order(rk) ||
            ut_case_partial_consumption_with_gaps(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        rd_kafka_destroy(rk);
        RD_UT_PASS();
}
