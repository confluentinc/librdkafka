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
 * 1. rd_kafka_share_filter_msg_from_acq_records() - Filter messages by acquired
 * ranges
 * 2. rd_kafka_share_build_response_rko() - Build SHARE_FETCH_RESPONSE with:
 *    - messages list (actual messages only, no GAP placeholders)
 *    - inflight_acks list (per-offset type mapping including GAPs)
 * 3. rd_kafka_share_build_ack_mapping() - Merge inflight_acks from RKO to
 * rkshare
 * 4. rd_kafka_share_build_ack_batches_for_fetch() - Collate map for ShareFetch
 * request
 * 5. rd_kafka_q_serve_share_rkmessages() - One op per call; returns error
 *    or fills messages
 */

#include "rd.h"
#include "rdunittest.h"
#include "rdkafka_int.h"
#include "rdkafka_queue.h"
#include "rdkafka_fetcher.h"
#include "rdkafka_partition.h"

#include <stdarg.h>

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

static rd_kafka_toppar_t *
ut_create_toppar(rd_kafka_t *rk, const char *topic, int32_t partition) {
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
                rktp->rktp_rkt->rkt_rk    = rk;
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
 * Our minimal test toppars don't have all fields initialized (queues, locks,
 * etc.) so we can't let rd_kafka_op_destroy call rd_kafka_toppar_destroy.
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

static rd_kafka_share_t *ut_create_rkshare(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = rd_calloc(1, sizeof(*rkshare));
        if (!rkshare)
                return NULL;

        rkshare->rkshare_rk          = rk;
        rkshare->rkshare_unacked_cnt = 0;

        /* Inflight acks map keyed by topic_id + partition */
        RD_MAP_INIT(&rkshare->rkshare_inflight_acks, 16,
                    rd_kafka_topic_partition_by_id_cmp,
                    rd_kafka_topic_partition_hash_by_id,
                    rd_kafka_topic_partition_destroy_free,
                    NULL /* value destructor handled manually */);

        return rkshare;
}

static void ut_destroy_rkshare(rd_kafka_share_t *rkshare) {
        if (!rkshare)
                return;

        /* Destroy inflight map entries (batches own rktpar) */
        const rd_kafka_topic_partition_t *tp_key;
        rd_kafka_share_ack_batches_t *batches;

        RD_MAP_FOREACH(tp_key, batches, &rkshare->rkshare_inflight_acks) {
                if (batches)
                        rd_kafka_share_ack_batches_destroy(batches, rd_true);
        }
        RD_MAP_DESTROY(&rkshare->rkshare_inflight_acks);

        rd_free(rkshare);
}

/**
 * @brief Create ack batches struct with initialized rktpar.
 *
 * @param topic Topic name
 * @param partition Partition number
 * @param topic_id Topic UUID
 * @returns Allocated batches with rktpar set up
 */
static rd_kafka_share_ack_batches_t *
ut_create_batches(const char *topic,
                  int32_t partition,
                  rd_kafka_Uuid_t topic_id) {
        rd_kafka_share_ack_batches_t *batches =
            rd_kafka_share_ack_batches_new();
        rd_kafka_topic_partition_private_t *parpriv;

        batches->rktpar            = rd_calloc(1, sizeof(*batches->rktpar));
        batches->rktpar->topic     = rd_strdup(topic);
        batches->rktpar->partition = partition;
        batches->rktpar->offset    = RD_KAFKA_OFFSET_INVALID;
        parpriv                    = rd_kafka_topic_partition_private_new();
        parpriv->topic_id          = topic_id;
        batches->rktpar->_private  = parpriv;

        batches->response_leader_id    = 1;
        batches->response_leader_epoch = 1;

        return batches;
}

/**
 * @brief Create ack batch entry with types array from variadic args.
 *
 * @param start_offset First offset in range
 * @param end_offset Last offset in range (inclusive)
 * @param types_cnt Number of types (must match offset range)
 * @param ... Variadic types (rd_kafka_share_internal_acknowledgement_type)
 * @returns Allocated entry with types filled in
 */
static rd_kafka_share_ack_batch_entry_t *ut_create_entry(int64_t start_offset,
                                                         int64_t end_offset,
                                                         int32_t types_cnt,
                                                         ...) {
        rd_kafka_share_ack_batch_entry_t *entry =
            rd_kafka_share_ack_batch_entry_new(start_offset, end_offset,
                                               types_cnt);
        va_list ap;
        va_start(ap, types_cnt);
        for (int i = 0; i < types_cnt; i++)
                entry->types[i] =
                    (rd_kafka_share_internal_acknowledgement_type)va_arg(ap,
                                                                         int);
        va_end(ap);
        return entry;
}

/**
 * @brief Add batches to rkshare inflight map with topic_id-based key.
 *
 * @param rkshare Share consumer handle
 * @param batches Batches to add (ownership transferred)
 * @param topic_id Topic UUID for map key
 * @param partition Partition number for map key
 */
static void ut_add_to_inflight(rd_kafka_share_t *rkshare,
                               rd_kafka_share_ack_batches_t *batches,
                               rd_kafka_Uuid_t topic_id,
                               int32_t partition) {
        rd_kafka_topic_partition_t *key =
            rd_kafka_topic_partition_new_with_topic_id(topic_id, partition);
        RD_MAP_SET(&rkshare->rkshare_inflight_acks, key, batches);
}

/**
 * @brief Assert collated entry has expected values.
 *
 * Use this macro to verify collated entries in test assertions.
 * The entry_ptr is cast to rd_kafka_share_ack_batch_entry_t* internally.
 */
#define UT_ASSERT_COLLATED(entry_ptr, exp_start, exp_end, exp_type)            \
        do {                                                                   \
                rd_kafka_share_ack_batch_entry_t *_e =                         \
                    (rd_kafka_share_ack_batch_entry_t *)(entry_ptr);           \
                RD_UT_ASSERT(                                                  \
                    _e->start_offset == (exp_start) &&                         \
                        _e->end_offset == (exp_end) &&                         \
                        _e->types[0] == (exp_type),                            \
                    "collated entry mismatch: %" PRId64 "-%" PRId64            \
                    " type=%d, expected %" PRId64 "-%" PRId64 " type=%d",      \
                    _e->start_offset, _e->end_offset, _e->types[0],            \
                    (int64_t)(exp_start), (int64_t)(exp_end), (exp_type));     \
        } while (0)

static rd_kafka_op_t *ut_make_fetch_op(rd_kafka_toppar_t *rktp,
                                       int64_t offset) {
        rd_kafka_op_t *rko = rd_kafka_op_new(RD_KAFKA_OP_FETCH);
        rko->rko_flags |= RD_KAFKA_OP_F_FREE;
        /* Increment refcnt since op holds a reference to rktp */
        rd_refcnt_add(&rktp->rktp_refcnt);
        rko->rko_rktp                                = rktp;
        rko->rko_u.fetch.rkm.rkm_rkmessage.partition = rktp->rktp_partition;
        rko->rko_u.fetch.rkm.rkm_rkmessage.offset    = offset;
        rko->rko_u.fetch.rkm.rkm_rkmessage.err = RD_KAFKA_RESP_ERR_NO_ERROR;
        return rko;
}

static rd_kafka_op_t *ut_make_error_op(rd_kafka_toppar_t *rktp,
                                       int64_t offset) {
        rd_kafka_op_t *rko = rd_kafka_op_new(RD_KAFKA_OP_CONSUMER_ERR);
        rko->rko_flags |= RD_KAFKA_OP_F_FREE;
        /* Increment refcnt since op holds a reference to rktp */
        rd_refcnt_add(&rktp->rktp_refcnt);
        rko->rko_rktp         = rktp;
        rko->rko_err          = RD_KAFKA_RESP_ERR__MSG_TIMED_OUT;
        rko->rko_u.err.offset = offset;
        rko->rko_u.err.rkm.rkm_rkmessage.partition = rktp->rktp_partition;
        rko->rko_u.err.rkm.rkm_rkmessage.offset    = offset;
        return rko;
}

/**
 * Create a mock SHARE_FETCH_RESPONSE rko for testing.
 *
 * The new design:
 * - messages list contains ONLY actual messages (ACQUIRED/REJECT), no GAP
 * placeholders
 * - inflight_acks list contains per-offset type mapping (including GAPs)
 *
 * @param ack_types Array describing type at each offset from acquired_start to
 * acquired_end GAP types mean no message at that offset
 */
static rd_kafka_op_t *
ut_make_share_fetch_response(rd_kafka_t *rk,
                             rd_kafka_toppar_t *rktp,
                             const char *topic,
                             int32_t partition,
                             rd_kafka_Uuid_t topic_id,
                             rd_kafka_share_internal_acknowledgement_type
                                 *ack_types, /* Types for each offset */
                             int64_t acquired_start,
                             int64_t acquired_end) {

        rd_kafka_op_t *response_rko =
            rd_kafka_op_new(RD_KAFKA_OP_SHARE_FETCH_RESPONSE);
        response_rko->rko_rk = rk;

        int64_t range_size = acquired_end - acquired_start + 1;

        /* Initialize lists */
        rd_list_init(&response_rko->rko_u.share_fetch_response.message_rkos,
                     (int)range_size, NULL);
        rd_list_init(&response_rko->rko_u.share_fetch_response.inflight_acks, 1,
                     NULL);

        /* Add messages ONLY for non-GAP offsets */
        for (int64_t i = 0; i < range_size; i++) {
                int64_t offset = acquired_start + i;

                /* Skip GAPs - they are tracked in inflight_acks only */
                if (ack_types[i] == RD_KAFKA_SHARE_INTERNAL_ACK_GAP)
                        continue;

                rd_kafka_op_t *msg_rko;
                rd_kafka_msg_t *rkm;

                if (ack_types[i] == RD_KAFKA_SHARE_INTERNAL_ACK_REJECT) {
                        msg_rko = ut_make_error_op(rktp, offset);
                        rkm     = &msg_rko->rko_u.err.rkm;
                } else {
                        msg_rko = ut_make_fetch_op(rktp, offset);
                        rkm     = &msg_rko->rko_u.fetch.rkm;
                }
                rkm->rkm_u.consumer.ack_type = (int8_t)ack_types[i];
                rd_list_add(
                    &response_rko->rko_u.share_fetch_response.message_rkos,
                    msg_rko);
        }

        /* Build inflight_acks - this is what broker thread does */
        rd_kafka_share_ack_batches_t *batches =
            rd_kafka_share_ack_batches_new();
        {
                rd_kafka_topic_partition_private_t *parpriv;
                batches->rktpar        = rd_calloc(1, sizeof(*batches->rktpar));
                batches->rktpar->topic = rd_strdup(topic);
                batches->rktpar->partition = partition;
                batches->rktpar->offset    = RD_KAFKA_OFFSET_INVALID;
                parpriv           = rd_kafka_topic_partition_private_new();
                parpriv->topic_id = topic_id;
                batches->rktpar->_private = parpriv;
        }
        batches->response_leader_id    = 1;
        batches->response_leader_epoch = 1;
        batches->response_msgs_count   = (int32_t)range_size;

        rd_kafka_share_ack_batch_entry_t *entry =
            rd_kafka_share_ack_batch_entry_new(acquired_start, acquired_end,
                                               (int32_t)range_size);
        memcpy(entry->types, ack_types, range_size * sizeof(*entry->types));

        /* Set is_error based on type (RELEASE/REJECT = error record) */
        for (int64_t k = 0; k < range_size; k++) {
                entry->is_error[k] =
                    (ack_types[k] == RD_KAFKA_INTERNAL_SHARE_ACK_RELEASE ||
                     ack_types[k] == RD_KAFKA_INTERNAL_SHARE_ACK_REJECT);
        }

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
        int i;
        RD_LIST_FOREACH(msg_rko, &rko->rko_u.share_fetch_response.message_rkos,
                        i) {
                ut_destroy_op(msg_rko);
        }
        rd_list_destroy(&rko->rko_u.share_fetch_response.message_rkos);

        /* Destroy inflight_acks */
        rd_kafka_share_ack_batches_t *batches;
        RD_LIST_FOREACH(batches, &rko->rko_u.share_fetch_response.inflight_acks,
                        i) {
                rd_kafka_share_ack_batches_destroy(batches, rd_true);
        }
        rd_list_destroy(&rko->rko_u.share_fetch_response.inflight_acks);

        rd_free(rko);
}

/*******************************************************************************
 * Test: rd_kafka_share_filter_msg_from_acq_records()
 * Tests filtering messages by acquired ranges
 ******************************************************************************/

static int ut_case_filter_all_in_range(rd_kafka_t *rk) {
        rd_kafka_toppar_t *rktp = ut_create_toppar(rk, "test-topic", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        rd_kafka_q_t *temp_fetchq = rd_kafka_q_new(rk);
        rd_list_t filtered_msgs;
        rd_list_init(&filtered_msgs, 0, NULL);

        /* Add messages with offsets 0-4 */
        for (int64_t off = 0; off <= 4; off++)
                rd_kafka_q_enq(temp_fetchq, ut_make_fetch_op(rktp, off));

        /* Acquired range: 0-4 (all messages) */
        int64_t first[] = {0};
        int64_t last[]  = {4};

        rd_kafka_share_filter_msg_from_acq_records(temp_fetchq, &filtered_msgs,
                                                   1, first, last);

        /* All 5 messages should be forwarded */
        RD_UT_ASSERT(rd_list_cnt(&filtered_msgs) == 5,
                     "filtered_msgs len %d != 5", rd_list_cnt(&filtered_msgs));

        /* Verify offsets and ack types */
        for (int64_t exp = 0; exp <= 4; exp++) {
                rd_kafka_op_t *rko = rd_list_elem(&filtered_msgs, (int)exp);
                RD_UT_ASSERT(rko != NULL,
                             "elem returned NULL at offset %" PRId64, exp);
                RD_UT_ASSERT(rko->rko_u.fetch.rkm.rkm_offset == exp,
                             "offset %" PRId64 " != %" PRId64,
                             rko->rko_u.fetch.rkm.rkm_offset, exp);
                RD_UT_ASSERT(rko->rko_u.fetch.rkm.rkm_u.consumer.ack_type ==
                                 RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                             "ack_type %d != ACQUIRED",
                             rko->rko_u.fetch.rkm.rkm_u.consumer.ack_type);
                ut_destroy_op(rko);
        }

        /* Note: temp_fetchq is already destroyed by
         * rd_kafka_share_filter_msg_from_acq_records */
        rd_list_destroy(&filtered_msgs);
        ut_destroy_toppar(rktp);

        RD_UT_SAY("  PASS: ut_case_filter_all_in_range");
        return 0;
}

static int ut_case_filter_partial_range(rd_kafka_t *rk) {
        rd_kafka_toppar_t *rktp = ut_create_toppar(rk, "test-topic", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        rd_kafka_q_t *temp_fetchq = rd_kafka_q_new(rk);
        rd_list_t filtered_msgs;
        rd_list_init(&filtered_msgs, 0, NULL);

        /* Add messages with offsets 0-9 */
        for (int64_t off = 0; off <= 9; off++)
                rd_kafka_q_enq(temp_fetchq, ut_make_fetch_op(rktp, off));

        /* Acquired range: 2-5 (only some messages) */
        int64_t first[] = {2};
        int64_t last[]  = {5};

        rd_kafka_share_filter_msg_from_acq_records(temp_fetchq, &filtered_msgs,
                                                   1, first, last);

        /* Only 4 messages should be forwarded (offsets 2,3,4,5) */
        RD_UT_ASSERT(rd_list_cnt(&filtered_msgs) == 4,
                     "filtered_msgs len %d != 4", rd_list_cnt(&filtered_msgs));

        /* Cleanup ops in the list */
        rd_kafka_op_t *rko;
        int i;
        RD_LIST_FOREACH(rko, &filtered_msgs, i) {
                ut_destroy_op(rko);
        }

        /* Note: temp_fetchq is already destroyed by
         * rd_kafka_share_filter_msg_from_acq_records */
        rd_list_destroy(&filtered_msgs);
        ut_destroy_toppar(rktp);

        RD_UT_SAY("  PASS: ut_case_filter_partial_range");
        return 0;
}

static int ut_case_filter_multiple_ranges(rd_kafka_t *rk) {
        rd_kafka_toppar_t *rktp = ut_create_toppar(rk, "test-topic", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        rd_kafka_q_t *temp_fetchq = rd_kafka_q_new(rk);
        rd_list_t filtered_msgs;
        rd_list_init(&filtered_msgs, 0, NULL);

        /* Add messages with offsets 0-9 */
        for (int64_t off = 0; off <= 9; off++)
                rd_kafka_q_enq(temp_fetchq, ut_make_fetch_op(rktp, off));

        /* Acquired ranges: 1-2, 5-6, 9-9 */
        int64_t first[] = {1, 5, 9};
        int64_t last[]  = {2, 6, 9};

        rd_kafka_share_filter_msg_from_acq_records(temp_fetchq, &filtered_msgs,
                                                   3, first, last);

        /* 5 messages should be forwarded (1,2,5,6,9) */
        RD_UT_ASSERT(rd_list_cnt(&filtered_msgs) == 5,
                     "filtered_msgs len %d != 5", rd_list_cnt(&filtered_msgs));

        /* Cleanup ops in the list */
        rd_kafka_op_t *rko;
        int i;
        RD_LIST_FOREACH(rko, &filtered_msgs, i) {
                ut_destroy_op(rko);
        }

        /* Note: temp_fetchq is already destroyed by
         * rd_kafka_share_filter_msg_from_acq_records */
        rd_list_destroy(&filtered_msgs);
        ut_destroy_toppar(rktp);

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
        rd_kafka_share_internal_acknowledgement_type types[] = {
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED};

        rd_kafka_op_t *response_rko = ut_make_share_fetch_response(
            rk, rktp, "T1", 0, RD_KAFKA_UUID_ZERO, types, 1, 6);

        /* Verify the response structure */
        /* All 6 messages should be present (no GAPs) */
        RD_UT_ASSERT(
            rd_list_cnt(
                &response_rko->rko_u.share_fetch_response.message_rkos) == 6,
            "message cnt %d != 6",
            rd_list_cnt(
                &response_rko->rko_u.share_fetch_response.message_rkos));

        /* Verify inflight_acks was created */
        RD_UT_ASSERT(
            rd_list_cnt(
                &response_rko->rko_u.share_fetch_response.inflight_acks) == 1,
            "inflight_acks cnt %d != 1",
            rd_list_cnt(
                &response_rko->rko_u.share_fetch_response.inflight_acks));

        ut_destroy_share_fetch_response(response_rko);
        ut_destroy_toppar(rktp);

        RD_UT_SAY("  PASS: ut_case_rko_structure_all_acquired");
        return 0;
}

static int ut_case_rko_structure_with_gaps(rd_kafka_t *rk) {
        rd_kafka_toppar_t *rktp = ut_create_toppar(rk, "T1", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        /* Create response with gaps: ACQ, ACQ, GAP, GAP, ACQ, ACQ */
        rd_kafka_share_internal_acknowledgement_type types[] = {
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
            RD_KAFKA_SHARE_INTERNAL_ACK_GAP,
            RD_KAFKA_SHARE_INTERNAL_ACK_GAP,
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED};

        rd_kafka_op_t *response_rko = ut_make_share_fetch_response(
            rk, rktp, "T1", 0, RD_KAFKA_UUID_ZERO, types, 1, 6);

        /* Only 4 messages should be present (2 GAPs excluded) */
        RD_UT_ASSERT(
            rd_list_cnt(
                &response_rko->rko_u.share_fetch_response.message_rkos) == 4,
            "message cnt %d != 4",
            rd_list_cnt(
                &response_rko->rko_u.share_fetch_response.message_rkos));

        /* Verify inflight_acks has all 6 offsets including GAPs */
        rd_kafka_share_ack_batches_t *batches = rd_list_elem(
            &response_rko->rko_u.share_fetch_response.inflight_acks, 0);
        RD_UT_ASSERT(batches != NULL, "inflight_acks[0] is NULL");

        rd_kafka_share_ack_batch_entry_t *entry =
            rd_list_elem(&batches->entries, 0);
        RD_UT_ASSERT(entry != NULL, "entries[0] is NULL");
        RD_UT_ASSERT(entry->size == 6, "entry size %" PRId64 " != 6",
                     entry->size);

        /* Verify types array has GAPs at positions 2,3 */
        RD_UT_ASSERT(entry->types[0] == RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                     "type[0] != ACQ");
        RD_UT_ASSERT(entry->types[1] == RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                     "type[1] != ACQ");
        RD_UT_ASSERT(entry->types[2] == RD_KAFKA_SHARE_INTERNAL_ACK_GAP,
                     "type[2] != GAP");
        RD_UT_ASSERT(entry->types[3] == RD_KAFKA_SHARE_INTERNAL_ACK_GAP,
                     "type[3] != GAP");
        RD_UT_ASSERT(entry->types[4] == RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                     "type[4] != ACQ");
        RD_UT_ASSERT(entry->types[5] == RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                     "type[5] != ACQ");

        ut_destroy_share_fetch_response(response_rko);
        ut_destroy_toppar(rktp);

        RD_UT_SAY("  PASS: ut_case_rko_structure_with_gaps");
        return 0;
}

static int ut_case_rko_structure_with_rejects(rd_kafka_t *rk) {
        rd_kafka_toppar_t *rktp = ut_create_toppar(rk, "T1", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        /* Create response with rejects: ACQ, REJ, GAP, REJ, REJ, ACQ */
        rd_kafka_share_internal_acknowledgement_type types[] = {
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
            RD_KAFKA_SHARE_INTERNAL_ACK_REJECT,
            RD_KAFKA_SHARE_INTERNAL_ACK_GAP,
            RD_KAFKA_SHARE_INTERNAL_ACK_REJECT,
            RD_KAFKA_SHARE_INTERNAL_ACK_REJECT,
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED};

        rd_kafka_op_t *response_rko = ut_make_share_fetch_response(
            rk, rktp, "T1", 0, RD_KAFKA_UUID_ZERO, types, 1, 6);

        /* 5 messages present (1 GAP excluded): ACQ, REJ, REJ, REJ, ACQ */
        RD_UT_ASSERT(
            rd_list_cnt(
                &response_rko->rko_u.share_fetch_response.message_rkos) == 5,
            "message cnt %d != 5",
            rd_list_cnt(
                &response_rko->rko_u.share_fetch_response.message_rkos));

        /* Count CONSUMER_ERR ops (should be 3 REJECTs) */
        rd_kafka_op_t *msg_rko;
        int i, reject_cnt = 0;
        RD_LIST_FOREACH(msg_rko,
                        &response_rko->rko_u.share_fetch_response.message_rkos,
                        i) {
                if (msg_rko->rko_type == RD_KAFKA_OP_CONSUMER_ERR)
                        reject_cnt++;
        }
        RD_UT_ASSERT(reject_cnt == 3, "reject cnt %d != 3", reject_cnt);

        /* Verify inflight_acks has correct types */
        rd_kafka_share_ack_batches_t *batches = rd_list_elem(
            &response_rko->rko_u.share_fetch_response.inflight_acks, 0);
        rd_kafka_share_ack_batch_entry_t *entry =
            rd_list_elem(&batches->entries, 0);

        RD_UT_ASSERT(entry->types[0] == RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                     "type[0] != ACQ");
        RD_UT_ASSERT(entry->types[1] == RD_KAFKA_SHARE_INTERNAL_ACK_REJECT,
                     "type[1] != REJ");
        RD_UT_ASSERT(entry->types[2] == RD_KAFKA_SHARE_INTERNAL_ACK_GAP,
                     "type[2] != GAP");
        RD_UT_ASSERT(entry->types[3] == RD_KAFKA_SHARE_INTERNAL_ACK_REJECT,
                     "type[3] != REJ");
        RD_UT_ASSERT(entry->types[4] == RD_KAFKA_SHARE_INTERNAL_ACK_REJECT,
                     "type[4] != REJ");
        RD_UT_ASSERT(entry->types[5] == RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                     "type[5] != ACQ");

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
        rd_kafka_share_internal_acknowledgement_type types[] = {
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
            RD_KAFKA_SHARE_INTERNAL_ACK_GAP,
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
            RD_KAFKA_SHARE_INTERNAL_ACK_REJECT};

        rd_kafka_op_t *response_rko = ut_make_share_fetch_response(
            rk, rktp, "T1", 0, RD_KAFKA_UUID_ZERO, types, 1, 6);

        /* Verify rkshare map is empty before merge */
        RD_UT_ASSERT(RD_MAP_CNT(&rkshare->rkshare_inflight_acks) == 0,
                     "rkshare map not empty before merge");

        /* Call the merge function (simulating app thread) */
        rd_kafka_share_build_ack_mapping(rkshare, response_rko);

        /* Verify rkshare map has 1 partition */
        RD_UT_ASSERT(RD_MAP_CNT(&rkshare->rkshare_inflight_acks) == 1,
                     "rkshare map cnt %d != 1",
                     (int)RD_MAP_CNT(&rkshare->rkshare_inflight_acks));

        /* Verify unacked count (only ACQUIRED types: 4 out of 6) */
        RD_UT_ASSERT(rkshare->rkshare_unacked_cnt == 4,
                     "unacked cnt %" PRId64 " != 4",
                     rkshare->rkshare_unacked_cnt);

        /* Lookup the merged batches (map keyed by topic_id + partition) */
        rd_kafka_topic_partition_t *lookup_key =
            rd_kafka_topic_partition_new_with_topic_id(RD_KAFKA_UUID_ZERO, 0);
        rd_kafka_share_ack_batches_t *merged =
            RD_MAP_GET(&rkshare->rkshare_inflight_acks, lookup_key);
        rd_kafka_topic_partition_destroy_free(lookup_key);
        RD_UT_ASSERT(merged != NULL, "merged batches not found");

        /* Verify merged data */
        RD_UT_ASSERT(merged->rktpar != NULL, "rktpar is NULL");
        RD_UT_ASSERT(strcmp(merged->rktpar->topic, "T1") == 0,
                     "topic mismatch");
        RD_UT_ASSERT(merged->rktpar->partition == 0, "partition mismatch");
        RD_UT_ASSERT(rd_list_cnt(&merged->entries) == 1,
                     "entries cnt mismatch");

        rd_kafka_share_ack_batch_entry_t *entry =
            rd_list_elem(&merged->entries, 0);
        RD_UT_ASSERT(entry->start_offset == 1 && entry->end_offset == 6,
                     "offset range mismatch");

        /* Verify types were copied correctly */
        RD_UT_ASSERT(entry->types[0] == RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                     "type[0]");
        RD_UT_ASSERT(entry->types[1] == RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                     "type[1]");
        RD_UT_ASSERT(entry->types[2] == RD_KAFKA_SHARE_INTERNAL_ACK_GAP,
                     "type[2]");
        RD_UT_ASSERT(entry->types[3] == RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                     "type[3]");
        RD_UT_ASSERT(entry->types[4] == RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                     "type[4]");
        RD_UT_ASSERT(entry->types[5] == RD_KAFKA_SHARE_INTERNAL_ACK_REJECT,
                     "type[5]");

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
        rd_kafka_share_internal_acknowledgement_type types1[] = {
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
            RD_KAFKA_SHARE_INTERNAL_ACK_GAP};
        rd_kafka_op_t *rko1 = ut_make_share_fetch_response(
            rk, rktp1, "T1", 0, RD_KAFKA_UUID_ZERO, types1, 1, 3);

        /* Second RKO from broker 2: T2-0 with ACQ, REJ, ACQ (distinct topic_id)
         */
        static const rd_kafka_Uuid_t ut_topic_id_t2           = {0, 1, ""};
        rd_kafka_share_internal_acknowledgement_type types2[] = {
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
            RD_KAFKA_SHARE_INTERNAL_ACK_REJECT,
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED};
        rd_kafka_op_t *rko2 = ut_make_share_fetch_response(
            rk, rktp2, "T2", 0, ut_topic_id_t2, types2, 10, 12);

        /* Merge first RKO */
        rd_kafka_share_build_ack_mapping(rkshare, rko1);

        /* Verify after first merge (2 ACQUIRED out of 3: ACQ, ACQ, GAP) */
        RD_UT_ASSERT(RD_MAP_CNT(&rkshare->rkshare_inflight_acks) == 1,
                     "map cnt %d != 1 after first merge",
                     (int)RD_MAP_CNT(&rkshare->rkshare_inflight_acks));
        RD_UT_ASSERT(rkshare->rkshare_unacked_cnt == 2,
                     "unacked %" PRId64 " != 2", rkshare->rkshare_unacked_cnt);

        /* Merge second RKO */
        rd_kafka_share_build_ack_mapping(rkshare, rko2);

        /* Verify after second merge (2 + 2 ACQUIRED: ACQ, REJ, ACQ) */
        RD_UT_ASSERT(RD_MAP_CNT(&rkshare->rkshare_inflight_acks) == 2,
                     "map cnt %d != 2 after second merge",
                     (int)RD_MAP_CNT(&rkshare->rkshare_inflight_acks));
        RD_UT_ASSERT(rkshare->rkshare_unacked_cnt == 4,
                     "unacked %" PRId64 " != 4", rkshare->rkshare_unacked_cnt);

        /* Verify T1-0 (map keyed by topic_id + partition) */
        rd_kafka_topic_partition_t *key1 =
            rd_kafka_topic_partition_new_with_topic_id(RD_KAFKA_UUID_ZERO, 0);
        rd_kafka_share_ack_batches_t *batches1 =
            RD_MAP_GET(&rkshare->rkshare_inflight_acks, key1);
        rd_kafka_topic_partition_destroy_free(key1);
        RD_UT_ASSERT(batches1 != NULL, "T1-0 not found");
        RD_UT_ASSERT(rd_list_cnt(&batches1->entries) == 1, "T1-0 entries cnt");

        /* Verify T2-0 (different topic_id) */
        rd_kafka_topic_partition_t *key2 =
            rd_kafka_topic_partition_new_with_topic_id(ut_topic_id_t2, 0);
        rd_kafka_share_ack_batches_t *batches2 =
            RD_MAP_GET(&rkshare->rkshare_inflight_acks, key2);
        rd_kafka_topic_partition_destroy_free(key2);
        RD_UT_ASSERT(batches2 != NULL, "T2-0 not found");
        RD_UT_ASSERT(rd_list_cnt(&batches2->entries) == 1, "T2-0 entries cnt");

        rd_kafka_share_ack_batch_entry_t *entry2 =
            rd_list_elem(&batches2->entries, 0);
        RD_UT_ASSERT(entry2->start_offset == 10 && entry2->end_offset == 12,
                     "T2-0 offset range mismatch");
        RD_UT_ASSERT(entry2->types[0] == RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                     "T2 type[0]");
        RD_UT_ASSERT(entry2->types[1] == RD_KAFKA_SHARE_INTERNAL_ACK_REJECT,
                     "T2 type[1]");
        RD_UT_ASSERT(entry2->types[2] == RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                     "T2 type[2]");

        ut_destroy_share_fetch_response(rko1);
        ut_destroy_share_fetch_response(rko2);
        ut_destroy_toppar(rktp1);
        ut_destroy_toppar(rktp2);
        ut_destroy_rkshare(rkshare);

        RD_UT_SAY("  PASS: ut_case_merge_multiple_rkos");
        return 0;
}

/*******************************************************************************
 * Test: rd_kafka_share_build_ack_batches_for_fetch()
 * Tests collating inflight map into ranges for ShareFetch
 ******************************************************************************/

static int ut_case_collate_all_same_type(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        /* Populate inflight map with all ACQUIRED */
        rd_kafka_share_ack_batches_t *batches =
            ut_create_batches("T1", 0, RD_KAFKA_UUID_ZERO);

        rd_kafka_share_ack_batch_entry_t *entry =
            ut_create_entry(1, 4, 4, RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED);
        rd_list_add(&batches->entries, entry);

        ut_add_to_inflight(rkshare, batches, RD_KAFKA_UUID_ZERO, 0);

        /* Call collate function */
        rd_list_t ack_batches_out;
        rd_kafka_share_build_ack_batches_for_fetch(rkshare, &ack_batches_out);

        /* Should produce 1 batch with 1 entry (all ACCEPT) */
        RD_UT_ASSERT(rd_list_cnt(&ack_batches_out) == 1,
                     "ack_batches cnt %d != 1", rd_list_cnt(&ack_batches_out));

        rd_kafka_share_ack_batches_t *out_batch =
            rd_list_elem(&ack_batches_out, 0);
        RD_UT_ASSERT(rd_list_cnt(&out_batch->entries) == 1,
                     "entries cnt %d != 1", rd_list_cnt(&out_batch->entries));

        rd_kafka_share_ack_batch_entry_t *collated =
            rd_list_elem(&out_batch->entries, 0);
        UT_ASSERT_COLLATED(collated, 1, 4, RD_KAFKA_SHARE_INTERNAL_ACK_ACCEPT);
        RD_UT_ASSERT(collated->size == 4, "size %" PRId64 " != 4",
                     collated->size);
        RD_UT_ASSERT(collated->types[0] == RD_KAFKA_INTERNAL_SHARE_ACK_ACCEPT,
                     "type %d != ACCEPT", collated->types[0]);

        rd_list_destroy(&ack_batches_out);
        ut_destroy_rkshare(rkshare);

        RD_UT_SAY("  PASS: ut_case_collate_all_same_type");
        return 0;
}

static int ut_case_collate_mixed_types(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        /* Populate: ACQ, ACQ, GAP, ACQ, ACQ, REJ, REJ, ACQ, ACQ */
        rd_kafka_share_ack_batches_t *batches =
            ut_create_batches("T1", 0, RD_KAFKA_UUID_ZERO);

        rd_kafka_share_ack_batch_entry_t *entry =
            ut_create_entry(1, 9, 9, RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                            RD_KAFKA_SHARE_INTERNAL_ACK_GAP,
                            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                            RD_KAFKA_SHARE_INTERNAL_ACK_REJECT,
                            RD_KAFKA_SHARE_INTERNAL_ACK_REJECT,
                            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED);
        rd_list_add(&batches->entries, entry);

        ut_add_to_inflight(rkshare, batches, RD_KAFKA_UUID_ZERO, 0);

        /* Call collate function */
        rd_list_t ack_batches_out;
        rd_kafka_share_build_ack_batches_for_fetch(rkshare, &ack_batches_out);

        /* Should produce 1 batch with 5 entries */
        RD_UT_ASSERT(rd_list_cnt(&ack_batches_out) == 1,
                     "ack_batches cnt %d != 1", rd_list_cnt(&ack_batches_out));

        rd_kafka_share_ack_batches_t *out_batch =
            rd_list_elem(&ack_batches_out, 0);
        RD_UT_ASSERT(rd_list_cnt(&out_batch->entries) == 5,
                     "entries cnt %d != 5", rd_list_cnt(&out_batch->entries));

        /* Verify entries: {1-2, ACCEPT}, {3-3, GAP}, {4-5, ACCEPT},
         *                 {6-7, REJECT}, {8-9, ACCEPT} */
        UT_ASSERT_COLLATED(rd_list_elem(&out_batch->entries, 0), 1, 2,
                           RD_KAFKA_SHARE_INTERNAL_ACK_ACCEPT);
        UT_ASSERT_COLLATED(rd_list_elem(&out_batch->entries, 1), 3, 3,
                           RD_KAFKA_SHARE_INTERNAL_ACK_GAP);
        UT_ASSERT_COLLATED(rd_list_elem(&out_batch->entries, 2), 4, 5,
                           RD_KAFKA_SHARE_INTERNAL_ACK_ACCEPT);
        UT_ASSERT_COLLATED(rd_list_elem(&out_batch->entries, 3), 6, 7,
                           RD_KAFKA_SHARE_INTERNAL_ACK_REJECT);
        UT_ASSERT_COLLATED(rd_list_elem(&out_batch->entries, 4), 8, 9,
                           RD_KAFKA_SHARE_INTERNAL_ACK_ACCEPT);

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
         * Test case:
         * Tp1: 1:ACQ, 2:ACQ, 3:GAP, 4:GAP, 5:ACQ, 6:ACQ
         * Tp2: 1:ACQ, 2:REJ, 3:GAP, 4:REJ, 5:REJ, 6:ACQ, 7:ACQ, 8:GAP
         */

        /* Populate Tp1 */
        rd_kafka_share_ack_batches_t *batches1 =
            ut_create_batches("Tp1", 0, RD_KAFKA_UUID_ZERO);
        rd_kafka_share_ack_batch_entry_t *entry1 = ut_create_entry(
            1, 6, 6, RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
            RD_KAFKA_SHARE_INTERNAL_ACK_GAP, RD_KAFKA_SHARE_INTERNAL_ACK_GAP,
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED);
        rd_list_add(&batches1->entries, entry1);
        ut_add_to_inflight(rkshare, batches1, RD_KAFKA_UUID_ZERO, 0);

        /* Populate Tp2 (distinct topic_id) */
        static const rd_kafka_Uuid_t ut_topic_id_tp2 = {0, 2, ""};
        rd_kafka_share_ack_batches_t *batches2 =
            ut_create_batches("Tp2", 0, ut_topic_id_tp2);
        batches2->response_leader_id             = 2; /* Override default */
        rd_kafka_share_ack_batch_entry_t *entry2 = ut_create_entry(
            1, 8, 8, RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
            RD_KAFKA_SHARE_INTERNAL_ACK_REJECT, RD_KAFKA_SHARE_INTERNAL_ACK_GAP,
            RD_KAFKA_SHARE_INTERNAL_ACK_REJECT,
            RD_KAFKA_SHARE_INTERNAL_ACK_REJECT,
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
            RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
            RD_KAFKA_SHARE_INTERNAL_ACK_GAP);
        rd_list_add(&batches2->entries, entry2);
        ut_add_to_inflight(rkshare, batches2, ut_topic_id_tp2, 0);

        /* Call collate function */
        rd_list_t ack_batches_out;
        rd_kafka_share_build_ack_batches_for_fetch(rkshare, &ack_batches_out);

        /* Should produce 2 batches */
        RD_UT_ASSERT(rd_list_cnt(&ack_batches_out) == 2,
                     "ack_batches cnt %d != 2", rd_list_cnt(&ack_batches_out));

        /* Find Tp1 and Tp2 batches */
        rd_kafka_share_ack_batches_t *tp1_batch = NULL;
        rd_kafka_share_ack_batches_t *tp2_batch = NULL;
        rd_kafka_share_ack_batches_t *batch;
        int i;

        RD_LIST_FOREACH(batch, &ack_batches_out, i) {
                if (batch->rktpar && strcmp(batch->rktpar->topic, "Tp1") == 0)
                        tp1_batch = batch;
                else if (batch->rktpar &&
                         strcmp(batch->rktpar->topic, "Tp2") == 0)
                        tp2_batch = batch;
        }

        RD_UT_ASSERT(tp1_batch != NULL, "Tp1 batch not found");
        RD_UT_ASSERT(tp2_batch != NULL, "Tp2 batch not found");

        /* Tp1 expected: {1-2, ACCEPT}, {3-4, GAP}, {5-6, ACCEPT} */
        RD_UT_ASSERT(rd_list_cnt(&tp1_batch->entries) == 3,
                     "Tp1 entries cnt %d != 3",
                     rd_list_cnt(&tp1_batch->entries));

        UT_ASSERT_COLLATED(rd_list_elem(&tp1_batch->entries, 0), 1, 2,
                           RD_KAFKA_SHARE_INTERNAL_ACK_ACCEPT);
        UT_ASSERT_COLLATED(rd_list_elem(&tp1_batch->entries, 1), 3, 4,
                           RD_KAFKA_SHARE_INTERNAL_ACK_GAP);
        UT_ASSERT_COLLATED(rd_list_elem(&tp1_batch->entries, 2), 5, 6,
                           RD_KAFKA_SHARE_INTERNAL_ACK_ACCEPT);

        /* Tp2 expected: {1-1, ACCEPT}, {2-2, REJECT}, {3-3, GAP},
         *               {4-5, REJECT}, {6-7, ACCEPT}, {8-8, GAP} */
        RD_UT_ASSERT(rd_list_cnt(&tp2_batch->entries) == 6,
                     "Tp2 entries cnt %d != 6",
                     rd_list_cnt(&tp2_batch->entries));

        UT_ASSERT_COLLATED(rd_list_elem(&tp2_batch->entries, 0), 1, 1,
                           RD_KAFKA_SHARE_INTERNAL_ACK_ACCEPT);
        UT_ASSERT_COLLATED(rd_list_elem(&tp2_batch->entries, 1), 2, 2,
                           RD_KAFKA_SHARE_INTERNAL_ACK_REJECT);
        UT_ASSERT_COLLATED(rd_list_elem(&tp2_batch->entries, 2), 3, 3,
                           RD_KAFKA_SHARE_INTERNAL_ACK_GAP);
        UT_ASSERT_COLLATED(rd_list_elem(&tp2_batch->entries, 3), 4, 5,
                           RD_KAFKA_SHARE_INTERNAL_ACK_REJECT);
        UT_ASSERT_COLLATED(rd_list_elem(&tp2_batch->entries, 4), 6, 7,
                           RD_KAFKA_SHARE_INTERNAL_ACK_ACCEPT);
        UT_ASSERT_COLLATED(rd_list_elem(&tp2_batch->entries, 5), 8, 8,
                           RD_KAFKA_SHARE_INTERNAL_ACK_GAP);

        rd_list_destroy(&ack_batches_out);
        ut_destroy_rkshare(rkshare);

        RD_UT_SAY("  PASS: ut_case_full_flow_multi_partition");
        return 0;
}

/*******************************************************************************
 * Main Test Entry Point
 ******************************************************************************/

int unittest_fetcher_share_filter_forward(void) {
        rd_kafka_t *rk = ut_create_rk();
        RD_UT_ASSERT(rk != NULL, "rd_kafka_new failed");

        RD_UT_SAY("Testing rd_kafka_share_filter_msg_from_acq_records()...");
        if (ut_case_filter_all_in_range(rk) ||
            ut_case_filter_partial_range(rk) ||
            ut_case_filter_multiple_ranges(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        RD_UT_SAY(
            "Testing RKO structure creation (broker thread simulation)...");
        if (ut_case_rko_structure_all_acquired(rk) ||
            ut_case_rko_structure_with_gaps(rk) ||
            ut_case_rko_structure_with_rejects(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        RD_UT_SAY("Testing rd_kafka_share_build_ack_mapping() merge...");
        if (ut_case_merge_single_partition(rk) ||
            ut_case_merge_multiple_rkos(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        RD_UT_SAY("Testing rd_kafka_share_build_ack_batches_for_fetch()...");
        if (ut_case_collate_all_same_type(rk) ||
            ut_case_collate_mixed_types(rk) || ut_case_collate_empty_map(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        RD_UT_SAY("Testing full flow with multi-partition...");
        if (ut_case_full_flow_multi_partition(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        rd_kafka_destroy(rk);
        RD_UT_PASS();
}
