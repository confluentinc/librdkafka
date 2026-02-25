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
 * @brief Unit tests for Share Consumer acknowledge APIs
 *
 * Tests the following public APIs:
 * 1. rd_kafka_share_acknowledge() - Acknowledge delivered record with ACCEPT
 * 2. rd_kafka_share_acknowledge_type() - Acknowledge delivered record with type
 * 3. rd_kafka_share_acknowledge_offset() - Acknowledge error record by offset
 */

#include "rd.h"
#include "rdunittest.h"
#include "rdkafka_int.h"
#include "rdkafka_partition.h"


/**
 * @brief Create a rd_kafka_t instance for testing.
 */
static rd_kafka_t *ut_ack_create_rk(void) {
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        char errstr[128];

        if (rd_kafka_conf_set(conf, "group.id", "ut-share-ack", errstr,
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

/**
 * @brief Create a mock rd_kafka_toppar_t for testing.
 */
static rd_kafka_toppar_t *
ut_ack_create_toppar(rd_kafka_t *rk, const char *topic, int32_t partition) {
        rd_kafka_toppar_t *rktp = rd_calloc(1, sizeof(*rktp));
        if (!rktp)
                return NULL;

        rktp->rktp_partition = partition;
        rd_refcnt_init(&rktp->rktp_refcnt, 1);

        rktp->rktp_rkt = rd_calloc(1, sizeof(*rktp->rktp_rkt));
        if (rktp->rktp_rkt) {
                rktp->rktp_rkt->rkt_topic = rd_kafkap_str_new(topic, -1);
                rktp->rktp_rkt->rkt_rk    = rk;
        }

        return rktp;
}

/**
 * @brief Destroy a mock rd_kafka_toppar_t.
 */
static void ut_ack_destroy_toppar(rd_kafka_toppar_t *rktp) {
        if (!rktp)
                return;

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
 * @brief Create rkshare with explicit acknowledgement mode enabled.
 */
static rd_kafka_share_t *ut_ack_create_rkshare(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = rd_calloc(1, sizeof(*rkshare));
        if (!rkshare)
                return NULL;

        rkshare->rkshare_rk          = rk;
        rkshare->rkshare_unacked_cnt = 0;

        /* Enable explicit acknowledgement mode
         * (share.acknowledgement.mode=explicit) */
        rk->rk_conf.share.explicit_acks = 1;

        RD_MAP_INIT(&rkshare->rkshare_inflight_acks, 16,
                    rd_kafka_topic_partition_cmp, rd_kafka_topic_partition_hash,
                    rd_kafka_topic_partition_destroy_free,
                    NULL /* value destructor handled manually */);

        return rkshare;
}

/**
 * @brief Create rkshare with implicit acknowledgement mode
 * (share.acknowledgement.mode=implicit).
 */
static rd_kafka_share_t *ut_ack_create_rkshare_implicit(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = rd_calloc(1, sizeof(*rkshare));
        if (!rkshare)
                return NULL;

        rkshare->rkshare_rk          = rk;
        rkshare->rkshare_unacked_cnt = 0;

        /* Implicit acknowledgement mode (share.acknowledgement.mode=implicit)
         */
        rk->rk_conf.share.explicit_acks = 0;

        RD_MAP_INIT(&rkshare->rkshare_inflight_acks, 16,
                    rd_kafka_topic_partition_cmp, rd_kafka_topic_partition_hash,
                    rd_kafka_topic_partition_destroy_free,
                    NULL /* value destructor handled manually */);

        return rkshare;
}

/**
 * @brief Destroy a rd_kafka_share_t and free all inflight ack entries.
 */
static void ut_ack_destroy_rkshare(rd_kafka_share_t *rkshare) {
        if (!rkshare)
                return;

        const rd_kafka_topic_partition_t *tp_key;
        rd_kafka_share_ack_batches_t *batches;

        RD_MAP_FOREACH(tp_key, batches, &rkshare->rkshare_inflight_acks) {
                if (batches) {
                        if (batches->rktpar)
                                rd_kafka_topic_partition_destroy(
                                    batches->rktpar);
                        rd_kafka_share_ack_batch_entry_t *entry;
                        int i;
                        RD_LIST_FOREACH(entry, &batches->entries, i) {
                                if (entry->types)
                                        rd_free(entry->types);
                                if (entry->is_error)
                                        rd_free(entry->is_error);
                                rd_free(entry);
                        }
                        rd_list_destroy(&batches->entries);
                        rd_free(batches);
                }
        }
        RD_MAP_DESTROY(&rkshare->rkshare_inflight_acks);

        rd_free(rkshare);
}

/**
 * @brief Add a partition with acquired offsets to the rkshare inflight map.
 *
 * Creates an entry with all offsets in ACQUIRED state (delivered records).
 * Delivered records can be acknowledged via record-based APIs.
 */
static void ut_ack_add_partition(rd_kafka_share_t *rkshare,
                                 const char *topic,
                                 int32_t partition,
                                 int64_t start_offset,
                                 int64_t end_offset) {
        rd_kafka_topic_partition_private_t *parpriv;
        rd_kafka_share_ack_batches_t *batches = rd_calloc(1, sizeof(*batches));

        batches->rktpar            = rd_calloc(1, sizeof(*batches->rktpar));
        batches->rktpar->topic     = rd_strdup(topic);
        batches->rktpar->partition = partition;
        batches->rktpar->offset    = RD_KAFKA_OFFSET_INVALID;
        parpriv                    = rd_kafka_topic_partition_private_new();
        batches->rktpar->_private  = parpriv;

        batches->response_leader_id    = 1;
        batches->response_leader_epoch = 1;

        int64_t size                 = end_offset - start_offset + 1;
        batches->response_msgs_count = (int32_t)size;

        rd_list_init(&batches->entries, 1, NULL);

        rd_kafka_share_ack_batch_entry_t *entry = rd_calloc(1, sizeof(*entry));
        entry->start_offset                     = start_offset;
        entry->end_offset                       = end_offset;
        entry->size                             = size;
        entry->types_cnt                        = (int32_t)size;
        entry->types    = rd_calloc(size, sizeof(*entry->types));
        entry->is_error = rd_calloc(size, sizeof(*entry->is_error));

        /* Initialize all offsets to ACQUIRED (delivered records, not errors) */
        for (int64_t i = 0; i < size; i++) {
                entry->types[i]    = RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED;
                entry->is_error[i] = rd_false; /* Delivered record */
        }

        rd_list_add(&batches->entries, entry);

        rd_kafka_topic_partition_t *key =
            rd_kafka_topic_partition_new(topic, partition);
        RD_MAP_SET(&rkshare->rkshare_inflight_acks, key, batches);
}

/**
 * @brief Add a partition with error records to the rkshare inflight map.
 *
 * Creates an entry with all offsets as error records (RELEASE state).
 * Error records can only be acknowledged via offset-based API.
 */
static void ut_ack_add_partition_error(rd_kafka_share_t *rkshare,
                                       const char *topic,
                                       int32_t partition,
                                       int64_t start_offset,
                                       int64_t end_offset) {
        rd_kafka_topic_partition_private_t *parpriv;
        rd_kafka_share_ack_batches_t *batches = rd_calloc(1, sizeof(*batches));

        batches->rktpar            = rd_calloc(1, sizeof(*batches->rktpar));
        batches->rktpar->topic     = rd_strdup(topic);
        batches->rktpar->partition = partition;
        batches->rktpar->offset    = RD_KAFKA_OFFSET_INVALID;
        parpriv                    = rd_kafka_topic_partition_private_new();
        batches->rktpar->_private  = parpriv;

        batches->response_leader_id    = 1;
        batches->response_leader_epoch = 1;

        int64_t size                 = end_offset - start_offset + 1;
        batches->response_msgs_count = (int32_t)size;

        rd_list_init(&batches->entries, 1, NULL);

        rd_kafka_share_ack_batch_entry_t *entry = rd_calloc(1, sizeof(*entry));
        entry->start_offset                     = start_offset;
        entry->end_offset                       = end_offset;
        entry->size                             = size;
        entry->types_cnt                        = (int32_t)size;
        entry->types    = rd_calloc(size, sizeof(*entry->types));
        entry->is_error = rd_calloc(size, sizeof(*entry->is_error));

        /* Initialize all offsets as error records (RELEASE state) */
        for (int64_t i = 0; i < size; i++) {
                entry->types[i]    = RD_KAFKA_SHARE_INTERNAL_ACK_RELEASE;
                entry->is_error[i] = rd_true; /* Error record */
        }

        rd_list_add(&batches->entries, entry);

        rd_kafka_topic_partition_t *key =
            rd_kafka_topic_partition_new(topic, partition);
        RD_MAP_SET(&rkshare->rkshare_inflight_acks, key, batches);
}

/**
 * @brief Set a specific offset as a GAP record.
 */
static void ut_ack_set_gap(rd_kafka_share_t *rkshare,
                           const char *topic,
                           int32_t partition,
                           int64_t offset) {
        rd_kafka_topic_partition_t lookup_key;
        lookup_key.topic     = (char *)topic;
        lookup_key.partition = partition;

        rd_kafka_share_ack_batches_t *batches =
            RD_MAP_GET(&rkshare->rkshare_inflight_acks, &lookup_key);
        if (!batches)
                return;

        rd_kafka_share_ack_batch_entry_t *entry;
        int i;
        RD_LIST_FOREACH(entry, &batches->entries, i) {
                if (offset >= entry->start_offset &&
                    offset <= entry->end_offset) {
                        int64_t idx       = offset - entry->start_offset;
                        entry->types[idx] = RD_KAFKA_SHARE_INTERNAL_ACK_GAP;
                        return;
                }
        }
}

/**
 * @brief Get the ack type for a specific offset in the inflight map.
 */
static rd_kafka_share_internal_acknowledgement_type
ut_ack_get_type(rd_kafka_share_t *rkshare,
                const char *topic,
                int32_t partition,
                int64_t offset) {
        rd_kafka_topic_partition_t lookup_key;
        lookup_key.topic     = (char *)topic;
        lookup_key.partition = partition;

        rd_kafka_share_ack_batches_t *batches =
            RD_MAP_GET(&rkshare->rkshare_inflight_acks, &lookup_key);
        if (!batches)
                return -99; /* Invalid marker */

        rd_kafka_share_ack_batch_entry_t *entry;
        int i;
        RD_LIST_FOREACH(entry, &batches->entries, i) {
                if (offset >= entry->start_offset &&
                    offset <= entry->end_offset) {
                        int64_t idx = offset - entry->start_offset;
                        return entry->types[idx];
                }
        }

        return -99; /* Invalid marker */
}

/**
 * @brief Create a mock rd_kafka_message_t for testing.
 */
static rd_kafka_message_t *ut_ack_create_message(rd_kafka_toppar_t *rktp,
                                                 int64_t offset) {
        rd_kafka_message_t *rkmessage = rd_calloc(1, sizeof(*rkmessage));
        rkmessage->rkt                = rktp->rktp_rkt;
        rkmessage->partition          = rktp->rktp_partition;
        rkmessage->offset             = offset;
        return rkmessage;
}

/**
 * @brief Destroy a mock rd_kafka_message_t.
 */
static void ut_ack_destroy_message(rd_kafka_message_t *rkmessage) {
        if (rkmessage)
                rd_free(rkmessage);
}

/**
 * @brief Test rd_kafka_share_acknowledge() - Basic ACCEPT acknowledgement.
 *
 * Verifies that rd_kafka_share_acknowledge() correctly updates an offset from
 * ACQUIRED to ACCEPT state, and that adjacent offsets remain unchanged.
 */

static int ut_case_acknowledge_accept(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_ack_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        rd_kafka_toppar_t *rktp = ut_ack_create_toppar(rk, "T1", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        /* Add partition with offsets 0-9 in ACQUIRED state */
        ut_ack_add_partition(rkshare, "T1", 0, 0, 9);

        /* Create a mock message for offset 5 */
        rd_kafka_message_t *msg = ut_ack_create_message(rktp, 5);

        /* Verify offset 5 is currently ACQUIRED */
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 5) ==
                         RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                     "offset 5 should be ACQUIRED before acknowledge");

        /* Call rd_kafka_share_acknowledge */
        rd_kafka_resp_err_t err = rd_kafka_share_acknowledge(rkshare, msg);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                     "acknowledge failed: %s", rd_kafka_err2str(err));

        /* Verify offset 5 is now ACCEPT */
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 5) ==
                         RD_KAFKA_SHARE_ACK_TYPE_ACCEPT,
                     "offset 5 should be ACCEPT after acknowledge");

        /* Verify other offsets are still ACQUIRED */
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 4) ==
                         RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                     "offset 4 should still be ACQUIRED");
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 6) ==
                         RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                     "offset 6 should still be ACQUIRED");

        ut_ack_destroy_message(msg);
        ut_ack_destroy_toppar(rktp);
        ut_ack_destroy_rkshare(rkshare);

        RD_UT_PASS();
}

/**
 * @brief Test rd_kafka_share_acknowledge_type() - Acknowledge with various
 * types.
 *
 * Tests that rd_kafka_share_acknowledge_type() correctly updates the offset
 * to the specified type (REJECT or RELEASE).
 */

static int ut_case_acknowledge_type_reject(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_ack_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        rd_kafka_toppar_t *rktp = ut_ack_create_toppar(rk, "T1", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        ut_ack_add_partition(rkshare, "T1", 0, 0, 9);

        rd_kafka_message_t *msg = ut_ack_create_message(rktp, 3);

        /* Acknowledge with REJECT */
        rd_kafka_resp_err_t err = rd_kafka_share_acknowledge_type(
            rkshare, msg, RD_KAFKA_SHARE_ACK_TYPE_REJECT);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                     "acknowledge_type REJECT failed: %s",
                     rd_kafka_err2str(err));

        /* Verify type changed to REJECT */
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 3) ==
                         RD_KAFKA_SHARE_ACK_TYPE_REJECT,
                     "offset 3 should be REJECT");

        ut_ack_destroy_message(msg);
        ut_ack_destroy_toppar(rktp);
        ut_ack_destroy_rkshare(rkshare);

        RD_UT_PASS();
}

/**
 * @brief Test rd_kafka_share_acknowledge_type() with RELEASE type.
 */
static int ut_case_acknowledge_type_release(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_ack_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        rd_kafka_toppar_t *rktp = ut_ack_create_toppar(rk, "T1", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        ut_ack_add_partition(rkshare, "T1", 0, 0, 9);

        rd_kafka_message_t *msg = ut_ack_create_message(rktp, 7);

        /* Acknowledge with RELEASE */
        rd_kafka_resp_err_t err = rd_kafka_share_acknowledge_type(
            rkshare, msg, RD_KAFKA_SHARE_ACK_TYPE_RELEASE);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                     "acknowledge_type RELEASE failed: %s",
                     rd_kafka_err2str(err));

        /* Verify type changed to RELEASE */
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 7) ==
                         RD_KAFKA_SHARE_ACK_TYPE_RELEASE,
                     "offset 7 should be RELEASE");

        ut_ack_destroy_message(msg);
        ut_ack_destroy_toppar(rktp);
        ut_ack_destroy_rkshare(rkshare);

        RD_UT_PASS();
}

/**
 * @brief Test rd_kafka_share_acknowledge_offset() - Error record
 * acknowledgement.
 *
 * Tests that rd_kafka_share_acknowledge_offset() can acknowledge error records.
 * This API is only for error records, not delivered records.
 */

static int ut_case_acknowledge_offset_error_record(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_ack_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        /* Add error records (not delivered records) */
        ut_ack_add_partition_error(rkshare, "T1", 0, 100, 109);

        /* Acknowledge error offset 105 with REJECT (changing from RELEASE) */
        rd_kafka_resp_err_t err = rd_kafka_share_acknowledge_offset(
            rkshare, "T1", 0, 105, RD_KAFKA_SHARE_ACK_TYPE_REJECT);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                     "acknowledge_offset REJECT failed: %s",
                     rd_kafka_err2str(err));

        /* Verify type changed to REJECT */
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 105) ==
                         RD_KAFKA_SHARE_ACK_TYPE_REJECT,
                     "offset 105 should be REJECT");

        ut_ack_destroy_rkshare(rkshare);

        RD_UT_PASS();
}

/**
 * @brief Test rd_kafka_share_acknowledge_offset() with multiple error records.
 *
 * Tests acknowledging multiple error offsets with different types and verifies
 * that unacknowledged offsets retain their initial state.
 */
static int ut_case_acknowledge_offset_multiple_errors(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_ack_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        /* Add error records */
        ut_ack_add_partition_error(rkshare, "T1", 0, 0, 9);

        /* Acknowledge multiple error offsets with RELEASE/REJECT */
        rd_kafka_resp_err_t err;

        err = rd_kafka_share_acknowledge_offset(
            rkshare, "T1", 0, 0, RD_KAFKA_SHARE_ACK_TYPE_RELEASE);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR, "ack offset 0 failed");

        err = rd_kafka_share_acknowledge_offset(rkshare, "T1", 0, 1,
                                                RD_KAFKA_SHARE_ACK_TYPE_REJECT);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR, "ack offset 1 failed");

        err = rd_kafka_share_acknowledge_offset(rkshare, "T1", 0, 2,
                                                RD_KAFKA_SHARE_ACK_TYPE_REJECT);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR, "ack offset 2 failed");

        err = rd_kafka_share_acknowledge_offset(
            rkshare, "T1", 0, 5, RD_KAFKA_SHARE_ACK_TYPE_RELEASE);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR, "ack offset 5 failed");

        /* Verify all types */
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 0) ==
                         RD_KAFKA_SHARE_ACK_TYPE_RELEASE,
                     "offset 0 should be RELEASE");
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 1) ==
                         RD_KAFKA_SHARE_ACK_TYPE_REJECT,
                     "offset 1 should be REJECT");
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 2) ==
                         RD_KAFKA_SHARE_ACK_TYPE_REJECT,
                     "offset 2 should be REJECT");
        /* Offset 3 was not acknowledged, should still be RELEASE (initial error
         * state) */
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 3) ==
                         RD_KAFKA_SHARE_INTERNAL_ACK_RELEASE,
                     "offset 3 should still be RELEASE");
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 5) ==
                         RD_KAFKA_SHARE_ACK_TYPE_RELEASE,
                     "offset 5 should be RELEASE");

        ut_ack_destroy_rkshare(rkshare);

        RD_UT_PASS();
}

/**
 * @brief Test error case - Partition not found.
 *
 * Verifies that acknowledging a partition not in the inflight map returns
 * RD_KAFKA_RESP_ERR__INVALID_ARG error. Tests both wrong partition
 * number and non-existent topic using offset-based API with error records.
 */

static int ut_case_error_partition_not_found(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_ack_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        /* Add error records for T1-0, but try to acknowledge T1-1 */
        ut_ack_add_partition_error(rkshare, "T1", 0, 0, 9);

        rd_kafka_resp_err_t err = rd_kafka_share_acknowledge_offset(
            rkshare, "T1", 1, 5, RD_KAFKA_SHARE_ACK_TYPE_RELEASE);

        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                     "expected INVALID_ARG, got %s", rd_kafka_err2str(err));

        /* Try non-existent topic */
        err = rd_kafka_share_acknowledge_offset(
            rkshare, "T2", 0, 5, RD_KAFKA_SHARE_ACK_TYPE_RELEASE);

        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                     "expected INVALID_ARG for T2, got %s",
                     rd_kafka_err2str(err));

        ut_ack_destroy_rkshare(rkshare);

        RD_UT_PASS();
}

/**
 * @brief Test error case - Offset not found (error records).
 *
 * Verifies that acknowledging an offset outside the error record range returns
 * RD_KAFKA_RESP_ERR__INVALID_ARG error. Tests offsets both before and after
 * the error record range.
 */

static int ut_case_error_offset_not_found(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_ack_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        /* Add error records with offsets 10-19 */
        ut_ack_add_partition_error(rkshare, "T1", 0, 10, 19);

        /* Try to acknowledge offset 5 (before range) */
        rd_kafka_resp_err_t err = rd_kafka_share_acknowledge_offset(
            rkshare, "T1", 0, 5, RD_KAFKA_SHARE_ACK_TYPE_RELEASE);

        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                     "expected INVALID_ARG for offset 5, got %s",
                     rd_kafka_err2str(err));

        /* Try to acknowledge offset 25 (after range) */
        err = rd_kafka_share_acknowledge_offset(
            rkshare, "T1", 0, 25, RD_KAFKA_SHARE_ACK_TYPE_RELEASE);

        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                     "expected INVALID_ARG for offset 25, got %s",
                     rd_kafka_err2str(err));

        ut_ack_destroy_rkshare(rkshare);

        RD_UT_PASS();
}

/**
 * @brief Test re-acknowledgement with record-based APIs.
 *
 * Verifies that re-acknowledging a delivered record with record-based APIs
 * succeeds and updates the type. This is allowed before commit.
 */

static int ut_case_reacknowledge_delivered(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_ack_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        rd_kafka_toppar_t *rktp = ut_ack_create_toppar(rk, "T1", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        ut_ack_add_partition(rkshare, "T1", 0, 0, 9);

        rd_kafka_message_t *msg = ut_ack_create_message(rktp, 5);

        /* First acknowledge with ACCEPT */
        rd_kafka_resp_err_t err = rd_kafka_share_acknowledge_type(
            rkshare, msg, RD_KAFKA_SHARE_ACK_TYPE_ACCEPT);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                     "first acknowledge failed: %s", rd_kafka_err2str(err));
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 5) ==
                         RD_KAFKA_SHARE_ACK_TYPE_ACCEPT,
                     "offset 5 should be ACCEPT");

        /* Re-acknowledge with REJECT - should succeed  */
        err = rd_kafka_share_acknowledge_type(rkshare, msg,
                                              RD_KAFKA_SHARE_ACK_TYPE_REJECT);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                     "re-acknowledge should succeed, got %s",
                     rd_kafka_err2str(err));

        /* Verify type changed to REJECT */
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 5) ==
                         RD_KAFKA_SHARE_ACK_TYPE_REJECT,
                     "offset 5 should be REJECT after re-acknowledge");

        /* Re-acknowledge again with RELEASE */
        err = rd_kafka_share_acknowledge_type(rkshare, msg,
                                              RD_KAFKA_SHARE_ACK_TYPE_RELEASE);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                     "second re-acknowledge should succeed");
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 5) ==
                         RD_KAFKA_SHARE_ACK_TYPE_RELEASE,
                     "offset 5 should be RELEASE after second re-acknowledge");

        ut_ack_destroy_message(msg);
        ut_ack_destroy_toppar(rktp);
        ut_ack_destroy_rkshare(rkshare);

        RD_UT_PASS();
}

/**
 * @brief Test re-acknowledgement with offset-based API for error records.
 *
 * Verifies that re-acknowledging an error record with offset-based API
 * succeeds and updates the type. Error records are identified by is_error flag,
 * so they can always be re-acknowledged via offset-based API.
 */

static int ut_case_reacknowledge_error(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_ack_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        /* Add error records (in RELEASE state, is_error=true) */
        ut_ack_add_partition_error(rkshare, "T1", 0, 0, 9);

        /* First acknowledge - change to REJECT */
        rd_kafka_resp_err_t err = rd_kafka_share_acknowledge_offset(
            rkshare, "T1", 0, 5, RD_KAFKA_SHARE_ACK_TYPE_REJECT);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                     "first acknowledge failed: %s", rd_kafka_err2str(err));
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 5) ==
                         RD_KAFKA_SHARE_ACK_TYPE_REJECT,
                     "offset 5 should be REJECT");

        /* Re-acknowledge with RELEASE - should succeed */
        err = rd_kafka_share_acknowledge_offset(
            rkshare, "T1", 0, 5, RD_KAFKA_SHARE_ACK_TYPE_RELEASE);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                     "re-acknowledge with RELEASE should succeed, got %s",
                     rd_kafka_err2str(err));
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 5) ==
                         RD_KAFKA_SHARE_ACK_TYPE_RELEASE,
                     "offset 5 should be RELEASE after re-acknowledge");

        /* Re-acknowledge with ACCEPT - should succeed (any type allowed) */
        err = rd_kafka_share_acknowledge_offset(rkshare, "T1", 0, 5,
                                                RD_KAFKA_SHARE_ACK_TYPE_ACCEPT);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                     "re-acknowledge with ACCEPT should succeed, got %s",
                     rd_kafka_err2str(err));
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 5) ==
                         RD_KAFKA_SHARE_ACK_TYPE_ACCEPT,
                     "offset 5 should be ACCEPT after re-acknowledge");

        /* Even after ACCEPT, offset-based API should STILL work (is_error=true)
         */
        err = rd_kafka_share_acknowledge_offset(rkshare, "T1", 0, 5,
                                                RD_KAFKA_SHARE_ACK_TYPE_REJECT);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                     "re-acknowledge after ACCEPT should succeed "
                     "(is_error=true), got %s",
                     rd_kafka_err2str(err));
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 5) ==
                         RD_KAFKA_SHARE_ACK_TYPE_REJECT,
                     "offset 5 should be REJECT after final re-acknowledge");

        ut_ack_destroy_rkshare(rkshare);

        RD_UT_PASS();
}

/**
 * @brief Test error case - GAP records cannot be acknowledged.
 *
 * Verifies that GAP records cannot be acknowledged by any API.
 */

static int ut_case_error_gap_record(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_ack_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        rd_kafka_toppar_t *rktp = ut_ack_create_toppar(rk, "T1", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        ut_ack_add_partition(rkshare, "T1", 0, 0, 9);

        /* Set offset 5 as a GAP record */
        ut_ack_set_gap(rkshare, "T1", 0, 5);

        rd_kafka_message_t *msg = ut_ack_create_message(rktp, 5);

        /* Try to acknowledge GAP with record-based API - should fail */
        rd_kafka_resp_err_t err = rd_kafka_share_acknowledge(rkshare, msg);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR__STATE,
                     "expected STATE error for GAP record, got %s",
                     rd_kafka_err2str(err));

        /* Verify GAP record is unchanged */
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 5) ==
                         RD_KAFKA_SHARE_INTERNAL_ACK_GAP,
                     "offset 5 should still be GAP");

        ut_ack_destroy_message(msg);
        ut_ack_destroy_toppar(rktp);
        ut_ack_destroy_rkshare(rkshare);

        RD_UT_PASS();
}

/**
 * @brief Test error case - Offset-based API fails on delivered records.
 *
 * Verifies that the offset-based API cannot be used to acknowledge
 * delivered records - it's only for error records.
 */

static int ut_case_error_offset_api_on_delivered(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_ack_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        /* Add delivered records (not error records) */
        ut_ack_add_partition(rkshare, "T1", 0, 0, 9);

        /* Try to use offset-based API on delivered record - should fail */
        rd_kafka_resp_err_t err = rd_kafka_share_acknowledge_offset(
            rkshare, "T1", 0, 5, RD_KAFKA_SHARE_ACK_TYPE_RELEASE);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR__STATE,
                     "expected STATE error for delivered record, got %s",
                     rd_kafka_err2str(err));

        /* Verify record is unchanged */
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 5) ==
                         RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                     "offset 5 should still be ACQUIRED");

        ut_ack_destroy_rkshare(rkshare);

        RD_UT_PASS();
}

/**
 * @brief Test error case - Record-based API fails on error records.
 *
 * Verifies that record-based APIs cannot be used to acknowledge
 * error records - they're only for delivered records.
 */

static int ut_case_error_record_api_on_error(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_ack_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        rd_kafka_toppar_t *rktp = ut_ack_create_toppar(rk, "T1", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        /* Add error records (not delivered records) */
        ut_ack_add_partition_error(rkshare, "T1", 0, 0, 9);

        rd_kafka_message_t *msg = ut_ack_create_message(rktp, 5);

        /* Try to use record-based API on error record - should fail */
        rd_kafka_resp_err_t err = rd_kafka_share_acknowledge(rkshare, msg);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR__STATE,
                     "expected STATE error for error record, got %s",
                     rd_kafka_err2str(err));

        /* Verify record is unchanged (still RELEASE - initial error state) */
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 5) ==
                         RD_KAFKA_SHARE_INTERNAL_ACK_RELEASE,
                     "offset 5 should still be RELEASE");

        ut_ack_destroy_message(msg);
        ut_ack_destroy_toppar(rktp);
        ut_ack_destroy_rkshare(rkshare);

        RD_UT_PASS();
}

/**
 * @brief Test error case - Invalid parameters (NULL).
 *
 * Verifies that NULL rkshare, message, or topic parameters return
 * RD_KAFKA_RESP_ERR__INVALID_ARG error.
 */

static int ut_case_error_null_parameters(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_ack_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        rd_kafka_toppar_t *rktp = ut_ack_create_toppar(rk, "T1", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        rd_kafka_message_t *msg = ut_ack_create_message(rktp, 5);

        /* Test NULL rkshare */
        rd_kafka_resp_err_t err = rd_kafka_share_acknowledge(NULL, msg);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                     "expected INVALID_ARG for NULL rkshare, got %s",
                     rd_kafka_err2str(err));

        /* Test NULL message */
        err = rd_kafka_share_acknowledge(rkshare, NULL);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                     "expected INVALID_ARG for NULL message, got %s",
                     rd_kafka_err2str(err));

        /* Test NULL topic in acknowledge_offset */
        err = rd_kafka_share_acknowledge_offset(rkshare, NULL, 0, 5,
                                                RD_KAFKA_SHARE_ACK_TYPE_ACCEPT);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                     "expected INVALID_ARG for NULL topic, got %s",
                     rd_kafka_err2str(err));

        ut_ack_destroy_message(msg);
        ut_ack_destroy_toppar(rktp);
        ut_ack_destroy_rkshare(rkshare);

        RD_UT_PASS();
}

/**
 * @brief Test error case - Invalid acknowledgement type.
 *
 * Verifies that invalid acknowledgement types (e.g., GAP which is
 * internal-only, or arbitrary values like 99) return
 * RD_KAFKA_RESP_ERR__INVALID_ARG error. Also verifies the offset remains in
 * ACQUIRED state after the failed attempt.
 */

static int ut_case_error_invalid_type(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_ack_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        rd_kafka_toppar_t *rktp = ut_ack_create_toppar(rk, "T1", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        ut_ack_add_partition(rkshare, "T1", 0, 0, 9);

        rd_kafka_message_t *msg = ut_ack_create_message(rktp, 5);

        /* Test invalid type value (e.g., 99) */
        rd_kafka_resp_err_t err = rd_kafka_share_acknowledge_type(
            rkshare, msg, (rd_kafka_share_ack_type_t)99);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                     "expected INVALID_ARG for invalid type, got %s",
                     rd_kafka_err2str(err));

        /* Test type 0 (GAP - not allowed in public API) */
        err = rd_kafka_share_acknowledge_type(rkshare, msg,
                                              (rd_kafka_share_ack_type_t)0);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                     "expected INVALID_ARG for GAP type, got %s",
                     rd_kafka_err2str(err));

        /* Verify offset is still ACQUIRED (not modified) */
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 5) ==
                         RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                     "offset 5 should still be ACQUIRED after invalid type");

        ut_ack_destroy_message(msg);
        ut_ack_destroy_toppar(rktp);
        ut_ack_destroy_rkshare(rkshare);

        RD_UT_PASS();
}

/**
 * @brief Test offset-based API accepts all types (ACCEPT, RELEASE, REJECT).
 *
 * Verifies that the offset-based API allows any acknowledgement type to be
 * sent for error records (records in RELEASE/REJECT state).
 */

static int ut_case_offset_api_all_types(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_ack_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        /* Add error records (in RELEASE state) */
        ut_ack_add_partition_error(rkshare, "T1", 0, 0, 9);

        /* ACCEPT type should succeed on error records */
        rd_kafka_resp_err_t err = rd_kafka_share_acknowledge_offset(
            rkshare, "T1", 0, 5, RD_KAFKA_SHARE_ACK_TYPE_ACCEPT);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                     "ACCEPT type should succeed, got %s",
                     rd_kafka_err2str(err));

        /* Use offset 6 for REJECT */
        err = rd_kafka_share_acknowledge_offset(rkshare, "T1", 0, 6,
                                                RD_KAFKA_SHARE_ACK_TYPE_REJECT);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                     "REJECT type should succeed, got %s",
                     rd_kafka_err2str(err));

        /* Use offset 7 for RELEASE */
        err = rd_kafka_share_acknowledge_offset(
            rkshare, "T1", 0, 7, RD_KAFKA_SHARE_ACK_TYPE_RELEASE);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                     "RELEASE type should succeed, got %s",
                     rd_kafka_err2str(err));

        /* Verify all types */
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 5) ==
                         RD_KAFKA_SHARE_ACK_TYPE_ACCEPT,
                     "offset 5 should be ACCEPT");
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 6) ==
                         RD_KAFKA_SHARE_ACK_TYPE_REJECT,
                     "offset 6 should be REJECT");
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 7) ==
                         RD_KAFKA_SHARE_ACK_TYPE_RELEASE,
                     "offset 7 should be RELEASE");

        ut_ack_destroy_rkshare(rkshare);

        RD_UT_PASS();
}

/**
 * @brief Test error case - Implicit acknowledgement mode.
 *
 * Verifies that all three acknowledge APIs return
 * RD_KAFKA_RESP_ERR__INVALID_ARG when called in implicit acknowledgement mode
 * (share.acknowledgement.mode=implicit). The explicit acknowledge APIs are only
 * valid in explicit mode.
 */

static int ut_case_error_implicit_mode(rd_kafka_t *rk) {
        /* Create rkshare with implicit mode
         * (share.acknowledgement.mode=implicit) */
        rd_kafka_share_t *rkshare = ut_ack_create_rkshare_implicit(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        rd_kafka_toppar_t *rktp = ut_ack_create_toppar(rk, "T1", 0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        ut_ack_add_partition(rkshare, "T1", 0, 0, 9);

        rd_kafka_message_t *msg = ut_ack_create_message(rktp, 5);

        /* Test rd_kafka_share_acknowledge in implicit mode */
        rd_kafka_resp_err_t err = rd_kafka_share_acknowledge(rkshare, msg);
        RD_UT_ASSERT(
            err == RD_KAFKA_RESP_ERR__INVALID_ARG,
            "expected INVALID_ARG for acknowledge in implicit mode, got %s",
            rd_kafka_err2str(err));

        /* Test rd_kafka_share_acknowledge_type in implicit mode */
        err = rd_kafka_share_acknowledge_type(rkshare, msg,
                                              RD_KAFKA_SHARE_ACK_TYPE_ACCEPT);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                     "expected INVALID_ARG for acknowledge_type in implicit "
                     "mode, got %s",
                     rd_kafka_err2str(err));

        /* Test rd_kafka_share_acknowledge_offset in implicit mode (with error
         * records) */
        ut_ack_add_partition_error(rkshare, "T1", 1, 0, 9);
        err = rd_kafka_share_acknowledge_offset(
            rkshare, "T1", 1, 5, RD_KAFKA_SHARE_ACK_TYPE_RELEASE);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                     "expected INVALID_ARG for acknowledge_offset in implicit "
                     "mode, got %s",
                     rd_kafka_err2str(err));

        /* Verify offset is still ACQUIRED (not modified) */
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 5) ==
                         RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                     "offset 5 should still be ACQUIRED in implicit mode");

        ut_ack_destroy_message(msg);
        ut_ack_destroy_toppar(rktp);
        ut_ack_destroy_rkshare(rkshare);

        RD_UT_PASS();
}

/**
 * @brief Test multiple partitions with record-based APIs.
 *
 * Tests acknowledging delivered records across multiple topics and partitions
 * using record-based APIs. Verifies that each partition's acknowledgements
 * are tracked independently.
 */

static int ut_case_acknowledge_multiple_partitions(rd_kafka_t *rk) {
        rd_kafka_share_t *rkshare = ut_ack_create_rkshare(rk);
        RD_UT_ASSERT(rkshare != NULL, "rkshare alloc failed");

        /* Add multiple partitions with delivered records */
        ut_ack_add_partition(rkshare, "T1", 0, 0, 9);
        ut_ack_add_partition(rkshare, "T1", 1, 100, 109);
        ut_ack_add_partition(rkshare, "T2", 0, 50, 59);

        /* Create toppars for each partition */
        rd_kafka_toppar_t *rktp1_0 = ut_ack_create_toppar(rk, "T1", 0);
        rd_kafka_toppar_t *rktp1_1 = ut_ack_create_toppar(rk, "T1", 1);
        rd_kafka_toppar_t *rktp2_0 = ut_ack_create_toppar(rk, "T2", 0);

        rd_kafka_message_t *msg1 = ut_ack_create_message(rktp1_0, 5);
        rd_kafka_message_t *msg2 = ut_ack_create_message(rktp1_1, 105);
        rd_kafka_message_t *msg3 = ut_ack_create_message(rktp2_0, 55);

        rd_kafka_resp_err_t err;

        /* Acknowledge across partitions using record-based APIs */
        err = rd_kafka_share_acknowledge_type(rkshare, msg1,
                                              RD_KAFKA_SHARE_ACK_TYPE_ACCEPT);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR, "T1-0 offset 5 failed");

        err = rd_kafka_share_acknowledge_type(rkshare, msg2,
                                              RD_KAFKA_SHARE_ACK_TYPE_REJECT);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                     "T1-1 offset 105 failed");

        err = rd_kafka_share_acknowledge_type(rkshare, msg3,
                                              RD_KAFKA_SHARE_ACK_TYPE_RELEASE);
        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                     "T2-0 offset 55 failed");

        /* Verify each partition independently */
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 5) ==
                         RD_KAFKA_SHARE_ACK_TYPE_ACCEPT,
                     "T1-0 offset 5 should be ACCEPT");
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 1, 105) ==
                         RD_KAFKA_SHARE_ACK_TYPE_REJECT,
                     "T1-1 offset 105 should be REJECT");
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T2", 0, 55) ==
                         RD_KAFKA_SHARE_ACK_TYPE_RELEASE,
                     "T2-0 offset 55 should be RELEASE");

        /* Verify other offsets unchanged */
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 0, 4) ==
                         RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                     "T1-0 offset 4 should be ACQUIRED");
        RD_UT_ASSERT(ut_ack_get_type(rkshare, "T1", 1, 104) ==
                         RD_KAFKA_SHARE_INTERNAL_ACK_ACQUIRED,
                     "T1-1 offset 104 should be ACQUIRED");

        ut_ack_destroy_message(msg1);
        ut_ack_destroy_message(msg2);
        ut_ack_destroy_message(msg3);
        ut_ack_destroy_toppar(rktp1_0);
        ut_ack_destroy_toppar(rktp1_1);
        ut_ack_destroy_toppar(rktp2_0);
        ut_ack_destroy_rkshare(rkshare);

        RD_UT_PASS();
}


/**
 * @brief Main entry point for Share Consumer acknowledge API unit tests.
 */
int unittest_share_acknowledge(void) {
        rd_kafka_t *rk;

        RD_UT_SAY("===============================================");
        RD_UT_SAY("Share Consumer Acknowledge API Unit Tests");
        RD_UT_SAY("===============================================");

        rk = ut_ack_create_rk();
        RD_UT_ASSERT(rk != NULL, "Failed to create rd_kafka_t");

        /* Record-based API tests (delivered records) */
        RD_UT_SAY("Testing rd_kafka_share_acknowledge() (ACCEPT)...");
        if (ut_case_acknowledge_accept(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        RD_UT_SAY("Testing rd_kafka_share_acknowledge_type() (REJECT)...");
        if (ut_case_acknowledge_type_reject(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        RD_UT_SAY("Testing rd_kafka_share_acknowledge_type() (RELEASE)...");
        if (ut_case_acknowledge_type_release(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        /* Offset-based API tests (error records) */
        RD_UT_SAY(
            "Testing rd_kafka_share_acknowledge_offset() (error record)...");
        if (ut_case_acknowledge_offset_error_record(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        RD_UT_SAY(
            "Testing rd_kafka_share_acknowledge_offset() (multiple errors)...");
        if (ut_case_acknowledge_offset_multiple_errors(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        /* Re-acknowledgement tests */
        RD_UT_SAY("Testing re-acknowledgement of delivered records...");
        if (ut_case_reacknowledge_delivered(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        RD_UT_SAY("Testing re-acknowledgement of error records...");
        if (ut_case_reacknowledge_error(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        /* Error case tests */
        RD_UT_SAY("Testing error: partition not found...");
        if (ut_case_error_partition_not_found(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        RD_UT_SAY("Testing error: offset not found...");
        if (ut_case_error_offset_not_found(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        RD_UT_SAY("Testing error: GAP records cannot be acknowledged...");
        if (ut_case_error_gap_record(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        RD_UT_SAY("Testing error: offset API on delivered records...");
        if (ut_case_error_offset_api_on_delivered(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        RD_UT_SAY("Testing error: record API on error records...");
        if (ut_case_error_record_api_on_error(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        RD_UT_SAY("Testing error: NULL parameters...");
        if (ut_case_error_null_parameters(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        RD_UT_SAY("Testing error: invalid type...");
        if (ut_case_error_invalid_type(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        RD_UT_SAY("Testing offset API accepts all types...");
        if (ut_case_offset_api_all_types(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        RD_UT_SAY("Testing error: implicit acknowledgement mode...");
        if (ut_case_error_implicit_mode(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        /* Multi-partition test */
        RD_UT_SAY("Testing multiple partitions...");
        if (ut_case_acknowledge_multiple_partitions(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        rd_kafka_destroy(rk);
        RD_UT_PASS();
}
