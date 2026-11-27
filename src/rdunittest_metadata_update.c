/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2026, Confluent Inc.
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
 * @name Unit tests for rd_kafka_metadata_update_op() and the metadata cache
 * @{
 */

#include "rd.h"
#include "rdunittest.h"
#include "rdkafka_int.h"
#include "rdkafka_metadata.h"
#include "rdkafka_request.h"


/**
 * @brief Counts the "not found in cache" warnings of
 *        rd_kafka_metadata_update_op() in the rd_atomic32_t opaque.
 */
static void ut_mdupd_log_cb(const rd_kafka_t *rk,
                            int level,
                            const char *fac,
                            const char *buf) {
        rd_atomic32_t *not_found_cnt = rd_kafka_opaque(rk);

        if (level <= LOG_WARNING && !strcmp(fac, "METADATAUPDATE") &&
            strstr(buf, "not found in cache"))
                rd_atomic32_add(not_found_cnt, 1);
}


/**
 * @brief Seed the metadata cache with \p partition_cnt partitions for
 *        \p topic, each with leader \p leader_id. Their ids are the sorted
 *        \p partition_ids, or 0 to partition_cnt-1 if NULL. Mirrors
 *        rd_ut_kafka_topic_set_topic_exists().
 */
static void ut_mdupd_seed_cache(rd_kafka_t *rk,
                                const char *topic,
                                const int32_t *partition_ids,
                                int partition_cnt,
                                int32_t leader_id) {
        rd_kafka_metadata_partition_internal_t *mdpi =
            rd_calloc(partition_cnt, sizeof(*mdpi));
        rd_kafka_metadata_topic_internal_t mdti = {.partitions = mdpi};
        struct rd_kafka_metadata_topic mdt;
        int i;

        memset(&mdt, 0, sizeof(mdt));
        mdt.topic         = (char *)topic;
        mdt.partition_cnt = partition_cnt;
        mdt.partitions    = rd_alloca(sizeof(*mdt.partitions) * partition_cnt);
        for (i = 0; i < partition_cnt; i++) {
                int32_t id = partition_ids ? partition_ids[i] : i;
                memset(&mdt.partitions[i], 0, sizeof(mdt.partitions[i]));
                mdt.partitions[i].id     = id;
                mdt.partitions[i].leader = leader_id;
                mdpi[i].id               = id;
                mdpi[i].leader_epoch     = 0;
        }

        rd_kafka_wrlock(rk);
        rd_kafka_metadata_cache_topic_update(rk, &mdt, &mdti, rd_true, rd_false,
                                             rd_true);
        rd_kafka_wrunlock(rk);
        rd_free(mdpi);
}


/**
 * @brief Run a one-partition leader change for \p topic through
 *        rd_kafka_metadata_update_op(), built the way the Produce and Fetch
 *        response handlers build it.
 */
static void ut_mdupd_run(rd_kafka_t *rk,
                         char *topic,
                         int32_t partition_id,
                         int32_t leader_id) {
        rd_tmpabuf_t tbuf;
        rd_kafka_metadata_internal_t *mdi;
        rd_kafkap_CurrentLeader_t CurrentLeader = {.LeaderId    = leader_id,
                                                   .LeaderEpoch = 1};

        rd_tmpabuf_new(&tbuf, 0, rd_true /*assert on fail*/);
        rd_tmpabuf_add_alloc(&tbuf, sizeof(*mdi));
        rd_kafkap_leader_discovery_tmpabuf_add_alloc_topics(&tbuf, 1);
        rd_kafkap_leader_discovery_tmpabuf_add_alloc_topic(&tbuf, topic, 1);
        rd_tmpabuf_finalize(&tbuf);

        mdi = rd_tmpabuf_alloc(&tbuf, sizeof(*mdi));
        rd_kafkap_leader_discovery_metadata_init(mdi, -1);
        rd_kafkap_leader_discovery_set_topic_cnt(&tbuf, mdi, 1);
        rd_kafkap_leader_discovery_set_topic(&tbuf, mdi, 0, RD_KAFKA_UUID_ZERO,
                                             topic, 1);
        rd_kafkap_leader_discovery_set_CurrentLeader(
            &tbuf, mdi, 0, 0, partition_id, &CurrentLeader);

        rd_kafka_metadata_update_op(rk, mdi);

        rd_tmpabuf_destroy(&tbuf);
}


/**
 * @brief A partition id outside [0, partition_cnt) in a metadata update
 *        must be skipped, leaving the cache untouched.
 */
static int unittest_metadata_update_partition_out_of_range(void) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_atomic32_t not_found_cnt;
        char errstr[256];
        char *topic          = "ut-md-upd";
        const int32_t leader = 2;
        const struct rd_kafka_metadata_cache_entry *rkmce;
        int32_t leaders[4] = {-1, -1, -1, -1};
        int partition_cnt  = -1;
        int i, cnt_valid, cnt_out_of_range;
        struct rd_kafka_metadata_broker mdb = {
            .id = leader, .host = (char *)"localhost", .port = 9092};

        RD_UT_SAY(
            "Verifying rd_kafka_metadata_update_op() skips partition ids "
            "outside the cached partitions");

        rd_atomic32_init(&not_found_cnt, 0);
        conf = rd_kafka_conf_new();
        rd_kafka_conf_set_log_cb(conf, ut_mdupd_log_cb);
        rd_kafka_conf_set_opaque(conf, &not_found_cnt);
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        RD_UT_ASSERT(rk != NULL, "rd_kafka_new failed: %s", errstr);

        /* Cache: topic with 4 partitions, current leader broker id 1. */
        ut_mdupd_seed_cache(rk, topic, NULL, 4, 1);

        /* The update's leader must be a known broker. */
        rd_kafka_broker_update(rk, rk->rk_conf.security_protocol, &mdb, NULL);

        /* A valid partition id is updated. */
        ut_mdupd_run(rk, topic, 2, leader);
        cnt_valid = rd_atomic32_get(&not_found_cnt);

        /* Partition ids outside [0, 4) are skipped. */
        ut_mdupd_run(rk, topic, -1, leader);
        ut_mdupd_run(rk, topic, 4, leader);
        cnt_out_of_range = rd_atomic32_get(&not_found_cnt) - cnt_valid;

        rd_kafka_rdlock(rk);
        rkmce = rd_kafka_metadata_cache_find(rk, topic, 1);
        if (rkmce) {
                partition_cnt = rkmce->rkmce_mtopic.partition_cnt;
                for (i = 0; i < partition_cnt && i < 4; i++)
                        leaders[i] = rkmce->rkmce_mtopic.partitions[i].leader;
        }
        rd_kafka_rdunlock(rk);

        rd_kafka_destroy(rk);

        RD_UT_ASSERT(cnt_valid == 0,
                     "expected partition 2 to be found in cache, got %d "
                     "warnings",
                     cnt_valid);
        RD_UT_ASSERT(cnt_out_of_range == 2,
                     "expected partitions -1 and 4 not to be found in cache, "
                     "got %d warnings",
                     cnt_out_of_range);
        RD_UT_ASSERT(partition_cnt == 4, "expected 4 partitions, got %d",
                     partition_cnt);
        for (i = 0; i < 4; i++) {
                int32_t exp = i == 2 ? leader : 1;
                RD_UT_ASSERT(leaders[i] == exp,
                             "partition %d: expected leader %" PRId32
                             ", got %" PRId32,
                             i, exp, leaders[i]);
        }

        RD_UT_PASS();
}


/**
 * @brief rd_kafka_metadata_cache_topic_partition_get() must return the
 *        internal metadata at the partition's position in the cache entry,
 *        which is not its id when partition ids are sparse.
 */
static int unittest_metadata_cache_topic_partition_get_sparse_ids(void) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        char errstr[256];
        const char *topic             = "ut-md-cache-sparse";
        const int32_t partition_ids[] = {0, 2};
        const struct rd_kafka_metadata_cache_entry *rkmce;
        const rd_kafka_metadata_topic_t *mtopic;
        const rd_kafka_metadata_partition_t *mpart;
        const rd_kafka_metadata_partition_internal_t *mdpi;
        int found[2]          = {-1, -1};
        rd_bool_t mpart_ok[2] = {rd_false, rd_false};
        rd_bool_t mdpi_ok[2]  = {rd_false, rd_false};
        rd_bool_t cache_found = rd_false;
        int i;

        RD_UT_SAY(
            "Verifying rd_kafka_metadata_cache_topic_partition_get() "
            "returns the internal metadata of sparse partition ids");

        conf = rd_kafka_conf_new();
        rk   = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        RD_UT_ASSERT(rk != NULL, "rd_kafka_new failed: %s", errstr);

        /* Cache: topic with partitions 0 and 2. */
        ut_mdupd_seed_cache(rk, topic, partition_ids, 2, 1);

        rd_kafka_rdlock(rk);
        rkmce = rd_kafka_metadata_cache_find(rk, topic, 1);
        if (rkmce) {
                cache_found = rd_true;
                for (i = 0; i < 2; i++) {
                        found[i] = rd_kafka_metadata_cache_topic_partition_get(
                            rk, &mtopic, &mpart, &mdpi, topic, partition_ids[i],
                            1 /*valid*/);
                        mpart_ok[i] =
                            mpart == &rkmce->rkmce_mtopic.partitions[i];
                        mdpi_ok[i] =
                            mdpi ==
                            &rkmce->rkmce_metadata_internal_topic.partitions[i];
                }
        }
        rd_kafka_rdunlock(rk);

        rd_kafka_destroy(rk);

        RD_UT_ASSERT(cache_found, "expected topic %s in cache", topic);
        for (i = 0; i < 2; i++) {
                RD_UT_ASSERT(found[i] == 1,
                             "partition %" PRId32
                             ": expected topic and partition to be found, "
                             "got %d",
                             partition_ids[i], found[i]);
                RD_UT_ASSERT(mpart_ok[i],
                             "partition %" PRId32
                             ": expected the partition metadata at "
                             "position %d",
                             partition_ids[i], i);
                RD_UT_ASSERT(mdpi_ok[i],
                             "partition %" PRId32
                             ": expected the internal partition metadata at "
                             "position %d",
                             partition_ids[i], i);
        }

        RD_UT_PASS();
}


int unittest_metadata_update(void) {
        int fails = 0;

        fails += unittest_metadata_update_partition_out_of_range();
        fails += unittest_metadata_cache_topic_partition_get_sparse_ids();

        return fails;
}

/**@}*/
