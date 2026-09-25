/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2026, Dave Protasowski
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

#include "test.h"

#include "../src/rdkafka_proto.h"

/**
 * @brief ListConsumerGroupOffsets must return coordinator errors rather than
 *        trying to refresh a nonexistent consumer group on an admin client.
 *        Also exercise a consumer with a group to retain the refresh path.
 */
static void do_test_admin_offset_fetch(rd_kafka_type_t type,
                                       rd_bool_t with_group) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        const char *topic    = "admin-offset-fetch";
        const char *group_id = "admin-offset-fetch-group";
        rd_kafka_queue_t *q;
        rd_kafka_AdminOptions_t *options;
        rd_kafka_ListConsumerGroupOffsets_t *request;
        rd_kafka_topic_partition_list_t *to_list;
        char errstr[512];
        size_t i;
        const rd_kafka_resp_err_t errors[] = {
            RD_KAFKA_RESP_ERR_NOT_COORDINATOR, RD_KAFKA_RESP_ERR_NO_ERROR,
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE,
            RD_KAFKA_RESP_ERR_NO_ERROR};

        SUB_TEST_QUICK("%s, group.id %s",
                       type == RD_KAFKA_PRODUCER ? "producer" : "consumer",
                       with_group ? "configured" : "unset");

        test_conf_init(&conf, NULL, 30);
        mcluster = test_mock_cluster_new(1, &bootstraps);
        TEST_CALL_ERR__(rd_kafka_mock_topic_create(mcluster, topic, 1, 1));
        TEST_CALL_ERR__(
            rd_kafka_mock_coordinator_set(mcluster, "group", group_id, 1));
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", with_group ? group_id : "");
        rk      = test_create_handle(type, conf);
        q       = rd_kafka_queue_new(rk);
        options = rd_kafka_AdminOptions_new(
            rk, RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS);
        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_request_timeout(
            options, tmout_multip(5000), errstr, sizeof(errstr)));

        to_list = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(to_list, topic, 0);
        request = rd_kafka_ListConsumerGroupOffsets_new(group_id, to_list);
        rd_kafka_topic_partition_list_destroy(to_list);

        for (i = 0; i < RD_ARRAYSIZE(errors); i++) {
                rd_kafka_event_t *event;
                const rd_kafka_ListConsumerGroupOffsets_result_t *result;

                TEST_SAY("ListConsumerGroupOffsets: injecting %s\n",
                         rd_kafka_err2name(errors[i]));
                rd_kafka_mock_push_request_errors(
                    mcluster, RD_KAFKAP_OffsetFetch, 1, errors[i]);
                rd_kafka_ListConsumerGroupOffsets(rk, &request, 1, options, q);
                event = rd_kafka_queue_poll(q, tmout_multip(10000));
                TEST_ASSERT(event, "Timed out waiting for admin result");
                result = rd_kafka_event_ListConsumerGroupOffsets_result(event);
                TEST_ASSERT(result,
                            "Expected ListConsumerGroupOffsets result, "
                            "got %s",
                            rd_kafka_event_name(event));
                TEST_ASSERT(rd_kafka_event_error(event) == errors[i],
                            "Expected %s, got %s: %s",
                            rd_kafka_err2name(errors[i]),
                            rd_kafka_err2name(rd_kafka_event_error(event)),
                            rd_kafka_event_error_string(event));

                if (!errors[i]) {
                        const rd_kafka_group_result_t **groups;
                        const rd_kafka_topic_partition_list_t *partitions;
                        size_t group_count;

                        groups =
                            rd_kafka_ListConsumerGroupOffsets_result_groups(
                                result, &group_count);
                        TEST_ASSERT(groups && group_count == 1,
                                    "Expected one group, got %" PRIusz,
                                    group_count);
                        TEST_ASSERT(
                            !strcmp(rd_kafka_group_result_name(groups[0]),
                                    group_id),
                            "Unexpected group in result");
                        TEST_ASSERT(!rd_kafka_group_result_error(groups[0]),
                                    "Unexpected group error");
                        partitions =
                            rd_kafka_group_result_partitions(groups[0]);
                        TEST_ASSERT(partitions && partitions->cnt == 1,
                                    "Expected one partition");
                        TEST_ASSERT(
                            !strcmp(partitions->elems[0].topic, topic) &&
                                partitions->elems[0].partition == 0,
                            "Unexpected topic-partition in result");
                        TEST_CALL_ERR__(partitions->elems[0].err);
                        TEST_ASSERT(
                            partitions->elems[0].offset ==
                                RD_KAFKA_OFFSET_INVALID,
                            "Expected no committed offset, got %" PRId64,
                            partitions->elems[0].offset);
                }
                rd_kafka_event_destroy(event);
        }

        rd_kafka_ListConsumerGroupOffsets_destroy(request);
        rd_kafka_AdminOptions_destroy(options);
        rd_kafka_queue_destroy(q);
        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);
        SUB_TEST_PASS();
}

int main_0193_admin_offset_fetch_mock(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);

        do_test_admin_offset_fetch(RD_KAFKA_PRODUCER, rd_false);
        do_test_admin_offset_fetch(RD_KAFKA_CONSUMER, rd_false);
        do_test_admin_offset_fetch(RD_KAFKA_CONSUMER, rd_true);

        return 0;
}
