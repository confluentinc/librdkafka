/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2023, Confluent Inc.
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
#include "../src/rdkafka_mock.h"

const int32_t retry_ms     = 100;
const int32_t retry_max_ms = 1000;

static void free_mock_requests(rd_kafka_mock_request_t **requests,
                               size_t request_cnt) {
        size_t i;
        for (i = 0; i < request_cnt; i++)
                rd_kafka_mock_request_destroy(requests[i]);
        rd_free(requests);
}

/**
 * @brief Produce API test
 * We test the Metadata Update Operation being triggered via getting not leader error,
 * where the metadata cache is updated which makes further produce calls based on the new updated metadata cache
 * rather than making another metadata call to get the updates.
 */
static void test_produce(rd_kafka_mock_cluster_t *mcluster,
                         const char *topic,
                         rd_kafka_conf_t *conf) {
        rd_kafka_mock_request_t **requests   = NULL;
        size_t request_cnt                   = 0;
        rd_bool_t request_to_broker0 = rd_false;
        rd_bool_t request_to_broker1 = rd_false;
        rd_bool_t Metadata_request_made = rd_false;
        size_t i;
        rd_kafka_t *producer;
        rd_kafka_topic_t *rkt;
        SUB_TEST();
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);

        producer = test_create_handle(RD_KAFKA_PRODUCER, conf);
        rkt      = test_create_producer_topic(producer, topic, NULL);

        rd_sleep(10);
        SUB_TEST();
        /* Leader for both the partition is broker 1 */
        rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 1);
        rd_kafka_mock_partition_set_leader(mcluster, topic, 1, 1);
        rd_sleep(10);
        rd_kafka_mock_start_request_tracking(mcluster);
        rd_kafka_mock_clear_requests(mcluster);

        /* Produce to Partition 0 (with leader Broker 1) */
        test_produce_msgs(producer, rkt, 0, 0, 0, 1,
                          "hello", 5);
        
        /* Verify that the produce call is made to Broker 1 */
        requests = rd_kafka_mock_get_requests(mcluster, &request_cnt);
        for (i = 0; i < request_cnt; i++) {
                TEST_SAY("Broker Id : %d API Key : %d Timestamp : %" PRId64
                         "\n",
                         rd_kafka_mock_request_id(requests[i]),
                         rd_kafka_mock_request_api_key(requests[i]),
                         rd_kafka_mock_request_timestamp(requests[i]));

                if ((rd_kafka_mock_request_api_key(requests[i]) ==
                    RD_KAFKAP_Produce) && (rd_kafka_mock_request_id(requests[i]) == 0))
                        request_to_broker0 = rd_true;
        }
        free_mock_requests(requests, request_cnt);
        rd_kafka_mock_clear_requests(mcluster);
        rd_kafka_mock_stop_request_tracking(mcluster);
        TEST_ASSERT(
            (!request_to_broker0),
            "Produce Request should have been made to only Brokers 1.");
        rd_sleep(10);
        /* Change the leader for Partition 0 to Broker 0 */
        rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 0);
        /* Make the Produce call again to trigger metadata update and cache changes */
        /* Produce to Partition 0 (with leader Broker 0) */
        test_produce_msgs(producer, rkt, 0, 0, 0, 1,
                          "hello", 5);
        rd_sleep(10);
        rd_kafka_mock_start_request_tracking(mcluster);
        rd_kafka_mock_clear_requests(mcluster);
        
        /* Make the Produce call again to Partition 0 (with leader Broker 0) */
        test_produce_msgs(producer, rkt, 0, 0, 0, 1,
                          "hello", 5);
        
        /* Verify that the produce call is made to only broker 0 */
        requests = rd_kafka_mock_get_requests(mcluster, &request_cnt);
        for (i = 0; i < request_cnt; i++) {
                TEST_SAY("Broker Id : %d API Key : %d Timestamp : %" PRId64
                         "\n",
                         rd_kafka_mock_request_id(requests[i]),
                         rd_kafka_mock_request_api_key(requests[i]),
                         rd_kafka_mock_request_timestamp(requests[i]));
                if ((rd_kafka_mock_request_api_key(requests[i]) ==
                    RD_KAFKAP_Produce) && (rd_kafka_mock_request_id(requests[i]) == 1))
                        request_to_broker1 = rd_true;
                
                if (rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_Metadata)
                        Metadata_request_made = rd_true;
        }
        free_mock_requests(requests, request_cnt);
        rd_kafka_mock_clear_requests(mcluster);
        rd_kafka_mock_stop_request_tracking(mcluster);
        TEST_ASSERT(
            (!request_to_broker1) && (!Metadata_request_made),
            "No separate Metadata Request should have been made. Further Produce request should have been made to only Brokers 0.");
        rd_sleep(10);

        /* Make the Produce call to Partition 1 (with leader Broker 1) */
        test_produce_msgs(producer, rkt, 0, 1, 0, 1,
                          "hello", 5);
        
        /* Verify that the produce call is made to only broker 1 */
        requests = rd_kafka_mock_get_requests(mcluster, &request_cnt);
        for (i = 0; i < request_cnt; i++) {
                TEST_SAY("Broker Id : %d API Key : %d Timestamp : %" PRId64
                         "\n",
                         rd_kafka_mock_request_id(requests[i]),
                         rd_kafka_mock_request_api_key(requests[i]),
                         rd_kafka_mock_request_timestamp(requests[i]));
                if ((rd_kafka_mock_request_api_key(requests[i]) ==
                    RD_KAFKAP_Produce) && (rd_kafka_mock_request_id(requests[i]) == 0))
                        request_to_broker0 = rd_true;
                
                if (rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_Metadata)
                        Metadata_request_made = rd_true;
        }
        free_mock_requests(requests, request_cnt);
        rd_kafka_mock_clear_requests(mcluster);
        rd_kafka_mock_stop_request_tracking(mcluster);
        TEST_ASSERT(
            (!request_to_broker0) && (!Metadata_request_made),
            "No separate Metadata Request should have been made. Further Produce request should have been made to only Brokers 0.");
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(producer);
        SUB_TEST_PASS();
}

/**
 * @brief Fetch API test
 * We test the Metadata Update Operation being triggered via getting not leader error,
 * where the metadata cache is updated which makes further fetch calls based on the new updated metadata cache
 * rather than making another metadata call to get the updates.
 */
static void test_fetch(rd_kafka_mock_cluster_t *mcluster, const char *topic,
                                         rd_kafka_conf_t *conf) {
        rd_kafka_mock_request_t **requests   = NULL;
        size_t request_cnt                   = 0;
        rd_kafka_t *consumer;
        rd_kafka_message_t *rkm;
        rd_bool_t request_to_broker0 = rd_false;
        rd_bool_t request_to_broker1 = rd_false;
        rd_bool_t Metadata_request_made = rd_false;
        size_t i;
        consumer = test_create_consumer(topic, NULL, conf, NULL);
        test_consumer_subscribe(consumer, topic);

        rd_sleep(10);
        SUB_TEST();
        /* Leader for both the partition is broker 1 */
        rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 1);
        rd_kafka_mock_partition_set_leader(mcluster, topic, 1, 1);
        rd_sleep(10);
        rd_kafka_mock_start_request_tracking(mcluster);
        rd_kafka_mock_clear_requests(mcluster);

        rkm = rd_kafka_consumer_poll(consumer, 10 * 1000);
        if (rkm)
                rd_kafka_message_destroy(rkm);
        /* Verify that the fetch call is not made to broker 0 */
        requests = rd_kafka_mock_get_requests(mcluster, &request_cnt);
        for (i = 0; i < request_cnt; i++) {
                TEST_SAY("Broker Id : %d API Key : %d Timestamp : %" PRId64
                         "\n",
                         rd_kafka_mock_request_id(requests[i]),
                         rd_kafka_mock_request_api_key(requests[i]),
                         rd_kafka_mock_request_timestamp(requests[i]));

                if ((rd_kafka_mock_request_api_key(requests[i]) ==
                    RD_KAFKAP_Fetch) && (rd_kafka_mock_request_id(requests[i]) == 0))
                        request_to_broker0 = rd_true;
        }
        free_mock_requests(requests, request_cnt);
        TEST_ASSERT(
            !request_to_broker0,
            "Fetch Request should have been made to only Brokers 1.");
        rd_sleep(10);
        rd_kafka_mock_clear_requests(mcluster);
        rd_kafka_mock_stop_request_tracking(mcluster);

        /* Change the leader for Partition 1 to Broker 0 */
        rd_kafka_mock_partition_set_leader(mcluster, topic, 1, 0);
        /* Make the Fetch call again to trigger metadata update and cache changes */
        rkm = rd_kafka_consumer_poll(consumer, 10 * 1000);
        if (rkm)
                rd_kafka_message_destroy(rkm);
        rd_sleep(10);

        rd_kafka_mock_start_request_tracking(mcluster);
        rd_kafka_mock_clear_requests(mcluster);
        /* Make the Fetch call again */
        rkm = rd_kafka_consumer_poll(consumer, 10 * 1000);
        if (rkm)
                rd_kafka_message_destroy(rkm);
        
        /* Verify that the fetch call is made to both broker 0 and 1 */
        requests = rd_kafka_mock_get_requests(mcluster, &request_cnt);
        for (i = 0; i < request_cnt; i++) {
                TEST_SAY("Broker Id : %d API Key : %d Timestamp : %" PRId64
                         "\n",
                         rd_kafka_mock_request_id(requests[i]),
                         rd_kafka_mock_request_api_key(requests[i]),
                         rd_kafka_mock_request_timestamp(requests[i]));

                if ((rd_kafka_mock_request_api_key(requests[i]) ==
                    RD_KAFKAP_Fetch)){
                        if(rd_kafka_mock_request_id(requests[i]) == 0)
                            request_to_broker0 = rd_true;
                        else
                            request_to_broker1 = rd_true;
                    }
                if (rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_Metadata){
                    Metadata_request_made = rd_true;
                }
        }
        free_mock_requests(requests, request_cnt);
        rd_kafka_mock_clear_requests(mcluster);
        TEST_ASSERT(
            request_to_broker1 && request_to_broker0 && (!Metadata_request_made),
            "No separate Metadata Request should have been made. Further Fetch Request should have been made to both the Brokers 0 & 1.");
        SUB_TEST_PASS();



        rd_sleep(10);
        SUB_TEST();
        /* Leader for both the partition is broker 1 */
        rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 1);
        rd_kafka_mock_partition_set_leader(mcluster, topic, 1, 1);
        rd_sleep(10);
        rd_kafka_mock_start_request_tracking(mcluster);
        rd_kafka_mock_clear_requests(mcluster);

        rkm = rd_kafka_consumer_poll(consumer, 10 * 1000);
        if (rkm)
                rd_kafka_message_destroy(rkm);
        /* Verify that the fetch call is not made to broker 0 */
        requests = rd_kafka_mock_get_requests(mcluster, &request_cnt);
        for (i = 0; i < request_cnt; i++) {
                TEST_SAY("Broker Id : %d API Key : %d Timestamp : %" PRId64
                         "\n",
                         rd_kafka_mock_request_id(requests[i]),
                         rd_kafka_mock_request_api_key(requests[i]),
                         rd_kafka_mock_request_timestamp(requests[i]));

                if ((rd_kafka_mock_request_api_key(requests[i]) ==
                    RD_KAFKAP_Fetch) && (rd_kafka_mock_request_id(requests[i]) == 0))
                        request_to_broker0 = rd_true;
        }
        free_mock_requests(requests, request_cnt);
        TEST_ASSERT(
            !request_to_broker0,
            "Fetch Request should have been made to only Brokers 1.");
        rd_sleep(10);
        rd_kafka_mock_clear_requests(mcluster);
        rd_kafka_mock_stop_request_tracking(mcluster);

        /* Change the leader for both Partitions to Broker 0 */
        rd_kafka_mock_partition_set_leader(mcluster, topic, 1, 0);
        rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 0);
        /* Make the Fetch call again to trigger metadata update and cache changes */
        rkm = rd_kafka_consumer_poll(consumer, 10 * 1000);
        if (rkm)
                rd_kafka_message_destroy(rkm);
        rd_sleep(10);

        rd_kafka_mock_start_request_tracking(mcluster);
        rd_kafka_mock_clear_requests(mcluster);
        /* Make the Fetch call again */
        rkm = rd_kafka_consumer_poll(consumer, 10 * 1000);
        if (rkm)
                rd_kafka_message_destroy(rkm);
        
        /* Verify that the fetch call is made to only Broker 0 */
        requests = rd_kafka_mock_get_requests(mcluster, &request_cnt);
        for (i = 0; i < request_cnt; i++) {
                TEST_SAY("Broker Id : %d API Key : %d Timestamp : %" PRId64
                         "\n",
                         rd_kafka_mock_request_id(requests[i]),
                         rd_kafka_mock_request_api_key(requests[i]),
                         rd_kafka_mock_request_timestamp(requests[i]));

                if ((rd_kafka_mock_request_api_key(requests[i]) ==
                    RD_KAFKAP_Fetch) && (rd_kafka_mock_request_id(requests[i]) == 1))
                    request_to_broker1 = rd_true;
                    
                if (rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_Metadata)
                    Metadata_request_made = rd_true;
                
        }
        free_mock_requests(requests, request_cnt);
        rd_kafka_mock_clear_requests(mcluster);
        rd_kafka_destroy(consumer);
        TEST_ASSERT(
            (!request_to_broker1) && (!Metadata_request_made),
            "No separate Metadata Request should have been made. Further Fetch Request should have been made only to Brokers 0.");
        SUB_TEST_PASS();
        
}

/**
 * @brief Metadata Update Operation (KIP 951)
 * We test the behaviour when with a fetch or produce call we get a NOT_LEADER error,
 * which should trigger the update_metadata_op and the metadata cache should be updated
 * accordingly only for the partition with the changed leader.
 */
int main_0145_metadata_update_op_mock(int argc, char **argv) {
        const char *topic = test_mk_topic_name("topic", 1);
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        const char *bootstraps;
        /* Spawning a mock cluster with 2 nodes */
        mcluster = test_mock_cluster_new(2, &bootstraps);

        test_conf_init(&conf, NULL, 30);

        /* Create 2 partitions for the topic */
        rd_kafka_mock_topic_create(mcluster, topic, 2, 1);
        /* This test may be slower when running with CI or Helgrind,
         * restart the timeout. */
        test_timeout_set(100);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "topic.metadata.refresh.interval.ms", "-1");

        /* Test the case for the Produce API */
        test_produce(mcluster, topic, rd_kafka_conf_dup(conf));
        /* Test the case for the Fetch API */
        test_fetch(mcluster, topic, rd_kafka_conf_dup(conf));

        test_mock_cluster_destroy(mcluster);
        rd_kafka_conf_destroy(conf);
        return 0;
}
