/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2019-2022, Magnus Edenhill
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


#include "../src/rdkafka_proto.h"
#include "test.h"
#include "../src/rdkafka_mock.h"
/**
 * @name Verify that the builtin mock cluster works by producing to a topic
 *       and then consuming from it.
 */
const int32_t retry_ms = 100;
const int32_t retry_max_ms = 1000; 

void test_FindCoordinator(rd_kafka_mock_cluster_t *mcluster, rd_kafka_t *consumer){
        rd_kafka_mock_request_t **requests = NULL;
        size_t request_cnt = 0;
        int64_t previous_request_ts = -1;
        int32_t retry_count = 0;
        int32_t num_retries = 4;
        const int32_t low = 1000;
        const int32_t high = 2000;
        int32_t buffer = 200; // 200 ms buffer added
        rd_kafka_mock_push_request_errors(mcluster, RD_KAFKAP_FindCoordinator, num_retries,
                                          RD_KAFKA_RESP_ERR_GROUP_COORDINATOR_NOT_AVAILABLE,
                                          RD_KAFKA_RESP_ERR_GROUP_COORDINATOR_NOT_AVAILABLE,
                                          RD_KAFKA_RESP_ERR_GROUP_COORDINATOR_NOT_AVAILABLE,
                                          RD_KAFKA_RESP_ERR_GROUP_COORDINATOR_NOT_AVAILABLE);
        
        rd_kafka_consumer_poll(consumer,10*1000);
        rd_sleep(4);
        requests = rd_kafka_mock_get_requests(mcluster,&request_cnt);
        for(int i = 0; (i < request_cnt) && (retry_count < num_retries); i++){
                TEST_SAY("Broker Id : %d API Key : %d Timestamp : %ld\n",rd_kafka_mock_request_id(requests[i]), rd_kafka_mock_request_api_key(requests[i]), rd_kafka_mock_request_timestamp(requests[i]));
                if(rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_FindCoordinator){
                        if(previous_request_ts != -1){
                                int64_t time_difference = (rd_kafka_mock_request_timestamp(requests[i]) - previous_request_ts)/1000;
                                TEST_ASSERT(((time_difference > high - buffer) && (time_difference < high + buffer)) || ((time_difference > low - buffer) && (time_difference < low + buffer)) ,"Time difference should be close to 1 or 2 seconds, it is %d ms instead.\n",time_difference);
                                retry_count++;
                        }
                        previous_request_ts = rd_kafka_mock_request_timestamp(requests[i]);
                }
        }
        rd_kafka_mock_clear_requests(mcluster);
        TEST_SAY("FindCoordinator Finished Successfully!\n");
}

void test_OffsetCommit(rd_kafka_mock_cluster_t *mcluster, rd_kafka_t *consumer, const char* topic){
        rd_kafka_mock_request_t **requests = NULL;
        size_t request_cnt = 0;
        int64_t previous_request_ts = -1;
        int32_t retry_count = 0;

        test_consumer_subscribe(consumer,topic);
        rd_kafka_consumer_poll(consumer,10*1000);
        rd_sleep(4);
        rd_kafka_mock_push_request_errors(mcluster, RD_KAFKAP_OffsetCommit, 2,
                                            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS,
                                            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS);

        rd_kafka_topic_partition_list_t *offsets = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_t *rktpar = rd_kafka_topic_partition_list_add(offsets,topic,0);
        rktpar->offset = 4;
        rd_kafka_commit(consumer,offsets,0);
        rd_sleep(3);
        requests = rd_kafka_mock_get_requests(mcluster,&request_cnt);
        for(int i = 0; i < request_cnt; i++){
                if(rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_OffsetCommit){
                        TEST_SAY("Broker Id : %d API Key : %d Timestamp : %ld\n",rd_kafka_mock_request_id(requests[i]), rd_kafka_mock_request_api_key(requests[i]), rd_kafka_mock_request_timestamp(requests[i]));
                        if(previous_request_ts != -1){
                                int64_t time_difference = (rd_kafka_mock_request_timestamp(requests[i]) - previous_request_ts)/1000;
                                int64_t low = ((1<<retry_count)*(retry_ms)*75)/100; 
                                int64_t high = ((1<<retry_count)*(retry_ms)*125)/100;
                                if (high > ((retry_max_ms*125)/100))
                                        high = (retry_max_ms*125)/100;
                                if (low > ((retry_max_ms*75)/100))
                                        low = (retry_max_ms*75)/100;
                                TEST_ASSERT((time_difference < high) && (time_difference > low),"Time difference is not respected!\n");
                                retry_count++;
                        }
                        previous_request_ts = rd_kafka_mock_request_timestamp(requests[i]);
                }        
        }
        rd_kafka_mock_clear_requests(mcluster);
        TEST_SAY("OffsetCommit Finished Successfully!\n");
}

void test_Produce(rd_kafka_mock_cluster_t *mcluster, rd_kafka_t *producer, rd_kafka_topic_t *rkt){
        rd_kafka_mock_request_t **requests = NULL;
        size_t request_cnt = 0;
        int64_t previous_request_ts = -1;
        int32_t retry_count = 0;
        rd_kafka_mock_push_request_errors(mcluster, RD_KAFKAP_Produce,  7,
                                            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS,
                                            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS,
                                            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS,
                                            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS,
                                            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS,
                                            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS,
                                            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS);
        
        test_produce_msgs(producer, rkt, 0, RD_KAFKA_PARTITION_UA, 0, 1, "hello",5);
        rd_sleep(3);
        requests = rd_kafka_mock_get_requests(mcluster,&request_cnt);
        
        for(int i = 0; i < request_cnt; i++){
                if(rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_Produce){
                        TEST_SAY("Broker Id : %d API Key : %d Timestamp : %ld\n",rd_kafka_mock_request_id(requests[i]), rd_kafka_mock_request_api_key(requests[i]), rd_kafka_mock_request_timestamp(requests[i]));
                        if(previous_request_ts != -1){
                                int64_t time_difference = (rd_kafka_mock_request_timestamp(requests[i]) - previous_request_ts)/1000;
                                int64_t low = ((1<<retry_count)*(retry_ms)*75)/100; 
                                int64_t high = ((1<<retry_count)*(retry_ms)*125)/100;
                                if (high > ((retry_max_ms*125)/100))
                                        high = (retry_max_ms*125)/100;
                                if (low > ((retry_max_ms*75)/100))
                                        low = (retry_max_ms*75)/100;
                                TEST_ASSERT((time_difference < high) && (time_difference > low),"Time difference is not respected!\n");
                                retry_count++;
                        }
                        previous_request_ts = rd_kafka_mock_request_timestamp(requests[i]);
                }
        }
        rd_kafka_mock_clear_requests(mcluster);
        TEST_SAY("Produce Finished Successfully!\n");
}

void test_Heartbeat_FindCoordinator(rd_kafka_mock_cluster_t *mcluster, rd_kafka_t *consumer,const char *topic){
        rd_kafka_mock_request_t **requests = NULL;
        size_t request_cnt = 0;
        int64_t previous_request_ts = -1;
        int32_t retry_count = 0;
        int32_t num_heartbeat = 0;
        rd_kafka_mock_push_request_errors(mcluster, RD_KAFKAP_Heartbeat,  1,
                                            RD_KAFKA_RESP_ERR_NOT_COORDINATOR_FOR_GROUP);
        rd_kafka_mock_clear_requests(mcluster);
        test_consumer_subscribe(consumer,topic);
        rd_kafka_consumer_poll(consumer,10*1000);
        rd_sleep(6);
        requests = rd_kafka_mock_get_requests(mcluster,&request_cnt);
        for(int i = 0; i < request_cnt; i++){
                TEST_SAY("Broker Id : %d API Key : %d Timestamp : %ld\n",rd_kafka_mock_request_id(requests[i]), rd_kafka_mock_request_api_key(requests[i]), rd_kafka_mock_request_timestamp(requests[i]));
                if(num_heartbeat == 0){
                        if(rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_Heartbeat){
                                num_heartbeat++;
                        }
                }else if(num_heartbeat == 1){
                        if(rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_FindCoordinator){
                                TEST_SAY("FindCoordinator request made after failing Heartbeat with NOT_COORDINATOR error.\n");
                                break;
                        }else if(rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_Heartbeat){
                                num_heartbeat++;
                                TEST_FAIL("Second Heartbeat Request made without any FindCoordinator Request in-between.\n");
                        }
                }
        }
        rd_kafka_mock_clear_requests(mcluster);
        TEST_ASSERT((num_heartbeat == 1),"No Heartbeat Request was made.\n");
        TEST_SAY("Heartbeat-FindCoordinator Finished Successfully!\n");
}

void test_JoinGroup_FindCoordinator(rd_kafka_mock_cluster_t *mcluster, rd_kafka_t *consumer, rd_kafka_t *topic){
        rd_kafka_mock_request_t **requests = NULL;
        size_t request_cnt = 0;
        int64_t previous_request_ts = -1;
        int32_t retry_count = 0;
        int32_t num_joingroup = 0;
        rd_kafka_mock_push_request_errors(mcluster, RD_KAFKAP_JoinGroup, 1 ,
                                            RD_KAFKA_RESP_ERR_NOT_COORDINATOR_FOR_GROUP);
        rd_kafka_mock_clear_requests(mcluster);
        test_consumer_subscribe(consumer,topic);
        rd_kafka_consumer_poll(consumer,10*1000);
        rd_sleep(4);
        requests = rd_kafka_mock_get_requests(mcluster,&request_cnt);
        for(int i = 0; i < request_cnt; i++){
                TEST_SAY("Broker Id : %d API Key : %d Timestamp : %ld\n",rd_kafka_mock_request_id(requests[i]), rd_kafka_mock_request_api_key(requests[i]), rd_kafka_mock_request_timestamp(requests[i]));
                if(num_joingroup == 0){
                        if(rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_JoinGroup){
                                num_joingroup++;
                        }
                }else if(num_joingroup == 1){
                        if(rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_FindCoordinator){
                                TEST_SAY("FindCoordinator request made after failing JoinGroup with NOT_COORDINATOR error.\n");
                                break;
                        }else if(rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_JoinGroup){
                                num_joingroup++;
                                TEST_FAIL("Second JoinGroup Request made without any FindCoordinator Request in-between.\n");
                        }
                }
        }

        rd_kafka_mock_clear_requests(mcluster);
        TEST_ASSERT((num_joingroup == 1),"No JoinGroup Request was made.\n");
        TEST_SAY("JoinGroup-FindCoordinator Finished Successfully!\n");
}

void test_Produce_FastleaderQuery(rd_kafka_mock_cluster_t *mcluster, rd_kafka_t *producer, rd_kafka_topic_t *rkt){
        rd_kafka_mock_request_t **requests = NULL;
        size_t request_cnt = 0;
        int64_t previous_request_ts = -1;
        int32_t retry_count = 0;
        rd_bool_t produced = rd_false;
        rd_kafka_mock_push_request_errors(mcluster, RD_KAFKAP_Produce,  1,
                                            RD_KAFKA_RESP_ERR_NOT_LEADER_OR_FOLLOWER);
        rd_kafka_mock_clear_requests(mcluster);
        test_produce_msgs(producer, rkt, 0, RD_KAFKA_PARTITION_UA, 0, 1, "hello",1);
        rd_sleep(10);
        requests = rd_kafka_mock_get_requests(mcluster,&request_cnt);
        
        for(int i = 0; i < request_cnt; i++){
                TEST_SAY("Broker Id : %d API Key : %d Timestamp : %ld\n",rd_kafka_mock_request_id(requests[i]), rd_kafka_mock_request_api_key(requests[i]), rd_kafka_mock_request_timestamp(requests[i]));
                if(!produced && rd_kafka_mock_request_api_key(requests[i])==RD_KAFKAP_Produce)
                        produced = rd_true;
                if(rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_Metadata && produced){
                        if(previous_request_ts != -1){
                                int64_t time_difference = (rd_kafka_mock_request_timestamp(requests[i]) - previous_request_ts)/1000;
                                int64_t low = ((1<<retry_count)*(retry_ms)*75)/100; 
                                int64_t high = ((1<<retry_count)*(retry_ms)*125)/100;
                                if (high > ((retry_max_ms*125)/100))
                                        high = (retry_max_ms*125)/100;
                                if (low > ((retry_max_ms*75)/100))
                                        low = (retry_max_ms*75)/100;
                                fprintf(stderr,"Time Difference is %d low is %d and high is %d.\n",time_difference,low,high);
                                // TEST_ASSERT((time_difference < high) && (time_difference > low),"Time difference is not respected!\n");
                                retry_count++;
                        }
                        previous_request_ts = rd_kafka_mock_request_timestamp(requests[i]);
                }
        }
        rd_kafka_mock_clear_requests(mcluster);
        TEST_SAY("Produce-FastLeaderQuery Finished Successfully!\n");
}

void test_Fetch_FastLeaderQuery(rd_kafka_mock_cluster_t *mcluster, rd_kafka_t *consumer, const char* topic){
        rd_kafka_mock_request_t **requests = NULL;
        size_t request_cnt = 0;
        int64_t previous_request_ts = -1;
        int32_t retry_count = 0;
        rd_bool_t fetched = rd_false;
        test_consumer_subscribe(consumer,topic);

        rd_kafka_mock_push_request_errors(mcluster, RD_KAFKAP_Fetch, 1,
                                            RD_KAFKA_RESP_ERR_NOT_LEADER_OR_FOLLOWER);
        rd_kafka_mock_clear_requests(mcluster);
        rd_kafka_consumer_poll(consumer,10*1000);
        rd_sleep(7);

        requests = rd_kafka_mock_get_requests(mcluster,&request_cnt);
        for(int i = 0; i < request_cnt; i++){
                TEST_SAY("Broker Id : %d API Key : %d Timestamp : %ld\n",rd_kafka_mock_request_id(requests[i]), rd_kafka_mock_request_api_key(requests[i]), rd_kafka_mock_request_timestamp(requests[i]));
                if(!fetched && rd_kafka_mock_request_api_key(requests[i])==RD_KAFKAP_Fetch)
                        fetched = rd_true;
                if(rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_Metadata && fetched){
                        if(previous_request_ts != -1){
                                int64_t time_difference = (rd_kafka_mock_request_timestamp(requests[i]) - previous_request_ts)/1000;
                                int64_t low = ((1<<retry_count)*(retry_ms)*75)/100; 
                                int64_t high = ((1<<retry_count)*(retry_ms)*125)/100;
                                if (high > ((retry_max_ms*125)/100))
                                        high = (retry_max_ms*125)/100;
                                if (low > ((retry_max_ms*75)/100))
                                        low = (retry_max_ms*75)/100;
                                fprintf(stderr,"Time Difference is %d low is %d and high is %d.\n",time_difference,low,high);
                                // TEST_ASSERT((time_difference < high) && (time_difference > low),"Time difference is not respected!\n");
                                retry_count++;
                        }
                        previous_request_ts = rd_kafka_mock_request_timestamp(requests[i]);
                }    
        }
        rd_kafka_mock_clear_requests(mcluster);
        TEST_SAY("Fetch FastLeaderQuery Finished Successfully!\n");
}

int main_0009_mock_cluster(int argc, char **argv) {
        const char *topic = test_mk_topic_name("topic", 1);
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_t *p, *c;
        rd_kafka_topic_t *rkt;
        rd_kafka_conf_t *conf;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *parts;
        rd_kafka_mock_request_t **requests = NULL;
        size_t request_cnt = 0;
        int64_t previous_request_ts = -1;
        int32_t retry_count = 0;
        const int32_t retry_ms = 100;
        const int32_t retry_max_ms = 1000; 
        if (test_needs_auth()) {
                TEST_SKIP("Mock cluster does not support SSL/SASL\n");
                return 0;
        }

        mcluster = test_mock_cluster_new(1, &bootstraps);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        // test_conf_set(conf, "debug", "protocol");
        test_conf_set(conf, "topic.metadata.refresh.interval.ms","-1");
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);

        p = test_create_handle(RD_KAFKA_PRODUCER, rd_kafka_conf_dup(conf));
        rkt = test_create_producer_topic(p, topic, NULL);
        
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "false");

        c = test_create_consumer(topic, NULL, conf, NULL);
        
        // test_Produce(mcluster,p,rkt);
        // test_OffsetCommit(mcluster,c,topic);
        // test_FindCoordinator(mcluster,c);
        // test_Heartbeat_FindCoordinator(mcluster,c,topic);
        // test_JoinGroup_FindCoordinator(mcluster,c,topic);

        // test_Produce_FastleaderQuery(mcluster,p,rkt);
        test_Fetch_FastLeaderQuery(mcluster,c,topic);
        rd_kafka_destroy(c);
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(p);

        test_mock_cluster_destroy(mcluster);
        return 0;

        /**
         * Test 
         * 
         * FindCoordinator works fine , so now we need to check that all the dependent rpc trigger the findcordinator on failure !
         * 3 sec heartbeat interval
         * inject heartbeat error -> NOT_COORDINATOR 
         * rd_sleep(6)
         *  there are only 2 heartbeat request at an interval of 3 sec difference
         * 
         * 
         * 
         * 
         * 
        */
}
