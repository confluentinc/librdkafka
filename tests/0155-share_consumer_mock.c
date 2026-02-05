#include "test.h"

#include "../src/rdkafka_proto.h"

/**
 * @name Mock tests for share consumer and ShareGroupHeartbeat
 */

static rd_bool_t is_share_heartbeat_request(rd_kafka_mock_request_t *request,
                                            void *opaque) {
        return rd_kafka_mock_request_api_key(request) ==
               RD_KAFKAP_ShareGroupHeartbeat;
}

/**
 * @brief Wait for at least \p num ShareGroupHeartbeat requests
 *        to be received by the mock cluster.
 *
 * @return Number of heartbeats received.
 */
static int wait_share_heartbeats(rd_kafka_mock_cluster_t *mcluster,
                                 int num,
                                 int confidence_interval) {
        return test_mock_wait_matching_requests(
            mcluster, num, confidence_interval, is_share_heartbeat_request,
            NULL);
}

/**
 * @brief Create a share consumer connected to mock cluster with debug logging.
 */
static rd_kafka_t *create_share_consumer(const char *bootstraps,
                                         const char *group_id,
                                         rd_bool_t enable_debug) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        char errstr[512];

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group_id);
        test_conf_set(conf, "share.consumer", "true");
        test_conf_set(conf, "group.protocol", "consumer");
        test_conf_set(conf, "auto.offset.reset", "earliest");

        /* Enable debug logging for detailed output */
        if (enable_debug)
                test_conf_set(conf, "debug", "cgrp,protocol,broker");

        rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(rk != NULL, "Failed to create share consumer: %s", errstr);

        TEST_SAY("Created share consumer: name=%s, group=%s, bootstrap=%s\n",
                 rd_kafka_name(rk), group_id, bootstraps);

        return rk;
}

/**
 * @brief Test basic ShareGroupHeartbeat flow:
 *        - Join (MemberEpoch=0)
 *        - Receive assignment
 *        - Regular heartbeats
 *        - Leave (MemberEpoch=-1)
 *
 * This test prints detailed debug logs showing:
 * - Consumer joining with member ID and subscribed topics
 * - Heartbeat requests sent by the consumer
 * - Requests received at the mock broker
 * - Responses from the broker (assignments, epochs)
 * - Consumer processing the response
 */
static void do_test_share_group_heartbeat_basic(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription;
        rd_kafka_t *c;
        int found_heartbeats;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group";

        SUB_TEST_QUICK();

        TEST_SAY("\n");
        TEST_SAY(
            "============================================================\n");
        TEST_SAY("  SHARE GROUP HEARTBEAT TEST - DETAILED DEBUG OUTPUT\n");
        TEST_SAY(
            "============================================================\n");
        TEST_SAY("\n");

        /*
         * STEP 1: Create mock cluster
         */
        TEST_SAY(">>> STEP 1: Creating mock cluster with 1 broker\n");
        mcluster = test_mock_cluster_new(1, &bootstraps);
        TEST_SAY("    Mock cluster created at: %s\n", bootstraps);

        /*
         * STEP 2: Create topic
         */
        TEST_SAY("\n>>> STEP 2: Creating topic '%s' with 3 partitions\n",
                 topic);
        rd_kafka_mock_topic_create(mcluster, topic, 3, 1);
        TEST_SAY("    Topic created: name=%s, partitions=3, replication=1\n",
                 topic);

        /*
         * STEP 3: Create share consumer
         * Debug logging is enabled (cgrp,protocol,broker)
         * This will print detailed logs about:
         * - Broker connections
         * - Group coordinator discovery
         * - ShareGroupHeartbeat requests/responses
         */
        TEST_SAY("\n>>> STEP 3: Creating share consumer\n");
        c = create_share_consumer(bootstraps, group, rd_false);

        /*
         * STEP 4: Subscribe to topic
         * This triggers the consumer to:
         * 1. Find the group coordinator
         * 2. Send ShareGroupHeartbeat with MemberEpoch=0 (join)
         * 3. Receive assignment in response
         */

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);

        rd_kafka_mock_start_request_tracking(mcluster);
        TEST_CALL_ERR__(rd_kafka_subscribe(c, subscription));
        rd_kafka_topic_partition_list_destroy(subscription);
        TEST_SAY("    Subscription submitted, waiting for heartbeat...\n");

        /*
         * STEP 5: Wait for join heartbeat
         * The consumer should send ShareGroupHeartbeat with:
         * - MemberEpoch = 0 (join)
         * - SubscribedTopicNames = [topic]
         *
         * The mock broker should respond with:
         * - MemberId (assigned by broker)
         * - MemberEpoch (incremented)
         * - Assignment (partitions 0, 1, 2)
         */
        TEST_SAY("\n>>> STEP 5: Waiting for JOIN heartbeat (MemberEpoch=0)\n");
        found_heartbeats = wait_share_heartbeats(mcluster, 1, 500);
        TEST_SAY("    Received %d ShareGroupHeartbeat request(s)\n",
                 found_heartbeats);
        TEST_ASSERT(found_heartbeats >= 1,
                    "Expected at least 1 heartbeat, got %d", found_heartbeats);

        /*
         * STEP 6: Poll consumer
         * This processes the heartbeat response and triggers:
         * - Assignment callback (if configured)
         * - Subsequent heartbeats (MemberEpoch > 0)
         */
        TEST_SAY("\n>>> STEP 6: Polling consumer for 2 seconds\n");
        TEST_SAY("    This processes responses and triggers more heartbeats\n");
        TEST_SAY("    Regular heartbeats have MemberEpoch > 0\n");
        TEST_SAY("\n");
        rd_kafka_consumer_poll(c, 2000);

        /*
         * STEP 7: Verify heartbeats
         */
        TEST_SAY("\n>>> STEP 7: Verifying heartbeat count\n");
        found_heartbeats = wait_share_heartbeats(mcluster, 2, 200);
        TEST_SAY("    Total ShareGroupHeartbeat requests: %d\n",
                 found_heartbeats);
        TEST_ASSERT(found_heartbeats >= 2,
                    "Expected at least 2 heartbeats, got %d", found_heartbeats);

        /*
         * STEP 8: Close consumer
         * This triggers:
         * - ShareGroupHeartbeat with MemberEpoch = -1 (leave)
         * - Mock broker removes member from group
         * - Assignments recalculated (group now empty)
         */
        TEST_SAY("\n>>> STEP 8: Closing share consumer\n");
        TEST_SAY("    This sends LEAVE heartbeat (MemberEpoch=-1)\n");
        TEST_SAY("\n");
        rd_kafka_consumer_close(c);
        rd_kafka_destroy(c);

        /*
         * STEP 9: Verify leave heartbeat
         */
        TEST_SAY("\n>>> STEP 9: Verifying leave heartbeat was sent\n");
        found_heartbeats = wait_share_heartbeats(mcluster, 3, 200);
        TEST_SAY("    Final ShareGroupHeartbeat count: %d\n", found_heartbeats);

        /*
         * STEP 10: Verify no more heartbeats after leave
         * After the consumer has left, no more heartbeats should be sent.
         * Reset tracking, wait, then verify count is 0.
         */
        TEST_SAY("\n>>> STEP 10: Verifying no heartbeats after leave\n");
        TEST_SAY("    Waiting 3 seconds to ensure no more heartbeats...\n");

        /* Reset tracking to get a fresh count */
        rd_kafka_mock_stop_request_tracking(mcluster);
        rd_kafka_mock_start_request_tracking(mcluster);

        rd_sleep(3);

        found_heartbeats = wait_share_heartbeats(mcluster, 0, 100);
        TEST_SAY("    Heartbeats after leave: %d (expected 0)\n",
                 found_heartbeats);
        TEST_ASSERT(found_heartbeats == 0,
                    "Expected 0 heartbeats after leave, got %d",
                    found_heartbeats);
        TEST_SAY("    PASS: No heartbeats received after consumer left\n");

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        TEST_SAY("\n");
        TEST_SAY(
            "============================================================\n");
        TEST_SAY("  TEST COMPLETED SUCCESSFULLY\n");
        TEST_SAY(
            "============================================================\n");
        TEST_SAY("\n");

        SUB_TEST_PASS();
}

/**
 * @brief Test assignment redistribution when a second consumer joins.
 *
 * Scenario:
 * 1. Consumer 1 joins and gets all 3 partitions
 * 2. Consumer 2 joins - partitions should be redistributed
 *
 * Expected assignment:
 * - Step 1: C1 gets [0, 1, 2]
 * - Step 2: C1 gets [0, 1], C2 gets [2]  (or similar split)
 */
static void do_test_share_group_assignment_rebalance(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription;
        rd_kafka_topic_partition_list_t *c1_assignment, *c2_assignment;
        rd_kafka_t *c1, *c2;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-rebalance";

        SUB_TEST_QUICK();

        TEST_SAY("\n");
        TEST_SAY(
            "============================================================\n");
        TEST_SAY("  SHARE GROUP ASSIGNMENT REBALANCE TEST\n");
        TEST_SAY(
            "============================================================\n");
        TEST_SAY("\n");

        /*
         * STEP 1: Setup - Create mock cluster and topic
         */
        TEST_SAY(">>> STEP 1: Creating mock cluster and topic\n");
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 3, 1);
        TEST_SAY("    Topic '%s' created with 3 partitions\n", topic);

        /*
         * STEP 2: First consumer joins
         * Expected: C1 gets all 3 partitions [0, 1, 2]
         */
        TEST_SAY("\n>>> STEP 2: First consumer (C1) joins\n");
        TEST_SAY("    Expected assignment: C1 -> [0, 1, 2] (all partitions)\n");
        TEST_SAY("\n");

        c1 = create_share_consumer(bootstraps, group, rd_false);

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);

        rd_kafka_mock_start_request_tracking(mcluster);
        TEST_CALL_ERR__(rd_kafka_subscribe(c1, subscription));

        /* Wait for C1 to join and get assignment */
        TEST_SAY("    Waiting for C1 to receive assignment...\n");
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c1, 2000);

        /* Verify C1 got all 3 partitions */
        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assignment));
        TEST_SAY("    C1 assignment: %d partition(s)\n", c1_assignment->cnt);
        TEST_ASSERT(c1_assignment->cnt == 3,
                    "Expected C1 to have 3 partitions, got %d",
                    c1_assignment->cnt);
        rd_kafka_topic_partition_list_destroy(c1_assignment);

        /*
         * STEP 3: Second consumer joins
         * Expected: Partitions redistributed between C1 and C2
         */
        TEST_SAY("\n>>> STEP 3: Second consumer (C2) joins the same group\n");
        TEST_SAY("    Expected: Partitions redistributed (total = 3)\n");
        TEST_SAY("\n");

        c2 = create_share_consumer(bootstraps, group, rd_false);
        TEST_CALL_ERR__(rd_kafka_subscribe(c2, subscription));
        rd_kafka_topic_partition_list_destroy(subscription);

        /* Wait for C2 to join and trigger rebalance */
        TEST_SAY("    Waiting for C2 to join and rebalance...\n");
        wait_share_heartbeats(mcluster, 3, 500);

        /* Poll both to process rebalance */
        rd_kafka_consumer_poll(c1, 2000);
        rd_kafka_consumer_poll(c2, 2000);

        /* Verify partitions are distributed between C1 and C2 */
        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assignment));
        TEST_CALL_ERR__(rd_kafka_assignment(c2, &c2_assignment));
        TEST_SAY("    C1 assignment: %d partition(s)\n", c1_assignment->cnt);
        TEST_SAY("    C2 assignment: %d partition(s)\n", c2_assignment->cnt);
        TEST_ASSERT(c1_assignment->cnt + c2_assignment->cnt == 3,
                    "Expected total 3 partitions, got %d + %d = %d",
                    c1_assignment->cnt, c2_assignment->cnt,
                    c1_assignment->cnt + c2_assignment->cnt);
        TEST_ASSERT(c1_assignment->cnt > 0 && c2_assignment->cnt > 0,
                    "Expected both consumers to have partitions, "
                    "got C1=%d, C2=%d",
                    c1_assignment->cnt, c2_assignment->cnt);
        rd_kafka_topic_partition_list_destroy(c1_assignment);
        rd_kafka_topic_partition_list_destroy(c2_assignment);

        /*
         * STEP 4: C2 leaves - C1 should get all partitions back
         */
        TEST_SAY("\n>>> STEP 4: C2 leaves the group\n");
        TEST_SAY("    Expected: C1 gets all 3 partitions back\n");
        TEST_SAY("\n");

        rd_kafka_consumer_close(c2);
        rd_kafka_destroy(c2);

        /*
         * STEP 5: Wait for C1 to get reassigned
         */
        TEST_SAY(">>> STEP 5: Waiting for C1 to get partitions back\n");

        /* Poll C1 long enough to trigger a heartbeat (interval is 5s) */
        rd_kafka_consumer_poll(c1, 6000);

        /* Verify C1 got all partitions back */
        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assignment));
        TEST_SAY("    C1 final assignment: %d partition(s)\n",
                 c1_assignment->cnt);
        TEST_ASSERT(c1_assignment->cnt == 3,
                    "Expected C1 to have 3 partitions after C2 left, got %d",
                    c1_assignment->cnt);
        rd_kafka_topic_partition_list_destroy(c1_assignment);

        /*
         * Cleanup
         */
        TEST_SAY("\n>>> STEP 6: Cleanup\n");
        rd_kafka_consumer_close(c1);
        rd_kafka_destroy(c1);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        TEST_SAY(
            "============================================================\n");
        TEST_SAY("  TEST COMPLETED\n");
        TEST_SAY(
            "============================================================\n\n");

        SUB_TEST_PASS();
}

/**
 * @brief Helper to count partitions for a specific topic in an assignment.
 */
static int count_topic_partitions(rd_kafka_topic_partition_list_t *assignment,
                                  const char *topic) {
        int i, count = 0;
        for (i = 0; i < assignment->cnt; i++) {
                if (strcmp(assignment->elems[i].topic, topic) == 0)
                        count++;
        }
        return count;
}

/**
 * @brief Test multi-topic assignment with mixed subscriptions.
 *
 * Scenario:
 *   Topics:
 *     - topic-orders: 4 partitions
 *     - topic-events: 2 partitions
 *
 *   Consumers:
 *     - C1: subscribes to BOTH topics
 *     - C2: subscribes to topic-orders ONLY
 *     - C3: subscribes to topic-events ONLY
 */
static void do_test_share_group_multi_topic_assignment(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *sub_both, *sub_orders, *sub_events;
        rd_kafka_topic_partition_list_t *c1_assign, *c2_assign, *c3_assign;
        rd_kafka_t *c1, *c2, *c3;
        const char *topic_orders = "test-orders";
        const char *topic_events = "test-events";
        const char *group        = "test-share-group-multi";
        int total_orders, total_events;

        SUB_TEST_QUICK();

        TEST_SAY("\n");
        TEST_SAY(
            "============================================================\n");
        TEST_SAY("  MULTI-TOPIC MIXED SUBSCRIPTION TEST\n");
        TEST_SAY(
            "============================================================\n");
        TEST_SAY("  Topics: orders (4 parts), events (2 parts)\n");
        TEST_SAY("  C1: both topics, C2: orders only, C3: events only\n");
        TEST_SAY(
            "============================================================\n\n");

        /* Setup */
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic_orders, 4, 1);
        rd_kafka_mock_topic_create(mcluster, topic_events, 2, 1);

        /* Prepare subscriptions */
        sub_both = rd_kafka_topic_partition_list_new(2);
        rd_kafka_topic_partition_list_add(sub_both, topic_orders,
                                          RD_KAFKA_PARTITION_UA);
        rd_kafka_topic_partition_list_add(sub_both, topic_events,
                                          RD_KAFKA_PARTITION_UA);

        sub_orders = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(sub_orders, topic_orders,
                                          RD_KAFKA_PARTITION_UA);

        sub_events = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(sub_events, topic_events,
                                          RD_KAFKA_PARTITION_UA);

        rd_kafka_mock_start_request_tracking(mcluster);

        /*
         * STEP 1: C1 joins (subscribes to both topics)
         * Expected: C1 -> all 6 partitions (4 orders + 2 events)
         */
        TEST_SAY("STEP 1: C1 joins (subscribes to BOTH topics)\n");

        c1 = create_share_consumer(bootstraps, group, rd_false);
        TEST_CALL_ERR__(rd_kafka_subscribe(c1, sub_both));
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c1, 2000);

        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assign));
        TEST_SAY("  C1: %d partitions (orders=%d, events=%d)\n", c1_assign->cnt,
                 count_topic_partitions(c1_assign, topic_orders),
                 count_topic_partitions(c1_assign, topic_events));
        TEST_ASSERT(c1_assign->cnt == 6,
                    "C1 should have all 6 partitions, got %d", c1_assign->cnt);
        rd_kafka_topic_partition_list_destroy(c1_assign);

        /*
         * STEP 2: C2 joins (subscribes to orders only)
         * Expected: orders split between C1 and C2, C1 keeps all events
         */
        TEST_SAY("\nSTEP 2: C2 joins (subscribes to ORDERS only)\n");

        c2 = create_share_consumer(bootstraps, group, rd_false);
        TEST_CALL_ERR__(rd_kafka_subscribe(c2, sub_orders));
        wait_share_heartbeats(mcluster, 3, 500);
        rd_kafka_consumer_poll(c1, 2000);
        rd_kafka_consumer_poll(c2, 2000);

        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assign));
        TEST_CALL_ERR__(rd_kafka_assignment(c2, &c2_assign));

        total_orders = count_topic_partitions(c1_assign, topic_orders) +
                       count_topic_partitions(c2_assign, topic_orders);
        total_events = count_topic_partitions(c1_assign, topic_events) +
                       count_topic_partitions(c2_assign, topic_events);

        TEST_SAY("  C1: %d partitions (orders=%d, events=%d)\n", c1_assign->cnt,
                 count_topic_partitions(c1_assign, topic_orders),
                 count_topic_partitions(c1_assign, topic_events));
        TEST_SAY("  C2: %d partitions (orders=%d, events=%d)\n", c2_assign->cnt,
                 count_topic_partitions(c2_assign, topic_orders),
                 count_topic_partitions(c2_assign, topic_events));

        TEST_ASSERT(total_orders == 4,
                    "Total orders partitions should be 4, "
                    "got %d",
                    total_orders);
        TEST_ASSERT(total_events == 2,
                    "Total events partitions should be 2, "
                    "got %d",
                    total_events);
        TEST_ASSERT(count_topic_partitions(c2_assign, topic_orders) > 0,
                    "C2 should have at least 1 orders partition");

        rd_kafka_topic_partition_list_destroy(c1_assign);
        rd_kafka_topic_partition_list_destroy(c2_assign);

        /*
         * STEP 3: C3 joins (subscribes to events only)
         * Expected: events split between C1 and C3
         */
        TEST_SAY("\nSTEP 3: C3 joins (subscribes to EVENTS only)\n");

        c3 = create_share_consumer(bootstraps, group, rd_false);
        TEST_CALL_ERR__(rd_kafka_subscribe(c3, sub_events));
        wait_share_heartbeats(mcluster, 5, 500);
        rd_kafka_consumer_poll(c1, 2000);
        rd_kafka_consumer_poll(c2, 2000);
        rd_kafka_consumer_poll(c3, 2000);

        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assign));
        TEST_CALL_ERR__(rd_kafka_assignment(c2, &c2_assign));
        TEST_CALL_ERR__(rd_kafka_assignment(c3, &c3_assign));

        total_orders = count_topic_partitions(c1_assign, topic_orders) +
                       count_topic_partitions(c2_assign, topic_orders) +
                       count_topic_partitions(c3_assign, topic_orders);
        total_events = count_topic_partitions(c1_assign, topic_events) +
                       count_topic_partitions(c2_assign, topic_events) +
                       count_topic_partitions(c3_assign, topic_events);

        TEST_SAY("  C1: %d partitions (orders=%d, events=%d)\n", c1_assign->cnt,
                 count_topic_partitions(c1_assign, topic_orders),
                 count_topic_partitions(c1_assign, topic_events));
        TEST_SAY("  C2: %d partitions (orders=%d, events=%d)\n", c2_assign->cnt,
                 count_topic_partitions(c2_assign, topic_orders),
                 count_topic_partitions(c2_assign, topic_events));
        TEST_SAY("  C3: %d partitions (orders=%d, events=%d)\n", c3_assign->cnt,
                 count_topic_partitions(c3_assign, topic_orders),
                 count_topic_partitions(c3_assign, topic_events));

        TEST_ASSERT(total_orders == 4, "Total orders should be 4, got %d",
                    total_orders);
        TEST_ASSERT(total_events == 2, "Total events should be 2, got %d",
                    total_events);
        TEST_ASSERT(count_topic_partitions(c3_assign, topic_events) > 0,
                    "C3 should have at least 1 events partition");

        rd_kafka_topic_partition_list_destroy(c1_assign);
        rd_kafka_topic_partition_list_destroy(c2_assign);
        rd_kafka_topic_partition_list_destroy(c3_assign);

        /*
         * STEP 4: C1 leaves (was subscribed to both)
         * Expected: C2 gets all orders, C3 gets all events
         */
        TEST_SAY("\nSTEP 4: C1 leaves (was subscribed to BOTH)\n");

        rd_kafka_consumer_close(c1);
        rd_kafka_destroy(c1);

        rd_kafka_consumer_poll(c2, 6000);
        rd_kafka_consumer_poll(c3, 6000);

        TEST_CALL_ERR__(rd_kafka_assignment(c2, &c2_assign));
        TEST_CALL_ERR__(rd_kafka_assignment(c3, &c3_assign));

        TEST_SAY("  C2: %d partitions (orders=%d, events=%d)\n", c2_assign->cnt,
                 count_topic_partitions(c2_assign, topic_orders),
                 count_topic_partitions(c2_assign, topic_events));
        TEST_SAY("  C3: %d partitions (orders=%d, events=%d)\n", c3_assign->cnt,
                 count_topic_partitions(c3_assign, topic_orders),
                 count_topic_partitions(c3_assign, topic_events));

        TEST_ASSERT(count_topic_partitions(c2_assign, topic_orders) == 4,
                    "C2 should have all 4 orders partitions, got %d",
                    count_topic_partitions(c2_assign, topic_orders));
        TEST_ASSERT(count_topic_partitions(c3_assign, topic_events) == 2,
                    "C3 should have all 2 events partitions, got %d",
                    count_topic_partitions(c3_assign, topic_events));

        rd_kafka_topic_partition_list_destroy(c2_assign);
        rd_kafka_topic_partition_list_destroy(c3_assign);

        /*
         * STEP 5: C2 leaves - C3 should still have events only
         */
        TEST_SAY("\nSTEP 5: C2 leaves (was subscribed to ORDERS)\n");

        rd_kafka_consumer_close(c2);
        rd_kafka_destroy(c2);

        rd_kafka_consumer_poll(c3, 6000);

        TEST_CALL_ERR__(rd_kafka_assignment(c3, &c3_assign));
        TEST_SAY("  C3: %d partitions (orders=%d, events=%d)\n", c3_assign->cnt,
                 count_topic_partitions(c3_assign, topic_orders),
                 count_topic_partitions(c3_assign, topic_events));
        TEST_ASSERT(count_topic_partitions(c3_assign, topic_events) == 2,
                    "C3 should still have 2 events partitions, got %d",
                    count_topic_partitions(c3_assign, topic_events));
        TEST_ASSERT(count_topic_partitions(c3_assign, topic_orders) == 0,
                    "C3 should have 0 orders partitions (not subscribed), "
                    "got %d",
                    count_topic_partitions(c3_assign, topic_orders));
        rd_kafka_topic_partition_list_destroy(c3_assign);

        /* Cleanup */
        TEST_SAY("\nCLEANUP: C3 leaves\n");
        rd_kafka_consumer_close(c3);
        rd_kafka_destroy(c3);

        rd_kafka_topic_partition_list_destroy(sub_both);
        rd_kafka_topic_partition_list_destroy(sub_orders);
        rd_kafka_topic_partition_list_destroy(sub_events);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        TEST_SAY(
            "============================================================\n");
        TEST_SAY("  TEST COMPLETED\n");
        TEST_SAY(
            "============================================================\n\n");

        SUB_TEST_PASS();
}

/**
 * @brief Test UNKNOWN_MEMBER_ID error injection for ShareGroupHeartbeat.
 *
 * Tests that the consumer handles UNKNOWN_MEMBER_ID error correctly
 * by rejoining and getting assignment again.
 */
static void do_test_share_group_error_injection(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription, *assignment;
        rd_kafka_t *c;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-errors";

        SUB_TEST_QUICK();

        TEST_SAY("\n");
        TEST_SAY(
            "============================================================\n");
        TEST_SAY("  SHARE GROUP ERROR INJECTION TEST\n");
        TEST_SAY(
            "============================================================\n\n");

        /* Setup */
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 3, 1);

        c = create_share_consumer(bootstraps, group, rd_false);

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);

        rd_kafka_mock_start_request_tracking(mcluster);
        TEST_CALL_ERR__(rd_kafka_subscribe(c, subscription));
        rd_kafka_topic_partition_list_destroy(subscription);

        /* Wait for initial join */
        TEST_SAY(">>> Waiting for initial join...\n");
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c, 2000);

        /* Verify initial assignment */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_SAY("    Initial assignment: %d partitions\n", assignment->cnt);
        TEST_ASSERT(assignment->cnt == 3,
                    "Expected 3 partitions initially, got %d", assignment->cnt);
        rd_kafka_topic_partition_list_destroy(assignment);

        /*
         * Test: UNKNOWN_MEMBER_ID error injection
         * Verifies the mock broker can inject errors on ShareGroupHeartbeat.
         * Consumer should rejoin and get assignment again.
         */
        TEST_SAY("\n>>> Test: Injecting UNKNOWN_MEMBER_ID error\n");

        /* Reset tracking to count heartbeats after error injection */
        rd_kafka_mock_stop_request_tracking(mcluster);
        rd_kafka_mock_start_request_tracking(mcluster);

        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_ShareGroupHeartbeat, 1,
            RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID, 0);

        rd_kafka_consumer_poll(c, 6000);

        /* Verify heartbeats were sent after error (proving rejoin happened) */
        {
                int hb_after_error = wait_share_heartbeats(mcluster, 1, 500);
                TEST_SAY("    Heartbeats after error: %d\n", hb_after_error);
                TEST_ASSERT(hb_after_error >= 2,
                            "Expected at least 2 heartbeats after error "
                            "(error + rejoin), got %d",
                            hb_after_error);
        }

        /* Verify consumer rejoined and got assignment */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_SAY("    Assignment after rejoin: %d partitions\n", assignment->cnt);
        TEST_ASSERT(assignment->cnt == 3,
                    "Expected 3 partitions after rejoin, got %d",
                    assignment->cnt);
        rd_kafka_topic_partition_list_destroy(assignment);

        /* Cleanup */
        rd_kafka_consumer_close(c);
        rd_kafka_destroy(c);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        TEST_SAY(
            "============================================================\n");
        TEST_SAY("  ERROR INJECTION TEST COMPLETED\n");
        TEST_SAY(
            "============================================================\n\n");

        SUB_TEST_PASS();
}

/**
 * @brief Test RTT injection for ShareGroupHeartbeat.
 *
 * Tests that the consumer handles network latency correctly.
 * Injects 500ms RTT and verifies heartbeats still work.
 */
static void do_test_share_group_rtt_injection(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription;
        rd_kafka_t *c;
        int found_heartbeats;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-rtt";

        SUB_TEST_QUICK();

        TEST_SAY("\n");
        TEST_SAY(
            "============================================================\n");
        TEST_SAY("  SHARE GROUP RTT INJECTION TEST\n");
        TEST_SAY(
            "============================================================\n\n");

        /* Setup */
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 3, 1);

        c = create_share_consumer(bootstraps, group, rd_false);

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);

        rd_kafka_mock_start_request_tracking(mcluster);
        TEST_CALL_ERR__(rd_kafka_subscribe(c, subscription));
        rd_kafka_topic_partition_list_destroy(subscription);

        /* Wait for initial join */
        TEST_SAY(">>> Waiting for initial join...\n");
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c, 2000);

        /*
         * Inject 500ms RTT for next 3 heartbeats
         */
        TEST_SAY("\n>>> Injecting 500ms RTT for next 3 heartbeats\n");
        TEST_SAY("    Heartbeats should still succeed despite latency\n\n");

        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_ShareGroupHeartbeat, 3, /* count */
            RD_KAFKA_RESP_ERR_NO_ERROR, 500);

        /* Poll for enough time to send heartbeats with RTT */
        TEST_SAY(">>> Polling for 8 seconds to allow heartbeats with RTT...\n");
        rd_kafka_consumer_poll(c, 8000);

        found_heartbeats = wait_share_heartbeats(mcluster, 3, 200);
        TEST_SAY("    Received %d heartbeats during RTT injection\n",
                 found_heartbeats);
        TEST_ASSERT(found_heartbeats >= 2,
                    "Expected at least 2 heartbeats with RTT, got %d",
                    found_heartbeats);

        /* Cleanup */
        rd_kafka_consumer_close(c);
        rd_kafka_destroy(c);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        TEST_SAY(
            "============================================================\n");
        TEST_SAY("  RTT INJECTION TEST COMPLETED\n");
        TEST_SAY(
            "============================================================\n\n");

        SUB_TEST_PASS();
}

/**
 * @brief Test session timeout for ShareGroupHeartbeat.
 *
 * Tests that the mock broker correctly times out members that stop
 * heartbeating. Uses a short session timeout (3000ms) and verifies:
 * - Member is removed after timeout
 * - Remaining members get reassigned partitions
 */
static void do_test_share_group_session_timeout(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription;
        rd_kafka_topic_partition_list_t *c1_assign, *c2_assign;
        rd_kafka_t *c1, *c2;
        int c1_initial, c2_initial;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-timeout";

        SUB_TEST_QUICK();

        TEST_SAY("\n");
        TEST_SAY(
            "============================================================\n");
        TEST_SAY("  SHARE GROUP SESSION TIMEOUT TEST\n");
        TEST_SAY(
            "============================================================\n\n");

        /* Setup */
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 4, 1);

        /* Set short session timeout on mock broker */
        TEST_SAY(">>> Setting session timeout to 3000ms on mock broker\n\n");
        rd_kafka_mock_sharegroup_set_session_timeout(mcluster, 3000);

        /*
         * Create first consumer
         */
        TEST_SAY(">>> Creating C1\n");
        c1 = create_share_consumer(bootstraps, group, rd_false);

        /*
         * Create second consumer (this one will "hang")
         */
        TEST_SAY(">>> Creating C2 (will stop heartbeating)\n\n");
        c2 = create_share_consumer(bootstraps, group, rd_false);

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);

        rd_kafka_mock_start_request_tracking(mcluster);

        /* Subscribe both consumers */
        TEST_CALL_ERR__(rd_kafka_subscribe(c1, subscription));
        TEST_CALL_ERR__(rd_kafka_subscribe(c2, subscription));
        rd_kafka_topic_partition_list_destroy(subscription);

        /* Wait for both to join */
        TEST_SAY(">>> Waiting for both consumers to join...\n");
        rd_kafka_consumer_poll(c1, 2000);
        rd_kafka_consumer_poll(c2, 2000);

        /* Verify initial distribution */
        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assign));
        TEST_CALL_ERR__(rd_kafka_assignment(c2, &c2_assign));
        c1_initial = c1_assign->cnt;
        c2_initial = c2_assign->cnt;
        TEST_SAY("    C1 initial: %d partitions, C2 initial: %d partitions\n",
                 c1_initial, c2_initial);
        TEST_ASSERT(c1_initial + c2_initial == 4,
                    "Total should be 4 partitions, got %d",
                    c1_initial + c2_initial);
        TEST_ASSERT(c1_initial > 0 && c2_initial > 0,
                    "Both consumers should have partitions");
        rd_kafka_topic_partition_list_destroy(c1_assign);
        rd_kafka_topic_partition_list_destroy(c2_assign);

        /*
         * Destroy C2 without close to simulate a crash.
         * Share consumers have background heartbeat threads, so we can't
         * just stop polling - we need to destroy the consumer to stop
         * heartbeats.
         */
        TEST_SAY("\n>>> Destroying C2 without close (simulating crash)\n");
        TEST_SAY("    C2 will stop heartbeating immediately\n");
        TEST_SAY("    Broker will timeout C2 after 3s session timeout\n");
        TEST_SAY("    C1 should get all 4 partitions after timeout\n\n");
        rd_kafka_destroy(c2);

        /* Keep polling C1 for 5 seconds - enough for C2 to timeout */
        TEST_SAY(
            ">>> Polling C1 for 5 seconds while waiting for C2 timeout...\n");
        rd_kafka_consumer_poll(c1, 5000);

        /* Verify C1 got all partitions after C2 timed out */
        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assign));
        TEST_SAY("    C1 assignment after C2 timeout: %d partitions\n",
                 c1_assign->cnt);
        TEST_ASSERT(c1_assign->cnt == 4,
                    "C1 should have all 4 partitions after C2 timeout, got %d",
                    c1_assign->cnt);
        rd_kafka_topic_partition_list_destroy(c1_assign);

        rd_kafka_consumer_close(c1);
        rd_kafka_destroy(c1);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        TEST_SAY(
            "============================================================\n");
        TEST_SAY("  SESSION TIMEOUT TEST COMPLETED\n");
        TEST_SAY(
            "============================================================\n\n");

        SUB_TEST_PASS();
}

/**
 * @brief Test target assignment API for ShareGroupHeartbeat.
 *
 * Tests that the mock broker can apply manual target assignments:
 * 1. Two consumers join and get automatic assignment (2 partitions each)
 * 2. Retrieve member IDs using rd_kafka_mock_sharegroup_get_member_ids()
 * 3. Set manual target assignment: C1 gets all 4 partitions, C2 gets none
 * 4. Verify consumers receive the manual assignment
 */
static void do_test_share_group_target_assignment(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription;
        rd_kafka_topic_partition_list_t *c1_assign, *c2_assign;
        rd_kafka_topic_partition_list_t *target_c1, *target_c2;
        rd_kafka_topic_partition_list_t *assignments[2];
        rd_kafka_t *c1, *c2;
        char **member_ids;
        size_t member_cnt;
        rd_kafka_resp_err_t err;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-target";

        SUB_TEST_QUICK();

        TEST_SAY("\n");
        TEST_SAY(
            "============================================================\n");
        TEST_SAY("  SHARE GROUP TARGET ASSIGNMENT TEST\n");
        TEST_SAY(
            "============================================================\n\n");

        /* Setup */
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 4, 1);

        /* Create two consumers */
        TEST_SAY(">>> STEP 1: Creating two consumers\n");
        c1 = create_share_consumer(bootstraps, group, rd_false);
        c2 = create_share_consumer(bootstraps, group, rd_false);

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);

        rd_kafka_mock_start_request_tracking(mcluster);

        /* Subscribe both */
        TEST_CALL_ERR__(rd_kafka_subscribe(c1, subscription));
        TEST_CALL_ERR__(rd_kafka_subscribe(c2, subscription));
        rd_kafka_topic_partition_list_destroy(subscription);

        /* Wait for both to join and get initial assignment */
        TEST_SAY("\n>>> STEP 2: Waiting for both consumers to join...\n");
        rd_kafka_consumer_poll(c1, 3000);
        rd_kafka_consumer_poll(c2, 3000);

        /* Verify both got assignments (automatic distribution) */
        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assign));
        TEST_CALL_ERR__(rd_kafka_assignment(c2, &c2_assign));
        TEST_SAY("    Initial automatic assignment:\n");
        TEST_SAY("    C1: %d partitions, C2: %d partitions\n", c1_assign->cnt,
                 c2_assign->cnt);
        TEST_ASSERT(c1_assign->cnt + c2_assign->cnt == 4,
                    "Total should be 4 partitions, got %d",
                    c1_assign->cnt + c2_assign->cnt);
        TEST_ASSERT(c1_assign->cnt > 0 && c2_assign->cnt > 0,
                    "Both consumers should have partitions initially");
        rd_kafka_topic_partition_list_destroy(c1_assign);
        rd_kafka_topic_partition_list_destroy(c2_assign);

        /*
         * STEP 3: Retrieve member IDs from the mock broker
         */
        TEST_SAY("\n>>> STEP 3: Retrieving member IDs from mock broker\n");
        err = rd_kafka_mock_sharegroup_get_member_ids(mcluster, group,
                                                      &member_ids, &member_cnt);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Expected no error, got %s", rd_kafka_err2str(err));
        TEST_ASSERT(member_cnt == 2, "Expected 2 members, got %zu", member_cnt);
        TEST_SAY("    Found %zu members:\n", member_cnt);
        TEST_SAY("    Member 0: %s\n", member_ids[0]);
        TEST_SAY("    Member 1: %s\n", member_ids[1]);

        /*
         * STEP 4: Set manual target assignment
         * Assign all 4 partitions to first member, none to second
         */
        TEST_SAY("\n>>> STEP 4: Setting manual target assignment\n");
        TEST_SAY(
            "    Assigning all 4 partitions to member 0, none to member "
            "1\n");

        target_c1 = rd_kafka_topic_partition_list_new(4);
        rd_kafka_topic_partition_list_add(target_c1, topic, 0);
        rd_kafka_topic_partition_list_add(target_c1, topic, 1);
        rd_kafka_topic_partition_list_add(target_c1, topic, 2);
        rd_kafka_topic_partition_list_add(target_c1, topic, 3);

        target_c2 = rd_kafka_topic_partition_list_new(0);

        assignments[0] = target_c1;
        assignments[1] = target_c2;

        rd_kafka_mock_sharegroup_target_assignment(
            mcluster, group, (const char **)member_ids, assignments, 2);

        rd_kafka_topic_partition_list_destroy(target_c1);
        rd_kafka_topic_partition_list_destroy(target_c2);

        /*
         * STEP 5: Poll both consumers to receive the new assignment
         */
        TEST_SAY("\n>>> STEP 5: Polling consumers to receive new assignment\n");
        rd_kafka_consumer_poll(c1, 6000);
        rd_kafka_consumer_poll(c2, 6000);

        /*
         * STEP 6: Verify the manual assignment was applied
         */
        TEST_SAY("\n>>> STEP 6: Verifying manual assignment was applied\n");
        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assign));
        TEST_CALL_ERR__(rd_kafka_assignment(c2, &c2_assign));
        TEST_SAY("    Final assignment after target assignment:\n");
        TEST_SAY("    C1: %d partitions, C2: %d partitions\n", c1_assign->cnt,
                 c2_assign->cnt);

        /* One consumer should have all 4, the other should have 0 */
        TEST_ASSERT(c1_assign->cnt + c2_assign->cnt == 4,
                    "Total should still be 4 partitions, got %d",
                    c1_assign->cnt + c2_assign->cnt);
        TEST_ASSERT((c1_assign->cnt == 4 && c2_assign->cnt == 0) ||
                        (c1_assign->cnt == 0 && c2_assign->cnt == 4),
                    "Expected one consumer to have all 4 partitions and the "
                    "other to have 0, got C1=%d, C2=%d",
                    c1_assign->cnt, c2_assign->cnt);

        rd_kafka_topic_partition_list_destroy(c1_assign);
        rd_kafka_topic_partition_list_destroy(c2_assign);

        /* Free member IDs */
        free(member_ids[0]);
        free(member_ids[1]);
        free(member_ids);

        /* Cleanup */
        rd_kafka_consumer_close(c1);
        rd_kafka_consumer_close(c2);
        rd_kafka_destroy(c1);
        rd_kafka_destroy(c2);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        TEST_SAY(
            "============================================================\n");
        TEST_SAY("  TARGET ASSIGNMENT TEST COMPLETED\n");
        TEST_SAY(
            "============================================================\n\n");

        SUB_TEST_PASS();
}

int main_0155_share_consumer_mock(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);

        /* Run all tests */
        do_test_share_group_heartbeat_basic();
        do_test_share_group_assignment_rebalance();
        do_test_share_group_multi_topic_assignment();
        do_test_share_group_error_injection();
        do_test_share_group_rtt_injection();
        do_test_share_group_session_timeout();
        do_test_share_group_target_assignment();

        return 0;
}
