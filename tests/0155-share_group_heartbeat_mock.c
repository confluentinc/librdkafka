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
 * @brief Create a share consumer connected to mock cluster.
 */
static rd_kafka_t *create_share_consumer(const char *bootstraps,
                                         const char *group_id) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        char errstr[512];

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group_id);
        test_conf_set(conf, "share.consumer", "true");
        test_conf_set(conf, "group.protocol", "consumer");
        test_conf_set(conf, "auto.offset.reset", "earliest");

        rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(rk != NULL, "Failed to create share consumer: %s", errstr);

        return rk;
}

/**
 * @brief Test basic ShareGroupHeartbeat flow:
 *        join, receive assignment, heartbeats, leave.
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

        /* Setup */
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 3, 1);

        c = create_share_consumer(bootstraps, group);

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);

        rd_kafka_mock_start_request_tracking(mcluster);
        TEST_CALL_ERR__(rd_kafka_subscribe(c, subscription));
        rd_kafka_topic_partition_list_destroy(subscription);

        /* Wait for join heartbeat */
        found_heartbeats = wait_share_heartbeats(mcluster, 1, 500);
        TEST_ASSERT(found_heartbeats >= 1,
                    "Expected at least 1 heartbeat, got %d", found_heartbeats);

        /* Poll to process response and trigger more heartbeats */
        rd_kafka_consumer_poll(c, 2000);

        /* Verify multiple heartbeats */
        found_heartbeats = wait_share_heartbeats(mcluster, 2, 200);
        TEST_ASSERT(found_heartbeats >= 2,
                    "Expected at least 2 heartbeats, got %d", found_heartbeats);

        /* Close consumer (sends leave heartbeat) */
        rd_kafka_consumer_close(c);
        rd_kafka_destroy(c);

        /* Verify leave heartbeat was sent */
        found_heartbeats = wait_share_heartbeats(mcluster, 3, 200);

        /* Verify no more heartbeats after leave */
        rd_kafka_mock_stop_request_tracking(mcluster);
        rd_kafka_mock_start_request_tracking(mcluster);
        rd_sleep(3);
        found_heartbeats = wait_share_heartbeats(mcluster, 0, 100);
        TEST_ASSERT(found_heartbeats == 0,
                    "Expected 0 heartbeats after leave, got %d",
                    found_heartbeats);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief Test assignment redistribution when consumers join/leave.
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

        /* Setup */
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 3, 1);

        c1 = create_share_consumer(bootstraps, group);

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);

        rd_kafka_mock_start_request_tracking(mcluster);
        TEST_CALL_ERR__(rd_kafka_subscribe(c1, subscription));

        /* C1 joins - should get all 3 partitions */
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c1, 2000);

        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assignment));
        TEST_ASSERT(c1_assignment->cnt == 3,
                    "Expected C1 to have 3 partitions, got %d",
                    c1_assignment->cnt);
        rd_kafka_topic_partition_list_destroy(c1_assignment);

        /* C2 joins - partitions should be redistributed */
        c2 = create_share_consumer(bootstraps, group);
        TEST_CALL_ERR__(rd_kafka_subscribe(c2, subscription));
        rd_kafka_topic_partition_list_destroy(subscription);

        wait_share_heartbeats(mcluster, 3, 500);
        rd_kafka_consumer_poll(c1, 2000);
        rd_kafka_consumer_poll(c2, 2000);

        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assignment));
        TEST_CALL_ERR__(rd_kafka_assignment(c2, &c2_assignment));
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

        /* C2 leaves - C1 should get all partitions back */
        rd_kafka_consumer_close(c2);
        rd_kafka_destroy(c2);

        rd_kafka_consumer_poll(c1, 6000);

        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assignment));
        TEST_ASSERT(c1_assignment->cnt == 3,
                    "Expected C1 to have 3 partitions after C2 left, got %d",
                    c1_assignment->cnt);
        rd_kafka_topic_partition_list_destroy(c1_assignment);

        /* Cleanup */
        rd_kafka_consumer_close(c1);
        rd_kafka_destroy(c1);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

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
 *        C1: both topics, C2: orders only, C3: events only
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

        /* Setup: orders (4 partitions), events (2 partitions) */
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic_orders, 4, 1);
        rd_kafka_mock_topic_create(mcluster, topic_events, 2, 1);

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

        /* C1 joins (both topics) - should get all 6 partitions */
        c1 = create_share_consumer(bootstraps, group);
        TEST_CALL_ERR__(rd_kafka_subscribe(c1, sub_both));
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c1, 2000);

        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assign));
        TEST_ASSERT(c1_assign->cnt == 6,
                    "C1 should have all 6 partitions, got %d", c1_assign->cnt);
        rd_kafka_topic_partition_list_destroy(c1_assign);

        /* C2 joins (orders only) - orders should split */
        c2 = create_share_consumer(bootstraps, group);
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

        TEST_ASSERT(total_orders == 4, "Total orders should be 4, got %d",
                    total_orders);
        TEST_ASSERT(total_events == 2, "Total events should be 2, got %d",
                    total_events);
        TEST_ASSERT(count_topic_partitions(c2_assign, topic_orders) > 0,
                    "C2 should have at least 1 orders partition");

        rd_kafka_topic_partition_list_destroy(c1_assign);
        rd_kafka_topic_partition_list_destroy(c2_assign);

        /* C3 joins (events only) - events should split */
        c3 = create_share_consumer(bootstraps, group);
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

        TEST_ASSERT(total_orders == 4, "Total orders should be 4, got %d",
                    total_orders);
        TEST_ASSERT(total_events == 2, "Total events should be 2, got %d",
                    total_events);
        TEST_ASSERT(count_topic_partitions(c3_assign, topic_events) > 0,
                    "C3 should have at least 1 events partition");

        rd_kafka_topic_partition_list_destroy(c1_assign);
        rd_kafka_topic_partition_list_destroy(c2_assign);
        rd_kafka_topic_partition_list_destroy(c3_assign);

        /* C1 leaves - C2 should get all orders, C3 all events */
        rd_kafka_consumer_close(c1);
        rd_kafka_destroy(c1);

        rd_kafka_consumer_poll(c2, 6000);
        rd_kafka_consumer_poll(c3, 6000);

        TEST_CALL_ERR__(rd_kafka_assignment(c2, &c2_assign));
        TEST_CALL_ERR__(rd_kafka_assignment(c3, &c3_assign));

        TEST_ASSERT(count_topic_partitions(c2_assign, topic_orders) == 4,
                    "C2 should have all 4 orders partitions, got %d",
                    count_topic_partitions(c2_assign, topic_orders));
        TEST_ASSERT(count_topic_partitions(c3_assign, topic_events) == 2,
                    "C3 should have all 2 events partitions, got %d",
                    count_topic_partitions(c3_assign, topic_events));

        rd_kafka_topic_partition_list_destroy(c2_assign);
        rd_kafka_topic_partition_list_destroy(c3_assign);

        /* C2 leaves - C3 should still have events only */
        rd_kafka_consumer_close(c2);
        rd_kafka_destroy(c2);

        rd_kafka_consumer_poll(c3, 6000);

        TEST_CALL_ERR__(rd_kafka_assignment(c3, &c3_assign));
        TEST_ASSERT(count_topic_partitions(c3_assign, topic_events) == 2,
                    "C3 should still have 2 events partitions, got %d",
                    count_topic_partitions(c3_assign, topic_events));
        TEST_ASSERT(count_topic_partitions(c3_assign, topic_orders) == 0,
                    "C3 should have 0 orders partitions (not subscribed), "
                    "got %d",
                    count_topic_partitions(c3_assign, topic_orders));
        rd_kafka_topic_partition_list_destroy(c3_assign);

        /* Cleanup */
        rd_kafka_consumer_close(c3);
        rd_kafka_destroy(c3);

        rd_kafka_topic_partition_list_destroy(sub_both);
        rd_kafka_topic_partition_list_destroy(sub_orders);
        rd_kafka_topic_partition_list_destroy(sub_events);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

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

        /* Setup */
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 3, 1);

        c = create_share_consumer(bootstraps, group);

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);

        rd_kafka_mock_start_request_tracking(mcluster);
        TEST_CALL_ERR__(rd_kafka_subscribe(c, subscription));
        rd_kafka_topic_partition_list_destroy(subscription);

        /* Wait for initial join */
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c, 2000);

        /* Verify initial assignment */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_ASSERT(assignment->cnt == 3,
                    "Expected 3 partitions initially, got %d", assignment->cnt);
        rd_kafka_topic_partition_list_destroy(assignment);

        /* Inject UNKNOWN_MEMBER_ID error - consumer should rejoin */
        rd_kafka_mock_stop_request_tracking(mcluster);
        rd_kafka_mock_start_request_tracking(mcluster);

        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_ShareGroupHeartbeat, 1,
            RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID, 0);

        rd_kafka_consumer_poll(c, 6000);

        /* Verify heartbeats were sent after error (proving rejoin happened) */
        {
                int hb_after_error = wait_share_heartbeats(mcluster, 1, 500);
                TEST_ASSERT(hb_after_error >= 2,
                            "Expected at least 2 heartbeats after error "
                            "(error + rejoin), got %d",
                            hb_after_error);
        }

        /* Verify consumer rejoined and got assignment */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_ASSERT(assignment->cnt == 3,
                    "Expected 3 partitions after rejoin, got %d",
                    assignment->cnt);
        rd_kafka_topic_partition_list_destroy(assignment);

        /* Cleanup */
        rd_kafka_consumer_close(c);
        rd_kafka_destroy(c);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

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

        /* Setup */
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 3, 1);

        c = create_share_consumer(bootstraps, group);

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);

        rd_kafka_mock_start_request_tracking(mcluster);
        TEST_CALL_ERR__(rd_kafka_subscribe(c, subscription));
        rd_kafka_topic_partition_list_destroy(subscription);

        /* Wait for initial join */
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c, 2000);

        /* Test RTT injection API - inject 500ms latency */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_ShareGroupHeartbeat, 1,
            RD_KAFKA_RESP_ERR_NO_ERROR, 500);

        rd_kafka_consumer_poll(c, 3000);

        found_heartbeats = wait_share_heartbeats(mcluster, 2, 500);
        TEST_ASSERT(found_heartbeats >= 1,
                    "Expected at least 1 heartbeat after RTT injection, got %d",
                    found_heartbeats);

        /* Cleanup */
        rd_kafka_consumer_close(c);
        rd_kafka_destroy(c);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

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

        /* Setup */
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 4, 1);

        /* Set short session timeout on mock broker */
        rd_kafka_mock_sharegroup_set_session_timeout(mcluster, 3000);

        c1 = create_share_consumer(bootstraps, group);
        c2 = create_share_consumer(bootstraps, group);

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);

        rd_kafka_mock_start_request_tracking(mcluster);

        TEST_CALL_ERR__(rd_kafka_subscribe(c1, subscription));
        TEST_CALL_ERR__(rd_kafka_subscribe(c2, subscription));
        rd_kafka_topic_partition_list_destroy(subscription);

        /* Wait for both to join and rebalance to complete */
        wait_share_heartbeats(mcluster, 3, 500);
        rd_kafka_consumer_poll(c1, 2000);
        rd_kafka_consumer_poll(c2, 2000);

        /* Verify initial distribution */
        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assign));
        TEST_CALL_ERR__(rd_kafka_assignment(c2, &c2_assign));
        c1_initial = c1_assign->cnt;
        c2_initial = c2_assign->cnt;
        TEST_ASSERT(c1_initial + c2_initial == 4,
                    "Total should be 4 partitions, got %d",
                    c1_initial + c2_initial);
        TEST_ASSERT(c1_initial > 0 && c2_initial > 0,
                    "Both consumers should have partitions");
        rd_kafka_topic_partition_list_destroy(c1_assign);
        rd_kafka_topic_partition_list_destroy(c2_assign);

        /* Destroy C2 without close to simulate crash */
        rd_kafka_destroy(c2);

        /* Poll C1 for 5 seconds - enough for C2 to timeout */
        rd_kafka_consumer_poll(c1, 5000);

        /* Verify C1 got all partitions after C2 timed out */
        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assign));
        TEST_ASSERT(c1_assign->cnt == 4,
                    "C1 should have all 4 partitions after C2 timeout, got %d",
                    c1_assign->cnt);
        rd_kafka_topic_partition_list_destroy(c1_assign);

        rd_kafka_consumer_close(c1);
        rd_kafka_destroy(c1);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

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

        /* Setup */
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 4, 1);

        c1 = create_share_consumer(bootstraps, group);
        c2 = create_share_consumer(bootstraps, group);

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);

        rd_kafka_mock_start_request_tracking(mcluster);

        TEST_CALL_ERR__(rd_kafka_subscribe(c1, subscription));
        TEST_CALL_ERR__(rd_kafka_subscribe(c2, subscription));
        rd_kafka_topic_partition_list_destroy(subscription);

        /* Wait for both to join and rebalance to complete */
        wait_share_heartbeats(mcluster, 3, 500);
        rd_kafka_consumer_poll(c1, 3000);
        rd_kafka_consumer_poll(c2, 3000);

        /* Verify initial automatic assignment */
        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assign));
        TEST_CALL_ERR__(rd_kafka_assignment(c2, &c2_assign));
        TEST_ASSERT(c1_assign->cnt + c2_assign->cnt == 4,
                    "Total should be 4 partitions, got %d",
                    c1_assign->cnt + c2_assign->cnt);
        TEST_ASSERT(c1_assign->cnt > 0 && c2_assign->cnt > 0,
                    "Both consumers should have partitions initially");
        rd_kafka_topic_partition_list_destroy(c1_assign);
        rd_kafka_topic_partition_list_destroy(c2_assign);

        /* Retrieve member IDs */
        err = rd_kafka_mock_sharegroup_get_member_ids(mcluster, group,
                                                      &member_ids, &member_cnt);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Expected no error, got %s", rd_kafka_err2str(err));
        TEST_ASSERT(member_cnt == 2, "Expected 2 members, got %zu", member_cnt);

        /* Set manual target assignment: all to first member */
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

        /* Poll to receive new assignment */
        rd_kafka_consumer_poll(c1, 6000);
        rd_kafka_consumer_poll(c2, 6000);

        /* Verify manual assignment was applied */
        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assign));
        TEST_CALL_ERR__(rd_kafka_assignment(c2, &c2_assign));

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

        SUB_TEST_PASS();
}

int main_0155_share_group_heartbeat_mock(int argc, char **argv) {
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
