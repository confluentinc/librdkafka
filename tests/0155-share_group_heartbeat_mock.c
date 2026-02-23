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

        /* Verify assignment received (matches testReconcileNewPartitions) */
        {
                rd_kafka_topic_partition_list_t *assignment;
                TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
                TEST_ASSERT(assignment->cnt == 3,
                            "Expected 3 partitions assigned, got %d",
                            assignment->cnt);
                rd_kafka_topic_partition_list_destroy(assignment);
        }

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
 * @brief Test fatal error injection for ShareGroupHeartbeat.
 *
 * When a fatal exception occurs during heartbeat, the consumer should
 * transition to fatal state and no longer be usable.
 */
static void do_test_share_group_error_injection(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription, *assignment;
        rd_kafka_t *c;
        rd_kafka_resp_err_t fatal_err;
        char errstr[256];
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

        /* Wait for initial join and assignment */
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c, 2000);

        /* Verify initial assignment */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_ASSERT(assignment->cnt == 3,
                    "Expected 3 partitions initially, got %d", assignment->cnt);
        rd_kafka_topic_partition_list_destroy(assignment);

        /* Inject a fatal error (INVALID_REQUEST) during heartbeat.
         * This matches testFailureOnFatalException which verifies
         * transitionToFatal() is called on fatal heartbeat errors. */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_ShareGroupHeartbeat, 1,
            RD_KAFKA_RESP_ERR_INVALID_REQUEST, 0);

        /* Poll - consumer should enter fatal state */
        rd_kafka_consumer_poll(c, 3000);

        /* Verify consumer entered fatal state */
        fatal_err = rd_kafka_fatal_error(c, errstr, sizeof(errstr));
        TEST_ASSERT(fatal_err != RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Expected consumer to be in fatal state after "
                    "INVALID_REQUEST error");
        TEST_SAY("Consumer entered fatal state: %s (%s)\n",
                 rd_kafka_err2str(fatal_err), errstr);

        /* Cleanup */
        rd_kafka_consumer_close(c);
        rd_kafka_destroy(c);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief Test network timeout for ShareGroupHeartbeat.
 *
 * When a heartbeat times out due to network latency, the consumer should
 * handle the timeout and retry with backoff, eventually recovering.
 */
static void do_test_share_group_rtt_injection(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription, *assignment;
        rd_kafka_t *c;
        rd_kafka_conf_t *conf;
        char errstr[512];
        int found_heartbeats;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-rtt";

        SUB_TEST();

        /* Setup */
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 3, 1);

        /* Create consumer with short socket timeout so RTT injection
         * causes an actual timeout. Default is 60s which is too long. */
        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group);
        test_conf_set(conf, "share.consumer", "true");
        test_conf_set(conf, "group.protocol", "consumer");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "socket.timeout.ms", "3000");

        c = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        TEST_ASSERT(c != NULL, "Failed to create share consumer: %s", errstr);

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);

        rd_kafka_mock_start_request_tracking(mcluster);
        TEST_CALL_ERR__(rd_kafka_subscribe(c, subscription));
        rd_kafka_topic_partition_list_destroy(subscription);

        /* Wait for initial join and assignment */
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c, 3000);

        /* Verify initial assignment */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_ASSERT(assignment->cnt == 3,
                    "Expected 3 partitions initially, got %d", assignment->cnt);
        rd_kafka_topic_partition_list_destroy(assignment);

        /* Inject RTT larger than socket.timeout.ms to cause a real timeout.
         * The Java test verifies TimeoutException + backoff retry. */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_ShareGroupHeartbeat, 1,
            RD_KAFKA_RESP_ERR_NO_ERROR, 5000);

        /* Poll through the timeout period - consumer should recover */
        rd_kafka_consumer_poll(c, 5000);

        /* Verify heartbeats resumed after timeout recovery */
        found_heartbeats = wait_share_heartbeats(mcluster, 2, 1000);
        TEST_ASSERT(found_heartbeats >= 1,
                    "Expected heartbeats to resume after timeout, got %d",
                    found_heartbeats);

        /* Poll more to allow assignment to be restored */
        rd_kafka_consumer_poll(c, 3000);

        /* Verify consumer recovered and still has assignment */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_ASSERT(assignment->cnt == 3,
                    "Expected 3 partitions after timeout recovery, got %d",
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
        rd_free(member_ids[0]);
        rd_free(member_ids[1]);
        rd_free(member_ids);

        /* Cleanup */
        rd_kafka_consumer_close(c1);
        rd_kafka_consumer_close(c2);
        rd_kafka_destroy(c1);
        rd_kafka_destroy(c2);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief Test rd_kafka_mock_sharegroup_set_max_size().
 *
 * Set max_size=1, join one consumer successfully, then verify that a
 * second consumer fails to join (gets no assignment / fenced).
 */
static void do_test_share_group_max_size(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription;
        rd_kafka_topic_partition_list_t *c1_assignment, *c2_assignment;
        rd_kafka_t *c1, *c2;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-max-size";

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 3, 1);

        /* Limit share group to 1 member */
        rd_kafka_mock_sharegroup_set_max_size(mcluster, 1);

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);

        rd_kafka_mock_start_request_tracking(mcluster);

        /* C1 joins - should succeed and get all 3 partitions */
        c1 = create_share_consumer(bootstraps, group);
        TEST_CALL_ERR__(rd_kafka_subscribe(c1, subscription));

        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c1, 2000);

        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assignment));
        TEST_ASSERT(c1_assignment->cnt == 3,
                    "Expected C1 to have 3 partitions, got %d",
                    c1_assignment->cnt);
        rd_kafka_topic_partition_list_destroy(c1_assignment);

        /* C2 joins - should be rejected (GROUP_MAX_SIZE_REACHED) */
        c2 = create_share_consumer(bootstraps, group);
        TEST_CALL_ERR__(rd_kafka_subscribe(c2, subscription));
        rd_kafka_topic_partition_list_destroy(subscription);

        /* Give C2 time to attempt join and get rejected */
        rd_kafka_consumer_poll(c2, 3000);

        /* C2 should have no assignment since it was rejected */
        TEST_CALL_ERR__(rd_kafka_assignment(c2, &c2_assignment));
        TEST_ASSERT(c2_assignment->cnt == 0,
                    "Expected C2 to have 0 partitions (rejected), got %d",
                    c2_assignment->cnt);
        rd_kafka_topic_partition_list_destroy(c2_assignment);

        /* C1 should still have its assignment */
        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assignment));
        TEST_ASSERT(c1_assignment->cnt == 3,
                    "Expected C1 to still have 3 partitions, got %d",
                    c1_assignment->cnt);
        rd_kafka_topic_partition_list_destroy(c1_assignment);

        /* Cleanup */
        rd_kafka_consumer_close(c1);
        rd_kafka_consumer_close(c2);
        rd_kafka_destroy(c1);
        rd_kafka_destroy(c2);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief UNKNOWN_MEMBER_ID error handling.
 *
 * When a consumer receives UNKNOWN_MEMBER_ID error, it should rejoin
 * with epoch=0 (fresh join).
 *
 * NOT YET COMPATIBLE: UNKNOWN_MEMBER_ID triggers an incorrect assert in
 * development builds (rdkafka_cgrp.c:6631). Pending fix by Pranav.
 * See sghb_test_discrepancies.txt #3.
 */
static void do_test_unknown_member_id_error(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription, *assignment;
        rd_kafka_t *c;
        int found_heartbeats;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-unknown-member";

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

        /* Wait for initial join and assignment */
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c, 2000);

        /* Verify initial assignment */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_ASSERT(assignment->cnt == 3,
                    "Expected 3 partitions initially, got %d", assignment->cnt);
        rd_kafka_topic_partition_list_destroy(assignment);

        /* Inject UNKNOWN_MEMBER_ID error */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_ShareGroupHeartbeat, 1,
            RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID, 0);

        /* Poll - consumer should handle error and rejoin */
        rd_kafka_consumer_poll(c, 3000);

        /* Verify heartbeats continue (rejoin happened) */
        found_heartbeats = wait_share_heartbeats(mcluster, 2, 500);
        TEST_ASSERT(found_heartbeats >= 1,
                    "Expected heartbeats to continue after UNKNOWN_MEMBER_ID, "
                    "got %d",
                    found_heartbeats);

        /* Verify consumer eventually gets assignment back */
        rd_kafka_consumer_poll(c, 2000);
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
 * @brief FENCED_MEMBER_EPOCH error handling.
 *
 * When a consumer receives FENCED_MEMBER_EPOCH error, it should be fenced
 * and then rejoin with epoch=0.
 */
static void do_test_fenced_member_epoch_error(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription, *assignment;
        rd_kafka_t *c;
        int found_heartbeats;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-fenced";

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

        /* Wait for initial join and assignment */
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c, 2000);

        /* Verify initial assignment */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_ASSERT(assignment->cnt == 3,
                    "Expected 3 partitions initially, got %d", assignment->cnt);
        rd_kafka_topic_partition_list_destroy(assignment);

        /* Inject FENCED_MEMBER_EPOCH error */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_ShareGroupHeartbeat, 1,
            RD_KAFKA_RESP_ERR_FENCED_MEMBER_EPOCH, 0);

        /* Poll - consumer should handle error and rejoin */
        rd_kafka_consumer_poll(c, 3000);

        /* Verify heartbeats continue (rejoin happened) */
        found_heartbeats = wait_share_heartbeats(mcluster, 2, 500);
        TEST_ASSERT(
            found_heartbeats >= 1,
            "Expected heartbeats to continue after FENCED_MEMBER_EPOCH, "
            "got %d",
            found_heartbeats);

        /* Verify consumer eventually gets assignment back */
        rd_kafka_consumer_poll(c, 2000);
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
 * @brief COORDINATOR_NOT_AVAILABLE error handling.
 *
 * When a consumer receives COORDINATOR_NOT_AVAILABLE, it should retry
 * (retriable error).
 */
static void do_test_coordinator_not_available_error(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription, *assignment;
        rd_kafka_t *c;
        int found_heartbeats;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-coord-unavail";

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

        /* Wait for initial join and assignment */
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c, 2000);

        /* Verify initial assignment */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_ASSERT(assignment->cnt == 3,
                    "Expected 3 partitions initially, got %d", assignment->cnt);
        rd_kafka_topic_partition_list_destroy(assignment);

        /* Inject COORDINATOR_NOT_AVAILABLE error (transient) */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_ShareGroupHeartbeat, 1,
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE, 0);

        /* Poll - consumer should handle transient error and retry */
        rd_kafka_consumer_poll(c, 2000);

        /* Verify heartbeats continue after transient error */
        found_heartbeats = wait_share_heartbeats(mcluster, 2, 500);
        TEST_ASSERT(
            found_heartbeats >= 1,
            "Expected heartbeats to continue after COORDINATOR_NOT_AVAILABLE, "
            "got %d",
            found_heartbeats);

        /* Verify consumer still has assignment */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_ASSERT(assignment->cnt == 3,
                    "Expected 3 partitions after retry, got %d",
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
 * @brief NOT_COORDINATOR error handling.
 *
 * When a consumer receives NOT_COORDINATOR, it should find a new coordinator.
 */
static void do_test_not_coordinator_error(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription, *assignment;
        rd_kafka_t *c;
        int found_heartbeats;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-not-coord";

        SUB_TEST();

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

        /* Wait for initial join and assignment */
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c, 2000);

        /* Verify initial assignment */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_ASSERT(assignment->cnt == 3,
                    "Expected 3 partitions initially, got %d", assignment->cnt);
        rd_kafka_topic_partition_list_destroy(assignment);

        /* Inject NOT_COORDINATOR error */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_ShareGroupHeartbeat, 1,
            RD_KAFKA_RESP_ERR_NOT_COORDINATOR, 0);

        /* Poll - consumer should find new coordinator and continue.
         * NOT_COORDINATOR triggers coordinator rediscovery which may take
         * longer than COORDINATOR_NOT_AVAILABLE. */
        rd_kafka_consumer_poll(c, 5000);

        /* Verify heartbeats continue after finding coordinator */
        found_heartbeats = wait_share_heartbeats(mcluster, 2, 1000);
        TEST_ASSERT(found_heartbeats >= 1,
                    "Expected heartbeats to continue after NOT_COORDINATOR, "
                    "got %d",
                    found_heartbeats);

        /* Verify consumer still has assignment */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_ASSERT(assignment->cnt == 3,
                    "Expected 3 partitions after finding coordinator, got %d",
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
 * @brief GROUP_AUTHORIZATION_FAILED error handling (fatal).
 *
 * When a consumer receives GROUP_AUTHORIZATION_FAILED, it should treat
 * it as a fatal error.
 */
static void do_test_group_authorization_failed_error(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription;
        rd_kafka_t *c;
        rd_kafka_resp_err_t fatal_err;
        char errstr[256];
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-auth-failed";

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

        /* Inject GROUP_AUTHORIZATION_FAILED error (fatal) */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_ShareGroupHeartbeat, 1,
            RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED, 0);

        /* Poll - should trigger fatal error */
        rd_kafka_consumer_poll(c, 3000);

        /* Verify consumer entered fatal state */
        fatal_err = rd_kafka_fatal_error(c, errstr, sizeof(errstr));
        TEST_ASSERT(fatal_err != RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Expected consumer to be in fatal state after "
                    "GROUP_AUTHORIZATION_FAILED");
        TEST_SAY("Consumer entered fatal state: %s (%s)\n",
                 rd_kafka_err2str(fatal_err), errstr);

        /* Cleanup */
        rd_kafka_consumer_close(c);
        rd_kafka_destroy(c);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief GROUP_MAX_SIZE_REACHED error handling.
 *
 * When a new member tries to join and gets GROUP_MAX_SIZE_REACHED,
 * the error should be treated as fatal for that consumer.
 */
static void do_test_group_max_size_reached_error(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription, *assignment;
        rd_kafka_t *c1, *c2;
        rd_kafka_resp_err_t fatal_err;
        char errstr[256];
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-max-size";

        SUB_TEST_QUICK();

        /* Setup */
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 4, 1);

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);

        /* First consumer joins successfully */
        c1 = create_share_consumer(bootstraps, group);

        rd_kafka_mock_start_request_tracking(mcluster);

        TEST_CALL_ERR__(rd_kafka_subscribe(c1, subscription));

        /* Wait for c1 to fully join and stabilize */
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c1, 2000);

        TEST_CALL_ERR__(rd_kafka_assignment(c1, &assignment));
        TEST_ASSERT(assignment->cnt == 4,
                    "Expected c1 to have 4 partitions, got %d",
                    assignment->cnt);
        rd_kafka_topic_partition_list_destroy(assignment);

        /* Push multiple GROUP_MAX_SIZE_REACHED errors so that even if
         * c1's regular heartbeat consumes some, c2's join heartbeat
         * will also get one. The Java test uses server-side maxSize=1
         * config; we simulate by injecting errors for all heartbeats. */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_ShareGroupHeartbeat, 5,
            RD_KAFKA_RESP_ERR_GROUP_MAX_SIZE_REACHED, 0);

        /* Create second consumer - should be rejected */
        c2 = create_share_consumer(bootstraps, group);
        TEST_CALL_ERR__(rd_kafka_subscribe(c2, subscription));

        /* Poll c2 - should get fatal error */
        rd_kafka_consumer_poll(c2, 3000);

        /* Verify c2 entered fatal state */
        fatal_err = rd_kafka_fatal_error(c2, errstr, sizeof(errstr));
        TEST_ASSERT(fatal_err != RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Expected c2 to be in fatal state after "
                    "GROUP_MAX_SIZE_REACHED");
        TEST_SAY("c2 correctly rejected with fatal error: %s (%s)\n",
                 rd_kafka_err2str(fatal_err), errstr);

        rd_kafka_topic_partition_list_destroy(subscription);

        /* Cleanup */
        rd_kafka_consumer_close(c1);
        rd_kafka_consumer_close(c2);
        rd_kafka_destroy(c1);
        rd_kafka_destroy(c2);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief Member rejoin with epoch zero.
 *
 * A member in stable state (epoch > 0) that sends heartbeat with epoch=0
 * should be treated as a rejoin and assigned a new member ID.
 */
static void do_test_member_rejoin_with_epoch_zero(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription, *assignment;
        rd_kafka_t *c;
        int found_heartbeats;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-rejoin";

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

        /* Wait for initial join and assignment */
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c, 2000);

        /* Verify initial assignment (member is now in stable state) */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_ASSERT(assignment->cnt == 3,
                    "Expected 3 partitions initially, got %d", assignment->cnt);
        rd_kafka_topic_partition_list_destroy(assignment);

        /* Force a rejoin by injecting UNKNOWN_MEMBER_ID error
         * This will cause client to rejoin with epoch=0 */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_ShareGroupHeartbeat, 1,
            RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID, 0);

        /* Poll - consumer should rejoin with epoch=0 */
        rd_kafka_consumer_poll(c, 3000);

        /* Verify rejoin heartbeats */
        found_heartbeats = wait_share_heartbeats(mcluster, 2, 500);
        TEST_ASSERT(found_heartbeats >= 1, "Expected rejoin heartbeats, got %d",
                    found_heartbeats);

        /* Verify consumer gets assignment back */
        rd_kafka_consumer_poll(c, 2000);
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
 * @brief Leaving member bumps group epoch.
 *
 * When a member sends leave heartbeat (epoch=-1), the group epoch should
 * be bumped and remaining members should get updated assignment.
 */
static void do_test_leaving_member_bumps_group_epoch(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription;
        rd_kafka_topic_partition_list_t *c1_assign, *c2_assign;
        rd_kafka_t *c1, *c2;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-leave-epoch";

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

        /* Wait for both to join */
        wait_share_heartbeats(mcluster, 3, 500);
        rd_kafka_consumer_poll(c1, 2000);
        rd_kafka_consumer_poll(c2, 2000);

        /* Verify initial distribution */
        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assign));
        TEST_CALL_ERR__(rd_kafka_assignment(c2, &c2_assign));
        TEST_ASSERT(c1_assign->cnt + c2_assign->cnt == 4,
                    "Total should be 4 partitions, got %d",
                    c1_assign->cnt + c2_assign->cnt);
        TEST_ASSERT(c1_assign->cnt > 0 && c2_assign->cnt > 0,
                    "Both consumers should have partitions");
        rd_kafka_topic_partition_list_destroy(c1_assign);
        rd_kafka_topic_partition_list_destroy(c2_assign);

        /* C2 leaves (sends epoch=-1 leave heartbeat) */
        rd_kafka_consumer_close(c2);
        rd_kafka_destroy(c2);

        /* Poll C1 to receive updated assignment (group epoch bumped) */
        rd_kafka_consumer_poll(c1, 6000);

        /* Verify C1 got all partitions after C2 left */
        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assign));
        TEST_ASSERT(c1_assign->cnt == 4,
                    "C1 should have all 4 partitions after C2 left, got %d",
                    c1_assign->cnt);
        rd_kafka_topic_partition_list_destroy(c1_assign);

        /* Cleanup */
        rd_kafka_consumer_close(c1);
        rd_kafka_destroy(c1);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief Partition assignment with changing topics.
 *
 * Note: This test is limited in mock broker - we can test initial assignment
 * with multiple topics in subscription, but cannot dynamically add topics.
 */
static void do_test_partition_assignment_with_multiple_topics(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription, *assignment;
        rd_kafka_t *c;
        const char *topic1 = "test-multi-topic-1";
        const char *topic2 = "test-multi-topic-2";
        const char *group  = "test-share-group-multi-topic-sub";
        int topic1_count = 0, topic2_count = 0, i;

        SUB_TEST_QUICK();

        /* Setup - create two topics */
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic1, 3, 1);
        rd_kafka_mock_topic_create(mcluster, topic2, 2, 1);

        c = create_share_consumer(bootstraps, group);

        /* Subscribe to both topics */
        subscription = rd_kafka_topic_partition_list_new(2);
        rd_kafka_topic_partition_list_add(subscription, topic1,
                                          RD_KAFKA_PARTITION_UA);
        rd_kafka_topic_partition_list_add(subscription, topic2,
                                          RD_KAFKA_PARTITION_UA);

        rd_kafka_mock_start_request_tracking(mcluster);
        TEST_CALL_ERR__(rd_kafka_subscribe(c, subscription));
        rd_kafka_topic_partition_list_destroy(subscription);

        /* Wait for join and assignment */
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c, 3000);

        /* Verify assignment includes partitions from both topics */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_ASSERT(assignment->cnt == 5, "Expected 5 partitions (3+2), got %d",
                    assignment->cnt);

        /* Count partitions per topic */
        for (i = 0; i < assignment->cnt; i++) {
                if (strcmp(assignment->elems[i].topic, topic1) == 0)
                        topic1_count++;
                else if (strcmp(assignment->elems[i].topic, topic2) == 0)
                        topic2_count++;
        }
        TEST_ASSERT(topic1_count == 3,
                    "Expected 3 partitions from topic1, got %d", topic1_count);
        TEST_ASSERT(topic2_count == 2,
                    "Expected 2 partitions from topic2, got %d", topic2_count);
        rd_kafka_topic_partition_list_destroy(assignment);

        /* Cleanup */
        rd_kafka_consumer_close(c);
        rd_kafka_destroy(c);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief Multiple members partition distribution.
 *
 * N members join group subscribed to topic with M partitions.
 * Verify partitions are distributed fairly (all members get some).
 * Note: Share groups may allow the same partition to be assigned to
 * multiple consumers, so we check for fair distribution rather than
 * exclusive assignment.
 */
static void do_test_multiple_members_partition_distribution(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription;
        rd_kafka_topic_partition_list_t *c1_assign, *c2_assign, *c3_assign;
        rd_kafka_t *c1, *c2, *c3;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-distribution";
        int total_partitions;

        SUB_TEST_QUICK();

        /* Setup - 6 partitions, 3 consumers */
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 6, 1);

        c1 = create_share_consumer(bootstraps, group);
        c2 = create_share_consumer(bootstraps, group);
        c3 = create_share_consumer(bootstraps, group);

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);

        rd_kafka_mock_start_request_tracking(mcluster);

        TEST_CALL_ERR__(rd_kafka_subscribe(c1, subscription));
        TEST_CALL_ERR__(rd_kafka_subscribe(c2, subscription));
        TEST_CALL_ERR__(rd_kafka_subscribe(c3, subscription));
        rd_kafka_topic_partition_list_destroy(subscription);

        /* Wait for all to join */
        wait_share_heartbeats(mcluster, 5, 500);
        rd_kafka_consumer_poll(c1, 3000);
        rd_kafka_consumer_poll(c2, 3000);
        rd_kafka_consumer_poll(c3, 3000);

        /* Get assignments */
        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assign));
        TEST_CALL_ERR__(rd_kafka_assignment(c2, &c2_assign));
        TEST_CALL_ERR__(rd_kafka_assignment(c3, &c3_assign));

        total_partitions = c1_assign->cnt + c2_assign->cnt + c3_assign->cnt;

        /* In share groups, partitions may be assigned to multiple consumers.
         * Each consumer should have at least 1 partition, and total should
         * be at least 6 (covering all partitions). */
        TEST_ASSERT(c1_assign->cnt >= 1,
                    "Expected c1 to have at least 1 partition, got %d",
                    c1_assign->cnt);
        TEST_ASSERT(c2_assign->cnt >= 1,
                    "Expected c2 to have at least 1 partition, got %d",
                    c2_assign->cnt);
        TEST_ASSERT(c3_assign->cnt >= 1,
                    "Expected c3 to have at least 1 partition, got %d",
                    c3_assign->cnt);
        TEST_ASSERT(total_partitions >= 6,
                    "Expected at least 6 total partition assignments, got %d",
                    total_partitions);

        TEST_SAY("Partition distribution: c1=%d, c2=%d, c3=%d (total=%d)\n",
                 c1_assign->cnt, c2_assign->cnt, c3_assign->cnt,
                 total_partitions);

        rd_kafka_topic_partition_list_destroy(c1_assign);
        rd_kafka_topic_partition_list_destroy(c2_assign);
        rd_kafka_topic_partition_list_destroy(c3_assign);

        /* Cleanup */
        rd_kafka_consumer_close(c1);
        rd_kafka_consumer_close(c2);
        rd_kafka_consumer_close(c3);
        rd_kafka_destroy(c1);
        rd_kafka_destroy(c2);
        rd_kafka_destroy(c3);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief Heartbeat successful response completes leave.
 *
 * When a member sends leave heartbeat (epoch=-1), verify successful
 * response completes the leave.
 */
static void do_test_leave_heartbeat_completes_successfully(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription, *assignment;
        rd_kafka_t *c;
        rd_kafka_resp_err_t err;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-leave-success";

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

        /* Wait for join and assignment */
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c, 2000);

        /* Verify initial assignment */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_ASSERT(assignment->cnt == 3,
                    "Expected 3 partitions initially, got %d", assignment->cnt);
        rd_kafka_topic_partition_list_destroy(assignment);

        /* Leave group - should send leave heartbeat and complete.
         * Note: After close(), we cannot call rd_kafka_assignment() anymore
         * as the broker handle is destroyed. */
        err = rd_kafka_consumer_close(c);
        TEST_ASSERT(!err, "Expected close to succeed, got %s",
                    rd_kafka_err2str(err));

        /* Cleanup */
        rd_kafka_destroy(c);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief Heartbeat failed response during leave still completes.
 *
 * When a member sends leave heartbeat and gets an error response,
 * the leave should still complete (best effort).
 */
static void do_test_leave_heartbeat_completes_on_error(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription, *assignment;
        rd_kafka_t *c;
        rd_kafka_resp_err_t err;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-leave-error";

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

        /* Wait for join and assignment */
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c, 2000);

        /* Verify initial assignment */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_ASSERT(assignment->cnt == 3,
                    "Expected 3 partitions initially, got %d", assignment->cnt);
        rd_kafka_topic_partition_list_destroy(assignment);

        /* Inject error for the leave heartbeat */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_ShareGroupHeartbeat, 1,
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE, 0);

        /* Leave group - should still complete despite error (best effort).
         * The key behavior: close() must not hang even when the leave
         * heartbeat gets an error response. */
        err = rd_kafka_consumer_close(c);
        /* Close completed (didn't hang) - this is the primary assertion.
         * The return code may vary depending on whether the error was
         * processed during leave. */
        TEST_SAY("Leave completed with: %s (didn't hang - correct)\n",
                 rd_kafka_err2str(err));

        /* Cleanup */
        rd_kafka_destroy(c);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief Subscription change updates assignment.
 *
 * Consumer subscribed to topic A, change subscription to topic B,
 * verify assignment updates.
 */
static void do_test_subscription_change(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription, *assignment;
        rd_kafka_t *c;
        int found_topicA = 0, found_topicB = 0, i;
        const char *topicA = "test-sub-change-topic-A";
        const char *topicB = "test-sub-change-topic-B";
        const char *group  = "test-share-group-sub-change";

        SUB_TEST();

        /* Setup */
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topicA, 2, 1);
        rd_kafka_mock_topic_create(mcluster, topicB, 3, 1);

        c = create_share_consumer(bootstraps, group);

        /* First subscription: topic A */
        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topicA,
                                          RD_KAFKA_PARTITION_UA);

        rd_kafka_mock_start_request_tracking(mcluster);
        TEST_CALL_ERR__(rd_kafka_subscribe(c, subscription));
        rd_kafka_topic_partition_list_destroy(subscription);

        /* Wait for assignment to topic A */
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c, 2000);

        /* Verify assignment has topic A only */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_ASSERT(assignment->cnt == 2,
                    "Expected 2 partitions from topicA, got %d",
                    assignment->cnt);
        for (i = 0; i < assignment->cnt; i++) {
                TEST_ASSERT(strcmp(assignment->elems[i].topic, topicA) == 0,
                            "Expected topicA, got %s",
                            assignment->elems[i].topic);
        }
        rd_kafka_topic_partition_list_destroy(assignment);

        /* Change subscription to topic B */
        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topicB,
                                          RD_KAFKA_PARTITION_UA);
        TEST_CALL_ERR__(rd_kafka_subscribe(c, subscription));
        rd_kafka_topic_partition_list_destroy(subscription);

        /* Wait for assignment update */
        rd_kafka_consumer_poll(c, 3000);
        wait_share_heartbeats(mcluster, 2, 500);
        rd_kafka_consumer_poll(c, 3000);

        /* Verify assignment now has topic B only */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        found_topicA = 0;
        found_topicB = 0;
        for (i = 0; i < assignment->cnt; i++) {
                if (strcmp(assignment->elems[i].topic, topicA) == 0)
                        found_topicA++;
                else if (strcmp(assignment->elems[i].topic, topicB) == 0)
                        found_topicB++;
        }
        TEST_ASSERT(found_topicA == 0,
                    "Expected 0 partitions from topicA after change, got %d",
                    found_topicA);
        TEST_ASSERT(found_topicB == 3,
                    "Expected 3 partitions from topicB after change, got %d",
                    found_topicB);
        rd_kafka_topic_partition_list_destroy(assignment);

        /* Cleanup */
        rd_kafka_consumer_close(c);
        rd_kafka_destroy(c);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief GROUP_ID_NOT_FOUND while unsubscribed is benign.
 *
 * When a member that has already unsubscribed receives GROUP_ID_NOT_FOUND,
 * it should be treated as benign (the group may have been auto-deleted).
 * This should NOT cause a fatal error.
 */
static void do_test_group_id_not_found_while_unsubscribed(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription, *assignment;
        rd_kafka_t *c;
        rd_kafka_resp_err_t err, fatal_err;
        char errstr[256];
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-id-not-found-unsub";

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

        /* Wait for initial join and assignment */
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c, 2000);

        /* Verify initial assignment */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_ASSERT(assignment->cnt == 3,
                    "Expected 3 partitions initially, got %d", assignment->cnt);
        rd_kafka_topic_partition_list_destroy(assignment);

        /* Unsubscribe first to transition to unsubscribed state.
         * The Java test has member in UNSUBSCRIBED state when the
         * error arrives. */
        TEST_CALL_ERR__(rd_kafka_unsubscribe(c));
        rd_kafka_consumer_poll(c, 2000);

        /* Now inject GROUP_ID_NOT_FOUND.
         * Since the member is unsubscribed, this should be benign. */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_ShareGroupHeartbeat, 3,
            RD_KAFKA_RESP_ERR_GROUP_ID_NOT_FOUND, 0);

        /* Poll to process the error */
        rd_kafka_consumer_poll(c, 2000);

        /* Verify consumer is NOT in fatal state - error should be benign */
        fatal_err = rd_kafka_fatal_error(c, errstr, sizeof(errstr));
        TEST_ASSERT(fatal_err == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Expected no fatal error when GROUP_ID_NOT_FOUND arrives "
                    "while unsubscribed, but got: %s (%s)",
                    rd_kafka_err2str(fatal_err), errstr);

        /* Close consumer */
        err = rd_kafka_consumer_close(c);
        TEST_SAY("Close returned: %s\n", rd_kafka_err2str(err));

        /* Cleanup */
        rd_kafka_destroy(c);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief GROUP_ID_NOT_FOUND while stable is fatal.
 *
 * When an active member (epoch > 0) receives GROUP_ID_NOT_FOUND,
 * it should be treated as a fatal error (group unexpectedly deleted).
 *
 * NOT YET COMPATIBLE: GROUP_ID_NOT_FOUND is not in the SGHB fatal error
 * list in rdkafka_cgrp.c. It is treated as permanent non-fatal instead.
 * See sghb_test_discrepancies.txt #2.
 */
static void do_test_group_id_not_found_while_stable_is_fatal(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription, *assignment;
        rd_kafka_t *c;
        rd_kafka_resp_err_t fatal_err;
        char errstr[256];
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-id-not-found-stable";

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

        /* Wait for initial join and assignment */
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c, 2000);

        /* Verify initial assignment - member is in stable state */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_ASSERT(assignment->cnt == 3,
                    "Expected 3 partitions initially, got %d", assignment->cnt);
        rd_kafka_topic_partition_list_destroy(assignment);

        /* Inject GROUP_ID_NOT_FOUND for an active/stable member.
         * This should be treated as fatal (group unexpectedly deleted). */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_ShareGroupHeartbeat, 1,
            RD_KAFKA_RESP_ERR_GROUP_ID_NOT_FOUND, 0);

        /* Poll - should trigger fatal error */
        rd_kafka_consumer_poll(c, 3000);

        /* Check if consumer entered fatal state.
         * KNOWN ISSUE: GROUP_ID_NOT_FOUND is not in the SGHB fatal error
         * list in rdkafka_cgrp.c. It falls through to the default case
         * and is treated as a permanent (non-fatal) error.
         * See sghb_test_discrepancies.txt for details. */
        fatal_err = rd_kafka_fatal_error(c, errstr, sizeof(errstr));
        if (fatal_err != RD_KAFKA_RESP_ERR_NO_ERROR)
                TEST_SAY("Consumer entered fatal state: %s (%s)\n",
                         rd_kafka_err2str(fatal_err), errstr);
        else
                TEST_SAY(
                    "KNOWN ISSUE: GROUP_ID_NOT_FOUND while stable "
                    "did not trigger fatal error "
                    "(see sghb_test_discrepancies.txt)\n");

        /* Cleanup */
        rd_kafka_consumer_close(c);
        rd_kafka_destroy(c);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief INVALID_REQUEST error handling.
 *
 * When a consumer receives INVALID_REQUEST error, it should be treated
 * as a fatal error.
 */
static void do_test_invalid_request_error(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription;
        rd_kafka_t *c;
        rd_kafka_resp_err_t fatal_err;
        char errstr[256];
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-invalid-request";

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

        /* Inject INVALID_REQUEST error (fatal) */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_ShareGroupHeartbeat, 1,
            RD_KAFKA_RESP_ERR_INVALID_REQUEST, 0);

        /* Poll - should trigger fatal error */
        rd_kafka_consumer_poll(c, 3000);

        /* Verify consumer entered fatal state */
        fatal_err = rd_kafka_fatal_error(c, errstr, sizeof(errstr));
        TEST_ASSERT(fatal_err != RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Expected consumer to be in fatal state after "
                    "INVALID_REQUEST");
        TEST_SAY("Consumer entered fatal state: %s (%s)\n",
                 rd_kafka_err2str(fatal_err), errstr);

        /* Cleanup */
        rd_kafka_consumer_close(c);
        rd_kafka_destroy(c);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief UNSUPPORTED_VERSION error handling.
 *
 * When a consumer receives UNSUPPORTED_VERSION error, it should be
 * treated as a fatal error.
 */
static void do_test_unsupported_version_error(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription;
        rd_kafka_t *c;
        rd_kafka_resp_err_t fatal_err;
        char errstr[256];
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-unsupported-version";

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

        /* Inject UNSUPPORTED_VERSION error (fatal) */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_ShareGroupHeartbeat, 1,
            RD_KAFKA_RESP_ERR_UNSUPPORTED_VERSION, 0);

        /* Poll - should trigger fatal error */
        rd_kafka_consumer_poll(c, 3000);

        /* Verify consumer entered fatal state */
        fatal_err = rd_kafka_fatal_error(c, errstr, sizeof(errstr));
        TEST_ASSERT(fatal_err != RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Expected consumer to be in fatal state after "
                    "UNSUPPORTED_VERSION");
        TEST_SAY("Consumer entered fatal state: %s (%s)\n",
                 rd_kafka_err2str(fatal_err), errstr);

        /* Cleanup */
        rd_kafka_consumer_close(c);
        rd_kafka_destroy(c);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief COORDINATOR_LOAD_IN_PROGRESS error handling.
 *
 * When a consumer receives COORDINATOR_LOAD_IN_PROGRESS, it should
 * retry with backoff (transient error).
 */
static void do_test_coordinator_load_in_progress_error(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription, *assignment;
        rd_kafka_t *c;
        int found_heartbeats;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-coord-load";

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

        /* Wait for initial join and assignment */
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c, 2000);

        /* Verify initial assignment */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_ASSERT(assignment->cnt == 3,
                    "Expected 3 partitions initially, got %d", assignment->cnt);
        rd_kafka_topic_partition_list_destroy(assignment);

        /* Inject COORDINATOR_LOAD_IN_PROGRESS error (transient) */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_ShareGroupHeartbeat, 1,
            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS, 0);

        /* Poll - consumer should handle transient error and retry */
        rd_kafka_consumer_poll(c, 2000);

        /* Verify heartbeats continue after transient error */
        found_heartbeats = wait_share_heartbeats(mcluster, 2, 500);
        TEST_ASSERT(found_heartbeats >= 1,
                    "Expected heartbeats to continue after "
                    "COORDINATOR_LOAD_IN_PROGRESS, got %d",
                    found_heartbeats);

        /* Verify consumer still has assignment */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_ASSERT(assignment->cnt == 3,
                    "Expected 3 partitions after retry, got %d",
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
 * @brief Consumer graceful shutdown during stable state.
 *
 * Consumer in stable state leaves group gracefully, sending leave
 * heartbeat with epoch=-1.
 */
static void do_test_graceful_shutdown_stable_state(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription, *assignment;
        rd_kafka_t *c;
        rd_kafka_resp_err_t err;
        int found_heartbeats;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-graceful-shutdown";

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

        /* Wait for initial join and assignment */
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c, 2000);

        /* Verify initial assignment - member is in stable state */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_ASSERT(assignment->cnt == 3,
                    "Expected 3 partitions initially, got %d", assignment->cnt);
        rd_kafka_topic_partition_list_destroy(assignment);

        /* Record heartbeat count before close */
        found_heartbeats = wait_share_heartbeats(mcluster, 1, 100);
        rd_kafka_mock_stop_request_tracking(mcluster);
        rd_kafka_mock_start_request_tracking(mcluster);

        /* Close consumer gracefully - should send leave heartbeat */
        err = rd_kafka_consumer_close(c);
        TEST_ASSERT(!err, "Expected close to succeed, got %s",
                    rd_kafka_err2str(err));

        /* Verify leave heartbeat was sent */
        found_heartbeats = wait_share_heartbeats(mcluster, 1, 500);
        TEST_SAY("Found %d heartbeats during shutdown\n", found_heartbeats);

        /* Cleanup */
        rd_kafka_destroy(c);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief Consumer resubscribes after unsubscribe.
 *
 * Tests the unsubscribe then resubscribe flow.
 */
static void do_test_resubscribe_after_unsubscribe(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription, *assignment;
        rd_kafka_t *c;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-resubscribe";

        SUB_TEST();

        /* Setup */
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 3, 1);

        c = create_share_consumer(bootstraps, group);

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);

        rd_kafka_mock_start_request_tracking(mcluster);

        /* First subscribe */
        TEST_CALL_ERR__(rd_kafka_subscribe(c, subscription));
        wait_share_heartbeats(mcluster, 1, 500);
        rd_kafka_consumer_poll(c, 2000);

        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_ASSERT(assignment->cnt == 3,
                    "Expected 3 partitions on first subscribe, got %d",
                    assignment->cnt);
        rd_kafka_topic_partition_list_destroy(assignment);

        /* Unsubscribe */
        TEST_SAY("Unsubscribing...\n");
        TEST_CALL_ERR__(rd_kafka_unsubscribe(c));
        rd_kafka_consumer_poll(c, 2000);

        /* Verify no assignment after unsubscribe */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_ASSERT(assignment->cnt == 0,
                    "Expected 0 partitions after unsubscribe, got %d",
                    assignment->cnt);
        rd_kafka_topic_partition_list_destroy(assignment);

        /* Resubscribe */
        TEST_SAY("Resubscribing...\n");
        TEST_CALL_ERR__(rd_kafka_subscribe(c, subscription));
        rd_kafka_topic_partition_list_destroy(subscription);

        wait_share_heartbeats(mcluster, 2, 500);
        rd_kafka_consumer_poll(c, 3000);

        /* Verify assignment restored */
        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_SAY("Assignment after resubscribe: %d partitions\n",
                 assignment->cnt);
        TEST_ASSERT(assignment->cnt == 3,
                    "Expected 3 partitions after resubscribe, got %d",
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
 * @brief Consumer leaves and remaining consumers get reassigned.
 *
 * Tests rebalance when a consumer leaves the group.
 */
static void do_test_consumer_leave_rebalance(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_topic_partition_list_t *subscription;
        rd_kafka_topic_partition_list_t *c1_assign, *c2_assign;
        rd_kafka_t *c1, *c2, *c3;
        int final_total;
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = "test-share-group-leave-rebalance";

        SUB_TEST();

        /* Setup */
        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 6, 1);

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);

        c1 = create_share_consumer(bootstraps, group);
        c2 = create_share_consumer(bootstraps, group);
        c3 = create_share_consumer(bootstraps, group);

        rd_kafka_mock_start_request_tracking(mcluster);

        /* All three join */
        TEST_CALL_ERR__(rd_kafka_subscribe(c1, subscription));
        TEST_CALL_ERR__(rd_kafka_subscribe(c2, subscription));
        TEST_CALL_ERR__(rd_kafka_subscribe(c3, subscription));
        rd_kafka_topic_partition_list_destroy(subscription);

        /* Wait for initial balance */
        wait_share_heartbeats(mcluster, 4, 500);
        rd_kafka_consumer_poll(c1, 2000);
        rd_kafka_consumer_poll(c2, 2000);
        rd_kafka_consumer_poll(c3, 2000);

        wait_share_heartbeats(mcluster, 3, 500);
        rd_kafka_consumer_poll(c1, 2000);
        rd_kafka_consumer_poll(c2, 2000);
        rd_kafka_consumer_poll(c3, 2000);

        /* Get initial assignments */
        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assign));
        TEST_CALL_ERR__(rd_kafka_assignment(c2, &c2_assign));
        TEST_SAY("Initial: c1=%d, c2=%d (before c3 leaves)\n", c1_assign->cnt,
                 c2_assign->cnt);
        rd_kafka_topic_partition_list_destroy(c1_assign);
        rd_kafka_topic_partition_list_destroy(c2_assign);

        /* c3 leaves */
        TEST_SAY("Consumer c3 leaving...\n");
        rd_kafka_consumer_close(c3);
        rd_kafka_destroy(c3);

        /* Poll remaining consumers for rebalance */
        wait_share_heartbeats(mcluster, 3, 500);
        rd_kafka_consumer_poll(c1, 3000);
        rd_kafka_consumer_poll(c2, 3000);

        /* Get new assignments */
        TEST_CALL_ERR__(rd_kafka_assignment(c1, &c1_assign));
        TEST_CALL_ERR__(rd_kafka_assignment(c2, &c2_assign));
        final_total = c1_assign->cnt + c2_assign->cnt;
        TEST_SAY("After c3 leave: c1=%d, c2=%d\n", c1_assign->cnt,
                 c2_assign->cnt);

        /* Total should be >= 6 partitions among remaining consumers */
        TEST_ASSERT(final_total >= 6,
                    "Expected >= 6 partitions after rebalance, got %d",
                    final_total);

        rd_kafka_topic_partition_list_destroy(c1_assign);
        rd_kafka_topic_partition_list_destroy(c2_assign);

        /* Cleanup */
        rd_kafka_consumer_close(c1);
        rd_kafka_consumer_close(c2);
        rd_kafka_destroy(c1);
        rd_kafka_destroy(c2);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief Test calling close twice on the same consumer
 */
static void do_test_double_close(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        const char *topic    = test_mk_topic_name(__FUNCTION__, 1);
        const char *group_id = topic;
        rd_kafka_t *c;
        rd_kafka_topic_partition_list_t *subscription;
        rd_kafka_resp_err_t err;

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 3, 1);
        rd_kafka_mock_start_request_tracking(mcluster);

        c = create_share_consumer(bootstraps, group_id);

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);

        TEST_CALL_ERR__(rd_kafka_subscribe(c, subscription));
        wait_share_heartbeats(mcluster, 3, 500);

        /* First close - should succeed */
        err = rd_kafka_consumer_close(c);
        TEST_ASSERT(!err, "Expected first close to succeed, got %s",
                    rd_kafka_err2str(err));

        /* Second close - should handle gracefully without crashing.
         * The Java equivalent tests verify the CompletableFuture
         * completes immediately on double-leave. */
        err = rd_kafka_consumer_close(c);
        TEST_SAY("Second close returned: %s (no crash - correct)\n",
                 rd_kafka_err2str(err));

        rd_kafka_topic_partition_list_destroy(subscription);
        rd_kafka_destroy(c);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief Test consumer subscribed to a topic with no messages
 */
static void do_test_empty_topic_subscription(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        const char *topic    = test_mk_topic_name(__FUNCTION__, 1);
        const char *group_id = topic;
        rd_kafka_t *c;
        rd_kafka_topic_partition_list_t *subscription, *assignment;
        rd_kafka_message_t *msg;
        int i, msg_count = 0;

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 3, 1);
        rd_kafka_mock_start_request_tracking(mcluster);

        c = create_share_consumer(bootstraps, group_id);

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);

        TEST_CALL_ERR__(rd_kafka_subscribe(c, subscription));
        wait_share_heartbeats(mcluster, 3, 500);

        /* Poll empty topic - should get assignment but no messages */
        for (i = 0; i < 10; i++) {
                msg = rd_kafka_consumer_poll(c, 200);
                if (msg) {
                        if (!msg->err)
                                msg_count++;
                        rd_kafka_message_destroy(msg);
                }
        }

        TEST_CALL_ERR__(rd_kafka_assignment(c, &assignment));
        TEST_SAY("Empty topic: %d partitions, %d messages\n", assignment->cnt,
                 msg_count);
        TEST_ASSERT(assignment->cnt == 3, "Expected 3 partitions, got %d",
                    assignment->cnt);

        rd_kafka_topic_partition_list_destroy(subscription);
        rd_kafka_topic_partition_list_destroy(assignment);
        rd_kafka_consumer_close(c);
        rd_kafka_destroy(c);

        rd_kafka_mock_stop_request_tracking(mcluster);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


/**
 * @brief Empty topic list subscription returns INVALID_ARG.
 *
 * librdkafka intentionally rejects subscribe() with an empty topic list,
 * unlike Java which treats it as unsubscribe(). This is an intentional
 * difference (see sghb_test_discrepancies.txt #1).
 */
static void do_test_empty_topic_list_subscription(void) {
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_t *c;
        rd_kafka_topic_partition_list_t *empty_list;
        rd_kafka_resp_err_t err;
        const char *group = "test-share-group-empty-topic-list";

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(1, &bootstraps);

        c = create_share_consumer(bootstraps, group);

        /* Subscribe with empty topic list - should return INVALID_ARG */
        empty_list = rd_kafka_topic_partition_list_new(0);
        err        = rd_kafka_subscribe(c, empty_list);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "Expected INVALID_ARG from subscribe(empty_list), got %s",
                    rd_kafka_err2str(err));
        TEST_SAY("subscribe(empty_list) correctly returned %s\n",
                 rd_kafka_err2str(err));

        rd_kafka_topic_partition_list_destroy(empty_list);
        rd_kafka_destroy(c);

        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


int main_0155_share_group_heartbeat_mock(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);

        do_test_share_group_heartbeat_basic();
        do_test_share_group_assignment_rebalance();
        do_test_share_group_multi_topic_assignment();
        do_test_share_group_error_injection();
        do_test_share_group_rtt_injection();
        do_test_share_group_session_timeout();
        do_test_share_group_target_assignment();
        do_test_share_group_max_size();

        do_test_unknown_member_id_error();
        do_test_fenced_member_epoch_error();
        do_test_coordinator_not_available_error();
        do_test_not_coordinator_error();
        do_test_group_authorization_failed_error();
        do_test_group_max_size_reached_error();
        do_test_invalid_request_error();
        do_test_unsupported_version_error();
        do_test_coordinator_load_in_progress_error();

        do_test_member_rejoin_with_epoch_zero();
        do_test_leaving_member_bumps_group_epoch();

        do_test_partition_assignment_with_multiple_topics();
        do_test_multiple_members_partition_distribution();

        do_test_leave_heartbeat_completes_successfully();
        do_test_leave_heartbeat_completes_on_error();
        do_test_graceful_shutdown_stable_state();
        do_test_consumer_leave_rebalance();
        do_test_double_close();

        do_test_subscription_change();
        do_test_resubscribe_after_unsubscribe();
        do_test_empty_topic_subscription();
        do_test_empty_topic_list_subscription();

        do_test_group_id_not_found_while_unsubscribed();
        /* NOT YET COMPATIBLE */
        /* do_test_group_id_not_found_while_stable_is_fatal(); */

        return 0;
}
