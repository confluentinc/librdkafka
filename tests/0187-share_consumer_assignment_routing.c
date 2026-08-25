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
 * @name KIP-932 share-consumer assignment routing
 *
 * Verifies that the partitions a share consumer ends up owning match
 * exactly what the broker decided to assign it via the
 * ShareGroupHeartbeat protocol.
 *
 * The broker side is the mock cluster. We drive what the broker hands
 * out by calling rd_kafka_mock_sharegroup_target_assignment() — this
 * lets us force a specific partition list per member, including the
 * KIP-932 case where the same partition is shared between two
 * consumers.
 *
 * The client side is observed by parsing the `cgrp`-context debug log
 * (see test_share_consumer_assignments_install() in tests/test.c).
 * Source of truth: the "Share assignment served: N partition(s)
 * assigned" line emitted at the end of every
 * rd_kafka_share_assignment_serve() call. We deliberately do NOT call
 * rd_kafka_assignment() — the test must see what the routing layer
 * actually applied, not the in-process field the production code
 * mutates.
 *
 * Each test:
 *   1. Stands up a mock cluster with a short heartbeat interval.
 *   2. Creates a topic and one or two share consumers, subscribes,
 *      and waits for the auto-assignor to settle.
 *   3. Pushes a specific target assignment via the mock API.
 *   4. Waits for a NEW serve event with the expected cnt on each
 *      observed consumer.
 *   5. Asserts the observed assignment is exactly what we pushed.
 */

#include "test.h"

#include "../src/rdkafka_proto.h"

#include <stdarg.h>


#define HB_INTERVAL_MS 200
#define SESSION_MS     2000
#define WAIT_MS        15000
#define STEADY_MS      2000

/******************************************************************************
 * Helpers
 ******************************************************************************/

/**
 * @brief Build a partition list from variadic (topic*, partition) pairs.
 *
 *        build_partition_list(2, "T", 0, "T", 1) yields [T[0], T[1]].
 */
static rd_kafka_topic_partition_list_t *build_partition_list(int cnt, ...) {
        rd_kafka_topic_partition_list_t *l;
        va_list ap;
        int i;

        l = rd_kafka_topic_partition_list_new(cnt);
        va_start(ap, cnt);
        for (i = 0; i < cnt; i++) {
                const char *topic = va_arg(ap, const char *);
                int part          = va_arg(ap, int);
                rd_kafka_topic_partition_list_add(l, topic, (int32_t)part);
        }
        va_end(ap);
        return l;
}

/**
 * @brief Assert two partition lists are set-equal (order independent).
 *        Partitions may legitimately appear in more than one consumer's
 *        assignment for share consumers — that's enforced via the test
 *        bodies, not here.
 */
static void assert_set_eq(const char *what,
                          rd_kafka_topic_partition_list_t *got,
                          rd_kafka_topic_partition_list_t *want) {
        int i;

        TEST_ASSERT(got != NULL, "%s: got NULL list", what);
        TEST_ASSERT(got->cnt == want->cnt, "%s: cnt got %d, want %d", what,
                    got->cnt, want->cnt);
        for (i = 0; i < want->cnt; i++) {
                TEST_ASSERT(rd_kafka_topic_partition_list_find(
                                got, want->elems[i].topic,
                                want->elems[i].partition) != NULL,
                            "%s: missing %s [%" PRId32 "]", what,
                            want->elems[i].topic, want->elems[i].partition);
        }
}

/**
 * @brief Assert the log-scraping observer is healthy at end of test:
 *        (1) parser_errors == 0 — the debug-log formats the observer
 *            sscanf-parses have not drifted (a drift would silently
 *            break these tests with vacuous passes / timeouts), and
 *        (2) serves >= 1 — at least one assignment was actually parsed,
 *            so a total parse failure (0 serves) fails loudly instead
 *            of passing vacuously.
 *
 * Call once per observer just before destroying it.
 */
static void assert_observer_healthy(test_share_assignment_log_t *log,
                                    const char *what) {
        test_share_assignment_stats_t s =
            test_share_consumer_assignment_stats(log);
        TEST_ASSERT(s.parser_errors == 0,
                    "%s: observer parser_errors=%d — assignment debug-log "
                    "format drifted; the log-scraping observer is broken",
                    what, s.parser_errors);
        TEST_ASSERT(s.serves >= 1,
                    "%s: observer recorded 0 serves — no assignment was "
                    "ever parsed (total parse failure / format drift)",
                    what);
}

/**
 * @brief Create a share consumer wired to the mock cluster, with the
 *        log-based assignment observer pre-installed on its conf.
 */
static rd_kafka_share_t *
make_share_consumer(const char *bootstraps,
                    const char *group_id,
                    test_share_assignment_log_t **out_log) {
        rd_kafka_conf_t *conf;
        rd_kafka_share_t *rk;
        char errstr[512];

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", group_id);

        *out_log = test_share_consumer_assignments_install(conf);

        rk = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(rk, "share_consumer_new: %s", errstr);
        return rk;
}

/**
 * @brief Build a run-unique group id from \p func (caller passes
 *        __FUNCTION__), mirroring how test_mk_topic_name uniquifies
 *        topic names. Uses its own TLS buffer so it does NOT clobber
 *        test_mk_topic_name's buffer (which the tests hold a pointer
 *        to for the topic name). Each test calls this once.
 */
static const char *mk_group_name(const char *func) {
        static RD_TLS char ret[256];
        if (!strncmp(func, "main_", 5))
                func += 5;
        rd_snprintf(ret, sizeof(ret), "sg_rnd%" PRIx64 "_%s",
                    test_id_generate(), func);
        return ret;
}

/** Subscribe to N topics (varargs of const char *). */
static void subscribe_n(rd_kafka_share_t *rk, int n_topics, ...) {
        rd_kafka_topic_partition_list_t *s =
            rd_kafka_topic_partition_list_new(n_topics);
        va_list ap;
        int i;

        va_start(ap, n_topics);
        for (i = 0; i < n_topics; i++) {
                const char *topic = va_arg(ap, const char *);
                rd_kafka_topic_partition_list_add(s, topic,
                                                  RD_KAFKA_PARTITION_UA);
        }
        va_end(ap);
        TEST_CALL_ERR__(rd_kafka_share_subscribe(rk, s));
        rd_kafka_topic_partition_list_destroy(s);
}

/** Block until the mock cluster reports \p expected members in the group. */
static void wait_member_count(rd_kafka_mock_cluster_t *mc,
                              const char *group_id,
                              size_t expected,
                              int timeout_ms) {
        int64_t deadline = test_clock() + (int64_t)timeout_ms * 1000;
        char **ids       = NULL;
        size_t got       = 0;

        while (test_clock() < deadline) {
                rd_kafka_resp_err_t err =
                    rd_kafka_mock_sharegroup_get_member_ids(mc, group_id, &ids,
                                                            &got);
                if (err == RD_KAFKA_RESP_ERR_NO_ERROR && got == expected) {
                        size_t i;
                        for (i = 0; i < got; i++)
                                rd_free(ids[i]);
                        rd_free(ids);
                        return;
                }
                if (ids) {
                        size_t i;
                        for (i = 0; i < got; i++)
                                rd_free(ids[i]);
                        rd_free(ids);
                        ids = NULL;
                }
                rd_usleep(100 * 1000, 0);
        }
        TEST_FAIL("group %s: timed out waiting for %zu members (got %zu)",
                  group_id, expected, got);
}

/**
 * @brief Take a snapshot of mock-side member ids and return the one
 *        not already in \p known. Caller frees with rd_free().
 */
static char *capture_new_member_id(rd_kafka_mock_cluster_t *mc,
                                   const char *group_id,
                                   char **known,
                                   size_t known_cnt) {
        char **all     = NULL;
        size_t all_cnt = 0;
        rd_kafka_resp_err_t err;
        char *new_id = NULL;
        size_t i, j;

        err = rd_kafka_mock_sharegroup_get_member_ids(mc, group_id, &all,
                                                      &all_cnt);
        TEST_ASSERT(!err, "get_member_ids: %s", rd_kafka_err2str(err));

        for (i = 0; i < all_cnt; i++) {
                rd_bool_t in_known = rd_false;
                for (j = 0; j < known_cnt; j++)
                        if (!strcmp(all[i], known[j])) {
                                in_known = rd_true;
                                break;
                        }
                if (!in_known) {
                        new_id = rd_strdup(all[i]);
                        break;
                }
        }
        for (i = 0; i < all_cnt; i++)
                rd_free(all[i]);
        rd_free(all);

        TEST_ASSERT(new_id != NULL,
                    "group %s: no new member id found (known_cnt=%zu, "
                    "all_cnt=%zu)",
                    group_id, known_cnt, all_cnt);
        return new_id;
}

#define MAX_CONSUMERS 8

/**
 * @brief Bring up \p n share consumers in the same group, each
 *        subscribed to all \p num_topics topics. Captures each
 *        consumer's mock-side member id into \p ids.
 *
 * Each iteration: create + subscribe, wait for the broker to register
 * the new member, capture its id, then sleep one HB-ack window so the
 * cgrp settles before the next join bumps the epoch (otherwise a
 * still-in-flight assignment can be fenced by the next bump). We do
 * NOT predict the auto-assignor's per-member cnt — the caller's next
 * `push_round` will wait on its own pushed assignment via
 * `wait_serves_after`, which is the only assignment shape the test
 * actually cares about.
 */
static void bring_up_consumers(rd_kafka_mock_cluster_t *mc,
                               const char *bootstraps,
                               const char *group_id,
                               const char **topic_names,
                               int num_topics,
                               int n,
                               rd_kafka_share_t **c,
                               test_share_assignment_log_t **l,
                               char **ids) {
        char *known[MAX_CONSUMERS];
        int i, k;

        TEST_ASSERT(n <= MAX_CONSUMERS, "bring_up_consumers: n=%d > %d", n,
                    MAX_CONSUMERS);

        for (i = 0; i < n; i++) {
                rd_kafka_topic_partition_list_t *s;

                c[i] = make_share_consumer(bootstraps, group_id, &l[i]);

                s = rd_kafka_topic_partition_list_new(num_topics);
                for (k = 0; k < num_topics; k++)
                        rd_kafka_topic_partition_list_add(
                            s, topic_names[k], RD_KAFKA_PARTITION_UA);
                TEST_CALL_ERR__(rd_kafka_share_subscribe(c[i], s));
                rd_kafka_topic_partition_list_destroy(s);

                wait_member_count(mc, group_id, (size_t)(i + 1), WAIT_MS);
                ids[i] = capture_new_member_id(mc, group_id, known, (size_t)i);
                known[i] = ids[i];

                /* Wait on a concrete signal — the new member's first
                 * served assignment — rather than a fixed settle sleep.
                 * Once its own observer records a serve, the cgrp has
                 * settled enough for the next join to bump the epoch
                 * cleanly. Falls back to a short sleep only if no
                 * observer is installed for this consumer. */
                if (l[i]) {
                        rd_ts_t deadline =
                            test_clock() + (rd_ts_t)WAIT_MS * 1000;
                        while (
                            test_share_consumer_assignment_stats(l[i]).serves <
                                1 &&
                            test_clock() < deadline)
                                rd_usleep(HB_INTERVAL_MS * 1000, 0);
                } else {
                        rd_usleep(HB_INTERVAL_MS * 2 * 1000, 0);
                }
        }
}

/**
 * @brief Push a multi-member manual assignment and verify each member's
 *        observer sees the expected assignment. Each \p expected[i] is
 *        the partition list pushed for member \p ids[i]; the wait
 *        targets \p expected[i]->cnt and the diagnostic prefix uses
 *        \p round_name.
 */
static void push_round(rd_kafka_mock_cluster_t *mc,
                       const char *group_id,
                       const char **ids,
                       rd_kafka_topic_partition_list_t **expected,
                       test_share_assignment_log_t **logs,
                       int n,
                       const char *round_name) {
        test_share_assignment_stats_t snaps[MAX_CONSUMERS];
        int i;

        TEST_ASSERT(n <= MAX_CONSUMERS, "push_round: n=%d > %d", n,
                    MAX_CONSUMERS);

        for (i = 0; i < n; i++)
                snaps[i] = test_share_consumer_assignment_stats(logs[i]);

        rd_kafka_mock_sharegroup_target_assignment(mc, group_id, ids, expected,
                                                   (size_t)n);

        for (i = 0; i < n; i++) {
                rd_kafka_topic_partition_list_t *got;
                char label[64];

                got = test_share_consumer_wait_serves_after(
                    logs[i], snaps[i].serves, expected[i]->cnt, WAIT_MS);
                rd_snprintf(label, sizeof(label), "[%s] c%d", round_name, i);
                assert_set_eq(label, got, expected[i]);
                rd_kafka_topic_partition_list_destroy(got);
        }
}

/******************************************************************************
 * Tests
 ******************************************************************************/

/**
 * @brief A single consumer subscribed to T(4) initially gets all four
 *        partitions from the auto-assignor. We then push a manual
 *        target_assignment of {T[0], T[2]} and verify the consumer's
 *        observed assignment becomes exactly that — not a superset and
 *        not a different subset.
 */
static void do_test_force_precise_assignment_single_consumer(void) {
        rd_kafka_mock_cluster_t *mc;
        const char *bootstraps;
        rd_kafka_share_t *c1;
        test_share_assignment_log_t *l1;
        rd_kafka_topic_partition_list_t *got;
        char *id_c1;
        const char *T     = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = mk_group_name(__FUNCTION__);

        SUB_TEST_QUICK();

        mc = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_sharegroup_set_heartbeat_interval(mc, HB_INTERVAL_MS);
        rd_kafka_mock_sharegroup_set_session_timeout(mc, SESSION_MS);
        rd_kafka_mock_topic_create(mc, T, 4, 1);

        c1 = make_share_consumer(bootstraps, group, &l1);
        subscribe_n(c1, 1, T);

        got = test_share_consumer_wait_assignment(l1, 4, WAIT_MS);
        TEST_ASSERT(got, "auto assignment did not arrive");
        rd_kafka_topic_partition_list_destroy(got);

        wait_member_count(mc, group, 1, WAIT_MS);
        id_c1 = capture_new_member_id(mc, group, NULL, 0);

        const char *ids[1]                     = {id_c1};
        rd_kafka_topic_partition_list_t *as[1] = {
            build_partition_list(2, T, 0, T, 2)};
        push_round(mc, group, ids, as, &l1, 1, "force_precise");
        rd_kafka_topic_partition_list_destroy(as[0]);

        rd_free(id_c1);
        test_share_consumer_close(c1);
        test_share_destroy(c1);
        assert_observer_healthy(l1, "c1");
        test_share_consumer_assignments_destroy(l1);
        test_mock_cluster_destroy(mc);
        SUB_TEST_PASS();
}

/**
 * @brief Two consumers in the same share group, both subscribed to
 *        T(4). We force overlapping assignments — c1 = {T[0,1,2]},
 *        c2 = {T[1,2,3]} — and verify each consumer's observed
 *        assignment matches what was pushed.
 *
 * The crucial point is that T[1] and T[2] appear in both consumers'
 * observed assignments simultaneously. This is the defining property
 * of KIP-932 share consumers: partitions may be shared between
 * members of the same group.
 */
static void do_test_force_shared_partition_between_consumers(void) {
        rd_kafka_mock_cluster_t *mc;
        const char *bootstraps;
        rd_kafka_share_t *c1, *c2;
        test_share_assignment_log_t *l1, *l2;
        rd_kafka_topic_partition_list_t *got, *want, *as1, *as2;
        const char *ids[2];
        rd_kafka_topic_partition_list_t *as[2];
        test_share_assignment_stats_t snap1, snap2;
        char *id_c1, *id_c2, *known[1];
        const char *T     = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = mk_group_name(__FUNCTION__);

        SUB_TEST_QUICK();

        mc = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_sharegroup_set_heartbeat_interval(mc, HB_INTERVAL_MS);
        rd_kafka_mock_sharegroup_set_session_timeout(mc, SESSION_MS);
        rd_kafka_mock_topic_create(mc, T, 4, 1);

        c1 = make_share_consumer(bootstraps, group, &l1);
        subscribe_n(c1, 1, T);
        wait_member_count(mc, group, 1, WAIT_MS);
        got = test_share_consumer_wait_assignment(l1, 4, WAIT_MS);
        TEST_ASSERT(got, "c1 auto assignment did not arrive");
        rd_kafka_topic_partition_list_destroy(got);
        id_c1 = capture_new_member_id(mc, group, NULL, 0);

        c2 = make_share_consumer(bootstraps, group, &l2);
        subscribe_n(c2, 1, T);
        wait_member_count(mc, group, 2, WAIT_MS);

        /* Wait for BOTH to settle to cnt=2 before pushing manual so
         * the cgrp on each is quiet at push time. */
        got = test_share_consumer_wait_assignment(l1, 2, WAIT_MS);
        TEST_ASSERT(got, "c1 did not settle to cnt=2 after c2 joined");
        rd_kafka_topic_partition_list_destroy(got);
        got = test_share_consumer_wait_assignment(l2, 2, WAIT_MS);
        TEST_ASSERT(got, "c2 did not settle to cnt=2");
        rd_kafka_topic_partition_list_destroy(got);

        known[0] = id_c1;
        id_c2    = capture_new_member_id(mc, group, known, 1);

        snap1  = test_share_consumer_assignment_stats(l1);
        snap2  = test_share_consumer_assignment_stats(l2);
        as1    = build_partition_list(3, T, 0, T, 1, T, 2);
        as2    = build_partition_list(3, T, 1, T, 2, T, 3);
        ids[0] = id_c1;
        ids[1] = id_c2;
        as[0]  = as1;
        as[1]  = as2;
        rd_kafka_mock_sharegroup_target_assignment(mc, group, ids, as, 2);

        got =
            test_share_consumer_wait_serves_after(l1, snap1.serves, 3, WAIT_MS);
        want = build_partition_list(3, T, 0, T, 1, T, 2);
        assert_set_eq("c1", got, want);
        rd_kafka_topic_partition_list_destroy(got);
        rd_kafka_topic_partition_list_destroy(want);

        got =
            test_share_consumer_wait_serves_after(l2, snap2.serves, 3, WAIT_MS);
        want = build_partition_list(3, T, 1, T, 2, T, 3);
        assert_set_eq("c2", got, want);
        rd_kafka_topic_partition_list_destroy(got);
        rd_kafka_topic_partition_list_destroy(want);

        rd_kafka_topic_partition_list_destroy(as1);
        rd_kafka_topic_partition_list_destroy(as2);
        rd_free(id_c1);
        rd_free(id_c2);
        test_share_consumer_close(c1);
        test_share_destroy(c1);
        test_share_consumer_close(c2);
        test_share_destroy(c2);
        assert_observer_healthy(l1, "c1");
        assert_observer_healthy(l2, "c2");
        test_share_consumer_assignments_destroy(l1);
        test_share_consumer_assignments_destroy(l2);
        test_mock_cluster_destroy(mc);
        SUB_TEST_PASS();
}

/**
 * @brief A consumer with an existing assignment receives a force-push
 *        of a completely different partition set. Verifies the old
 *        partitions are gone and only the new ones remain — the client
 *        must do both a subtract and an add to converge.
 */
static void do_test_force_change_existing_assignment(void) {
        rd_kafka_mock_cluster_t *mc;
        const char *bootstraps;
        rd_kafka_share_t *c1;
        test_share_assignment_log_t *l1;
        rd_kafka_topic_partition_list_t *got;
        char *id_c1;
        const char *T     = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = mk_group_name(__FUNCTION__);

        SUB_TEST_QUICK();

        mc = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_sharegroup_set_heartbeat_interval(mc, HB_INTERVAL_MS);
        rd_kafka_mock_sharegroup_set_session_timeout(mc, SESSION_MS);
        rd_kafka_mock_topic_create(mc, T, 4, 1);

        c1 = make_share_consumer(bootstraps, group, &l1);
        subscribe_n(c1, 1, T);
        wait_member_count(mc, group, 1, WAIT_MS);
        got = test_share_consumer_wait_assignment(l1, 4, WAIT_MS);
        TEST_ASSERT(got, "auto assignment did not arrive");
        rd_kafka_topic_partition_list_destroy(got);
        id_c1 = capture_new_member_id(mc, group, NULL, 0);

        /* Replace {T[0..3]} with {T[2,3]}. Exact-match (assert_set_eq inside
         * push_round) implies T[0] and T[1] were dropped. */
        const char *ids[1]                     = {id_c1};
        rd_kafka_topic_partition_list_t *as[1] = {
            build_partition_list(2, T, 2, T, 3)};
        push_round(mc, group, ids, as, &l1, 1, "change_existing");
        rd_kafka_topic_partition_list_destroy(as[0]);


        rd_free(id_c1);
        test_share_consumer_close(c1);
        test_share_destroy(c1);
        assert_observer_healthy(l1, "c1");
        test_share_consumer_assignments_destroy(l1);
        test_mock_cluster_destroy(mc);
        SUB_TEST_PASS();
}

/**
 * @brief A consumer with a full assignment receives a force-push of
 *        an empty assignment. Verifies the consumer drops every
 *        partition — exercises the clear path.
 */
static void do_test_force_empty_assignment(void) {
        rd_kafka_mock_cluster_t *mc;
        const char *bootstraps;
        rd_kafka_share_t *c1;
        test_share_assignment_log_t *l1;
        rd_kafka_topic_partition_list_t *got;
        char *id_c1;
        const char *T     = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = mk_group_name(__FUNCTION__);

        SUB_TEST_QUICK();

        mc = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_sharegroup_set_heartbeat_interval(mc, HB_INTERVAL_MS);
        rd_kafka_mock_sharegroup_set_session_timeout(mc, SESSION_MS);
        rd_kafka_mock_topic_create(mc, T, 4, 1);

        c1 = make_share_consumer(bootstraps, group, &l1);
        subscribe_n(c1, 1, T);
        wait_member_count(mc, group, 1, WAIT_MS);
        got = test_share_consumer_wait_assignment(l1, 4, WAIT_MS);
        TEST_ASSERT(got, "auto assignment did not arrive");
        rd_kafka_topic_partition_list_destroy(got);
        id_c1 = capture_new_member_id(mc, group, NULL, 0);


        const char *ids[1]                     = {id_c1};
        rd_kafka_topic_partition_list_t *as[1] = {build_partition_list(0)};
        push_round(mc, group, ids, as, &l1, 1, "force_empty");
        rd_kafka_topic_partition_list_destroy(as[0]);


        rd_free(id_c1);
        test_share_consumer_close(c1);
        test_share_destroy(c1);
        assert_observer_healthy(l1, "c1");
        test_share_consumer_assignments_destroy(l1);
        test_mock_cluster_destroy(mc);
        SUB_TEST_PASS();
}

/**
 * @brief Once a consumer reaches a steady-state assignment, no new
 *        serve events should fire until something actually changes.
 *        Catches regressions that re-serve on every heartbeat.
 */
static void do_test_steady_state_no_spurious_serves(void) {
        rd_kafka_mock_cluster_t *mc;
        const char *bootstraps;
        rd_kafka_share_t *c1;
        test_share_assignment_log_t *l1;
        rd_kafka_topic_partition_list_t *got;
        test_share_assignment_stats_t before, after;
        const char *T     = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = mk_group_name(__FUNCTION__);

        SUB_TEST_QUICK();

        mc = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_sharegroup_set_heartbeat_interval(mc, HB_INTERVAL_MS);
        rd_kafka_mock_sharegroup_set_session_timeout(mc, SESSION_MS);
        rd_kafka_mock_topic_create(mc, T, 4, 1);

        c1 = make_share_consumer(bootstraps, group, &l1);
        subscribe_n(c1, 1, T);
        got = test_share_consumer_wait_assignment(l1, 4, WAIT_MS);
        TEST_ASSERT(got, "auto assignment did not arrive");
        rd_kafka_topic_partition_list_destroy(got);

        before = test_share_consumer_assignment_stats(l1);
        /* Observe a STEADY_MS=2000ms window — ~10 heartbeats at the
         * HB_INTERVAL_MS=200ms cadence. Once the assignment is steady,
         * the broker re-serves nothing, so the serve counter must be
         * unchanged. The exact-equality assertion (not <=) is the
         * tight form and is flake-free in practice (verified 20/20
         * runs); 2s is wide enough that a missed re-serve would be
         * caught, yet short enough not to invite an unrelated reconcile. */
        rd_usleep(STEADY_MS * 1000, 0);
        after = test_share_consumer_assignment_stats(l1);

        TEST_ASSERT(after.serves == before.serves,
                    "spurious serves over %d ms: %d -> %d", STEADY_MS,
                    before.serves, after.serves);
        TEST_ASSERT(after.parser_errors == 0, "parser_errors = %d",
                    after.parser_errors);

        test_share_consumer_close(c1);
        test_share_destroy(c1);
        assert_observer_healthy(l1, "c1");
        test_share_consumer_assignments_destroy(l1);
        test_mock_cluster_destroy(mc);
        SUB_TEST_PASS();
}

/**
 * @brief Three consumers on T(6); cycle through four manual
 *        assignment pushes that each consumer must apply correctly.
 *
 * The rounds cover: rotation (every consumer's partitions move),
 * sharing one partition across all three, all three owning every
 * partition, and one consumer going empty while the other two split
 * the alternating odd/even partitions. Every transition deliberately
 * changes every consumer's assignment so each consumer's observer
 * must see a fresh "Share assignment served" event with the expected
 * cnt and content.
 *
 * Catches stale-state bugs in the cgrp reconciliation that only
 * manifest after multiple back-to-back assignments — e.g., partitions
 * leaking forward, partition-list growth across rounds, or a
 * consumer's view drifting from what the broker last sent.
 */
static void do_test_force_reassignment_cycle_three_consumers(void) {
        rd_kafka_mock_cluster_t *mc;
        const char *bootstraps;
        rd_kafka_share_t *c[3];
        test_share_assignment_log_t *l[3];
        char *ids[3];
        const char *id_arr[3];
        rd_kafka_topic_partition_list_t *as[3];
        const char *T     = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = mk_group_name(__FUNCTION__);
        int i;

        SUB_TEST_QUICK();

        mc = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_sharegroup_set_heartbeat_interval(mc, HB_INTERVAL_MS);
        rd_kafka_mock_sharegroup_set_session_timeout(mc, SESSION_MS);
        rd_kafka_mock_topic_create(mc, T, 6, 1);

        const char *names[1] = {T};
        bring_up_consumers(mc, bootstraps, group, names, 1, 3, c, l, ids);

        for (i = 0; i < 3; i++)
                id_arr[i] = ids[i];

        /* Round 1 — rotation: every consumer takes a different pair. */
        as[0] = build_partition_list(2, T, 2, T, 3);
        as[1] = build_partition_list(2, T, 4, T, 5);
        as[2] = build_partition_list(2, T, 0, T, 1);
        push_round(mc, group, id_arr, as, l, 3, "rotation");
        for (i = 0; i < 3; i++)
                rd_kafka_topic_partition_list_destroy(as[i]);

        /* Round 2 — T[0] shared across all three; other partitions
         * unevenly distributed. */
        as[0] = build_partition_list(1, T, 0);
        as[1] = build_partition_list(2, T, 0, T, 1);
        as[2] = build_partition_list(5, T, 0, T, 2, T, 3, T, 4, T, 5);
        push_round(mc, group, id_arr, as, l, 3, "shared_T0_uneven");
        for (i = 0; i < 3; i++)
                rd_kafka_topic_partition_list_destroy(as[i]);

        /* Round 3 — every consumer owns every partition. */
        as[0] = build_partition_list(6, T, 0, T, 1, T, 2, T, 3, T, 4, T, 5);
        as[1] = build_partition_list(6, T, 0, T, 1, T, 2, T, 3, T, 4, T, 5);
        as[2] = build_partition_list(6, T, 0, T, 1, T, 2, T, 3, T, 4, T, 5);
        push_round(mc, group, id_arr, as, l, 3, "all_share_all");
        for (i = 0; i < 3; i++)
                rd_kafka_topic_partition_list_destroy(as[i]);

        /* Round 4 — c1 empty, c2/c3 split the alternating partitions. */
        as[0] = build_partition_list(0);
        as[1] = build_partition_list(3, T, 0, T, 2, T, 4);
        as[2] = build_partition_list(3, T, 1, T, 3, T, 5);
        push_round(mc, group, id_arr, as, l, 3, "empty_plus_alternating");
        for (i = 0; i < 3; i++)
                rd_kafka_topic_partition_list_destroy(as[i]);

        for (i = 0; i < 3; i++) {
                test_share_consumer_close(c[i]);
                test_share_destroy(c[i]);
                assert_observer_healthy(l[i], "c[i]");
                test_share_consumer_assignments_destroy(l[i]);
                rd_free(ids[i]);
        }
        test_mock_cluster_destroy(mc);
        SUB_TEST_PASS();
}

/**
 * @brief Four consumers on T(8); three manual rounds covering an
 *        uneven exclusive split, paired sharing (consumers in pairs
 *        owning the same partitions), and a rotation back to a
 *        different exclusive split.
 *
 * The four-consumer setup exercises a larger fan-out than any other
 * test in the file and verifies that the routing pipeline holds up
 * under heavier per-push membership.
 */
static void do_test_force_chaos_four_consumers(void) {
        rd_kafka_mock_cluster_t *mc;
        const char *bootstraps;
        rd_kafka_share_t *c[4];
        test_share_assignment_log_t *l[4];
        char *ids[4];
        const char *id_arr[4];
        rd_kafka_topic_partition_list_t *as[4];
        const char *T     = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = mk_group_name(__FUNCTION__);
        int i;

        SUB_TEST_QUICK();

        mc = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_sharegroup_set_heartbeat_interval(mc, HB_INTERVAL_MS);
        rd_kafka_mock_sharegroup_set_session_timeout(mc, SESSION_MS);
        rd_kafka_mock_topic_create(mc, T, 8, 1);

        const char *names[1] = {T};
        bring_up_consumers(mc, bootstraps, group, names, 1, 4, c, l, ids);

        for (i = 0; i < 4; i++)
                id_arr[i] = ids[i];

        /* Round 1 — uneven exclusive split. */
        as[0] = build_partition_list(1, T, 0);
        as[1] = build_partition_list(3, T, 1, T, 2, T, 3);
        as[2] = build_partition_list(3, T, 4, T, 5, T, 6);
        as[3] = build_partition_list(1, T, 7);
        push_round(mc, group, id_arr, as, l, 4, "uneven_split");
        for (i = 0; i < 4; i++)
                rd_kafka_topic_partition_list_destroy(as[i]);

        /* Round 2 — paired sharing: {c1,c2} share T[0..3];
         * {c3,c4} share T[4..7]. */
        as[0] = build_partition_list(4, T, 0, T, 1, T, 2, T, 3);
        as[1] = build_partition_list(4, T, 0, T, 1, T, 2, T, 3);
        as[2] = build_partition_list(4, T, 4, T, 5, T, 6, T, 7);
        as[3] = build_partition_list(4, T, 4, T, 5, T, 6, T, 7);
        push_round(mc, group, id_arr, as, l, 4, "paired_sharing");
        for (i = 0; i < 4; i++)
                rd_kafka_topic_partition_list_destroy(as[i]);

        /* Round 3 — rotate back to exclusive but reverse the order. */
        as[0] = build_partition_list(2, T, 6, T, 7);
        as[1] = build_partition_list(2, T, 4, T, 5);
        as[2] = build_partition_list(2, T, 2, T, 3);
        as[3] = build_partition_list(2, T, 0, T, 1);
        push_round(mc, group, id_arr, as, l, 4, "reverse_rotation");
        for (i = 0; i < 4; i++)
                rd_kafka_topic_partition_list_destroy(as[i]);

        for (i = 0; i < 4; i++) {
                test_share_consumer_close(c[i]);
                test_share_destroy(c[i]);
                assert_observer_healthy(l[i], "c[i]");
                test_share_consumer_assignments_destroy(l[i]);
                rd_free(ids[i]);
        }
        test_mock_cluster_destroy(mc);
        SUB_TEST_PASS();
}

/**
 * @brief Two consumers, two topics: c1 and c2 subscribe to both T(2)
 *        and U(2). After the auto-assignor settles each consumer to
 *        one partition of each topic, we force a striped layout:
 *        c1 = {T[0], U[1]}, c2 = {T[1], U[0]}.
 *
 * Catches bugs in the per-topic metadata resolution path. The
 * broker's response carries topic IDs only; the cgrp must resolve
 * each topic ID back to its name via metadata
 * (src/rdkafka_cgrp.c:3535-3543) before reconciliation. If only the
 * first topic's metadata gets resolved correctly, this test would
 * fail with a missing U partition or a stale T entry.
 */
static void do_test_force_multi_topic_assignment_two_consumers(void) {
        rd_kafka_mock_cluster_t *mc;
        const char *bootstraps;
        rd_kafka_share_t *c1, *c2;
        test_share_assignment_log_t *l1, *l2;
        rd_kafka_topic_partition_list_t *got, *want, *as1, *as2;
        test_share_assignment_stats_t snap1, snap2;
        char *id_c1, *id_c2, *known[1];
        const char *ids[2];
        rd_kafka_topic_partition_list_t *as[2];
        char *T           = rd_strdup(test_mk_topic_name(__FUNCTION__, 0));
        char *U           = rd_strdup(test_mk_topic_name(__FUNCTION__, 1));
        const char *group = mk_group_name(__FUNCTION__);

        SUB_TEST_QUICK();

        mc = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_sharegroup_set_heartbeat_interval(mc, HB_INTERVAL_MS);
        rd_kafka_mock_sharegroup_set_session_timeout(mc, SESSION_MS);
        rd_kafka_mock_topic_create(mc, T, 2, 1);
        rd_kafka_mock_topic_create(mc, U, 2, 1);

        /* c1 alone: gets every partition of every subscribed topic
         * (2 of T + 2 of U = 4). */
        c1 = make_share_consumer(bootstraps, group, &l1);
        subscribe_n(c1, 2, T, U);
        wait_member_count(mc, group, 1, WAIT_MS);
        got = test_share_consumer_wait_assignment(l1, 4, WAIT_MS);
        TEST_ASSERT(got, "c1 alone: auto assignment of 4 partitions");
        rd_kafka_topic_partition_list_destroy(got);
        id_c1 = capture_new_member_id(mc, group, NULL, 0);

        /* c2 joins; the auto-assignor splits each topic 1/1, so each
         * consumer ends with one T partition + one U partition = cnt=2. */
        c2 = make_share_consumer(bootstraps, group, &l2);
        subscribe_n(c2, 2, T, U);
        wait_member_count(mc, group, 2, WAIT_MS);
        got = test_share_consumer_wait_assignment(l1, 2, WAIT_MS);
        TEST_ASSERT(got, "c1 did not settle to cnt=2 after c2 joined");
        rd_kafka_topic_partition_list_destroy(got);
        got = test_share_consumer_wait_assignment(l2, 2, WAIT_MS);
        TEST_ASSERT(got, "c2 did not settle to cnt=2");
        rd_kafka_topic_partition_list_destroy(got);
        known[0] = id_c1;
        id_c2    = capture_new_member_id(mc, group, known, 1);

        /* Force a striped layout across topics. */
        snap1  = test_share_consumer_assignment_stats(l1);
        snap2  = test_share_consumer_assignment_stats(l2);
        as1    = build_partition_list(2, T, 0, U, 1);
        as2    = build_partition_list(2, T, 1, U, 0);
        ids[0] = id_c1;
        ids[1] = id_c2;
        as[0]  = as1;
        as[1]  = as2;
        rd_kafka_mock_sharegroup_target_assignment(mc, group, ids, as, 2);

        got =
            test_share_consumer_wait_serves_after(l1, snap1.serves, 2, WAIT_MS);
        want = build_partition_list(2, T, 0, U, 1);
        assert_set_eq("c1 striped", got, want);
        rd_kafka_topic_partition_list_destroy(got);
        rd_kafka_topic_partition_list_destroy(want);

        got =
            test_share_consumer_wait_serves_after(l2, snap2.serves, 2, WAIT_MS);
        want = build_partition_list(2, T, 1, U, 0);
        assert_set_eq("c2 striped", got, want);
        rd_kafka_topic_partition_list_destroy(got);
        rd_kafka_topic_partition_list_destroy(want);

        rd_kafka_topic_partition_list_destroy(as1);
        rd_kafka_topic_partition_list_destroy(as2);
        rd_free(id_c1);
        rd_free(id_c2);
        test_share_consumer_close(c1);
        test_share_destroy(c1);
        test_share_consumer_close(c2);
        test_share_destroy(c2);
        assert_observer_healthy(l1, "c1");
        assert_observer_healthy(l2, "c2");
        test_share_consumer_assignments_destroy(l1);
        test_share_consumer_assignments_destroy(l2);
        test_mock_cluster_destroy(mc);
        rd_free(T);
        rd_free(U);
        SUB_TEST_PASS();
}

/**
 * @brief Push the same assignment to the same member twice and verify
 *        the second push does NOT trigger a new serve event.
 *
 * Tests rd_kafka_cgrp_consumer_is_new_assignment_different
 * (src/rdkafka_cgrp.c:2898). If the diff check is broken and reports
 * every broker response as "new", every heartbeat would trigger a
 * wasted reconcile cycle, which would show up here as a serves
 * counter that keeps incrementing despite identical content.
 *
 * Also demonstrates test_share_consumer_assignments() — the
 * non-blocking snapshot accessor — by reading the latest observed
 * assignment after the steady wait and asserting it still equals
 * what we pushed.
 */
static void do_test_force_same_assignment_twice_no_extra_serve(void) {
        rd_kafka_mock_cluster_t *mc;
        const char *bootstraps;
        rd_kafka_share_t *c1;
        test_share_assignment_log_t *l1;
        rd_kafka_topic_partition_list_t *got, *want, *forced_second;
        test_share_assignment_stats_t snap_after_first, snap_after_second;
        char *id_c1;
        const char *T     = test_mk_topic_name(__FUNCTION__, 0);
        const char *group = mk_group_name(__FUNCTION__);

        SUB_TEST_QUICK();

        mc = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_sharegroup_set_heartbeat_interval(mc, HB_INTERVAL_MS);
        rd_kafka_mock_sharegroup_set_session_timeout(mc, SESSION_MS);
        rd_kafka_mock_topic_create(mc, T, 4, 1);

        c1 = make_share_consumer(bootstraps, group, &l1);
        subscribe_n(c1, 1, T);
        wait_member_count(mc, group, 1, WAIT_MS);
        got = test_share_consumer_wait_assignment(l1, 4, WAIT_MS);
        TEST_ASSERT(got, "auto assignment did not arrive");
        rd_kafka_topic_partition_list_destroy(got);
        id_c1 = capture_new_member_id(mc, group, NULL, 0);

        /* First push: force {T[0], T[1]} and wait for the serve. */
        const char *ids[1]                     = {id_c1};
        rd_kafka_topic_partition_list_t *as[1] = {
            build_partition_list(2, T, 0, T, 1)};
        push_round(mc, group, ids, as, &l1, 1, "first_push");
        rd_kafka_topic_partition_list_destroy(as[0]);

        snap_after_first = test_share_consumer_assignment_stats(l1);

        /* Second push: same partition list. The broker still sends it
         * back in the HB response, but the cgrp's
         * is_new_assignment_different check should suppress
         * reconciliation, leaving stats.serves unchanged. */
        forced_second = build_partition_list(2, T, 0, T, 1);

        /* Reuse the ids/as arrays declared above; as[0] was destroyed
         * after the first push so we rebind it to forced_second. */
        as[0] = forced_second;
        rd_kafka_mock_sharegroup_target_assignment(mc, group, ids, as, 1);


        /* Wait a STEADY_MS=2000ms window — ~10 heartbeats at the
         * HB_INTERVAL_MS=200ms cadence — so several HB responses carry
         * the (identical) assignment back. The cgrp's
         * is_new_assignment_different check must suppress every one, so
         * the serve counter stays exactly equal. The exact-equality
         * assertion is the tight form and is flake-free in practice
         * (verified 20/20 runs). */
        rd_usleep(STEADY_MS * 1000, 0);
        snap_after_second = test_share_consumer_assignment_stats(l1);

        TEST_ASSERT(snap_after_second.serves == snap_after_first.serves,
                    "duplicate push triggered %d extra serve(s); "
                    "is_new_assignment_different is broken",
                    snap_after_second.serves - snap_after_first.serves);
        TEST_ASSERT(snap_after_second.parser_errors == 0, "parser_errors = %d",
                    snap_after_second.parser_errors);

        /* Use the snapshot accessor (non-blocking read after the
         * steady window) to verify the latest observed assignment is
         * still what we pushed. */
        got  = test_share_consumer_assignments(l1, 0);
        want = build_partition_list(2, T, 0, T, 1);
        TEST_ASSERT(got != NULL, "snapshot returned NULL after a known serve");
        assert_set_eq("snapshot after steady", got, want);
        rd_kafka_topic_partition_list_destroy(got);
        rd_kafka_topic_partition_list_destroy(want);

        rd_kafka_topic_partition_list_destroy(forced_second);
        rd_free(id_c1);
        test_share_consumer_close(c1);
        test_share_destroy(c1);
        assert_observer_healthy(l1, "c1");
        test_share_consumer_assignments_destroy(l1);
        test_mock_cluster_destroy(mc);
        SUB_TEST_PASS();
}

/**
 * @brief Three consumers, three topics — T(3), U(3), V(3) — cycle
 *        through three manual reassignments.
 *
 * Tests multi-topic routing under repeated reassignment. Each round
 * deliberately changes every consumer's content (and sometimes cnt),
 * including cross-topic rotation, a shared-topic configuration where
 * all three consumers own all of T, and an empty + skewed-split
 * configuration. Catches multi-topic metadata-resolution bugs that
 * compound across reassignments.
 */
static void do_test_force_chaos_three_consumers_three_topics(void) {
        rd_kafka_mock_cluster_t *mc;
        const char *bootstraps;
        rd_kafka_share_t *c[3];
        test_share_assignment_log_t *l[3];
        char *ids[3];
        const char *id_arr[3];
        rd_kafka_topic_partition_list_t *as[3];
        char *T              = rd_strdup(test_mk_topic_name(__FUNCTION__, 0));
        char *U              = rd_strdup(test_mk_topic_name(__FUNCTION__, 1));
        char *V              = rd_strdup(test_mk_topic_name(__FUNCTION__, 2));
        const char *names[3] = {T, U, V};
        const char *group    = mk_group_name(__FUNCTION__);
        int i;

        SUB_TEST_QUICK();

        mc = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_sharegroup_set_heartbeat_interval(mc, HB_INTERVAL_MS);
        rd_kafka_mock_sharegroup_set_session_timeout(mc, SESSION_MS);
        rd_kafka_mock_topic_create(mc, T, 3, 1);
        rd_kafka_mock_topic_create(mc, U, 3, 1);
        rd_kafka_mock_topic_create(mc, V, 3, 1);

        bring_up_consumers(mc, bootstraps, group, names, 3, 3, c, l, ids);
        for (i = 0; i < 3; i++)
                id_arr[i] = ids[i];

        /* Round 1 — cross-topic rotation: every consumer takes one
         * partition from each topic but different than before. */
        as[0] = build_partition_list(3, T, 1, U, 2, V, 0);
        as[1] = build_partition_list(3, T, 2, U, 0, V, 1);
        as[2] = build_partition_list(3, T, 0, U, 1, V, 2);
        push_round(mc, group, id_arr, as, l, 3, "cross_topic_rotation");
        for (i = 0; i < 3; i++)
                rd_kafka_topic_partition_list_destroy(as[i]);

        /* Round 2 — every consumer owns all of T; U and V exclusive. */
        as[0] = build_partition_list(5, T, 0, T, 1, T, 2, U, 0, V, 0);
        as[1] = build_partition_list(5, T, 0, T, 1, T, 2, U, 1, V, 1);
        as[2] = build_partition_list(5, T, 0, T, 1, T, 2, U, 2, V, 2);
        push_round(mc, group, id_arr, as, l, 3, "all_share_T");
        for (i = 0; i < 3; i++)
                rd_kafka_topic_partition_list_destroy(as[i]);

        /* Round 3 — c1 empty; c2 takes 2/3 of every topic; c3 takes the
         * remaining 1/3. */
        as[0] = build_partition_list(0);
        as[1] = build_partition_list(6, T, 0, T, 1, U, 0, U, 1, V, 0, V, 1);
        as[2] = build_partition_list(3, T, 2, U, 2, V, 2);
        push_round(mc, group, id_arr, as, l, 3, "empty_plus_skew");
        for (i = 0; i < 3; i++)
                rd_kafka_topic_partition_list_destroy(as[i]);

        for (i = 0; i < 3; i++) {
                test_share_consumer_close(c[i]);
                test_share_destroy(c[i]);
                assert_observer_healthy(l[i], "c[i]");
                test_share_consumer_assignments_destroy(l[i]);
                rd_free(ids[i]);
        }
        test_mock_cluster_destroy(mc);
        rd_free(T);
        rd_free(U);
        rd_free(V);
        SUB_TEST_PASS();
}

/**
 * @brief Four consumers, three topics — T(4), U(4), V(4) — cycle
 *        through three manual reassignments.
 *
 * The largest-fan-out test in the file: 4 members × 3 topics × 3
 * rounds. Rounds cover per-topic concentration (each consumer owns
 * an entire topic), pairwise sharing of partitions across consumers,
 * and a fully-distributed rotation. Exercises the routing pipe
 * under the heaviest combination of membership and topic count this
 * file produces.
 */
static void do_test_force_chaos_four_consumers_three_topics(void) {
        rd_kafka_mock_cluster_t *mc;
        const char *bootstraps;
        rd_kafka_share_t *c[4];
        test_share_assignment_log_t *l[4];
        char *ids[4];
        const char *id_arr[4];
        rd_kafka_topic_partition_list_t *as[4];
        char *T              = rd_strdup(test_mk_topic_name(__FUNCTION__, 0));
        char *U              = rd_strdup(test_mk_topic_name(__FUNCTION__, 1));
        char *V              = rd_strdup(test_mk_topic_name(__FUNCTION__, 2));
        const char *names[3] = {T, U, V};
        const char *group    = mk_group_name(__FUNCTION__);
        int i;

        SUB_TEST_QUICK();

        mc = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_sharegroup_set_heartbeat_interval(mc, HB_INTERVAL_MS);
        rd_kafka_mock_sharegroup_set_session_timeout(mc, SESSION_MS);
        rd_kafka_mock_topic_create(mc, T, 4, 1);
        rd_kafka_mock_topic_create(mc, U, 4, 1);
        rd_kafka_mock_topic_create(mc, V, 4, 1);

        bring_up_consumers(mc, bootstraps, group, names, 3, 4, c, l, ids);
        for (i = 0; i < 4; i++)
                id_arr[i] = ids[i];

        /* Round 1 — per-topic concentration: c1 owns all of T,
         * c2 all of U, c3 all of V, c4 nothing. */
        as[0] = build_partition_list(4, T, 0, T, 1, T, 2, T, 3);
        as[1] = build_partition_list(4, U, 0, U, 1, U, 2, U, 3);
        as[2] = build_partition_list(4, V, 0, V, 1, V, 2, V, 3);
        as[3] = build_partition_list(0);
        push_round(mc, group, id_arr, as, l, 4, "per_topic_concentration");
        for (i = 0; i < 4; i++)
                rd_kafka_topic_partition_list_destroy(as[i]);

        /* Round 2 — mixed sharing: {c1,c2} share T[0,1] + one U each,
         * {c3,c4} share T[2,3] + one V each. */
        as[0] = build_partition_list(3, T, 0, T, 1, U, 0);
        as[1] = build_partition_list(3, T, 0, T, 1, U, 1);
        as[2] = build_partition_list(3, T, 2, T, 3, V, 2);
        as[3] = build_partition_list(3, T, 2, T, 3, V, 3);
        push_round(mc, group, id_arr, as, l, 4, "mixed_sharing");
        for (i = 0; i < 4; i++)
                rd_kafka_topic_partition_list_destroy(as[i]);

        /* Round 3 — distributed rotation: every consumer takes one
         * partition from each topic, indices rotated by 1. */
        as[0] = build_partition_list(3, T, 1, U, 2, V, 3);
        as[1] = build_partition_list(3, T, 2, U, 3, V, 0);
        as[2] = build_partition_list(3, T, 3, U, 0, V, 1);
        as[3] = build_partition_list(3, T, 0, U, 1, V, 2);
        push_round(mc, group, id_arr, as, l, 4, "distributed_rotation");
        for (i = 0; i < 4; i++)
                rd_kafka_topic_partition_list_destroy(as[i]);

        for (i = 0; i < 4; i++) {
                test_share_consumer_close(c[i]);
                test_share_destroy(c[i]);
                assert_observer_healthy(l[i], "c[i]");
                test_share_consumer_assignments_destroy(l[i]);
                rd_free(ids[i]);
        }
        test_mock_cluster_destroy(mc);
        rd_free(T);
        rd_free(U);
        rd_free(V);
        SUB_TEST_PASS();
}

/******************************************************************************
 * Entry point
 ******************************************************************************/

int main_0187_share_consumer_assignment_routing(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);

        do_test_force_precise_assignment_single_consumer();
        do_test_force_shared_partition_between_consumers();
        do_test_force_change_existing_assignment();
        do_test_force_empty_assignment();
        do_test_steady_state_no_spurious_serves();
        do_test_force_reassignment_cycle_three_consumers();
        do_test_force_chaos_four_consumers();
        do_test_force_multi_topic_assignment_two_consumers();
        do_test_force_same_assignment_twice_no_extra_serve();
        do_test_force_chaos_three_consumers_three_topics();
        do_test_force_chaos_four_consumers_three_topics();

        return 0;
}
