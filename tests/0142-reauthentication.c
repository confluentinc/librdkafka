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
/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"  /* for Kafka driver */
#include "rdatomic.h" /* for the share set_token callback counters */

static int delivered_msg = 0;
static int expect_err    = 0;
static int error_seen    = 0;

static void
dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
        if (rkmessage->err)
                TEST_FAIL("Message delivery failed: %s\n",
                          rd_kafka_err2str(rkmessage->err));
        else {
                delivered_msg++;
        }
}

static void
auth_error_cb(rd_kafka_t *rk, int err, const char *reason, void *opaque) {
        if (expect_err && (err == RD_KAFKA_RESP_ERR__AUTHENTICATION ||
                           err == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN)) {
                TEST_SAY("Expected error: %s: %s\n", rd_kafka_err2str(err),
                         reason);
                error_seen = rd_true;
        } else
                TEST_FAIL("Unexpected error: %s: %s", rd_kafka_err2str(err),
                          reason);
        rd_kafka_yield(rk);
}


/* ------------------------------------------------------------------------
 * Tests for the share-consumer OAUTHBEARER APIs added in this commit:
 *   rd_kafka_share_oauthbearer_set_token()
 *   rd_kafka_share_oauthbearer_set_token_failure()
 *   rd_kafka_share_queue_get_sasl()
 *
 * These exercise OAUTHBEARER DEFAULT mode against the *real* setup (the AWS
 * IAM / Confluent Cloud configuration validated with the Python oauth_cb
 * tests): the application obtains a real bearer token out-of-band and hands it
 * to librdkafka through the share APIs above. The C equivalent of the Python
 * boto3-STS step is to run a token command (e.g. `aws sts
 * get-web-identity-token ...` or a boto3 python one-liner) and read the token
 * from its stdout.
 *
 * Cluster config (bootstrap, SASL_SSL, sasl.mechanism=OAUTHBEARER,
 * sasl.oauthbearer.method=default) comes from test.conf; the token command and
 * the Confluent Cloud routing extensions come from env vars:
 *
 *   OAUTHBEARER_TOKEN_CMD         shell command that prints the token to stdout
 *   OAUTHBEARER_LOGICAL_CLUSTER   logicalCluster SASL extension (lkc-xxxxx)
 *   OAUTHBEARER_IDENTITY_POOL_ID  identityPoolId SASL extension (pool-xxxxx)
 *
 * The functional tests self-skip unless that real setup is configured. The
 * argument-validation test runs against any OAUTHBEARER/default config and
 * needs no broker or token command.
 * ------------------------------------------------------------------------ */

#define SHARE_TOKEN_LIFETIME_S 300

typedef enum {
        TOKEN_MODE_SUCCESS = 0, /* fetch a real token, call set_token */
        TOKEN_MODE_FAILURE = 1  /* report failure via set_token_failure */
} share_token_mode_t;

struct share_token_ctx {
        rd_kafka_share_t *rkshare; /* assigned right after creation */
        share_token_mode_t mode;
        const char *token_cmd;       /* command whose stdout is the token */
        const char *logical_cluster; /* logicalCluster SASL extension */
        const char *identity_pool;   /* identityPoolId SASL extension */
        rd_atomic32_t set_token_calls;
        rd_atomic32_t set_token_failure_calls;
        rd_atomic32_t auth_error_seen;
};

/* Run the token command and return its stdout as a NUL-terminated bearer
 * token (caller frees), or NULL on failure. This is the C equivalent of the
 * Python oauth_cb's boto3-STS step: the real token is minted out-of-band (e.g.
 * `aws sts get-web-identity-token ...` or a boto3 one-liner) and read from the
 * command's stdout, then handed to librdkafka via the share APIs. */
static char *share_exec_token(const char *cmd) {
        FILE *fp;
        char *token;
        size_t cap = 16384, len = 0;
        int rc;

#ifdef _WIN32
        fp = _popen(cmd, "r");
#else
        fp = popen(cmd, "r");
#endif
        if (!fp) {
                TEST_SAY("token command popen() failed for: %s\n", cmd);
                return NULL;
        }

        token = malloc(cap);
        for (;;) {
                size_t n;
                if (len + 1 >= cap) {
                        cap *= 2;
                        token = realloc(token, cap);
                }
                n = fread(token + len, 1, cap - len - 1, fp);
                len += n;
                if (n == 0)
                        break;
        }
        token[len] = '\0';

#ifdef _WIN32
        rc = _pclose(fp);
#else
        rc = pclose(fp);
#endif
        if (rc != 0) {
                TEST_SAY("token command exited non-zero (%d): %s\n", rc, cmd);
                free(token);
                return NULL;
        }

        /* Trim trailing whitespace the command may append. */
        while (len > 0 && (token[len - 1] == '\n' || token[len - 1] == '\r' ||
                           token[len - 1] == ' ' || token[len - 1] == '\t'))
                token[--len] = '\0';

        if (len == 0) {
                free(token);
                return NULL;
        }
        return token;
}

/* Producer OAUTHBEARER refresh callback: fetch a real token and set it on the
 * producer's own handle via the non-share API, so the producer can
 * authenticate to the same cluster/pool and create the topic + produce. */
static void producer_token_refresh_cb(rd_kafka_t *rk,
                                      const char *oauthbearer_config,
                                      void *opaque) {
        struct share_token_ctx *ctx = opaque;
        const char *ext[]           = {"logicalCluster", ctx->logical_cluster,
                                       "identityPoolId", ctx->identity_pool};
        char errstr[512];
        char *token;
        rd_kafka_resp_err_t err;

        token = share_exec_token(ctx->token_cmd);
        if (!token) {
                rd_kafka_oauthbearer_set_token_failure(
                    rk, "producer: token command failed");
                return;
        }
        err = rd_kafka_oauthbearer_set_token(
            rk, token, (int64_t)(time(NULL) + SHARE_TOKEN_LIFETIME_S) * 1000,
            "", ext, 4, errstr, sizeof(errstr));
        if (err)
                TEST_FAIL("producer oauthbearer_set_token failed: %s (%s)",
                          errstr, rd_kafka_err2name(err));
        free(token);
}

/* Share consumer OAUTHBEARER refresh callback. SUCCESS mode fetches a real
 * token and supplies it via rd_kafka_share_oauthbearer_set_token(); FAILURE
 * mode reports failure via rd_kafka_share_oauthbearer_set_token_failure().
 * These are the two token APIs under test. */
static void share_token_refresh_cb(rd_kafka_t *rk,
                                   const char *oauthbearer_config,
                                   void *opaque) {
        struct share_token_ctx *ctx = opaque;
        rd_kafka_resp_err_t err;

        /* Refresh may fire before the share handle is assigned; report a
         * transient failure so librdkafka retries. */
        if (!ctx->rkshare) {
                rd_kafka_oauthbearer_set_token_failure(
                    rk, "share handle not ready yet");
                return;
        }

        if (ctx->mode == TOKEN_MODE_FAILURE) {
                err = rd_kafka_share_oauthbearer_set_token_failure(
                    ctx->rkshare, "test: simulated token acquisition failure");
                TEST_ASSERT(!err,
                            "share_oauthbearer_set_token_failure returned %s",
                            rd_kafka_err2name(err));
                rd_atomic32_add(&ctx->set_token_failure_calls, 1);
                return;
        }

        {
                const char *ext[] = {"logicalCluster", ctx->logical_cluster,
                                     "identityPoolId", ctx->identity_pool};
                char errstr[512];
                char *token = share_exec_token(ctx->token_cmd);

                if (!token) {
                        rd_kafka_share_oauthbearer_set_token_failure(
                            ctx->rkshare, "token command failed");
                        rd_atomic32_add(&ctx->set_token_failure_calls, 1);
                        return;
                }
                err = rd_kafka_share_oauthbearer_set_token(
                    ctx->rkshare, token,
                    (int64_t)(time(NULL) + SHARE_TOKEN_LIFETIME_S) * 1000, "",
                    ext, 4, errstr, sizeof(errstr));
                TEST_ASSERT(!err, "share_oauthbearer_set_token failed: %s (%s)",
                            errstr, rd_kafka_err2name(err));
                free(token);
                rd_atomic32_add(&ctx->set_token_calls, 1);
        }
}

static void share_token_error_cb(rd_kafka_t *rk,
                                 int err,
                                 const char *reason,
                                 void *opaque) {
        struct share_token_ctx *ctx = opaque;
        if (err == RD_KAFKA_RESP_ERR__AUTHENTICATION ||
            err == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN) {
                rd_atomic32_add(&ctx->auth_error_seen, 1);
                TEST_SAY("share set_token: expected error %s: %s\n",
                         rd_kafka_err2name(err), reason);
        }
}

/* Argument validation (no broker required): the exposed share APIs must
 * reject invalid arguments with _INVALID_ARG. */
static void do_test_share_set_token_invalid_args(void) {
        rd_kafka_share_t *sc1;
        rd_kafka_queue_t *saslq;
        char errstr[512];
        rd_kafka_resp_err_t err;
        const char *odd_ext[] = {"keyWithoutValue"}; /* size 1: not even */

        SUB_TEST("share OAUTHBEARER set_token/_failure argument validation");

        if (rd_strcasecmp(test_conf_get(NULL, "sasl.mechanism"),
                          "oauthbearer")) {
                SUB_TEST_SKIP("`sasl.mechanism=OAUTHBEARER` required\n");
                return;
        }
        if (!rd_strcasecmp(test_conf_get(NULL, "sasl.oauthbearer.method"),
                           "oidc")) {
                SUB_TEST_SKIP("requires sasl.oauthbearer.method=default\n");
                return;
        }

        sc1 = test_create_share_consumer("share-set-token-args", NULL);

        /* rd_kafka_share_queue_get_sasl() must return the SASL callback queue
         * for an OAUTHBEARER share consumer (NULL otherwise). */
        saslq = rd_kafka_share_queue_get_sasl(sc1);
        TEST_ASSERT(saslq != NULL,
                    "share_queue_get_sasl returned NULL for an OAUTHBEARER "
                    "share consumer");
        rd_kafka_queue_destroy(saslq);

        /* set_token_failure() with no error string -> _INVALID_ARG. */
        err = rd_kafka_share_oauthbearer_set_token_failure(sc1, NULL);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "set_token_failure(NULL) expected _INVALID_ARG, got %s",
                    rd_kafka_err2name(err));

        /* set_token() with an already-expired lifetime -> _INVALID_ARG. */
        err = rd_kafka_share_oauthbearer_set_token(
            sc1, "header.payload.", 1000 /* ms since epoch: long expired */,
            "admin", NULL, 0, errstr, sizeof(errstr));
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "set_token(expired) expected _INVALID_ARG, got %s",
                    rd_kafka_err2name(err));

        /* set_token() with an odd extension_size -> _INVALID_ARG. */
        err = rd_kafka_share_oauthbearer_set_token(
            sc1, "header.payload.", (int64_t)(time(NULL) + 300) * 1000, "admin",
            odd_ext, 1, errstr, sizeof(errstr));
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "set_token(odd extension_size) expected _INVALID_ARG, "
                    "got %s",
                    rd_kafka_err2name(err));

        test_share_destroy(sc1);
        SUB_TEST_PASS();
}

/* Functional test against the real OAUTHBEARER setup (Confluent Cloud + token
 * command). A real bearer token is obtained by running OAUTHBEARER_TOKEN_CMD
 * and supplied to the share consumer through the APIs under test.
 *   fail_mode=rd_false: set_token with a real token -> auth + consume works
 *   fail_mode=rd_true : set_token_failure           -> auth fails, no consume
 * Skips unless OAUTHBEARER/default + the token command and routing extensions
 * are configured. */
void do_test_share_set_token(rd_bool_t fail_mode) {
        rd_kafka_t *p1;
        rd_kafka_share_t *sc1;
        rd_kafka_conf_t *conf;
        rd_kafka_queue_t *saslq;
        rd_kafka_topic_partition_list_t *subs;
        rd_kafka_error_t *error;
        const char *grp_conf[] = {"share.auto.offset.reset", "SET", "earliest"};
        const char *group      = "share-set-token-test";
        const char *topic      = test_mk_topic_name("share_set_token", 1);
        const char *expected_topics[1];
        const char *token_cmd, *logical_cluster, *identity_pool;
        char errstr[512];
        struct share_token_ctx ctx;
        int consumed;

        SUB_TEST("share consumer rd_kafka_share_oauthbearer_set_token%s (%s)",
                 fail_mode ? "_failure" : "",
                 fail_mode ? "expect auth failure" : "expect auth + consume");

        if (rd_strcasecmp(test_conf_get(NULL, "sasl.mechanism"),
                          "oauthbearer")) {
                SUB_TEST_SKIP("`sasl.mechanism=OAUTHBEARER` required\n");
                return;
        }
        if (!rd_strcasecmp(test_conf_get(NULL, "sasl.oauthbearer.method"),
                           "oidc")) {
                SUB_TEST_SKIP("requires sasl.oauthbearer.method=default\n");
                return;
        }
        token_cmd       = test_getenv("OAUTHBEARER_TOKEN_CMD", NULL);
        logical_cluster = test_getenv("OAUTHBEARER_LOGICAL_CLUSTER", NULL);
        identity_pool   = test_getenv("OAUTHBEARER_IDENTITY_POOL_ID", NULL);
        if (!token_cmd || !logical_cluster || !identity_pool) {
                SUB_TEST_SKIP(
                    "set OAUTHBEARER_TOKEN_CMD, OAUTHBEARER_LOGICAL_CLUSTER "
                    "and "
                    "OAUTHBEARER_IDENTITY_POOL_ID to run against the real "
                    "OAUTHBEARER setup\n");
                return;
        }

        test_timeout_set(120);

        memset(&ctx, 0, sizeof(ctx));
        ctx.mode      = fail_mode ? TOKEN_MODE_FAILURE : TOKEN_MODE_SUCCESS;
        ctx.token_cmd = token_cmd;
        ctx.logical_cluster = logical_cluster;
        ctx.identity_pool   = identity_pool;

        /* Producer: authenticates with a real token (same URL) so it can create
         * the topic, set the group config and produce. */
        test_conf_init(&conf, NULL, 30);
        rd_kafka_conf_set_oauthbearer_token_refresh_cb(
            conf, producer_token_refresh_cb);
        rd_kafka_conf_set_opaque(conf, &ctx);
        rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
        p1 = test_create_handle(RD_KAFKA_PRODUCER, conf);
        test_create_topic_wait_exists(p1, topic, 1, -1, 5000);
        test_IncrementalAlterConfigs_simple(p1, RD_KAFKA_RESOURCE_GROUP, group,
                                            grp_conf, 1);

        /* Share consumer: the token is supplied ONLY through the share APIs
         * under test, served on a background SASL callback thread. */
        test_conf_init(&conf, NULL, 30);
        rd_kafka_conf_enable_sasl_queue(conf, rd_true);
        rd_kafka_conf_set_oauthbearer_token_refresh_cb(conf,
                                                       share_token_refresh_cb);
        rd_kafka_conf_set_error_cb(conf, share_token_error_cb);
        rd_kafka_conf_set_opaque(conf, &ctx);
        rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));

        sc1 = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(sc1 != NULL, "Failed to create share consumer: %s", errstr);
        ctx.rkshare = sc1; /* the refresh callback may now use the handle */

        /* rd_kafka_share_queue_get_sasl(): exercise the third new API. */
        saslq = rd_kafka_share_queue_get_sasl(sc1);
        TEST_ASSERT(saslq != NULL, "share_queue_get_sasl returned NULL");
        rd_kafka_queue_destroy(saslq);

        error = rd_kafka_share_sasl_background_callbacks_enable(sc1);
        TEST_ASSERT(!error, "share_sasl_background_callbacks_enable failed: %s",
                    error ? rd_kafka_error_string(error) : "");

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(sc1, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        expected_topics[0] = topic;

        if (!fail_mode) {
                /* Produce one message and consume it: proves the real token
                 * supplied via set_token authenticated end-to-end. */
                test_produce_msgs2(p1, topic, 0, 0, 0, 1, NULL, 0);
                rd_kafka_flush(p1, 10 * 1000);

                consumed = test_share_consume_msgs(sc1, 1, 30, 1000,
                                                   expected_topics, 1);

                TEST_ASSERT(rd_atomic32_get(&ctx.set_token_calls) > 0,
                            "expected share_oauthbearer_set_token to be "
                            "called");
                TEST_ASSERT(consumed > 0,
                            "expected to consume after set_token, consumed=%d",
                            consumed);
                TEST_SAY("set_token OK: %d call(s), consumed %d msg(s)\n",
                         rd_atomic32_get(&ctx.set_token_calls), consumed);

                test_share_consumer_close(sc1);
        } else {
                /* No valid token is ever supplied -> auth fails, nothing
                 * consumed. test_share_poll() swallows poll errors -> 0. */
                consumed = test_share_consume_msgs(sc1, 1, 15, 1000,
                                                   expected_topics, 1);

                TEST_ASSERT(rd_atomic32_get(&ctx.set_token_failure_calls) > 0,
                            "expected share_oauthbearer_set_token_failure to "
                            "be called");
                TEST_ASSERT(consumed == 0,
                            "expected NO messages consumed on token failure, "
                            "consumed=%d",
                            consumed);
                TEST_SAY(
                    "set_token_failure OK: %d call(s), auth_error_seen=%d,"
                    " consumed=%d\n",
                    rd_atomic32_get(&ctx.set_token_failure_calls),
                    rd_atomic32_get(&ctx.auth_error_seen), consumed);

                /* A never-authenticated consumer's close() may return an error;
                 * ignore it (don't use test_share_consumer_close(), which
                 * TEST_FAILs on error). */
                error = rd_kafka_share_consumer_close(sc1);
                if (error)
                        rd_kafka_error_destroy(error);
        }

        test_share_destroy(sc1);
        rd_kafka_destroy(p1);
        SUB_TEST_PASS();
}

/* Test producer message loss while reauth happens between produce. */
void do_test_producer(int64_t reauth_time, const char *topic) {
        rd_kafka_topic_t *rkt = NULL;
        rd_kafka_conf_t *conf = NULL;
        rd_kafka_t *rk        = NULL;
        uint64_t testid       = test_id_generate();
        rd_kafka_resp_err_t err;
        int msgrate, msgcnt, sent_msg;
        test_timing_t t_produce;

        msgrate = 200; /* msg/sec */
        /* Messages should be produced such that at least one reauth happens.
         * The 1.2 is added as a buffer to avoid flakiness. */
        msgcnt        = msgrate * reauth_time / 1000 * 1.2;
        delivered_msg = 0;
        sent_msg      = 0;

        SUB_TEST("test producer message loss while reauthenticating");

        test_conf_init(&conf, NULL, 30);
        rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

        rk  = test_create_handle(RD_KAFKA_PRODUCER, conf);
        rkt = test_create_producer_topic(rk, topic, NULL);
        test_wait_topic_exists(rk, topic, 5000);

        /* Create the topic to make sure connections are up and ready. */
        err = test_auto_create_topic_rkt(rk, rkt, tmout_multip(5000));
        TEST_ASSERT(!err, "topic creation failed: %s", rd_kafka_err2str(err));

        TIMING_START(&t_produce, "PRODUCE");
        /* Produce enough messages such that we have time enough for at least
         * one reauth. */
        test_produce_msgs_nowait(rk, rkt, testid, 0, 0, msgcnt, NULL, 0,
                                 msgrate, &sent_msg);
        TIMING_STOP(&t_produce);

        rd_kafka_flush(rk, 10 * 1000);

        TEST_ASSERT(TIMING_DURATION(&t_produce) >= reauth_time * 1000,
                    "time enough for one reauth should pass (%ld vs %ld)",
                    TIMING_DURATION(&t_produce), reauth_time * 1000);
        TEST_ASSERT(delivered_msg == sent_msg,
                    "did not deliver as many messages as sent (%d vs %d)",
                    delivered_msg, sent_msg);

        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);

        SUB_TEST_PASS();
}

/* Test share consumer message loss while reauth happens between consume. */
void do_test_share_consumer(int64_t reauth_time) {
        rd_kafka_t *p1;
        rd_kafka_share_t *sc1;
        rd_kafka_conf_t *conf;
        rd_kafka_topic_partition_list_t *subs;
        rd_kafka_messages_t *batch = NULL;
        rd_kafka_error_t *err;
        const char *grp_conf[] = {"share.auto.offset.reset", "SET", "earliest"};
        const char *group      = "share-reauth-test";
        const char *topic      = test_mk_topic_name("share_reauth", 1);
        int64_t start_time     = 0;
        int64_t wait_time      = reauth_time * 1.2 * 1000;
        int recv_cnt = 0, sent_cnt = 0;
        int attempts;
        size_t rcvd, m;

        SUB_TEST("test share consumer message loss while reauthenticating");

        test_conf_init(&conf, NULL, 30);
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);

        p1 = test_create_handle(RD_KAFKA_PRODUCER, conf);

        test_create_topic_wait_exists(p1, topic, 1, 3, 5000);
        TEST_SAY("Topic: %s is created\n", topic);

        /* Create share consumer */
        sc1 = test_create_share_consumer(group, NULL);

        /* Set group config for earliest offset */
        test_IncrementalAlterConfigs_simple(p1, RD_KAFKA_RESOURCE_GROUP, group,
                                            grp_conf, 1);

        /* Subscribe */
        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(sc1, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        /* Wait for the share consumer to join the group by producing
         * a message and polling until it is received. */
        test_produce_msgs2(p1, topic, 0, 0, 0, 1, NULL, 0);
        rd_kafka_flush(p1, 10 * 1000);
        attempts = 50;
        rcvd     = 0;
        while (rcvd == 0 && attempts-- > 0) {
                rd_kafka_messages_destroy(batch);
                batch = NULL;
                err   = rd_kafka_share_poll(sc1, 2000, &batch);
                if (err)
                        rd_kafka_error_destroy(err);
                rcvd = rd_kafka_messages_count(batch);
        }
        TEST_ASSERT(rcvd > 0,
                    "Share consumer failed to join group "
                    "and consume warmup message");
        rd_kafka_messages_destroy(batch);
        batch = NULL;
        TEST_SAY("Share consumer joined group, starting reauth test\n");

        start_time = test_clock();
        while ((test_clock() - start_time) <= wait_time) {
                rcvd = 0;
                rd_kafka_messages_destroy(batch);
                batch = NULL;

                /* Produce one message. */
                test_produce_msgs2(p1, topic, 0, 0, 0, 1, NULL, 0);
                sent_cnt++;

                err = rd_kafka_share_poll(sc1, 100, &batch);
                if (err) {
                        rd_kafka_error_destroy(err);
                        rd_kafka_flush(p1, 50);
                } else {
                        rcvd = rd_kafka_messages_count(batch);
                        for (m = 0; m < rcvd; m++) {
                                rd_kafka_message_t *rkm =
                                    rd_kafka_messages_get(batch, m);
                                if (!rkm->err)
                                        recv_cnt++;
                        }
                        rd_kafka_messages_destroy(batch);
                        batch = NULL;
                }

                /* Maintain the message rate at ~200 msg/s regardless
                 * of consume errors during reauthentication. */
                rd_usleep(1000 * 50, NULL);
        }

        /* Final flush and receive any remaining messages. */
        rd_kafka_flush(p1, 10 * 1000);
        attempts = 100;
        while (recv_cnt < sent_cnt && attempts-- > 0) {
                rcvd = 0;
                rd_kafka_messages_destroy(batch);
                batch = NULL;

                err = rd_kafka_share_poll(sc1, 3000, &batch);
                if (err) {
                        rd_kafka_error_destroy(err);
                        continue;
                }

                rcvd = rd_kafka_messages_count(batch);
                for (m = 0; m < rcvd; m++) {
                        rd_kafka_message_t *rkm =
                            rd_kafka_messages_get(batch, m);
                        if (!rkm->err)
                                recv_cnt++;
                }
                rd_kafka_messages_destroy(batch);
                batch = NULL;
        }

        rd_kafka_share_consumer_close(sc1);

        TEST_ASSERT(sent_cnt == recv_cnt,
                    "did not receive as many messages as sent (%d vs %d)",
                    sent_cnt, recv_cnt);

        rd_kafka_destroy(p1);
        rd_kafka_share_destroy(sc1);
        SUB_TEST_PASS();
}


/* Test consumer message loss while reauth happens between consume. */
void do_test_consumer(int64_t reauth_time, const char *topic) {
        uint64_t testid;
        rd_kafka_t *p1;
        rd_kafka_t *c1;
        rd_kafka_conf_t *conf;
        int64_t start_time = 0;
        int64_t wait_time  = reauth_time * 1.2 * 1000;
        int recv_cnt = 0, sent_cnt = 0;

        SUB_TEST("test consumer message loss while reauthenticating");

        testid = test_id_generate();

        test_conf_init(&conf, NULL, 30);
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);

        p1 = test_create_handle(RD_KAFKA_PRODUCER, rd_kafka_conf_dup(conf));

        test_create_topic_wait_exists(p1, topic, 1, 3, 5000);
        TEST_SAY("Topic: %s is created\n", topic);

        test_conf_set(conf, "auto.offset.reset", "earliest");
        c1 = test_create_consumer(topic, NULL, conf, NULL);
        test_consumer_subscribe(c1, topic);

        start_time = test_clock();
        while ((test_clock() - start_time) <= wait_time) {
                /* Produce one message. */
                test_produce_msgs2(p1, topic, testid, 0, 0, 1, NULL, 0);
                sent_cnt++;

                rd_kafka_message_t *rkm = rd_kafka_consumer_poll(c1, 100);
                if (!rkm || rkm->err) {
                        /* Ignore errors. Add a flush for good measure so maybe
                         * we'll have messages in the next iteration. */
                        rd_kafka_flush(p1, 50);
                        continue;
                }
                recv_cnt++;
                rd_kafka_message_destroy(rkm);

                /* An approximate way of maintaining the message rate as 200
                 * msg/s */
                rd_usleep(1000 * 50, NULL);
        }

        /* Final flush and receive any remaining messages. */
        rd_kafka_flush(p1, 10 * 1000);
        recv_cnt +=
            test_consumer_poll_timeout("timeout", c1, testid, -1, -1,
                                       sent_cnt - recv_cnt, NULL, 10 * 1000);

        test_consumer_close(c1);

        TEST_ASSERT(sent_cnt == recv_cnt,
                    "did not receive as many messages as sent (%d vs %d)",
                    sent_cnt, recv_cnt);

        rd_kafka_destroy(p1);
        rd_kafka_destroy(c1);
        SUB_TEST_PASS();
}



/* Test produce from a transactional producer while there is a reauth, and check
 * consumed messages for a committed or an aborted transaction. */
void do_test_txn_producer(int64_t reauth_time,
                          const char *topic,
                          rd_bool_t abort_txn) {
        rd_kafka_topic_t *rkt = NULL;
        rd_kafka_conf_t *conf = NULL;
        rd_kafka_t *rk        = NULL;
        uint64_t testid       = test_id_generate();
        rd_kafka_resp_err_t err;
        int msgrate, msgcnt, sent_msg;
        test_timing_t t_produce;

        delivered_msg = 0;
        sent_msg      = 0;
        msgrate       = 200; /* msg/sec */
        /* Messages should be produced such that at least one reauth happens.
         * The 1.2 is added as a buffer to avoid flakiness. */
        msgcnt = msgrate * reauth_time / 1000 * 1.2;

        SUB_TEST("test reauth in the middle of a txn, txn is %s",
                 abort_txn ? "aborted" : "committed");

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "transactional.id", topic);
        test_conf_set(conf, "transaction.timeout.ms",
                      tsprintf("%ld", (int64_t)(reauth_time * 1.2 + 60000)));
        rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

        rk  = test_create_handle(RD_KAFKA_PRODUCER, conf);
        rkt = test_create_producer_topic(rk, topic, NULL);
        test_wait_topic_exists(rk, topic, 5000);

        err = test_auto_create_topic_rkt(rk, rkt, tmout_multip(5000));
        TEST_ASSERT(!err, "topic creation failed: %s", rd_kafka_err2str(err));

        TEST_CALL_ERROR__(rd_kafka_init_transactions(rk, -1));
        TEST_CALL_ERROR__(rd_kafka_begin_transaction(rk));


        TIMING_START(&t_produce, "PRODUCE");
        /* Produce enough messages such that we have time enough for at least
         * one reauth. */
        test_produce_msgs_nowait(rk, rkt, testid, 0, 0, msgcnt, NULL, 0,
                                 msgrate, &sent_msg);
        TIMING_STOP(&t_produce);

        rd_kafka_flush(rk, 10 * 1000);

        TEST_ASSERT(TIMING_DURATION(&t_produce) >= reauth_time * 1000,
                    "time enough for one reauth should pass (%ld vs %ld)",
                    TIMING_DURATION(&t_produce), reauth_time * 1000);
        TEST_ASSERT(delivered_msg == sent_msg,
                    "did not deliver as many messages as sent (%d vs %d)",
                    delivered_msg, sent_msg);

        if (abort_txn) {
                rd_kafka_t *c = NULL;

                TEST_CALL_ERROR__(rd_kafka_abort_transaction(rk, 30 * 1000));

                /* We can reuse conf because the old one's been moved to rk
                 * already. */
                test_conf_init(&conf, NULL, 30);
                test_conf_set(conf, "isolation.level", "read_committed");
                c = test_create_consumer("mygroup", NULL, conf, NULL);
                test_consumer_poll_no_msgs("mygroup", c, testid, 10 * 1000);

                rd_kafka_destroy(c);
        } else {
                TEST_CALL_ERROR__(rd_kafka_commit_transaction(rk, 30 * 1000));
                test_consume_txn_msgs_easy("mygroup", topic, testid, -1,
                                           sent_msg, NULL);
        }

        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);

        SUB_TEST_PASS();
}


/* Check reauthentication in case of OAUTHBEARER mechanism, with different
 * reauth times and token lifetimes. */
void do_test_share_oauthbearer(int64_t reauth_time,
                               int64_t token_lifetime_ms,
                               rd_bool_t use_sasl_queue) {
        rd_kafka_t *p1;
        rd_kafka_share_t *sc1;
        rd_kafka_conf_t *conf;
        rd_kafka_topic_partition_list_t *subs;
        rd_kafka_messages_t *batch = NULL;
        rd_kafka_error_t *err;
        const char *grp_conf[] = {"share.auto.offset.reset", "SET", "earliest"};
        const char *group      = "share-oauthbearer-reauth-test";
        const char *topic      = test_mk_topic_name("share_oauth_reauth", 1);
        char *mechanism, *oauthbearer_method;
        char errstr[512];
        int token_lifetime_s = token_lifetime_ms / 1000;
        int64_t start_time   = 0;
        int64_t wait_time    = reauth_time * 1.2 * 1000;
        int recv_cnt = 0, sent_cnt = 0;
        int attempts;
        size_t rcvd, m;

        SUB_TEST(
            "test share consumer reauthentication with oauthbearer, "
            "reauth_time = %ld, token_lifetime = %ld, use_sasl_queue = %s",
            reauth_time, token_lifetime_ms, use_sasl_queue ? "yes" : "no");

        /* Producer */
        test_conf_init(&conf, NULL, 30);
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);

        mechanism = test_conf_get(conf, "sasl.mechanism");
        if (rd_strcasecmp(mechanism, "oauthbearer")) {
                rd_kafka_conf_destroy(conf);
                SUB_TEST_SKIP(
                    "`sasl.mechanism=OAUTHBEARER` is required, have %s\n",
                    mechanism);
        }

        oauthbearer_method = test_conf_get(conf, "sasl.oauthbearer.method");
        if (rd_strcasecmp(oauthbearer_method, "oidc")) {
                test_conf_set(
                    conf, "sasl.oauthbearer.config",
                    tsprintf(
                        "principal=admin scope=requiredScope lifeSeconds=%d",
                        token_lifetime_s));
                test_conf_set(conf, "enable.sasl.oauthbearer.unsecure.jwt",
                              "true");
        }

        p1 = test_create_handle(RD_KAFKA_PRODUCER, conf);
        test_create_topic_wait_exists(p1, topic, 1, 3, 5000);

        /* Share consumer */
        test_conf_init(&conf, NULL, 30);
        rd_kafka_conf_enable_sasl_queue(conf, use_sasl_queue);

        oauthbearer_method = test_conf_get(conf, "sasl.oauthbearer.method");
        if (rd_strcasecmp(oauthbearer_method, "oidc")) {
                test_conf_set(
                    conf, "sasl.oauthbearer.config",
                    tsprintf(
                        "principal=admin scope=requiredScope lifeSeconds=%d",
                        token_lifetime_s));
                test_conf_set(conf, "enable.sasl.oauthbearer.unsecure.jwt",
                              "true");
        }

        rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));
        sc1 = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(sc1 != NULL, "Failed to create share consumer: %s", errstr);

        if (use_sasl_queue) {
                rd_kafka_error_t *error =
                    rd_kafka_share_sasl_background_callbacks_enable(sc1);
                TEST_ASSERT(!error,
                            "share_sasl_background_callbacks_enable failed: %s",
                            error ? rd_kafka_error_string(error) : "");
        }

        /* Set group config for earliest offset */
        test_IncrementalAlterConfigs_simple(p1, RD_KAFKA_RESOURCE_GROUP, group,
                                            grp_conf, 1);

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(sc1, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        /* Wait for the share consumer to join the group by producing
         * a message and polling until it is received. */
        test_produce_msgs2(p1, topic, 0, 0, 0, 1, NULL, 0);
        rd_kafka_flush(p1, 10 * 1000);
        attempts = 50;
        rcvd     = 0;
        while (rcvd == 0 && attempts-- > 0) {
                rd_kafka_messages_destroy(batch);
                batch = NULL;
                err   = rd_kafka_share_poll(sc1, 2000, &batch);
                if (err)
                        rd_kafka_error_destroy(err);
                rcvd = rd_kafka_messages_count(batch);
        }
        TEST_ASSERT(rcvd > 0,
                    "Share consumer failed to join group "
                    "and consume warmup message");
        rd_kafka_messages_destroy(batch);
        batch = NULL;
        TEST_SAY(
            "Share consumer joined group, starting oauthbearer "
            "reauth test\n");

        start_time = test_clock();
        while ((test_clock() - start_time) <= wait_time) {
                rcvd = 0;
                rd_kafka_messages_destroy(batch);
                batch = NULL;

                test_produce_msgs2(p1, topic, 0, 0, 0, 1, NULL, 0);
                sent_cnt++;

                err = rd_kafka_share_poll(sc1, 100, &batch);
                if (err) {
                        rd_kafka_error_destroy(err);
                        rd_kafka_flush(p1, 50);
                } else {
                        rcvd = rd_kafka_messages_count(batch);
                        for (m = 0; m < rcvd; m++) {
                                rd_kafka_message_t *rkm =
                                    rd_kafka_messages_get(batch, m);
                                if (!rkm->err)
                                        recv_cnt++;
                        }
                        rd_kafka_messages_destroy(batch);
                        batch = NULL;
                }

                rd_usleep(1000 * 50, NULL);
        }

        /* Final flush and receive any remaining messages. */
        rd_kafka_flush(p1, 10 * 1000);
        attempts = 100;
        while (recv_cnt < sent_cnt && attempts-- > 0) {
                rcvd = 0;
                rd_kafka_messages_destroy(batch);
                batch = NULL;

                err = rd_kafka_share_poll(sc1, 3000, &batch);
                if (err) {
                        rd_kafka_error_destroy(err);
                        continue;
                }

                rcvd = rd_kafka_messages_count(batch);
                for (m = 0; m < rcvd; m++) {
                        rd_kafka_message_t *rkm =
                            rd_kafka_messages_get(batch, m);
                        if (!rkm->err)
                                recv_cnt++;
                }
                rd_kafka_messages_destroy(batch);
                batch = NULL;
        }

        rd_kafka_share_consumer_close(sc1);

        TEST_ASSERT(sent_cnt == recv_cnt,
                    "did not receive as many messages as sent (%d vs %d)",
                    sent_cnt, recv_cnt);

        rd_kafka_destroy(p1);
        rd_kafka_share_destroy(sc1);
        SUB_TEST_PASS();
}


void do_test_oauthbearer(int64_t reauth_time,
                         const char *topic,
                         int64_t token_lifetime_ms,
                         rd_bool_t use_sasl_queue) {
        rd_kafka_topic_t *rkt = NULL;
        rd_kafka_conf_t *conf = NULL;
        rd_kafka_t *rk        = NULL;
        uint64_t testid       = test_id_generate();
        rd_kafka_resp_err_t err;
        char *mechanism, *oauthbearer_method;
        int msgrate, msgcnt, sent_msg;
        test_timing_t t_produce;
        int token_lifetime_s = token_lifetime_ms / 1000;

        SUB_TEST(
            "test reauthentication with oauthbearer, reauth_time = %ld, "
            "token_lifetime = %ld",
            reauth_time, token_lifetime_ms);

        test_conf_init(&conf, NULL, 30);
        rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
        rd_kafka_conf_enable_sasl_queue(conf, use_sasl_queue);

        mechanism = test_conf_get(conf, "sasl.mechanism");
        if (rd_strcasecmp(mechanism, "oauthbearer")) {
                rd_kafka_conf_destroy(conf);
                SUB_TEST_SKIP(
                    "`sasl.mechanism=OAUTHBEARER` is required, have %s\n",
                    mechanism);
        }

        oauthbearer_method = test_conf_get(conf, "sasl.oauthbearer.method");
        if (rd_strcasecmp(oauthbearer_method, "oidc")) {
                test_conf_set(
                    conf, "sasl.oauthbearer.config",
                    tsprintf(
                        "principal=admin scope=requiredScope lifeSeconds=%d",
                        token_lifetime_s));
                test_conf_set(conf, "enable.sasl.oauthbearer.unsecure.jwt",
                              "true");
        }
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* Enable to background queue since we don't want to poll the SASL
         * queue. */
        if (use_sasl_queue)
                rd_kafka_sasl_background_callbacks_enable(rk);

        rkt = test_create_producer_topic(rk, topic, NULL);
        test_wait_topic_exists(rk, topic, 5000);

        /* Create the topic to make sure connections are up and ready. */
        err = test_auto_create_topic_rkt(rk, rkt, tmout_multip(5000));
        TEST_ASSERT(!err, "topic creation failed: %s", rd_kafka_err2str(err));

        msgrate = 200; /* msg/sec */
        /* Messages should be produced such that at least one reauth happens.
         * The 1.2 is added as a buffer to avoid flakiness. */
        msgcnt        = msgrate * reauth_time / 1000 * 1.2;
        delivered_msg = 0;
        sent_msg      = 0;

        TIMING_START(&t_produce, "PRODUCE");
        test_produce_msgs_nowait(rk, rkt, testid, 0, 0, msgcnt, NULL, 0,
                                 msgrate, &sent_msg);
        TIMING_STOP(&t_produce);

        rd_kafka_flush(rk, 10 * 1000);

        TEST_ASSERT(TIMING_DURATION(&t_produce) >= reauth_time * 1000,
                    "time enough for one reauth should pass (%ld vs %ld)",
                    TIMING_DURATION(&t_produce), reauth_time * 1000);
        TEST_ASSERT(delivered_msg == sent_msg,
                    "did not deliver as many messages as sent (%d vs %d)",
                    delivered_msg, sent_msg);

        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);

        SUB_TEST_PASS();
}


/* Check that credentials changed into wrong ones cause authentication errors.
 */
void do_test_reauth_failure(int64_t reauth_time, const char *topic) {
        rd_kafka_topic_t *rkt = NULL;
        rd_kafka_conf_t *conf = NULL;
        rd_kafka_t *rk        = NULL;
        uint64_t testid       = test_id_generate();
        char *mechanism;
        rd_kafka_resp_err_t err;
        int msgrate, msgcnt, sent_msg;
        test_timing_t t_produce;

        msgrate = 200; /* msg/sec */
        /* Messages should be produced such that at least one reauth happens.
         * The 1.2 is added as a buffer to avoid flakiness. */
        msgcnt     = msgrate * reauth_time / 1000 * 1.2;
        error_seen = 0;
        expect_err = 0;

        SUB_TEST("test reauth failure with wrong credentials for reauth");

        test_conf_init(&conf, NULL, 30);
        rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
        rd_kafka_conf_set_error_cb(conf, auth_error_cb);

        mechanism = test_conf_get(conf, "sasl.mechanism");

        if (!rd_strcasecmp(mechanism, "oauthbearer")) {
                rd_kafka_conf_destroy(conf);
                SUB_TEST_SKIP(
                    "PLAIN or SCRAM mechanism is required is required, have "
                    "OAUTHBEARER");
        }

        rk  = test_create_handle(RD_KAFKA_PRODUCER, conf);
        rkt = test_create_producer_topic(rk, topic, NULL);
        test_wait_topic_exists(rk, topic, 5000);

        /* Create the topic to make sure connections are up and ready. */
        err = test_auto_create_topic_rkt(rk, rkt, tmout_multip(5000));
        TEST_ASSERT(!err, "topic creation failed: %s", rd_kafka_err2str(err));

        rd_kafka_sasl_set_credentials(rk, "somethingwhich", "isnotright");
        expect_err = 1;

        TIMING_START(&t_produce, "PRODUCE");
        /* Produce enough messages such that we have time enough for at least
         * one reauth. */
        test_produce_msgs_nowait(rk, rkt, testid, 0, 0, msgcnt, NULL, 0,
                                 msgrate, &sent_msg);
        TIMING_STOP(&t_produce);

        TEST_ASSERT(TIMING_DURATION(&t_produce) >= reauth_time * 1000,
                    "time enough for one reauth should pass (%ld vs %ld)",
                    TIMING_DURATION(&t_produce), reauth_time * 1000);
        TEST_ASSERT(error_seen, "should have had an authentication error");

        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);

        SUB_TEST_PASS();
}


/* Test share consumer reauth failure with wrong PLAIN/SCRAM credentials. */
void do_test_share_reauth_failure(int64_t reauth_time) {
        rd_kafka_t *p1;
        rd_kafka_share_t *sc1;
        rd_kafka_conf_t *conf;
        rd_kafka_topic_partition_list_t *subs;
        rd_kafka_messages_t *batch = NULL;
        rd_kafka_error_t *err;
        const char *group = "share-reauth-failure-test";
        const char *topic = test_mk_topic_name("share_reauth_fail", 1);
        char *mechanism;
        char errstr[512];
        int64_t start_time = 0;
        int64_t wait_time  = reauth_time * 1.2 * 1000;
        int attempts;
        size_t rcvd;

        error_seen = 0;
        expect_err = 0;

        SUB_TEST("test share consumer reauth failure with wrong credentials");

        test_conf_init(&conf, NULL, 30);
        mechanism = test_conf_get(conf, "sasl.mechanism");
        if (!rd_strcasecmp(mechanism, "oauthbearer")) {
                rd_kafka_conf_destroy(conf);
                SUB_TEST_SKIP(
                    "PLAIN or SCRAM mechanism is required, have "
                    "OAUTHBEARER");
        }

        /* Create producer with correct credentials */
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);
        p1 = test_create_handle(RD_KAFKA_PRODUCER, conf);
        test_create_topic_wait_exists(p1, topic, 1, 3, 5000);

        /* Create share consumer with correct credentials + error callback */
        test_conf_init(&conf, NULL, 30);
        rd_kafka_conf_set_error_cb(conf, auth_error_cb);
        rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));
        sc1 = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        TEST_ASSERT(sc1 != NULL, "Failed to create share consumer: %s", errstr);

        /* Set group config for earliest offset */
        test_share_set_auto_offset_reset(group, "earliest");

        subs = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subs, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_share_subscribe(sc1, subs);
        rd_kafka_topic_partition_list_destroy(subs);

        test_produce_msgs_simple(p1, topic, 0, 1);
        attempts = 50;
        rcvd     = 0;
        while (rcvd == 0 && attempts-- > 0) {
                rd_kafka_messages_destroy(batch);
                batch = NULL;
                err   = rd_kafka_share_poll(sc1, 2000, &batch);
                if (err)
                        rd_kafka_error_destroy(err);
                rcvd = rd_kafka_messages_count(batch);
        }
        TEST_ASSERT(rcvd > 0,
                    "Share consumer failed to consume warmup message");
        rd_kafka_messages_destroy(batch);
        batch = NULL;
        TEST_SAY("Share consumer connected, producing messages\n");

        test_produce_msgs_simple(p1, topic, 0, 200);
        TEST_SAY("Produced messages\n");

        /* Corrupt credentials */
        TEST_SAY("Corrupting credentials\n");
        rd_kafka_share_sasl_set_credentials(sc1, "somethingwhich",
                                            "isnotright");
        expect_err = 1;

        /* Consume for 1.2x reauth interval.
         * Reauth happens during consumption and fails with wrong creds. */
        start_time = test_clock();
        while ((test_clock() - start_time) <= wait_time) {
                rcvd = 0;
                rd_kafka_messages_destroy(batch);
                batch = NULL;

                err = rd_kafka_share_poll(sc1, 100, &batch);
                if (err) {
                        rd_kafka_error_destroy(err);
                } else {
                        rd_kafka_messages_destroy(batch);
                        batch = NULL;
                }
        }

        TEST_ASSERT(error_seen, "should have had an authentication error");

        rd_kafka_share_consumer_close(sc1);
        rd_kafka_destroy(p1);
        rd_kafka_share_destroy(sc1);
        SUB_TEST_PASS();
}


int main_0142_reauthentication(int argc, char **argv) {
        size_t broker_id_cnt;
        int32_t *broker_ids   = NULL;
        rd_kafka_conf_t *conf = NULL;
        const char *security_protocol;
        rd_bool_t sasl_mechanism_oauthbearer = rd_false;
        rd_bool_t oauthbearer_method_default = rd_false;

        size_t i;
        int64_t reauth_time = INT64_MAX;
        const char *topic   = test_mk_topic_name(__FUNCTION__ + 5, 1);

        test_conf_init(&conf, NULL, 30);
        security_protocol = test_conf_get(NULL, "security.protocol");

        if (strncmp(security_protocol, "sasl", 4)) {
                rd_kafka_conf_destroy(conf);
                TEST_SKIP("Test requires SASL_PLAINTEXT or SASL_SSL, got %s\n",
                          security_protocol);
                return 0;
        }

        sasl_mechanism_oauthbearer = !rd_strcasecmp(
            test_conf_get(NULL, "sasl.mechanism"), "oauthbearer");
        oauthbearer_method_default = !rd_strcasecmp(
            test_conf_get(NULL, "sasl.oauthbearer.method"), "default");
        if (sasl_mechanism_oauthbearer && oauthbearer_method_default) {
                test_conf_set(conf, "enable.sasl.oauthbearer.unsecure.jwt",
                              "true");

                do_test_share_set_token_invalid_args();
                do_test_share_set_token(rd_false /* success */);
                do_test_share_set_token(rd_true /* failure */);
        }

        rd_kafka_t *rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        TEST_SAY("Fetching broker IDs\n");
        broker_ids = test_get_broker_ids(rk, &broker_id_cnt);

        TEST_ASSERT(broker_id_cnt != 0);

        for (i = 0; i < broker_id_cnt; i++) {
                char *property_value = test_get_broker_config_entry(
                    rk, broker_ids[i], "connections.max.reauth.ms");

                int64_t parsed_value;

                if (!property_value)
                        continue;

                parsed_value = strtoll(property_value, NULL, 0);
                if (parsed_value < reauth_time)
                        reauth_time = parsed_value;

                free(property_value);
        }

        if (broker_ids)
                free(broker_ids);
        if (rk)
                rd_kafka_destroy(rk);

        if (reauth_time ==
                INT64_MAX /* denotes property is unset on all brokers */
            ||
            reauth_time == 0 /* denotes at least one broker without timeout */
        ) {
                TEST_SKIP(
                    "Test requires all brokers to have non-zero "
                    "connections.max.reauth.ms\n");
                return 0;
        }

        /* Each test (15 of them) will take slightly more than 1 reauth_time
         * interval. Additional 30s provide a reasonable buffer. */
        test_timeout_set(17 * reauth_time / 1000 + 30);


        do_test_consumer(reauth_time, topic);
        do_test_share_consumer(reauth_time);
        do_test_producer(reauth_time, topic);
        do_test_txn_producer(reauth_time, topic, rd_false /* abort txn */);
        do_test_txn_producer(reauth_time, topic, rd_true /* abort txn */);

        /* Case when token_lifetime is shorter than the maximum reauth time
         * configured on the broker.
         * In this case, the broker returns the time to the next
         * reauthentication based on the expiry provided in the token.
         * We should recreate the token and reauthenticate before this
         * reauth time. */
        do_test_oauthbearer(reauth_time, topic, reauth_time / 2, rd_true);
        do_test_oauthbearer(reauth_time, topic, reauth_time / 2, rd_false);
        do_test_share_oauthbearer(reauth_time, reauth_time / 2, rd_true);
        do_test_share_oauthbearer(reauth_time, reauth_time / 2, rd_false);

        /* With OIDC the expiration time is fixed so do the testing only
         * once. */
        if (oauthbearer_method_default) {
                /* Case when the token_lifetime is greater than the maximum
                 * reauth time configured. In this case, the broker returns the
                 * maximum reauth time configured. We don't need to recreate the
                 * token, but we need to reauthenticate using the same token. */
                do_test_oauthbearer(reauth_time, topic, reauth_time * 2,
                                    rd_true);
                do_test_oauthbearer(reauth_time, topic, reauth_time * 2,
                                    rd_false);
                do_test_share_oauthbearer(reauth_time, reauth_time * 2,
                                          rd_true);
                do_test_share_oauthbearer(reauth_time, reauth_time * 2,
                                          rd_false);
        }

        do_test_reauth_failure(reauth_time, topic);
        do_test_share_reauth_failure(reauth_time);

        return 0;
}
