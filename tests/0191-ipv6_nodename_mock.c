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

#include "test.h"

/**
 * @name Verify that an IPv6 address advertised in a Metadata response is
 *       turned into a resolvable broker nodename.
 *
 * A broker that advertises a compressed IPv6 literal (one ending in "::")
 * used to produce a nodename such as "2001:db8:::9092" — the trailing
 * "::" of the address concatenated directly against the ":port", giving a
 * ":::" run that name resolution rejects with "Name or service not known".
 * The literal must instead be enclosed in brackets, as in an URL authority:
 * "[2001:db8::]:9092".
 *
 * Two subtests, each with its own advertised address; neither skips:
 *  - do_test_ipv6_loopback():  the IPv6 loopback "::1", which resolves on
 *    every host, so the full build -> resolve -> connect path is exercised
 *    and asserted. This owns the resolution coverage: it proves the
 *    bracketed nodename is understood by rd_addrinfo_prepare().
 *  - do_test_ipv6_compressed(): a compressed documentation literal (RFC 3849
 *    2001:db8::/32) ending in "::", mirroring the customer report. This owns
 *    the construction coverage: it proves the "::"-tail literal is bracketed
 *    rather than left as the malformed ":::" form. Resolution is not asserted
 *    here (the loopback subtest covers it) so it needs no IPv6 and never
 *    skips.
 */

/* Broker address advertised by the current subtest and the nodename it must
 * produce. Set by each subtest before creating the client; read by the log
 * interceptor. */
static const char *ipv6_host;
static const char *exp_nodename;

static rd_bool_t nodename_built;
static rd_bool_t resolve_succeeded;

/**
 * @brief Inspect broker log lines for the nodename derived from the
 *        advertised IPv6 address.
 */
static void ipv6_nodename_mock_log_cb(const rd_kafka_t *rk,
                                      int level,
                                      const char *fac,
                                      const char *buf) {
        /* Resolution succeeded once a connection to the advertised address is
         * attempted. The connection itself then fails (nothing is listening),
         * which is expected and ignored by ipv6_nodename_is_fatal_cb(). */
        if (strstr(buf, "Connecting to") && strstr(buf, ipv6_host))
                resolve_succeeded = rd_true;

        if (strstr(buf, "Nodename changed") && strstr(buf, exp_nodename))
                nodename_built = rd_true;
}

/**
 * @brief Treat the broker connection errors caused by the (deliberately
 *        unreachable) advertised address as non-fatal so the test can
 *        inspect the constructed nodename.
 */
static int ipv6_nodename_is_fatal_cb(rd_kafka_t *rk,
                                     rd_kafka_resp_err_t err,
                                     const char *reason) {
        if (err == RD_KAFKA_RESP_ERR__RESOLVE ||
            err == RD_KAFKA_RESP_ERR__TRANSPORT ||
            err == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN) {
                TEST_SAY("Ignoring expected error: %s: %s\n",
                         rd_kafka_err2name(err), reason);
                return 0;
        }
        return 1;
}

/**
 * @brief Advertise \p host as the broker address and wait for the nodename to
 *        be built.
 *
 * When \p wait_for_resolve is true, also wait until the client attempts to
 * connect to the advertised address (resolve_succeeded), so the caller can
 * assert on the resolution outcome; otherwise it returns as soon as the
 * nodename is built.
 *
 * Sets the module-level ipv6_host / exp_nodename and the outcome flags, which
 * the caller inspects.
 */
static void do_advertise_and_wait(const char *host,
                                  const char *nodename,
                                  rd_bool_t wait_for_resolve) {
        rd_kafka_mock_cluster_t *cluster;
        const char *bootstraps;
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        const rd_kafka_metadata_t *md;
        test_conf_log_interceptor_t *log_interceptor;
        const char *debug_contexts[2] = {"broker", NULL};
        int i;

        ipv6_host         = host;
        exp_nodename      = nodename;
        nodename_built    = rd_false;
        resolve_succeeded = rd_false;

        cluster = test_mock_cluster_new(1, &bootstraps);

        test_conf_init(&conf, NULL, tmout_multip(10));
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        log_interceptor = test_conf_set_log_interceptor(
            conf, ipv6_nodename_mock_log_cb, debug_contexts);

        test_curr->is_fatal_cb = ipv6_nodename_is_fatal_cb;

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        TEST_SAY("Initial metadata request (learns 127.0.0.1 nodename)\n");
        if (!rd_kafka_metadata(rk, 0, NULL, &md, tmout_multip(5000)))
                rd_kafka_metadata_destroy(md);

        TEST_SAY("Advertising IPv6 broker host %s\n", ipv6_host);
        rd_kafka_mock_broker_set_host_port(cluster, 1, ipv6_host, 9092);

        TEST_SAY("Metadata request that learns the IPv6 nodename\n");
        /* The nodename change is applied when this Metadata response arrives
         * over the still-open bootstrap connection; the request itself may
         * then time out as the broker becomes unreachable, so don't wait long
         * for it. */
        if (!rd_kafka_metadata(rk, 0, NULL, &md, tmout_multip(1000)))
                rd_kafka_metadata_destroy(md);

        /* Wait for the resolution *outcome*, not merely for the nodename to be
         * built: "Nodename changed" is logged before the address is resolved,
         * so stopping at the nodename would miss a bracketed form that no
         * longer parses back. When only construction is under test, stop as
         * soon as the nodename is built. */
        for (i = 0; i < 50 && !(nodename_built &&
                                (!wait_for_resolve || resolve_succeeded));
             i++)
                rd_kafka_poll(rk, 100);

        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(cluster);
        rd_free(log_interceptor);
}

/**
 * @brief The IPv6 loopback "::1" resolves on every host, so the full
 *        build -> resolve -> connect path is verified with no skip.
 */
static void do_test_ipv6_loopback(void) {
        SUB_TEST();

        do_advertise_and_wait("::1", "[::1]:9092",
                              rd_true /*wait_for_resolve*/);

        TEST_ASSERT(nodename_built,
                    "expected the advertised IPv6 literal to produce the "
                    "nodename \"%s\"",
                    exp_nodename);
        TEST_ASSERT(resolve_succeeded,
                    "nodename \"%s\" was not resolved: the bracketed literal "
                    "was not split back into address and port",
                    exp_nodename);

        TEST_SAY("IPv6 nodename \"%s\" built and resolved\n", exp_nodename);

        SUB_TEST_PASS();
}

/**
 * @brief A compressed documentation literal ending in "::" mirrors the
 *        customer report. Verifies only that it is bracketed, not that it
 *        resolves (the loopback subtest owns that), so it needs no IPv6 and
 *        never skips.
 */
static void do_test_ipv6_compressed(void) {
        SUB_TEST();

        do_advertise_and_wait("2001:db8::", "[2001:db8::]:9092",
                              rd_false /*wait_for_resolve*/);

        /* nodename_built matches only the bracketed exp_nodename, so a
         * regression to the malformed ":::" form (the bug) fails this
         * assertion rather than slipping through. */
        TEST_ASSERT(nodename_built,
                    "expected the advertised \"::\"-tail literal to produce "
                    "the bracketed nodename \"%s\"",
                    exp_nodename);

        TEST_SAY("IPv6 nodename \"%s\" built\n", exp_nodename);

        SUB_TEST_PASS();
}

int main_0191_ipv6_nodename_mock(int argc, char **argv) {
        if (test_needs_auth()) {
                TEST_SKIP("Mock cluster does not support SSL/SASL\n");
                return 0;
        }

        do_test_ipv6_loopback();
        do_test_ipv6_compressed();

        return 0;
}
