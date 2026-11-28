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
 * @name A request to the consumer group that is still queued when the group
 *       terminates must be answered, not dropped.
 *
 * The consumer group's ops queue is forwarded to the main ops queue, so a
 * request enqueued just before the group terminates is served after it has
 * terminated. Such requests used to be destroyed without a reply, so a caller
 * waiting for the reply without a timeout blocked forever.
 * With the cooperative assignor this could happen during consumer close to
 * rd_kafka_incremental_unassign() called from the rebalance callback for the
 * revoke event, when the call for an earlier assign event had already let the
 * group terminate.
 *
 * Here several threads keep calling rd_kafka_assignment(), which waits for the
 * group's reply without a timeout, while the consumer is closed. Requests are
 * then queued at the moment the group terminates, and every call must return.
 */

#define THREAD_CNT 8
#define ITERATIONS 10

typedef struct requester_s {
        rd_kafka_t *rk;
        rd_atomic32_t *stop;
        int calls;
        int errors;
} requester_t;

static int requester_thread(void *arg) {
        requester_t *requester = arg;

        while (!rd_atomic32_get(requester->stop)) {
                rd_kafka_topic_partition_list_t *partitions = NULL;
                rd_kafka_resp_err_t err;

                err = rd_kafka_assignment(requester->rk, &partitions);
                if (partitions)
                        rd_kafka_topic_partition_list_destroy(partitions);

                requester->calls++;
                /* Fails with __DESTROY once the group has terminated */
                if (err)
                        requester->errors++;
        }

        return 0;
}

static void do_test_close_with_queued_requests(int iteration) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_atomic32_t stop;
        thrd_t threads[THREAD_CNT];
        requester_t requesters[THREAD_CNT];
        int calls = 0, errors = 0;
        int i;

        SUB_TEST_QUICK("iteration %d", iteration);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "group.id", "0193-cgrp_terminate_queued_op");
        rk = test_create_handle(RD_KAFKA_CONSUMER, conf);

        rd_atomic32_init(&stop, 0);
        for (i = 0; i < THREAD_CNT; i++) {
                requesters[i].rk     = rk;
                requesters[i].stop   = &stop;
                requesters[i].calls  = 0;
                requesters[i].errors = 0;
                if (thrd_create(&threads[i], requester_thread,
                                &requesters[i]) != thrd_success)
                        TEST_FAIL("Failed to create thread %d", i);
        }

        /* Let the threads keep the group busy with requests */
        rd_usleep(100 * 1000, NULL);

        TEST_SAY(
            "Closing consumer while %d threads call "
            "rd_kafka_assignment()\n",
            THREAD_CNT);
        rd_kafka_consumer_close(rk);

        /* Every requester must return: a request that was dropped by the
         * terminated group leaves its thread blocked forever. */
        rd_atomic32_set(&stop, 1);
        for (i = 0; i < THREAD_CNT; i++) {
                thrd_join(threads[i], NULL);
                calls += requesters[i].calls;
                errors += requesters[i].errors;
        }

        TEST_SAY("%d rd_kafka_assignment() calls returned, %d with an error\n",
                 calls, errors);

        rd_kafka_destroy(rk);

        SUB_TEST_PASS();
}

int main_0193_cgrp_terminate_queued_op(int argc, char **argv) {
        int i;

        for (i = 0; i < ITERATIONS; i++)
                do_test_close_with_queued_requests(i);

        return 0;
}
