/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2026, librdkafka contributors.
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
 * Fuzzer for the Kafka Fetch-response MessageSet / record-batch decoder
 * (src/rdkafka_msgset_reader.c). The fuzz bytes are treated as the MessageSet
 * payload of a broker Fetch response and decoded via rd_kafka_msgset_parse().
 * A consumer instance, topic and toppar are created once so the decoder runs
 * against valid state and crashes are genuine parser bugs.
 */

#include "rdkafka_int.h"
#include "rdkafka_buf.h"
#include "rdkafka_msgset.h"
#include "rdkafka_partition.h"
#include "rdkafka_broker.h"
#include "rdkafka_proto.h"

#include <stdint.h>
#include <stddef.h>
#include <string.h>

static rd_kafka_t *rk;
static rd_kafka_topic_t *rkt;
static rd_kafka_toppar_t *rktp;
static rd_kafka_broker_t *rkb;

int LLVMFuzzerInitialize(int *argc, char ***argv) {
        char errstr[256];
        rd_kafka_conf_t *conf = rd_kafka_conf_new();

        rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        if (!rk)
                return 0;

        rkb = rd_kafka_broker_internal(rk);
        rkt = rd_kafka_topic_new(rk, "fuzz", NULL);
        if (rkt)
                rktp = rd_kafka_toppar_new(rkt, 0);

        return 0;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
        rd_kafka_buf_t *rkbuf;
        rd_kafka_buf_t *request;
        struct rd_kafka_toppar_ver tver;

        if (!rktp || !rkb || size == 0 || size > (1u << 20))
                return 0;

        /* Shadow buffer is created read-ready (rd_slice_init_full). */
        rkbuf = rd_kafka_buf_new_shadow(data, size, NULL);
        if (!rkbuf)
                return 0;
        rkbuf->rkbuf_rkb = rkb;

        /* The decoder selects its wire format from the (request) ApiVersion. */
        request                          = rd_kafka_buf_new(1, 0);
        request->rkbuf_reqhdr.ApiKey     = RD_KAFKAP_Fetch;
        request->rkbuf_reqhdr.ApiVersion = 11;

        memset(&tver, 0, sizeof(tver));
        tver.rktp    = rktp;
        tver.version = 0;

        (void)rd_kafka_msgset_parse(rkbuf, request, rktp, NULL, &tver);

        /* Drop any messages the decoder enqueued so they don't accumulate. */
        rd_kafka_q_purge(rktp->rktp_fetchq);

        /* Don't let buf destroy decref our shared internal broker. */
        rkbuf->rkbuf_rkb = NULL;
        rd_kafka_buf_destroy(rkbuf);
        rd_kafka_buf_destroy(request);

        return 0;
}

#if WITH_MAIN
#include "helpers.h"

int main(int argc, char **argv) {
        int i;

        LLVMFuzzerInitialize(&argc, &argv);

        for (i = 1; i < argc; i++) {
                size_t size;
                uint8_t *buf = read_file(argv[i], &size);
                LLVMFuzzerTestOneInput(buf, size);
                free(buf);
        }

        return 0;
}
#endif
