/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2017 Magnus Edenhill
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

#include "rd.h"
#include "rdkafka_int.h"
#include "rdkafka_request.h"

struct _rk_admin_state {
        rd_kafka_resp_err_t err;
        rd_kafka_topic_partition_list_t *topics;
};

static void rd_kafka_admin_delete_topics_resp_cb (rd_kafka_t *rk,
                                                  rd_kafka_broker_t *rkb,
                                                  rd_kafka_resp_err_t err,
                                                  rd_kafka_buf_t *rkbuf,
                                                  rd_kafka_buf_t *request,
                                                  void *opaque) {
        struct _rk_admin_state *state = opaque;

        state->err = rd_kafka_handle_DeleteTopics(rk, rkb, err, rkbuf, request,
                                                  state->topics);
}


rd_kafka_resp_err_t
rd_kafka_admin_delete_topics (rd_kafka_t *rk,
                              rd_kafka_topic_partition_list_t *topics,
                              int timeout_ms) {
        rd_kafka_broker_t *rkb;
        rd_kafka_q_t *rkq;
        int actual_timeout_ms = timeout_ms < 0 ? -timeout_ms : timeout_ms;
        rd_ts_t abs_timeout = rd_timeout_init(actual_timeout_ms);
        struct _rk_admin_state state;

        rkb = rd_kafka_broker_controller(rk, actual_timeout_ms, 1/*usable*/,
                                         1);
        if (!rkb)
                return RD_KAFKA_RESP_ERR__TRANSPORT;

        rkq = rd_kafka_q_new(rk);

        state.topics = topics;
        state.err = RD_KAFKA_RESP_ERR__IN_PROGRESS;

        rd_kafka_DeleteTopics(rkb, topics,
                              timeout_ms < 0 ? -1 :
                              rd_timeout_remains(abs_timeout),
                              RD_KAFKA_REPLYQ(rkq, 0),
                              rd_kafka_admin_delete_topics_resp_cb, &state, 0);

        rd_kafka_broker_destroy(rkb);

        while (state.err == RD_KAFKA_RESP_ERR__IN_PROGRESS)
                rd_kafka_q_serve(rkq, 100, 0, RD_KAFKA_Q_CB_CALLBACK,
                                 rd_kafka_poll_cb, NULL);
        rd_kafka_q_destroy(rkq);

        return state.err;
}
