/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2019 Magnus Edenhill
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

#ifndef _RDKAFKA_LEADER_H_
#define _RDKAFKA_LEADER_H_


/**
 * @name Partition leader requests
 */

/**
 * @brief Request to be sent to leader.
 *        Includes looking up, caching, and connecting to, the leader.
 */
typedef struct rd_kafka_leader_req_s {
        TAILQ_ENTRY(rd_kafka_leader_req_s) lreq_link; /**< rk_leader_reqs */

        char          *lreq_topic;           /**< Topic name */
        int32_t        lreq_partition;       /**< Partition */

        rd_kafka_op_t *lreq_rko;             /**< Requester's rko that is
                                              *   provided as opaque on
                                              *   to lreq_resp_cb. */
        rd_ts_t        lreq_ts_timeout;      /**< Absolute timeout.
                                              *   Will fail with an error
                                              *   code pertaining to the
                                              *   current state */

        rd_kafka_send_req_cb_t *lreq_send_req_cb; /**< Sender callback */

        rd_kafka_replyq_t   lreq_replyq;     /**< Reply queue */
        rd_kafka_resp_cb_t *lreq_resp_cb;    /**< Reply queue response
                                              *   parsing callback for the
                                              *   request sent by
                                              *   send_req_cb */
        void               *lreq_reply_opaque; /**< Opaque passed to
                                                *   resp_cb */

        int                 lreq_refcnt;     /**< Refcount allows
                                              *   destroying the lreq even
                                              *   with outstanding
                                              *   Metadata requests. */
} rd_kafka_leader_req_t;


void rd_kafka_leader_req (rd_kafka_t *rk,
                          const char *topic, int32_t partition,
                         rd_kafka_send_req_cb_t *send_req_cb,
                         rd_kafka_op_t *rko,
                         int timeout_ms,
                         rd_kafka_replyq_t replyq,
                         rd_kafka_resp_cb_t *resp_cb,
                         void *reply_opaque);

void rd_kafka_leader_rkb_monitor_cb (rd_kafka_broker_t *rkb);

void rd_kafka_leader_reqs_term (rd_kafka_t *rk);
void rd_kafka_leader_reqs_init (rd_kafka_t *rk);
#endif /* _RDKAFKA_LEADER_H_ */
