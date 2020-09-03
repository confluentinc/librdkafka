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


#include "rdkafka_int.h"
#include "rdkafka_request.h"
#include "rdkafka_leader.h"


/**
 * @name Asynchronous partition leader requests.
 * @brief Provides an asynchronous abstraction to look up the partition
 *        leader broker, make sure there's a connection, and then calls the
 *        send_req_cb_t callback to construct the request.
 *        Responses are enqueued with resp_cb callbacks on the provided
 *        reply queue.
 * @{
 *
 */



static void rd_kafka_leader_req_fsm (rd_kafka_t *rk,
                                     rd_kafka_leader_req_t *lreq);




/**
 * @brief Look up leader for \p topic \p partition, either from rktp cache
 *        or by Metadata request, make sure there is a connection to the leader
 *        and then call \p send_req_cb, passing the leader broker instance
 *        and \p rko to send the request.
 *        These steps may be performed by this function, or asynchronously
 *        at a later time.
 *
 * Response, or error, is sent on \p replyq with callback \p rkbuf_cb.
 *
 * @locality rdkafka main thread
 * @locks none
 */
void rd_kafka_leader_req (rd_kafka_t *rk,
                          const char *topic, int32_t partition,
                          rd_kafka_send_req_cb_t *send_req_cb,
                          rd_kafka_op_t *rko,
                          int timeout_ms,
                          rd_kafka_replyq_t replyq,
                          rd_kafka_resp_cb_t *resp_cb,
                          void *reply_opaque) {
        rd_kafka_leader_req_t *lreq;

        rd_assert(partition != RD_KAFKA_PARTITION_UA);

        lreq = rd_calloc(1, sizeof(*lreq));
        lreq->lreq_topic = rd_strdup(topic);
        lreq->lreq_partition = partition;
        lreq->lreq_ts_timeout = rd_timeout_init(timeout_ms);
        lreq->lreq_send_req_cb = send_req_cb;
        lreq->lreq_rko = rko;
        lreq->lreq_replyq = replyq;
        lreq->lreq_resp_cb = resp_cb;
        lreq->lreq_reply_opaque = reply_opaque;
        lreq->lreq_refcnt = 1;

        TAILQ_INSERT_TAIL(&rk->rk_leader_reqs, lreq, lreq_link);

        rd_kafka_leader_req_fsm(rk, lreq);
}


/**
 * @brief Decrease refcount of lreq and free it if no more references.
 *
 * @returns true if lreq was destroyed, else false.
 */
static rd_bool_t
rd_kafka_leader_req_destroy (rd_kafka_t *rk, rd_kafka_leader_req_t *lreq) {
        rd_assert(lreq->lreq_refcnt > 0);
        if (--lreq->lreq_refcnt > 0)
                return rd_false;

        rd_kafka_replyq_destroy(&lreq->lreq_replyq);
        TAILQ_REMOVE(&rk->rk_leader_reqs, lreq, lreq_link);
        rd_free(lreq->lreq_topic);
        rd_free(lreq);

        return rd_true;
}

static void rd_kafka_leader_req_keep (rd_kafka_leader_req_t *lreq) {
        lreq->lreq_refcnt++;
}

static void rd_kafka_leader_req_fail (rd_kafka_t *rk,
                                      rd_kafka_leader_req_t *lreq,
                                      rd_kafka_resp_err_t err) {
        rd_kafka_op_t *reply;
        rd_kafka_buf_t *rkbuf;

        reply = rd_kafka_op_new(RD_KAFKA_OP_RECV_BUF);
        reply->rko_rk = rk;  /* Set rk since the rkbuf will not have a rkb
                              * to reach it. */
        reply->rko_err = err;

        /* Need a dummy rkbuf to pass state to the buf resp_cb */
        rkbuf = rd_kafka_buf_new(0, 0);
        rkbuf->rkbuf_cb = lreq->lreq_resp_cb;
        rkbuf->rkbuf_opaque = lreq->lreq_reply_opaque;
        reply->rko_u.xbuf.rkbuf = rkbuf;

        rd_kafka_replyq_enq(&lreq->lreq_replyq, reply, 0);

        rd_kafka_leader_req_destroy(rk, lreq);
}


/** FIXME: on metadata-update trigger leader_req_fsm() */




/**
 * @brief State machine for async leader requests.
 *
 * @remark May destroy the \p lreq.
 *
 * @locality any
 * @locks none
 */
static void
rd_kafka_leader_req_fsm (rd_kafka_t *rk, rd_kafka_leader_req_t *lreq) {
        rd_kafka_broker_t *rkb = NULL;
        rd_kafka_resp_err_t err;
        int32_t leader_id;

        if (unlikely(rd_kafka_terminating(rk))) {
                rd_kafka_leader_req_fail(rk, lreq, RD_KAFKA_RESP_ERR__DESTROY);
                return;
        }

        /* Check metadata cache first */
        rd_kafka_rdlock(rk);
        err = rd_kafka_metadata_cache_topic_partition_leader_get(
                rk, lreq->lreq_topic, lreq->lreq_partition, &leader_id);
        if (!err)
                rkb = rd_kafka_broker_find_by_nodeid(rk, leader_id);
        rd_kafka_rdunlock(rk);

        if (err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC) {
                /* No cache entry for this topic, request update. */
                rd_kafka_metadata_refresh_topic(rk, NULL, topic,
                                                rd_false /*dont force*/,
                                                rd_false /*no cgrp update */,
                                                "leader request lookup");
                /* Ignore any errors, this fsm will be retriggered on
                 * broker state change or metadata update, or eventually
                 * time out. */
                return;

        } else if (err == RD_KAFKA_RESP_ERR__WAIT_CACHE) {
                /* A metadata request is in progress, wait for metadata cache
                 * update to trigger this fsm again. */
                rd_kafka_leader_req_fail(rk, lreq, err);
                return;

        } else if (err) {
                /* Treat all other errors as permanent. */

        } else if (!rkb) {
                /* Leader is known but we don't have a broker object yet,
                 * this shouldn't really happen but we rely on a future
                 * metadata update to fix it. */
                return;

        } else if (!rd_kafka_broker_is_up(rkb)) {
                /* No connection yet. We'll be re-triggered on
                 * broker state broadcast. */
                rd_kafka_broker_schedule_connection(rkb);
                rd_kafka_broker_destroy(rkb);
                return;
        }

        /* FIXME: Send req */
        rd_kafka_broker_destroy(rkb);
}



/**
 * @brief Callback called from rdkafka main thread on each
 *        broker state change from or to UP.
 *
 * @locality rdkafka main thread
 * @locks none
 */
void rd_kafka_leader_rkb_monitor_cb (rd_kafka_broker_t *rkb) {
        rd_kafka_t *rk = rkb->rkb_rk;
        rd_kafka_leader_req_t *lreq, *tmp;

        /* Run through all coord_req fsms */

        TAILQ_FOREACH_SAFE(lreq, &rk->rk_leader_reqs, lreq_link, tmp)
                rd_kafka_leader_req_fsm(rk, lreq);
}



/**
 * @brief Instance is terminating: destroy all coord reqs
 */
void rd_kafka_leader_reqs_term (rd_kafka_t *rk) {
        rd_kafka_leader_req_t *lreq;

        while ((lreq = TAILQ_FIRST(&rk->rk_leader_reqs)))
                rd_kafka_leader_req_destroy(rk, lreq);
}


/**
 * @brief Initialize coord reqs list.
 */
void rd_kafka_leader_reqs_init (rd_kafka_t *rk) {
        TAILQ_INIT(&rk->rk_leader_reqs);
}

/**@}*/


void dosomething (rd_kafka_topic_partition_list_t *partitions) {

        rd_kafka_leader_req(partitions,
                            my_send_cb,
                            my_queue,
                            
}
