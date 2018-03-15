/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2018 Magnus Edenhill
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
#include "rdkafka_admin.h"
#include "rdkafka_request.h"
#include "rdkafka_aux.h"

#include <stdarg.h>



/** @brief Descriptive strings for rko_u.admin_request.state */
static const char *rd_kafka_admin_state_desc[] = {
        "initializing",
        "waiting for controller",
        "waiting for response from broker"
};



/**
 * @brief Admin API implementation.
 *
 * The public Admin API in librdkafka exposes a completely asynchronous
 * interface where the initial request API (e.g., ..CreateTopics())
 * is non-blocking and returns immediately, and the application polls
 * a ..queue_t for the result.
 *
 * Let's illustrate this with an example:
 *  TBD
 *
 *
 * Due to Admin requests async nature (from the callers perspective) the
 * Admin requests are served in rdkafka's main background thread.
 * An rko is set up in the public request API method (application thread)
 * with a callback and then enqueued on the main rdkafka ops queue,
 * which will trigger the callback to be called from rdkafka's main
 * background thread.
 * These handling callbacks are typically designed to be called
 * multiple times until a required state is fullfilled, such as
 * waiting for the controller broker to come up.
 * When the callback is finally done it posts a reply op on
 * the user's provided rkqu queue which is polled by the user
 * to retrieve the results.
 */



/**
 * @brief Create a new admin_result op based on the request op \p rko_req
 */
static rd_kafka_op_t *rd_kafka_admin_result_new (const rd_kafka_op_t *rko_req) {
        rd_kafka_op_t *rko_result;

        rko_result = rd_kafka_op_new(RD_KAFKA_OP_ADMIN_RESULT);
        rko_result->rko_rk = rko_req->rko_rk;

        rko_result->rko_u.admin_result.opaque =
                rd_kafka_confval_get_ptr(&rko_req->rko_u.admin_request.
                                         options.opaque);
        rko_result->rko_u.admin_result.reqtype = rko_req->rko_type;
        rko_result->rko_evtype = rko_req->rko_u.admin_request.reply_event_type;

        return rko_result;
}


/**
 * @brief Set error code and error string on admin_result op \p rko.
 */
static void rd_kafka_admin_result_set_err0 (rd_kafka_op_t *rko,
                                            rd_kafka_resp_err_t err,
                                            const char *fmt, va_list ap) {
        char buf[512];

        rd_vsnprintf(buf, sizeof(buf), fmt, ap);

        rko->rko_err = err;

        if (rko->rko_u.admin_result.errstr)
                rd_free(rko->rko_u.admin_result.errstr);
        rko->rko_u.admin_result.errstr = rd_strdup(buf);

        rd_kafka_dbg(rko->rko_rk, ADMIN, "ADMINFAIL",
                     "Admin %s result error: %s",
                     rd_kafka_op2str(rko->rko_u.admin_result.reqtype),
                     rko->rko_u.admin_result.errstr);
}

/**
 * @sa rd_kafka_admin_result_set_err0
 */
static RD_UNUSED void rd_kafka_admin_result_set_err (rd_kafka_op_t *rko,
                                                     rd_kafka_resp_err_t err,
                                                     const char *fmt, ...) {
        va_list ap;

        va_start(ap, fmt);
        rd_kafka_admin_result_set_err0(rko, err, fmt, ap);
        va_end(ap);
}

/**
 * @brief Enqueue admin_result on application's queue.
 */
static RD_INLINE
void rd_kafka_admin_result_enq (rd_kafka_op_t *rko_req,
                                       rd_kafka_op_t *rko_result) {
        rd_kafka_replyq_enq(&rko_req->rko_u.admin_request.replyq, rko_result,
                            rko_req->rko_u.admin_request.replyq.version);
}

/**
 * @brief Set request-level error code and string in reply op.
 */
static void rd_kafka_admin_result_fail (rd_kafka_op_t *rko_req,
                                        rd_kafka_resp_err_t err,
                                        const char *fmt, ...) {
        va_list ap;
        rd_kafka_op_t *rko_result;

        rko_result = rd_kafka_admin_result_new(rko_req);

        va_start(ap, fmt);
        rd_kafka_admin_result_set_err0(rko_result, err, fmt, ap);
        va_end(ap);

        rd_kafka_admin_result_enq(rko_req, rko_result);
}




rd_kafka_resp_err_t
rd_kafka_AdminOptions_set_request_timeout (rd_kafka_AdminOptions_t *options,
                                           int timeout_ms,
                                           char *errstr, size_t errstr_size) {
        return rd_kafka_confval_set_type(&options->request_timeout,
                                         RD_KAFKA_CONFVAL_INT, &timeout_ms,
                                         errstr, errstr_size);
}


rd_kafka_resp_err_t
rd_kafka_AdminOptions_set_operation_timeout (rd_kafka_AdminOptions_t *options,
                                             int timeout_ms,
                                             char *errstr, size_t errstr_size) {
        return rd_kafka_confval_set_type(&options->operation_timeout,
                                         RD_KAFKA_CONFVAL_INT, &timeout_ms,
                                         errstr, errstr_size);
}


rd_kafka_resp_err_t
rd_kafka_AdminOptions_set_validate_only (rd_kafka_AdminOptions_t *options,
                                        int true_or_false,
                                        char *errstr, size_t errstr_size) {
        return rd_kafka_confval_set_type(&options->validate_only,
                                         RD_KAFKA_CONFVAL_INT, &true_or_false,
                                         errstr, errstr_size);
}

void
rd_kafka_AdminOptions_set_opaque (rd_kafka_AdminOptions_t *options,
                                  void *opaque) {
        rd_kafka_confval_set_type(&options->opaque,
                                  RD_KAFKA_CONFVAL_PTR, opaque, NULL, 0);
}


/**
 * @brief Initialize and set up defaults for AdminOptions
 */
static void rd_kafka_AdminOptions_init (rd_kafka_t *rk,
                                        rd_kafka_AdminOptions_t *options) {
        rd_kafka_confval_init_int(&options->request_timeout, "request_timeout",
                                  0, 3600*1000,
                                  rk->rk_conf.admin.request_timeout_ms);
        rd_kafka_confval_init_int(&options->operation_timeout,
                                  "operation_timeout",
                                  -1, 3600*1000, 0);
        rd_kafka_confval_init_int(&options->validate_only, "validate_only",
                                  0, 1, 0);
        rd_kafka_confval_init_ptr(&options->opaque, "opaque");
}


rd_kafka_AdminOptions_t *rd_kafka_AdminOptions_new (rd_kafka_t *rk) {
        rd_kafka_AdminOptions_t *options;

        options = rd_calloc(1, sizeof(*options));

        rd_kafka_AdminOptions_init(rk, options);

        return options;
}

void rd_kafka_AdminOptions_destroy (rd_kafka_AdminOptions_t *options) {
        rd_free(options);
}





rd_kafka_NewTopic_t *
rd_kafka_NewTopic_new (const char *topic,
                       int num_partitions,
                       int replication_factor) {
        rd_kafka_NewTopic_t *new_topic;

        if (!topic ||
            num_partitions < 0 || num_partitions > RD_KAFKAP_PARTITIONS_MAX ||
            replication_factor < -1 ||
            replication_factor > RD_KAFKAP_BROKERS_MAX)
                return NULL;

        new_topic = rd_calloc(1, sizeof(*new_topic));
        new_topic->topic = rd_strdup(topic);
        new_topic->num_partitions = num_partitions;
        new_topic->replication_factor = replication_factor;

        rd_list_init(&new_topic->replicas, 0, rd_list_destroy_free);

        rd_list_init(&new_topic->config, 0, rd_strtup_free);

        return new_topic;

}


/**
 * @brief Topic name comparator for NewTopic_t
 */
static int rd_kafka_NewTopic_cmp (const void *_a, const void *_b) {
        const rd_kafka_NewTopic_t *a = _a, *b = _b;
        return strcmp(a->topic, b->topic);
}


/**
 * @brief rd_list_copy() callback for NewTopic.replicas elements.
 */
static void *rd_kafka_NewTopic_replica_copy (const void *elem, void *opaque) {
        const rd_list_t *src = elem;
        rd_list_t *dst = rd_list_new(0, src->rl_free_cb);
        int i;

        rd_list_prealloc_elems(dst, sizeof(int32_t), rd_list_cnt(src),
                               rd_false);

        for (i = 0 ; i < rd_list_cnt(src) ; i++)
                *(int32_t *)rd_list_add(dst, NULL) =
                        *(int32_t *)rd_list_elem(src, i);
        return dst;
}


/**
 * @brief Allocate a new NewTopic and make a copy of \p src
 */
static rd_kafka_NewTopic_t *
rd_kafka_NewTopic_copy (const rd_kafka_NewTopic_t *src) {
        rd_kafka_NewTopic_t *dst;

        dst = rd_kafka_NewTopic_new(src->topic, src->num_partitions,
                                    src->replication_factor);

        rd_list_init_copy(&dst->replicas, &src->replicas);
        rd_list_copy_to(&dst->replicas, &src->replicas,
                        rd_kafka_NewTopic_replica_copy, NULL);

        rd_list_init_copy(&dst->config, &src->config);
        rd_list_copy_to(&dst->config, &src->config, rd_strtup_list_copy, NULL);

        return dst;
}

void rd_kafka_NewTopic_destroy (rd_kafka_NewTopic_t *new_topic) {
        rd_list_destroy(&new_topic->replicas);
        rd_list_destroy(&new_topic->config);
        rd_free(new_topic->topic);
        rd_free(new_topic);
}

static void rd_kafka_NewTopic_free (void *ptr) {
        rd_kafka_NewTopic_destroy(ptr);
}

void
rd_kafka_NewTopic_destroy_array (rd_kafka_NewTopic_t **new_topics,
                                 size_t new_topic_cnt) {
        size_t i;
        for (i = 0 ; i < new_topic_cnt ; i++)
                rd_kafka_NewTopic_destroy(new_topics[i]);
}


rd_kafka_resp_err_t
rd_kafka_NewTopic_set_replica_assignment (rd_kafka_NewTopic_t *new_topic,
                                          int32_t partition,
                                          int32_t *broker_ids,
                                          size_t broker_id_cnt) {
        rd_list_t *rl;
        int i;

        if (new_topic->replication_factor != -1 ||
            partition < 0 || partition >= new_topic->num_partitions ||
            broker_id_cnt > RD_KAFKAP_BROKERS_MAX)
                return RD_KAFKA_RESP_ERR__INVALID_ARG;

        if (rd_list_cnt(&new_topic->replicas) == 0) {
                /* First time use, initialize */
                rd_list_prealloc_elems(&new_topic->replicas, sizeof(rd_list_t),
                                       new_topic->num_partitions, 1/*zeroed*/);
                rd_list_set_cnt(&new_topic->replicas,
                                new_topic->num_partitions);
        }

        rl = rd_list_elem(&new_topic->replicas, (int)partition);
        /* Destroy previous replica set for this partition, if any */
        rd_list_destroy(rl);

        rd_list_init(rl, 0, NULL);
        rd_list_prealloc_elems(rl, sizeof(int32_t), broker_id_cnt, 0);

        for (i = 0 ; i < (int)broker_id_cnt ; i++)
                *(int32_t *)rd_list_add(rl, NULL) = broker_ids[i];

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}


rd_kafka_resp_err_t
rd_kafka_NewTopic_add_config (rd_kafka_NewTopic_t *new_topic,
                              const char *name, const char *value) {
        rd_strtup_t *tup;

        if (!name)
                return RD_KAFKA_RESP_ERR__INVALID_ARG;

        tup = rd_strtup_new(name, value);

        rd_list_add(&new_topic->config, tup);

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}





/**
 * @brief Create a new admin_request op of type \p optype and sets up the
 *        generic (type independent files).
 *
 *        The caller shall then populate the admin_request.args list
 *        and enqueue the op on rk_ops for further processing work.
 *
 * @param worker_cb Queue-triggered worker callback for API request.
 *                  The callback's passed rko->rko_err may be one of:
 *                  RD_KAFKA_RESP_ERR_NO_ERROR, or
 *                  RD_KAFKA_RESP_ERR__DESTROY for queue destruction cleanup, or
 *                  RD_KAFKA_RESP_ERR__TIMED_OUT if request has timed out.
 *
 * @param options Optional options, may be NULL to use defaults.
 *
 * @locks none
 * @locality application thread
 */
static rd_kafka_op_t *
rd_kafka_admin_request_op_new (rd_kafka_t *rk,
                               rd_kafka_op_type_t optype,
                               rd_kafka_event_type_t reply_event_type,
                               rd_kafka_op_cb_t *worker_cb,
                               const rd_kafka_AdminOptions_t *options,
                               rd_kafka_queue_t *rkqu) {
        rd_kafka_op_t *rko;

        rd_assert(rk);
        rd_assert(rkqu);
        rd_assert(worker_cb);

        rko = rd_kafka_op_new_cb(rk, optype, worker_cb);

        rko->rko_u.admin_request.reply_event_type = reply_event_type;

        /* Make a copy of the options */
        if (options)
                rko->rko_u.admin_request.options = *options;
        else
                rd_kafka_AdminOptions_init(rk,
                                           &rko->rko_u.admin_request.options);

        /* Calculate absolute timeout */
        rko->rko_u.admin_request.abs_timeout =
                rd_timeout_init(
                        rd_kafka_confval_get_int(&rko->rko_u.admin_request.
                                                 options.request_timeout));

        /* Setup enq-op-once, which is triggered by either timer code
         * or future wait-controller code. */
        rko->rko_u.admin_request.eonce =
                rd_kafka_enq_once_new(rko, RD_KAFKA_REPLYQ(rk->rk_ops, 0));

        /* The timer itself must be started from the rdkafka main thread,
         * not here. */

        /* Set up replyq */
        rd_kafka_set_replyq(&rko->rko_u.admin_request.replyq,
                            rkqu->rkqu_q, 0);

        rko->rko_u.admin_request.state = RD_KAFKA_ADMIN_STATE_INIT;
        return rko;
}


/**
 * @brief Timer timeout callback for the admin rko's eonce object.
 */
static void rd_kafka_admin_eonce_timeout_cb (rd_kafka_timers_t *rkts,
                                             void *arg) {
        rd_kafka_enq_once_t *eonce = arg;

        rd_kafka_enq_once_trigger(eonce, RD_KAFKA_RESP_ERR__TIMED_OUT,
                                  "timer timeout");
}


/**
 * @brief Handle CreateTopics response from broker
 *
 * @param opaque is the eonce from the CreateTopicsRequest call.
 */
static void
rd_kafka_admin_CreateTopics_handle_response (rd_kafka_t *rk,
                                             rd_kafka_broker_t *rkb,
                                             rd_kafka_resp_err_t err,
                                             rd_kafka_buf_t *reply,
                                             rd_kafka_buf_t *request,
                                             void *opaque) {
        rd_kafka_enq_once_t *eonce = opaque;
        rd_kafka_op_t *rko;

        /* From "send CreateTopics" */
        rko = rd_kafka_enq_once_disable(eonce);

        if (!rko) {
                /* The operation timed out and the worker was
                 * dismantled while we were waiting for broker response,
                 * do nothing - everything has been cleaned up. */
                rd_kafka_dbg(rk, ADMIN, "ADMIN",
                             "Dropping outdated %sResponse with return code %s",
                             request ?
                             rd_kafka_ApiKey2str(request->rkbuf_reqhdr.ApiKey):
                             "???",
                             rd_kafka_err2str(err));
                return;
        }

        /* Attach reply buffer to rko for parsing in the worker. */
        rd_assert(!rko->rko_u.admin_request.reply_buf);
        rko->rko_u.admin_request.reply_buf = reply;
        rko->rko_err = err;

        if (rko->rko_op_cb(rk, NULL, rko) == RD_KAFKA_OP_RES_HANDLED)
                rd_kafka_op_destroy(rko);

}


/**
 * @brief Parse CreateTopicsResponse and create ADMIN_RESULT op.
 */
static rd_kafka_resp_err_t
rd_kafka_admin_CreateTopics_parse_response (rd_kafka_op_t *rko_req,
                                            rd_kafka_op_t **rko_resultp,
                                            rd_kafka_buf_t *reply,
                                            char *errstr, size_t errstr_size) {
        const int log_decode_errors = LOG_ERR;
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
        rd_kafka_broker_t *rkb = reply->rkbuf_rkb;
        rd_kafka_t *rk = rkb->rkb_rk;
        rd_kafka_op_t *rko_result = NULL;
        int32_t topic_cnt;
        int i;

        if (rd_kafka_buf_ApiVersion(reply) >= 2) {
                int32_t Throttle_Time;
                rd_kafka_buf_read_i32(reply, &Throttle_Time);
                rd_kafka_op_throttle_time(rkb, rk->rk_rep, Throttle_Time);
        }

        rd_kafka_buf_read_i32(reply, &topic_cnt);

        if (topic_cnt > rd_list_cnt(&rko_req->rko_u.admin_request.args)) {
                rd_snprintf(errstr, errstr_size,
                            "Received %"PRId32" topics in response "
                            "when only %d were requested", topic_cnt,
                            rd_list_cnt(&rko_req->rko_u.admin_request.args));
                return RD_KAFKA_RESP_ERR__BAD_MSG;
        }

        rko_result = rd_kafka_admin_result_new(rko_req);

        rd_list_init(&rko_result->rko_u.admin_result.topics, topic_cnt,
                     rd_kafka_topic_result_free);

        for (i = 0 ; i < (int)topic_cnt ; i++) {
                rd_kafkap_str_t ktopic;
                rd_kafka_resp_err_t error_code;
                rd_kafkap_str_t error_msg = RD_KAFKAP_STR_INITIALIZER;
                char *errstr;
                rd_kafka_topic_result_t *terr;
                rd_kafka_NewTopic_t skel;
                int orig_pos;

                rd_kafka_buf_read_str(reply, &ktopic);
                rd_kafka_buf_read_i16(reply, &error_code);

                if (rd_kafka_buf_ApiVersion(reply) >= 1)
                        rd_kafka_buf_read_str(reply, &error_msg);

                if (error_code) {
                        if (RD_KAFKAP_STR_IS_NULL(&error_msg) ||
                            RD_KAFKAP_STR_LEN(&error_msg) == 0)
                                errstr = (char *)rd_kafka_err2str(error_code);
                        else
                                RD_KAFKAP_STR_DUPA(&errstr, &error_msg);

                        /* For non-blocking CreateTopicsRequests the broker
                         * will returned REQUEST_TIMED_OUT for topics
                         * that were triggered for creation -
                         * we hide this error code from the application
                         * since the topic creation is in fact in progress. */
                        if (error_code == RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT &&
                            rd_kafka_confval_get_int(&rko_req->rko_u.
                                                     admin_request.options.
                                                     operation_timeout) <= 0) {
                                error_code = RD_KAFKA_RESP_ERR_NO_ERROR;
                                errstr = NULL;
                        }
                } else {
                        errstr = NULL;
                }

                terr = rd_kafka_topic_result_new(ktopic.str,
                                                 RD_KAFKAP_STR_LEN(&ktopic),
                                                 error_code, errstr);

                /* As a convenience to the application we insert topic result
                 * in the same order as they were requested. The broker
                 * does not maintain ordering unfortunately. */
                skel.topic = terr->topic;
                orig_pos = rd_list_index(&rko_req->rko_u.admin_request.args,
                                         &skel, rd_kafka_NewTopic_cmp);
                if (orig_pos == -1) {
                        rd_snprintf(errstr, errstr_size,
                                    "Broker returned topic %s that was not "
                                    "included in the original request",
                                    terr->topic);
                        rd_kafka_topic_result_destroy(terr);
                        rd_kafka_op_destroy(rko_result);
                        return RD_KAFKA_RESP_ERR__BAD_MSG;
                }

                if (rd_list_elem(&rko_result->rko_u.admin_result.topics,
                                 orig_pos) != NULL) {
                        rd_snprintf(errstr, errstr_size,
                                    "Broker returned topic %s multiple times",
                                    terr->topic);
                        rd_kafka_topic_result_destroy(terr);
                        rd_kafka_op_destroy(rko_result);
                        return RD_KAFKA_RESP_ERR__BAD_MSG;
                }

                rd_list_set(&rko_result->rko_u.admin_result.topics, orig_pos,
                            terr);
        }

        *rko_resultp = rko_result;

        return RD_KAFKA_RESP_ERR_NO_ERROR;

 err_parse:
        if (rko_result)
                rd_kafka_op_destroy(rko_result);

        rd_snprintf(errstr, errstr_size,
                    "CreateTopics response protocol parse failure: %s",
                    rd_kafka_err2str(err));

        return err;
}

/**
 * @brief Asynchronous worker for CreateTopics
 */
static rd_kafka_op_res_t
rd_kafka_admin_CreateTopics_worker (rd_kafka_t *rk,
                                    rd_kafka_q_t *rkq,
                                    rd_kafka_op_t *rko) {
        rd_kafka_resp_err_t err = rko->rko_err;
        rd_kafka_broker_t *rkb;
        rd_ts_t timeout_in;
        rd_kafka_op_t *rko_result;
        char errstr[512];

        rd_kafka_dbg(rk, ADMIN, "CREATETOPICS",
                     "CreateTopics worker called in state %s: %s",
                     rd_kafka_admin_state_desc[rko->rko_u.admin_request.state],
                     rd_kafka_err2str(err));

        if (err == RD_KAFKA_RESP_ERR__DESTROY) /* Terminating */
                goto destroy;

        rd_assert(thrd_is_current(rko->rko_rk->rk_thread));

        /* Check for timeout */
        timeout_in = rd_timeout_remains_us(rko->rko_u.admin_request.abs_timeout);

        if (timeout_in <= 0) {
                rd_kafka_admin_result_fail(
                        rko, err, "Request timed out %s",
                        rd_kafka_admin_state_desc[rko->rko_u.
                                                  admin_request.state]);
                goto destroy;
        }

        switch (rko->rko_u.admin_request.state)
        {
        case RD_KAFKA_ADMIN_STATE_INIT:
                /* First call. */

                if (err) {
                        rd_kafka_admin_result_fail(
                                rko, err, "Failed to initialize CreateTopics "
                                "worker: %s", rd_kafka_err2str(err));
                        goto destroy;
                }

                /* Set up timeout timer. */
                rd_kafka_enq_once_add_source(rko->rko_u.admin_request.eonce,
                                             "timeout timer");
                rd_kafka_timer_start_oneshot(&rk->rk_timers,
                                             &rko->rko_u.admin_request.tmr,
                                             timeout_in,
                                             rd_kafka_admin_eonce_timeout_cb,
                                             rko->rko_u.admin_request.eonce);

                rko->rko_u.admin_request.state =
                        RD_KAFKA_ADMIN_STATE_WAIT_CONTROLLER;
                /*FALLTHRU*/

        case RD_KAFKA_ADMIN_STATE_WAIT_CONTROLLER:
                if (err == RD_KAFKA_RESP_ERR__TIMED_OUT) {
                        rd_kafka_admin_result_fail(
                                rko, RD_KAFKA_RESP_ERR__TRANSPORT,
                                "Timed out waiting for controller "
                                "to become available");
                        goto destroy;
                } else if (err) {
                        rd_kafka_admin_result_fail(
                                rko, RD_KAFKA_RESP_ERR__TRANSPORT,
                                "CreateTopics worker failed %s: %s",
                                rd_kafka_admin_state_desc[rko->rko_u.
                                                          admin_request.state],
                                rd_kafka_err2str(err));
                        goto destroy;
                }

                rd_kafka_dbg(rk, ADMIN, "ADMIN",
                             "CreateTopics: looking up controller");

                /* Since we're iterating over this controller_async() call
                 * (asynchronously) until a controller is availabe (or timeout)
                 * we need to re-enable the eonce to be triggered again (which
                 * is not necessary the first time we get here, but there
                 * is no harm doing it then either). */
                rd_kafka_enq_once_reenable(rko->rko_u.admin_request.eonce,
                                           rko, RD_KAFKA_REPLYQ(rk->rk_ops, 0));

                /* Look up the controller asynchronously, if the controller
                 * is not available the eonce is registered for broker
                 * state changes which will cause our function to be called
                 * again as soon as (any) broker state changes.
                 * When we are called again we perfomr the controller lookup
                 * again and hopefully get an rkb back, otherwise defer a new
                 * async wait. Repeat until success or timeout. */
                if (!(rkb = rd_kafka_broker_controller_async(
                              rk, RD_KAFKA_BROKER_STATE_UP,
                              rko->rko_u.admin_request.eonce))) {
                        /* Controller not available, wait asynchronously
                         * for controller code to trigger eonce. */
                        return RD_KAFKA_OP_RES_KEEP;
                }

                rd_kafka_dbg(rk, ADMIN, "ADMIN",
                             "Got controller %s", rkb->rkb_name);

                /* Still need to use the eonce since this worker may
                 * time out while waiting for response from broker, in which
                 * case the broker response will hit an empty eonce (ok). */
                rd_kafka_enq_once_add_source(rko->rko_u.admin_request.eonce,
                                             "send CreateTopics");

                /* Send request (async) */
                err = rd_kafka_CreateTopicsRequest(
                        rkb,
                        &rko->rko_u.admin_request.args,
                        &rko->rko_u.admin_request.options,
                        errstr, sizeof(errstr),
                        RD_KAFKA_REPLYQ(rk->rk_ops, 0),
                        rd_kafka_admin_CreateTopics_handle_response,
                        rko->rko_u.admin_request.eonce);

                /* Loose refcount from controller_async() */
                rd_kafka_broker_destroy(rkb);

                if (err) {
                        rd_kafka_enq_once_del_source(
                                rko->rko_u.admin_request.eonce,
                                "send CreateTopics");
                        rd_kafka_admin_result_fail(rko, err, "%s", errstr);
                        goto destroy;
                }

                rko->rko_u.admin_request.state =
                        RD_KAFKA_ADMIN_STATE_WAIT_RESPONSE;

                /* Wait asynchronously for broker response, which will
                 * trigger the eonce. */
                return RD_KAFKA_OP_RES_KEEP;


        case RD_KAFKA_ADMIN_STATE_WAIT_RESPONSE:
                rd_kafka_enq_once_del_source(rko->rko_u.admin_request.eonce,
                                             "send CreateTopics");

                /* CreateTopics response (either ok or with error). */
                if (err) {
                        rd_kafka_admin_result_fail(
                                rko, RD_KAFKA_RESP_ERR__TRANSPORT,
                                "CreateTopics worker failed %s: %s",
                                rd_kafka_admin_state_desc[rko->rko_u.
                                                          admin_request.state],
                                rd_kafka_err2str(err));
                        goto destroy;
                }

                /* Parse response and populate result to application */
                err = rd_kafka_admin_CreateTopics_parse_response(
                        rko, &rko_result,
                        rko->rko_u.admin_request.reply_buf,
                        errstr, sizeof(errstr));
                if (err) {
                        rd_kafka_admin_result_fail(
                                rko, err,
                                "CreateTopics worker failed parse response: %s",
                                errstr);
                        goto destroy;
                }

                /* Enqueue result on application queue, we're done. */
                rd_kafka_admin_result_enq(rko, rko_result);

                goto destroy;
        }


        return RD_KAFKA_OP_RES_KEEP;

 destroy:
        /* Free resources for this op. */
        rd_kafka_timer_stop(&rk->rk_timers, &rko->rko_u.admin_request.tmr,
                            rd_true);

        if (rko->rko_u.admin_request.eonce) {
                /* This is thread-safe to do even if there are outstanding
                 * timers or wait-controller references to the eonce
                 * since they only hold direct reference to the eonce,
                 * not the rko (the eonce holds a reference to the rko but
                 * it is cleared here). */
                rd_kafka_enq_once_destroy(rko->rko_u.admin_request.eonce);
                rko->rko_u.admin_request.eonce = NULL;
        }

        return RD_KAFKA_OP_RES_HANDLED; /* trigger's op_destroy() */
}


void rd_kafka_admin_CreateTopics (rd_kafka_t *rk,
                                  rd_kafka_NewTopic_t **new_topics,
                                  size_t new_topic_cnt,
                                  const rd_kafka_AdminOptions_t *options,
                                  rd_kafka_queue_t *rkqu) {
        rd_kafka_op_t *rko;
        size_t i;

        rko = rd_kafka_admin_request_op_new(rk,
                                            RD_KAFKA_OP_CREATETOPICS,
                                            RD_KAFKA_EVENT_CREATETOPICS_RESULT,
                                            rd_kafka_admin_CreateTopics_worker,
                                            options, rkqu);

        rd_list_init(&rko->rko_u.admin_request.args, new_topic_cnt,
                     rd_kafka_NewTopic_free);

        for (i = 0 ; i < new_topic_cnt ; i++)
                rd_list_add(&rko->rko_u.admin_request.args,
                            rd_kafka_NewTopic_copy(new_topics[i]));

        rd_kafka_q_enq(rk->rk_ops, rko);
}


rd_kafka_resp_err_t
rd_kafka_CreateTopics_result_error (
        const rd_kafka_CreateTopics_result_t *result,
        const char **errstrp) {
        const rd_kafka_op_t *rko = (const rd_kafka_op_t *)result;

        if (errstrp) {
                if (rko->rko_err)
                        *errstrp = rko->rko_u.admin_result.errstr;
                else
                        *errstrp = NULL;
        }

        return rko->rko_err;
}


/**
 * @brief Get an array of topic results from a CreateTopics result.
 *
 * The returned \p topics life-time is the same as the \p result object.
 * @param cntp is updated to the number of elements in the array.
 */
const rd_kafka_topic_result_t **
rd_kafka_CreateTopics_result_topics (
        const rd_kafka_CreateTopics_result_t *result,
        size_t *cntp) {
        const rd_kafka_op_t *rko = (const rd_kafka_op_t *)result;

        *cntp = rd_list_cnt(&rko->rko_u.admin_result.topics);
        return (const rd_kafka_topic_result_t **)rko->rko_u.admin_result.
                topics.rl_elems;
}

