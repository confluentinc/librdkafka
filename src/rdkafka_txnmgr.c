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

/**
 * @name Transaction Manager
 *
 */

#include <stdarg.h>

#include "rd.h"
#include "rdkafka_int.h"
#include "rdkafka_txnmgr.h"
#include "rdkafka_idempotence.h"
#include "rdkafka_request.h"
#include "rdunittest.h"
#include "rdrand.h"


static void
rd_kafka_txn_curr_api_reply (rd_kafka_q_t *rkq,
                             rd_kafka_resp_err_t err,
                             const char *errstr_fmt, ...);


/**
 * @brief Ensure client is configured as a transactional producer,
 *        else return error.
 *
 * @locality application thread
 * @locks none
 */
static RD_INLINE rd_kafka_resp_err_t
rd_kafka_ensure_transactional (const rd_kafka_t *rk,
                               char *errstr, size_t errstr_size) {
        if (unlikely(rk->rk_type != RD_KAFKA_PRODUCER)) {
                rd_snprintf(errstr, errstr_size,
                            "The Transactional API can only be used "
                            "on producer instances");
                return RD_KAFKA_RESP_ERR__INVALID_ARG;
        }

        if (unlikely(!rk->rk_conf.eos.transactional_id)) {
                rd_snprintf(errstr, errstr_size,
                            "The Transactional API requires "
                            "transactional.id to be configured");
                return RD_KAFKA_RESP_ERR__NOT_CONFIGURED;
        }

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}



/**
 * @brief Ensure transaction state is one of \p states.
 *
 * @param the required states, ended by a -1 sentinel.
 *
 * @locks rd_kafka_*lock(rk) MUST be held
 * @locality any
 */
static RD_INLINE rd_kafka_resp_err_t
rd_kafka_txn_require_states0 (rd_kafka_t *rk,
                              char *errstr, size_t errstr_size,
                              rd_kafka_txn_state_t states[]) {
        rd_kafka_resp_err_t err;
        size_t i;

        if (unlikely((err = rd_kafka_ensure_transactional(rk, errstr,
                                                          errstr_size))))
                return err;

        for (i = 0 ; (int)states[i] != -1 ; i++)
                if (rk->rk_eos.txn_state == states[i])
                        return RD_KAFKA_RESP_ERR_NO_ERROR;

        rd_snprintf(errstr, errstr_size,
                    "Operation not valid in state %s",
                    rd_kafka_txn_state2str(rk->rk_eos.txn_state));
        return RD_KAFKA_RESP_ERR__STATE;
}

/** @brief \p ... is a list of states */
#define rd_kafka_txn_require_state(rk,errstr,errstr_size,...)           \
        rd_kafka_txn_require_states0(rk, errstr, errstr_size,           \
                                     (rd_kafka_txn_state_t[]){          \
                                                     __VA_ARGS__, -1 })



/**
 * @param ignore Will be set to true if the state transition should be
 *               completely ignored.
 * @returns true if the state transition is valid, else false.
 */
static rd_bool_t
rd_kafka_txn_state_transition_is_valid (rd_kafka_txn_state_t curr,
                                        rd_kafka_txn_state_t new_state,
                                        rd_bool_t *ignore) {

        *ignore = rd_false;

        switch (new_state)
        {
        case RD_KAFKA_TXN_STATE_INIT:
                /* This is the initialized value and this transition will
                 * never happen. */
                return rd_false;

        case RD_KAFKA_TXN_STATE_WAIT_PID:
                return curr == RD_KAFKA_TXN_STATE_INIT;

        case RD_KAFKA_TXN_STATE_READY_NOT_ACKED:
                return curr == RD_KAFKA_TXN_STATE_WAIT_PID;

        case RD_KAFKA_TXN_STATE_READY:
                return curr == RD_KAFKA_TXN_STATE_READY_NOT_ACKED ||
                        curr == RD_KAFKA_TXN_STATE_COMMITTING_TRANSACTION ||
                        curr == RD_KAFKA_TXN_STATE_ABORTING_TRANSACTION;

        case RD_KAFKA_TXN_STATE_IN_TRANSACTION:
                return curr == RD_KAFKA_TXN_STATE_READY;

        case RD_KAFKA_TXN_STATE_BEGIN_COMMIT:
                return curr == RD_KAFKA_TXN_STATE_IN_TRANSACTION;

        case RD_KAFKA_TXN_STATE_COMMITTING_TRANSACTION:
                return curr == RD_KAFKA_TXN_STATE_BEGIN_COMMIT;

        case RD_KAFKA_TXN_STATE_ABORTING_TRANSACTION:
                return curr == RD_KAFKA_TXN_STATE_IN_TRANSACTION ||
                        curr == RD_KAFKA_TXN_STATE_ABORTABLE_ERROR;

        case RD_KAFKA_TXN_STATE_ABORTABLE_ERROR:
                if (curr == RD_KAFKA_TXN_STATE_ABORTING_TRANSACTION ||
                    curr == RD_KAFKA_TXN_STATE_FATAL_ERROR) {
                        /* Ignore sub-sequent abortable errors in
                         * these states. */
                        *ignore = rd_true;
                        return 1;
                }

                return curr == RD_KAFKA_TXN_STATE_IN_TRANSACTION ||
                        curr == RD_KAFKA_TXN_STATE_BEGIN_COMMIT ||
                        curr == RD_KAFKA_TXN_STATE_COMMITTING_TRANSACTION;

        case RD_KAFKA_TXN_STATE_FATAL_ERROR:
                /* Any state can transition to a fatal error */
                return rd_true;

        default:
                RD_NOTREACHED();
                return rd_false;
        }
}


/**
 * @brief Transition the transaction state to \p new_state.
 *
 * @returns 0 on success or an error code if the state transition
 *          was invalid.
 *
 * @locality rdkafka main thread
 * @locks rd_kafka_wrlock MUST be held
 */
static void rd_kafka_txn_set_state (rd_kafka_t *rk,
                                    rd_kafka_txn_state_t new_state) {
        rd_bool_t ignore;

        if (rk->rk_eos.txn_state == new_state)
                return;

        /* Check if state transition is valid */
        if (!rd_kafka_txn_state_transition_is_valid(rk->rk_eos.txn_state,
                                                    new_state, &ignore)) {
                rd_kafka_log(rk, LOG_CRIT, "TXNSTATE",
                             "BUG: Invalid transaction state transition "
                             "attempted: %s -> %s",
                             rd_kafka_txn_state2str(rk->rk_eos.txn_state),
                             rd_kafka_txn_state2str(new_state));

                rd_assert(!*"BUG: Invalid transaction state transition");
        }

        if (ignore) {
                /* Ignore this state change */
                return;
        }

        rd_kafka_dbg(rk, EOS, "TXNSTATE",
                     "Transaction state change %s -> %s",
                     rd_kafka_txn_state2str(rk->rk_eos.txn_state),
                     rd_kafka_txn_state2str(new_state));

        /* If transitioning from IN_TRANSACTION, the app is no longer
         * allowed to enqueue (produce) messages. */
        if (rk->rk_eos.txn_state == RD_KAFKA_TXN_STATE_IN_TRANSACTION)
                rd_atomic32_set(&rk->rk_eos.txn_may_enq, 0);
        else if (new_state == RD_KAFKA_TXN_STATE_IN_TRANSACTION)
                rd_atomic32_set(&rk->rk_eos.txn_may_enq, 1);

        rk->rk_eos.txn_state = new_state;
}


/**
 * @brief An unrecoverable transactional error has occurred.
 *
 * @param do_lock RD_DO_LOCK: rd_kafka_wrlock(rk) will be acquired and released,
 *                RD_DONT_LOCK: rd_kafka_wrlock(rk) MUST be held by the caller.
 * @locality any
 * @locks rd_kafka_wrlock MUST NOT be held
 */
void rd_kafka_txn_set_fatal_error (rd_kafka_t *rk, rd_dolock_t do_lock,
                                   rd_kafka_resp_err_t err,
                                   const char *fmt, ...) {
        char errstr[512];
        va_list ap;

        va_start(ap, fmt);
        vsnprintf(errstr, sizeof(errstr), fmt, ap);
        va_end(ap);

        rd_kafka_log(rk, LOG_ALERT, "TXNERR",
                     "Fatal transaction error: %s (%s)",
                     errstr, rd_kafka_err2name(err));

        if (do_lock)
                rd_kafka_wrlock(rk);
        rd_kafka_set_fatal_error0(rk, RD_DONT_LOCK, err, "%s", errstr);

        rk->rk_eos.txn_err = err;
        if (rk->rk_eos.txn_errstr)
                rd_free(rk->rk_eos.txn_errstr);
        rk->rk_eos.txn_errstr = rd_strdup(errstr);

        rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_FATAL_ERROR);

        if (rk->rk_eos.txn_init_rkq) {
                /* If application has called init_transactions() and
                 * it has now failed, reply to the app. */
                rd_kafka_txn_curr_api_reply(
                        rk->rk_eos.txn_init_rkq, err, "%s", errstr);
                rk->rk_eos.txn_init_rkq = NULL;
        }

        if (do_lock)
                rd_kafka_wrunlock(rk);
}


/**
 * @brief An abortable/recoverable transactional error has occured.
 *
 * @locality rdkafka main thread
 * @locks rd_kafka_wrlock MUST NOT be held
 */
void rd_kafka_txn_set_abortable_error (rd_kafka_t *rk,
                                       rd_kafka_resp_err_t err,
                                       const char *fmt, ...) {
        char errstr[512];
        va_list ap;

        if (rd_kafka_fatal_error(rk, NULL, 0)) {
                rd_kafka_dbg(rk, EOS, "FATAL",
                             "Not propagating abortable transactional "
                             "error (%s) "
                             "since previous fatal error already raised",
                             rd_kafka_err2name(err));
                return;
        }

        va_start(ap, fmt);
        vsnprintf(errstr, sizeof(errstr), fmt, ap);
        va_end(ap);

        rd_kafka_wrlock(rk);
        if (rk->rk_eos.txn_err) {
                rd_kafka_dbg(rk, EOS, "TXNERR",
                             "Ignoring sub-sequent abortable transaction "
                             "error: %s (%s): "
                             "previous error (%s) already raised",
                             errstr,
                             rd_kafka_err2name(err),
                             rd_kafka_err2name(rk->rk_eos.txn_err));
                rd_kafka_wrunlock(rk);
                return;
        }

        rk->rk_eos.txn_err = err;
        if (rk->rk_eos.txn_errstr)
                rd_free(rk->rk_eos.txn_errstr);
        rk->rk_eos.txn_errstr = rd_strdup(errstr);

        rd_kafka_log(rk, LOG_ERR, "TXNERR",
                     "Current transaction failed: %s (%s)",
                     errstr, rd_kafka_err2name(err));

        rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_ABORTABLE_ERROR);
        rd_kafka_wrunlock(rk);

        /* Purge all messages in queue/flight */
        rd_kafka_purge(rk,
                       RD_KAFKA_PURGE_F_QUEUE |
                       RD_KAFKA_PURGE_F_ABORT_TXN |
                       RD_KAFKA_PURGE_F_NON_BLOCKING);

}



/**
 * @brief Send op reply to the application which is blocking
 *        on one of the transaction APIs and reset the current API.
 *
 * @param rkq is the queue to send the reply on, which may be NULL or disabled.
 *            The \p rkq refcount is decreased by this function.
 * @param err API error code.
 * @param errstr_fmt If err is set, a human readable error format string.
 *
 * @locality rdkafka main thread
 * @locks any
 */
static void
rd_kafka_txn_curr_api_reply (rd_kafka_q_t *rkq,
                             rd_kafka_resp_err_t err,
                             const char *errstr_fmt, ...) {
        rd_kafka_op_t *rko;

        if (!rkq)
                return;

        rko = rd_kafka_op_new(RD_KAFKA_OP_TXN|RD_KAFKA_OP_REPLY);

        rko->rko_err = err;

        if (err && errstr_fmt && *errstr_fmt) {
                char errstr[512];
                va_list ap;
                va_start(ap, errstr_fmt);
                rd_vsnprintf(errstr, sizeof(errstr), errstr_fmt, ap);
                va_end(ap);
                rko->rko_u.txn.errstr = rd_strdup(errstr);
        }

        rd_kafka_q_enq(rkq, rko);

        rd_kafka_q_destroy(rkq);
}


/**
 * @brief The underlying idempotent producer state changed,
 *        see if this affects the transactional operations.
 *
 * @locality any thread
 * @locks rd_kafka_wrlock(rk) MUST be held
 */
void rd_kafka_txn_idemp_state_change (rd_kafka_t *rk,
                                      rd_kafka_idemp_state_t idemp_state) {

        if (idemp_state == RD_KAFKA_IDEMP_STATE_ASSIGNED &&
            rk->rk_eos.txn_state == RD_KAFKA_TXN_STATE_WAIT_PID) {
                RD_UT_COVERAGE(1);
                rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_READY_NOT_ACKED);

                if (rk->rk_eos.txn_init_rkq) {
                        /* Application has called init_transactions() and
                         * it is now complete, reply to the app. */
                        rd_kafka_txn_curr_api_reply(rk->rk_eos.txn_init_rkq,
                                                    RD_KAFKA_RESP_ERR_NO_ERROR,
                                                    "");
                        rk->rk_eos.txn_init_rkq = NULL;
                }

        } else if (idemp_state == RD_KAFKA_IDEMP_STATE_FATAL_ERROR &&
                   rk->rk_eos.txn_state != RD_KAFKA_TXN_STATE_FATAL_ERROR) {
                /* A fatal error has been raised. */

                rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_FATAL_ERROR);

                if (rk->rk_eos.txn_init_rkq) {
                        /* Application has called init_transactions() and
                         * it has now failed, reply to the app. */
                        rd_kafka_txn_curr_api_reply(
                                rk->rk_eos.txn_init_rkq,
                                RD_KAFKA_RESP_ERR__FATAL,
                                "Fatal error raised while retrieving PID");
                        rk->rk_eos.txn_init_rkq = NULL;
                }
        }
}


/**
 * @brief Moves a partition from the pending list to the proper list.
 *
 * @locality rdkafka main thread
 * @locks none
 */
static void rd_kafka_txn_partition_registered (rd_kafka_toppar_t *rktp) {
        rd_kafka_t *rk = rktp->rktp_rkt->rkt_rk;

        rd_kafka_toppar_lock(rktp);

        if (unlikely(!(rktp->rktp_flags & RD_KAFKA_TOPPAR_F_PEND_TXN))) {
                rd_kafka_dbg(rk, EOS|RD_KAFKA_DBG_PROTOCOL,
                             "ADDPARTS",
                             "\"%.*s\" [%"PRId32"] is not in pending "
                             "list but returned in AddPartitionsToTxn "
                             "response: ignoring",
                             RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                             rktp->rktp_partition);
                rd_kafka_toppar_unlock(rktp);
                return;
        }

        rd_kafka_dbg(rk, EOS|RD_KAFKA_DBG_TOPIC, "ADDPARTS",
                     "%.*s [%"PRId32"] registered with transaction",
                     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                     rktp->rktp_partition);

        rd_assert((rktp->rktp_flags & (RD_KAFKA_TOPPAR_F_PEND_TXN|
                                       RD_KAFKA_TOPPAR_F_IN_TXN)) ==
                  RD_KAFKA_TOPPAR_F_PEND_TXN);

        rktp->rktp_flags = (rktp->rktp_flags & ~RD_KAFKA_TOPPAR_F_PEND_TXN) |
                RD_KAFKA_TOPPAR_F_IN_TXN;

        rd_kafka_toppar_unlock(rktp);

        mtx_lock(&rk->rk_eos.txn_pending_lock);
        TAILQ_REMOVE(&rk->rk_eos.txn_waitresp_rktps, rktp, rktp_txnlink);
        mtx_unlock(&rk->rk_eos.txn_pending_lock);

        TAILQ_INSERT_TAIL(&rk->rk_eos.txn_rktps, rktp, rktp_txnlink);
}



/**
 * @brief Handle AddPartitionsToTxnResponse
 *
 * @locality rdkafka main thread
 * @locks none
 */
static void rd_kafka_txn_handle_AddPartitionsToTxn (rd_kafka_t *rk,
                                                    rd_kafka_broker_t *rkb,
                                                    rd_kafka_resp_err_t err,
                                                    rd_kafka_buf_t *rkbuf,
                                                    rd_kafka_buf_t *request,
                                                    void *opaque) {
        const int log_decode_errors = LOG_ERR;
        int32_t TopicCnt;
        int okcnt = 0, errcnt = 0;
        int actions = 0;
        int retry_backoff_ms = 500; /* retry backoff */
        rd_kafka_resp_err_t reset_coord_err = RD_KAFKA_RESP_ERR_NO_ERROR;

        if (err)
                goto done;

        rd_kafka_rdlock(rk);
        rd_assert(rk->rk_eos.txn_state !=
                  RD_KAFKA_TXN_STATE_COMMITTING_TRANSACTION);

        if (rk->rk_eos.txn_state != RD_KAFKA_TXN_STATE_IN_TRANSACTION &&
            rk->rk_eos.txn_state != RD_KAFKA_TXN_STATE_BEGIN_COMMIT) {
                /* Response received after aborting transaction */
                rd_rkb_dbg(rkb, EOS, "ADDPARTS",
                           "Ignoring outdated AddPartitionsToTxn response in "
                           "state %s",
                           rd_kafka_txn_state2str(rk->rk_eos.txn_state));
                rd_kafka_rdunlock(rk);
                err = RD_KAFKA_RESP_ERR__OUTDATED;
                goto done;
        }
        rd_kafka_rdunlock(rk);

        rd_kafka_buf_read_throttle_time(rkbuf);

        rd_kafka_buf_read_i32(rkbuf, &TopicCnt);

        while (TopicCnt-- > 0) {
                rd_kafkap_str_t Topic;
                rd_kafka_itopic_t *rkt;
                int32_t PartCnt;
                int p_actions = 0;

                rd_kafka_buf_read_str(rkbuf, &Topic);
                rd_kafka_buf_read_i32(rkbuf, &PartCnt);

                rkt = rd_kafka_topic_find0(rk, &Topic);
                if (rkt)
                        rd_kafka_topic_rdlock(rkt); /* for toppar_get() */

                while (PartCnt-- > 0) {
                        shptr_rd_kafka_toppar_t *s_rktp = NULL;
                        rd_kafka_toppar_t *rktp;
                        int32_t Partition;
                        int16_t ErrorCode;

                        rd_kafka_buf_read_i32(rkbuf, &Partition);
                        rd_kafka_buf_read_i16(rkbuf, &ErrorCode);

                        if (rkt)
                                s_rktp = rd_kafka_toppar_get(rkt,
                                                             Partition,
                                                             rd_false);

                        if (!s_rktp) {
                                rd_rkb_dbg(rkb, EOS|RD_KAFKA_DBG_PROTOCOL,
                                           "ADDPARTS",
                                           "Unknown partition \"%.*s\" "
                                           "[%"PRId32"] in AddPartitionsToTxn "
                                           "response: ignoring",
                                           RD_KAFKAP_STR_PR(&Topic),
                                           Partition);
                                continue;
                        }

                        rktp = rd_kafka_toppar_s2i(s_rktp);

                        switch (ErrorCode)
                        {
                        case RD_KAFKA_RESP_ERR_NO_ERROR:
                                /* Move rktp from pending to proper list */
                                rd_kafka_txn_partition_registered(rktp);
                                break;

                        case RD_KAFKA_RESP_ERR_NOT_COORDINATOR:
                        case RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE:
                        case RD_KAFKA_RESP_ERR__TRANSPORT:
                                reset_coord_err = ErrorCode;
                                p_actions |= RD_KAFKA_ERR_ACTION_RETRY;
                                break;

                        case RD_KAFKA_RESP_ERR_CONCURRENT_TRANSACTIONS:
                                retry_backoff_ms = 20;
                                /* FALLTHRU */
                        case RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS:
                        case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
                                p_actions |= RD_KAFKA_ERR_ACTION_RETRY;
                                break;

                        case RD_KAFKA_RESP_ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED:
                        case RD_KAFKA_RESP_ERR_INVALID_PRODUCER_ID_MAPPING:
                        case RD_KAFKA_RESP_ERR_INVALID_PRODUCER_EPOCH:
                        case RD_KAFKA_RESP_ERR_INVALID_TXN_STATE:
                                p_actions |= RD_KAFKA_ERR_ACTION_FATAL;
                                err = ErrorCode;
                                break;

                        case RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED:
                                p_actions |= RD_KAFKA_ERR_ACTION_PERMANENT;
                                err = ErrorCode;
                                break;

                        case RD_KAFKA_RESP_ERR_OPERATION_NOT_ATTEMPTED:
                                /* Partition skipped due to other partition's
                                 * errors */
                                break;

                        default:
                                /* Unhandled error, fail transaction */
                                p_actions |= RD_KAFKA_ERR_ACTION_PERMANENT;
                                break;
                        }

                        if (ErrorCode) {
                                errcnt++;
                                actions |= p_actions;

                                if (!(p_actions &
                                      (RD_KAFKA_ERR_ACTION_FATAL |
                                       RD_KAFKA_ERR_ACTION_PERMANENT)))
                                        rd_rkb_dbg(
                                                rkb, EOS,
                                                "ADDPARTS",
                                                "AddPartitionsToTxn response: "
                                                "partition \"%.*s\": "
                                                "[%"PRId32"]: %s",
                                                RD_KAFKAP_STR_PR(&Topic),
                                                Partition,
                                                rd_kafka_err2str(
                                                        ErrorCode));
                                else
                                        rd_rkb_log(rkb, LOG_ERR,
                                                   "ADDPARTS",
                                                   "Failed to add partition "
                                                   "\"%.*s\" [%"PRId32"] to "
                                                   "transaction: %s",
                                                   RD_KAFKAP_STR_PR(&Topic),
                                                   Partition,
                                                   rd_kafka_err2str(
                                                           ErrorCode));
                        } else {
                                okcnt++;
                        }

                        rd_kafka_toppar_destroy(s_rktp);
                }

                if (rkt) {
                        rd_kafka_topic_rdunlock(rkt);
                        rd_kafka_topic_destroy0(rkt);
                }
        }

        if (actions) /* Actions set from encountered errors '*/
                goto done;

        /* Since these partitions are now allowed to produce
         * we wake up all broker threads. */
        rd_kafka_all_brokers_wakeup(rk, RD_KAFKA_BROKER_STATE_INIT);

        goto done;

 err_parse:
        err = rkbuf->rkbuf_err;

 done:
        if (err)
                rk->rk_eos.txn_req_cnt--;

        if (err == RD_KAFKA_RESP_ERR__DESTROY ||
            err == RD_KAFKA_RESP_ERR__OUTDATED)
                return;

        if (reset_coord_err) {
                rd_kafka_wrlock(rk);
                rd_kafka_txn_coord_set(rk, NULL,
                                       "AddPartitionsToTxn failed: %s",
                                       rd_kafka_err2str(reset_coord_err));
                rd_kafka_wrunlock(rk);
        }


        mtx_lock(&rk->rk_eos.txn_pending_lock);
        TAILQ_CONCAT(&rk->rk_eos.txn_pending_rktps,
                     &rk->rk_eos.txn_waitresp_rktps,
                     rktp_txnlink);
        mtx_unlock(&rk->rk_eos.txn_pending_lock);

        if (okcnt + errcnt == 0) {
                /* Shouldn't happen */
                rd_kafka_dbg(rk, EOS, "ADDPARTS",
                             "No known partitions in "
                             "AddPartitionsToTxn response");
        }

        if (actions & RD_KAFKA_ERR_ACTION_FATAL) {
                rd_kafka_txn_set_fatal_error(rk, RD_DO_LOCK, err,
                                             "Failed to add partitions to "
                                             "transaction: %s",
                                             rd_kafka_err2str(err));

        } else if (actions & RD_KAFKA_ERR_ACTION_RETRY) {
                rd_kafka_txn_schedule_register_partitions(rk, retry_backoff_ms);

        } else if (errcnt > 0) {
                /* Treat all other errors as abortable errors */
                rd_kafka_txn_set_abortable_error(
                        rk, err,
                        "Failed to add %d/%d partition(s) to transaction: %s",
                        errcnt, errcnt + okcnt, rd_kafka_err2str(err));
        }
}


/**
 * @brief Send AddPartitionsToTxnRequest to the transaction coordinator.
 *
 * @returns an error code if the transaction coordinator is not known
 *          or not available.
 *
 * @locality rdkafka main thread
 * @locks none
 */
static rd_kafka_resp_err_t rd_kafka_txn_register_partitions (rd_kafka_t *rk) {
        char errstr[512];
        rd_kafka_resp_err_t err;
        rd_kafka_pid_t pid;

        mtx_lock(&rk->rk_eos.txn_pending_lock);
        if (TAILQ_EMPTY(&rk->rk_eos.txn_pending_rktps)) {
                mtx_unlock(&rk->rk_eos.txn_pending_lock);
                return RD_KAFKA_RESP_ERR_NO_ERROR;
        }

        err = rd_kafka_txn_require_state(rk, errstr, sizeof(errstr),
                                         RD_KAFKA_TXN_STATE_IN_TRANSACTION,
                                         RD_KAFKA_TXN_STATE_BEGIN_COMMIT);
        if (err)
                goto err;

        pid = rd_kafka_idemp_get_pid0(rk, rd_false/*dont-lock*/);
        if (!rd_kafka_pid_valid(pid)) {
                rd_dassert(!*"BUG: No PID despite proper transaction state");
                err = RD_KAFKA_RESP_ERR__STATE;
                rd_snprintf(errstr, sizeof(errstr),
                            "No PID available (idempotence state %s)",
                            rd_kafka_idemp_state2str(rk->rk_eos.idemp_state));
                goto err;
        }

        if (!rd_kafka_broker_is_up(rk->rk_eos.txn_coord)) {
                err = RD_KAFKA_RESP_ERR__TRANSPORT;
                rd_snprintf(errstr, sizeof(errstr), "Broker is not up");
                goto err;
        }


        /* Send request to coordinator */
        err = rd_kafka_AddPartitionsToTxnRequest(
                rk->rk_eos.txn_coord,
                rk->rk_conf.eos.transactional_id,
                pid,
                &rk->rk_eos.txn_pending_rktps,
                errstr, sizeof(errstr),
                RD_KAFKA_REPLYQ(rk->rk_ops, 0),
                rd_kafka_txn_handle_AddPartitionsToTxn, NULL);
        if (err)
                goto err;

        TAILQ_CONCAT(&rk->rk_eos.txn_waitresp_rktps,
                     &rk->rk_eos.txn_pending_rktps,
                     rktp_txnlink);

        mtx_unlock(&rk->rk_eos.txn_pending_lock);

        rk->rk_eos.txn_req_cnt++;

        rd_rkb_dbg(rk->rk_eos.txn_coord, EOS, "ADDPARTS",
                   "Adding partitions to transaction");

        return RD_KAFKA_RESP_ERR_NO_ERROR;

 err:
        mtx_unlock(&rk->rk_eos.txn_pending_lock);

        rd_kafka_dbg(rk, EOS, "ADDPARTS",
                     "Unable to register partitions with transaction: "
                     "%s", errstr);
        return err;
}

static void rd_kafka_txn_register_partitions_tmr_cb (rd_kafka_timers_t *rkts,
                                                     void *arg) {
        rd_kafka_t *rk = arg;

        rd_kafka_txn_register_partitions(rk);
}


/**
 * @brief Schedule register_partitions() as soon as possible.
 *
 * @locality any
 * @locks any
 */
void rd_kafka_txn_schedule_register_partitions (rd_kafka_t *rk,
                                                int backoff_ms) {
        rd_kafka_timer_start_oneshot(
                &rk->rk_timers,
                &rk->rk_eos.txn_register_parts_tmr, rd_false/*dont-restart*/,
                backoff_ms ? backoff_ms * 1000 : 1 /* immediate */,
                rd_kafka_txn_register_partitions_tmr_cb,
                rk);
}



/**
 * @brief Clears \p flag from all rktps in \p tqh
 */
static void rd_kafka_txn_clear_partitions_flag (rd_kafka_toppar_tqhead_t *tqh,
                                                int flag) {
        rd_kafka_toppar_t *rktp;

        TAILQ_FOREACH(rktp, tqh, rktp_txnlink) {
                rd_kafka_toppar_lock(rktp);
                rd_dassert(rktp->rktp_flags & flag);
                rktp->rktp_flags &= ~flag;
                rd_kafka_toppar_unlock(rktp);
        }
}


/**
 * @brief Clear all pending partitions.
 *
 * @locks txn_pending_lock MUST be held
 */
static void rd_kafka_txn_clear_pending_partitions (rd_kafka_t *rk) {
        rd_kafka_txn_clear_partitions_flag(&rk->rk_eos.txn_pending_rktps,
                                           RD_KAFKA_TOPPAR_F_PEND_TXN);
        rd_kafka_txn_clear_partitions_flag(&rk->rk_eos.txn_waitresp_rktps,
                                           RD_KAFKA_TOPPAR_F_PEND_TXN);
        TAILQ_INIT(&rk->rk_eos.txn_pending_rktps);
        TAILQ_INIT(&rk->rk_eos.txn_waitresp_rktps);
}

/**
 * @brief Clear all added partitions.
 *
 * @locks rd_kafka_wrlock(rk) MUST be held
 */
static void rd_kafka_txn_clear_partitions (rd_kafka_t *rk) {
        rd_kafka_txn_clear_partitions_flag(&rk->rk_eos.txn_rktps,
                                           RD_KAFKA_TOPPAR_F_IN_TXN);
        TAILQ_INIT(&rk->rk_eos.txn_rktps);
}




/**
 * @brief Op timeout callback which fails the current transaction.
 *
 * @locality rdkafka main thread
 * @locks none
 */
static void
rd_kafka_txn_curr_api_abort_timeout_cb (rd_kafka_timers_t *rkts, void *arg) {
        rd_kafka_q_t *rkq = arg;

        rd_kafka_txn_set_abortable_error(
                rkts->rkts_rk,
                RD_KAFKA_RESP_ERR__TIMED_OUT,
                "Transactional operation timed out");

        rd_kafka_txn_curr_api_reply(rkq,
                                    RD_KAFKA_RESP_ERR__TIMED_OUT,
                                    "Transactional operation timed out");
}

/**
 * @brief Op timeout callback which does not fail the current transaction.
 *
 * @locality rdkafka main thread
 * @locks none
 */
static void
rd_kafka_txn_curr_api_timeout_cb (rd_kafka_timers_t *rkts, void *arg) {
        rd_kafka_q_t *rkq = arg;

        rd_kafka_txn_curr_api_reply(rkq, RD_KAFKA_RESP_ERR__TIMED_OUT,
                                    "Transactional operation timed out");
}




/**
 * @brief Reset the current API, typically because it was completed
 *        without timeout.
 *
 * @locality rdkafka main thread
 * @locks rd_kafka_wrlock(rk) MUST be held
 */
static void rd_kafka_txn_curr_api_reset (rd_kafka_t *rk) {
        rd_bool_t timer_was_stopped;
        rd_kafka_q_t *rkq;

        rkq = rk->rk_eos.txn_curr_api.tmr.rtmr_arg;
        timer_was_stopped = rd_kafka_timer_stop(
                &rk->rk_timers,
                &rk->rk_eos.txn_curr_api.tmr,
                RD_DO_LOCK);

        if (rkq && timer_was_stopped) {
                /* Remove the stopped timer's reply queue reference
                 * since the timer callback will not have fired if
                 * we stopped the timer. */
                rd_kafka_q_destroy(rkq);
        }

        RD_MEMZERO(rk->rk_eos.txn_curr_api);
}


/**
 * @brief Sets the current API op (representing a blocking application API call)
 *        and a timeout for the same, and sends the op to the transaction
 *        manager thread (rdkafka main thread) for processing.
 *
 * If the timeout expires the rko will fail with ERR__TIMED_OUT
 * and the txnmgr state will be adjusted according to \p abort_on_timeout:
 * if true, the txn will transition to ABORTABLE_ERROR, else remain in
 * the current state.
 *
 * This call will block until a response is received from the rdkafka
 * main thread.
 *
 * Use rd_kafka_txn_curr_api_reset() when operation finishes prior
 * to the timeout.
 *
 * @param rko Op to send to txnmgr, or NULL if no op to send (yet).
 * @param flags See RD_KAFKA_TXN_CURR_API_F_.. flags in rdkafka_int.h.
 *
 * @returns the response op.
 *
 * @locality application thread
 * @locks none
 */
static rd_kafka_resp_err_t
rd_kafka_txn_curr_api_req (rd_kafka_t *rk, const char *name,
                           rd_kafka_op_t *rko,
                           int timeout_ms, int flags,
                           char *errstr, size_t errstr_size) {
        rd_kafka_resp_err_t err;
        rd_kafka_op_t *reply;
        rd_bool_t reuse = rd_false;
        rd_bool_t for_reuse;
        rd_kafka_q_t *tmpq = NULL;

        /* Strip __FUNCTION__ name's rd_kafka_ prefix since it will
         * not make sense in high-level language bindings. */
        if (!strncmp(name, "rd_kafka_", strlen("rd_kafka_")))
                name += strlen("rd_kafka_");

        rd_kafka_dbg(rk, EOS, "TXNAPI", "Transactional API called: %s", name);

        if (flags & RD_KAFKA_TXN_CURR_API_F_REUSE) {
                /* Reuse the current API call state. */
                flags &= ~RD_KAFKA_TXN_CURR_API_F_REUSE;
                reuse = rd_true;
        }

        rd_kafka_wrlock(rk);

        /* First set for_reuse to the current flags to match with
         * the passed flags. */
        for_reuse = !!(rk->rk_eos.txn_curr_api.flags &
                       RD_KAFKA_TXN_CURR_API_F_FOR_REUSE);

        if ((for_reuse && !reuse) ||
            (!for_reuse && *rk->rk_eos.txn_curr_api.name)) {
                rd_snprintf(errstr, errstr_size,
                            "Conflicting %s call already in progress",
                            rk->rk_eos.txn_curr_api.name);
                rd_kafka_wrunlock(rk);
                if (rko)
                        rd_kafka_op_destroy(rko);
                return RD_KAFKA_RESP_ERR__STATE;
        }

        rd_assert(for_reuse == reuse);

        rd_snprintf(rk->rk_eos.txn_curr_api.name,
                    sizeof(rk->rk_eos.txn_curr_api.name),
                    "%s", name);

        if (rko)
                tmpq = rd_kafka_q_new(rk);

        rk->rk_eos.txn_curr_api.flags |= flags;

        /* Then update for_reuse to the passed flags so that
         * api_reset() will not reset curr APIs that are to be reused,
         * but a sub-sequent _F_REUSE call will reset it. */
        for_reuse = !!(flags & RD_KAFKA_TXN_CURR_API_F_FOR_REUSE);

        if (!reuse && timeout_ms >= 0) {
                rd_kafka_q_keep(tmpq);
                rd_kafka_timer_start_oneshot(
                        &rk->rk_timers,
                        &rk->rk_eos.txn_curr_api.tmr,
                        rd_false,
                        timeout_ms * 1000,
                        flags & RD_KAFKA_TXN_CURR_API_F_ABORT_ON_TIMEOUT ?
                        rd_kafka_txn_curr_api_abort_timeout_cb :
                        rd_kafka_txn_curr_api_timeout_cb,
                        tmpq);
        }
        rd_kafka_wrunlock(rk);

        if (!rko)
                return RD_KAFKA_RESP_ERR_NO_ERROR;

        /* Send op to rdkafka main thread and wait for reply */
        reply = rd_kafka_op_req0(rk->rk_ops, tmpq, rko, RD_POLL_INFINITE);

        rd_kafka_q_destroy_owner(tmpq);

        if ((err = reply->rko_err)) {
                rd_snprintf(errstr, errstr_size, "%s",
                            reply->rko_u.txn.errstr ?
                            reply->rko_u.txn.errstr :
                            rd_kafka_err2str(err));
                for_reuse = rd_false;
        }

        rd_kafka_op_destroy(reply);

        if (!for_reuse)
                rd_kafka_txn_curr_api_reset(rk);

        return err;
}


/**
 * @brief Async handler for init_transactions()
 *
 * @locks none
 * @locality rdkafka main thread
 */
static rd_kafka_op_res_t
rd_kafka_txn_op_init_transactions (rd_kafka_t *rk,
                                   rd_kafka_q_t *rkq,
                                   rd_kafka_op_t *rko) {
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
        char errstr[512];

        if (rko->rko_err == RD_KAFKA_RESP_ERR__DESTROY)
                return RD_KAFKA_OP_RES_HANDLED;

        *errstr = '\0';

        rd_kafka_wrlock(rk);
        if ((err = rd_kafka_txn_require_state(
                     rk, errstr, sizeof(errstr),
                     RD_KAFKA_TXN_STATE_INIT,
                     RD_KAFKA_TXN_STATE_WAIT_PID,
                     RD_KAFKA_TXN_STATE_READY_NOT_ACKED))) {
                rd_kafka_wrunlock(rk);
                goto done;
        }

        if (rk->rk_eos.txn_state == RD_KAFKA_TXN_STATE_READY_NOT_ACKED) {
                /* A previous init_transactions() called finished successfully
                 * after timeout, the application has called init_transactions()
                 * again, we do nothin here, ack_init_transactions() will
                 * transition the state from READY_NOT_ACKED to READY. */
                rd_kafka_wrunlock(rk);
                goto done;
        }

        /* Possibly a no-op if already in WAIT_PID state */
        rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_WAIT_PID);

        /* Destroy previous reply queue for a previously timed out
         * init_transactions() call. */
        if (rk->rk_eos.txn_init_rkq)
                rd_kafka_q_destroy(rk->rk_eos.txn_init_rkq);

        /* Grab a separate reference to use in state_change(),
         * outside the curr_api to allow the curr_api to timeout while
         * the background init continues. */
        rk->rk_eos.txn_init_rkq = rd_kafka_q_keep(rko->rko_replyq.q);

        rd_kafka_wrunlock(rk);

        /* Start idempotent producer to acquire PID */
        rd_kafka_idemp_start(rk, rd_true/*immediately*/);

        return RD_KAFKA_OP_RES_HANDLED;

 done:
        rd_kafka_txn_curr_api_reply(rd_kafka_q_keep(rko->rko_replyq.q),
                                    err, errstr);

        return RD_KAFKA_OP_RES_HANDLED;
}


/**
 * @brief Async handler for the application to acknowledge
 *        successful background completion of init_transactions().
 *
 * @locks none
 * @locality rdkafka main thread
 */
static rd_kafka_op_res_t
rd_kafka_txn_op_ack_init_transactions (rd_kafka_t *rk,
                                       rd_kafka_q_t *rkq,
                                       rd_kafka_op_t *rko) {
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
        char errstr[512];

        if (rko->rko_err == RD_KAFKA_RESP_ERR__DESTROY)
                return RD_KAFKA_OP_RES_HANDLED;

        *errstr = '\0';

        rd_kafka_wrlock(rk);
        if ((err = rd_kafka_txn_require_state(
                     rk, errstr, sizeof(errstr),
                     RD_KAFKA_TXN_STATE_READY_NOT_ACKED))) {
                rd_kafka_wrunlock(rk);
                goto done;
        }

        rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_READY);

        rd_kafka_wrunlock(rk);
        /* FALLTHRU */

 done:
        rd_kafka_txn_curr_api_reply(rd_kafka_q_keep(rko->rko_replyq.q),
                                    err, "%s", errstr);

        return RD_KAFKA_OP_RES_HANDLED;
}



rd_kafka_resp_err_t
rd_kafka_init_transactions (rd_kafka_t *rk, int timeout_ms,
                            char *errstr, size_t errstr_size) {
        rd_kafka_resp_err_t err;

        if ((err = rd_kafka_ensure_transactional(rk, errstr, errstr_size)))
                return err;

        /* init_transactions() will continue to operate in the background
         * if the timeout expires, and the application may call
         * init_transactions() again to "continue" with the initialization
         * process.
         * For this reason we need two states:
         *  - TXN_STATE_READY_NOT_ACKED for when initialization is done
         *    but the API call timed out prior to success, meaning the
         *    application does not know initialization finished and
         *    is thus not allowed to call sub-sequent txn APIs, e.g. begin..()
         *  - TXN_STATE_READY for when initialization is done and this
         *    function has returned successfully to the application.
         *
         * And due to the two states we need two calls to the rdkafka main
         * thread (to keep txn_state synchronization in one place). */

        /* First call is to trigger initialization */
        err = rd_kafka_txn_curr_api_req(
                rk, __FUNCTION__,
                rd_kafka_op_new_cb(rk, RD_KAFKA_OP_TXN,
                                   rd_kafka_txn_op_init_transactions),
                timeout_ms,
                RD_KAFKA_TXN_CURR_API_F_FOR_REUSE,
                errstr, errstr_size);
        if (err)
                return err;


        /* Second call is to transition from READY_NOT_ACKED -> READY,
         * if necessary. */
        return rd_kafka_txn_curr_api_req(
                rk, __FUNCTION__,
                rd_kafka_op_new_cb(rk, RD_KAFKA_OP_TXN,
                                   rd_kafka_txn_op_ack_init_transactions),
                RD_POLL_INFINITE,
                RD_KAFKA_TXN_CURR_API_F_REUSE,
                errstr, errstr_size);
}



/**
 * @brief Handler for begin_transaction()
 *
 * @locks none
 * @locality rdkafka main thread
 */
static rd_kafka_op_res_t
rd_kafka_txn_op_begin_transaction (rd_kafka_t *rk,
                                   rd_kafka_q_t *rkq,
                                   rd_kafka_op_t *rko) {
        rd_kafka_resp_err_t err;
        char errstr[512];
        rd_bool_t wakeup_brokers = rd_false;

        if (rko->rko_err == RD_KAFKA_RESP_ERR__DESTROY)
                return RD_KAFKA_OP_RES_HANDLED;

        *errstr = '\0';

        rd_kafka_wrlock(rk);
        if (!(err = rd_kafka_txn_require_state(rk, errstr, sizeof(errstr),
                                              RD_KAFKA_TXN_STATE_READY))) {
                rd_assert(TAILQ_EMPTY(&rk->rk_eos.txn_rktps));

                rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_IN_TRANSACTION);

                rk->rk_eos.txn_req_cnt = 0;
                rk->rk_eos.txn_err = RD_KAFKA_RESP_ERR_NO_ERROR;
                RD_IF_FREE(rk->rk_eos.txn_errstr, rd_free);
                rk->rk_eos.txn_errstr = NULL;

                /* Wake up all broker threads (that may have messages to send
                 * that were waiting for this transaction state.
                 * But needs to be done below with no lock held. */
                wakeup_brokers = rd_true;

        }
        rd_kafka_wrunlock(rk);

        if (wakeup_brokers)
                rd_kafka_all_brokers_wakeup(rk, RD_KAFKA_BROKER_STATE_INIT);

        rd_kafka_txn_curr_api_reply(rd_kafka_q_keep(rko->rko_replyq.q),
                                    err, "%s", errstr);

        return RD_KAFKA_OP_RES_HANDLED;
}


rd_kafka_resp_err_t rd_kafka_begin_transaction (rd_kafka_t *rk,
                                                char *errstr,
                                                size_t errstr_size) {
        rd_kafka_op_t *reply;
        rd_kafka_resp_err_t err;

        if ((err = rd_kafka_ensure_transactional(rk, errstr, errstr_size)))
                return err;

        reply = rd_kafka_op_req(
                rk->rk_ops,
                rd_kafka_op_new_cb(rk, RD_KAFKA_OP_TXN,
                                   rd_kafka_txn_op_begin_transaction),
                RD_POLL_INFINITE);

        if ((err = reply->rko_err))
                rd_snprintf(errstr, errstr_size, "%s",
                            reply->rko_u.txn.errstr);

        rd_kafka_op_destroy(reply);

        return err;
}


static rd_kafka_resp_err_t
rd_kafka_txn_send_TxnOffsetCommitRequest (rd_kafka_broker_t *rkb,
                                          rd_kafka_op_t *rko,
                                          rd_kafka_replyq_t replyq,
                                          rd_kafka_resp_cb_t *resp_cb,
                                          void *reply_opaque);

/**
 * @brief Handle TxnOffsetCommitResponse
 *
 * @locality rdkafka main thread
 * @locks none
 */
static void rd_kafka_txn_handle_TxnOffsetCommit (rd_kafka_t *rk,
                                                 rd_kafka_broker_t *rkb,
                                                 rd_kafka_resp_err_t err,
                                                 rd_kafka_buf_t *rkbuf,
                                                 rd_kafka_buf_t *request,
                                                 void *opaque) {
        const int log_decode_errors = LOG_ERR;
        rd_kafka_op_t *rko = opaque;
        int actions = 0;
        rd_kafka_topic_partition_list_t *partitions = NULL;
        char errstr[512];

        *errstr = '\0';

        if (err != RD_KAFKA_RESP_ERR__DESTROY &&
            !rd_kafka_q_ready(rko->rko_replyq.q))
                err = RD_KAFKA_RESP_ERR__OUTDATED;

        if (err)
                goto done;

        rd_kafka_buf_read_throttle_time(rkbuf);

        partitions = rd_kafka_buf_read_topic_partitions(rkbuf, 0);
        if (!partitions)
                goto err_parse;

        err = rd_kafka_topic_partition_list_get_err(partitions);
        if (err) {
                char errparts[256];
                rd_kafka_topic_partition_list_str(partitions,
                                                  errparts, sizeof(errparts),
                                                  RD_KAFKA_FMT_F_ONLY_ERR);
                rd_snprintf(errstr, sizeof(errstr),
                            "Failed to commit offsets to transaction: %s",
                            errparts);
        }

        goto done;

 err_parse:
        err = rkbuf->rkbuf_err;

 done:
        if (err)
                rk->rk_eos.txn_req_cnt--;

        if (partitions)
                rd_kafka_topic_partition_list_destroy(partitions);

        switch (err)
        {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
                break;

        case RD_KAFKA_RESP_ERR__DESTROY:
        case RD_KAFKA_RESP_ERR__OUTDATED:
                rd_kafka_op_destroy(rko);
                return;

        case RD_KAFKA_RESP_ERR_NOT_COORDINATOR:
        case RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR__TRANSPORT:
                /* Note: this is the group coordinator, not the
                 *       transaction coordinator. */
                rd_kafka_coord_cache_evict(&rk->rk_coord_cache, rkb);
                actions |= RD_KAFKA_ERR_ACTION_RETRY;
                break;

        case RD_KAFKA_RESP_ERR_CONCURRENT_TRANSACTIONS:
        case RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS:
        case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
                actions |= RD_KAFKA_ERR_ACTION_RETRY;
                break;

        case RD_KAFKA_RESP_ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED:
        case RD_KAFKA_RESP_ERR_INVALID_PRODUCER_ID_MAPPING:
        case RD_KAFKA_RESP_ERR_INVALID_PRODUCER_EPOCH:
        case RD_KAFKA_RESP_ERR_INVALID_TXN_STATE:
        case RD_KAFKA_RESP_ERR_UNSUPPORTED_FOR_MESSAGE_FORMAT:
                actions |= RD_KAFKA_ERR_ACTION_FATAL;
                break;

        case RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED:
        case RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED:
                actions |= RD_KAFKA_ERR_ACTION_PERMANENT;
                break;

        default:
                /* Unhandled error, fail transaction */
                actions |= RD_KAFKA_ERR_ACTION_PERMANENT;
                break;
        }

        if (actions & RD_KAFKA_ERR_ACTION_FATAL) {
                rd_kafka_txn_set_fatal_error(rk, RD_DO_LOCK, err,
                                             "Failed to commit offsets to "
                                             "transaction: %s",
                                             rd_kafka_err2str(err));

        } else if (actions & RD_KAFKA_ERR_ACTION_RETRY) {
                int remains_ms = rd_timeout_remains(rko->rko_u.txn.abs_timeout);

                if (!rd_timeout_expired(remains_ms)) {
                        rd_kafka_coord_req(
                                rk,
                                RD_KAFKA_COORD_GROUP,
                                rko->rko_u.txn.group_id,
                                rd_kafka_txn_send_TxnOffsetCommitRequest,
                                rko,
                                rd_timeout_remains_limit0(
                                        remains_ms,
                                        rk->rk_conf.socket_timeout_ms),
                                RD_KAFKA_REPLYQ(rk->rk_ops, 0),
                                rd_kafka_txn_handle_TxnOffsetCommit,
                                rko);
                        return;
                } else if (!err)
                        err = RD_KAFKA_RESP_ERR__TIMED_OUT;
                actions |= RD_KAFKA_ERR_ACTION_PERMANENT;
        }

        if (actions & RD_KAFKA_ERR_ACTION_PERMANENT)
                rd_kafka_txn_set_abortable_error(rk, err,
                                                 "Failed to commit offsets to "
                                                 "transaction: %s",
                                                 rd_kafka_err2str(err));

        if (err)
                rd_kafka_txn_curr_api_reply(
                        rd_kafka_q_keep(rko->rko_replyq.q), err,
                        "Failed to commit offsets to transaction: %s",
                        rd_kafka_err2str(err));
        else
                rd_kafka_txn_curr_api_reply(rd_kafka_q_keep(rko->rko_replyq.q),
                                            RD_KAFKA_RESP_ERR_NO_ERROR,
                                            "");

        rd_kafka_op_destroy(rko);
}



/**
 * @brief Construct and send TxnOffsetCommitRequest.
 *
 * @locality rdkafka main thread
 * @locks none
 */
static rd_kafka_resp_err_t
rd_kafka_txn_send_TxnOffsetCommitRequest (rd_kafka_broker_t *rkb,
                                          rd_kafka_op_t *rko,
                                          rd_kafka_replyq_t replyq,
                                          rd_kafka_resp_cb_t *resp_cb,
                                          void *reply_opaque) {
        rd_kafka_t *rk = rkb->rkb_rk;
        rd_kafka_buf_t *rkbuf;
        int16_t ApiVersion;
        rd_kafka_pid_t pid;
        int cnt;

        rd_kafka_rdlock(rk);
        if (rk->rk_eos.txn_state != RD_KAFKA_TXN_STATE_IN_TRANSACTION) {
                rd_kafka_rdunlock(rk);
                rd_kafka_op_destroy(rko);
                return RD_KAFKA_RESP_ERR__OUTDATED;
        }

        pid = rd_kafka_idemp_get_pid0(rk, RD_DONT_LOCK);
        rd_kafka_rdunlock(rk);
        if (!rd_kafka_pid_valid(pid)) {
                rd_kafka_op_destroy(rko);
                return RD_KAFKA_RESP_ERR__STATE;
        }

        ApiVersion = rd_kafka_broker_ApiVersion_supported(
                rkb, RD_KAFKAP_TxnOffsetCommit, 0, 0, NULL);
        if (ApiVersion == -1) {
                rd_kafka_op_destroy(rko);
                return RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE;
        }

        rkbuf = rd_kafka_buf_new_request(rkb,
                                         RD_KAFKAP_TxnOffsetCommit, 1,
                                         rko->rko_u.txn.offsets->cnt * 50);

        /* transactional_id */
        rd_kafka_buf_write_str(rkbuf, rk->rk_conf.eos.transactional_id, -1);

        /* group_id */
        rd_kafka_buf_write_str(rkbuf, rko->rko_u.txn.group_id, -1);

        /* PID */
        rd_kafka_buf_write_i64(rkbuf, pid.id);
        rd_kafka_buf_write_i16(rkbuf, pid.epoch);

        /* Write per-partition offsets list */
        cnt = rd_kafka_buf_write_topic_partitions(
                rkbuf,
                rko->rko_u.txn.offsets,
                rd_true /*skip invalid offsets*/,
                rd_false/*dont write Epoch*/,
                rd_true /*write Metadata*/);

        if (!cnt) {
                /* No valid partition offsets, don't commit. */
                rd_kafka_buf_destroy(rkbuf);
                rd_kafka_op_destroy(rko);
                return RD_KAFKA_RESP_ERR__NO_OFFSET;
        }

        rd_kafka_buf_ApiVersion_set(rkbuf, ApiVersion, 0);

        rkbuf->rkbuf_max_retries = 3;

        rd_kafka_broker_buf_enq_replyq(rkb, rkbuf,
                                       replyq, resp_cb, reply_opaque);

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}


/**
 * @brief Handle AddOffsetsToTxnResponse
 *
 * @locality rdkafka main thread
 * @locks none
 */
static void rd_kafka_txn_handle_AddOffsetsToTxn (rd_kafka_t *rk,
                                                 rd_kafka_broker_t *rkb,
                                                 rd_kafka_resp_err_t err,
                                                 rd_kafka_buf_t *rkbuf,
                                                 rd_kafka_buf_t *request,
                                                 void *opaque) {
        const int log_decode_errors = LOG_ERR;
        rd_kafka_op_t *rko = opaque;
        int16_t ErrorCode;
        int actions = 0;
        int remains_ms;

        if (err == RD_KAFKA_RESP_ERR__DESTROY) {
                rd_kafka_op_destroy(rko);
                return;
        }

        if (!rd_kafka_q_ready(rko->rko_replyq.q))
                err = RD_KAFKA_RESP_ERR__OUTDATED;

        if (err)
                goto done;

        rd_kafka_buf_read_throttle_time(rkbuf);
        rd_kafka_buf_read_i16(rkbuf, &ErrorCode);

        err = ErrorCode;
        goto done;

 err_parse:
        err = rkbuf->rkbuf_err;

 done:
        if (err)
                rk->rk_eos.txn_req_cnt--;

        remains_ms = rd_timeout_remains(rko->rko_u.txn.abs_timeout);

        if (rd_timeout_expired(remains_ms) && !err)
                err = RD_KAFKA_RESP_ERR__TIMED_OUT;

        switch (err)
        {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
                break;

        case RD_KAFKA_RESP_ERR__OUTDATED:
        case RD_KAFKA_RESP_ERR__DESTROY:
                /* Producer is being terminated, ignore the response. */
                break;

        case RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_NOT_COORDINATOR:
        case RD_KAFKA_RESP_ERR__TRANSPORT:
        case RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT:
                actions |= RD_KAFKA_ERR_ACTION_RETRY|
                        RD_KAFKA_ERR_ACTION_REFRESH;
                break;

        case RD_KAFKA_RESP_ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED:
        case RD_KAFKA_RESP_ERR_INVALID_PRODUCER_EPOCH:
        case RD_KAFKA_RESP_ERR_INVALID_TXN_STATE:
        case RD_KAFKA_RESP_ERR_UNSUPPORTED_FOR_MESSAGE_FORMAT:
                actions |= RD_KAFKA_ERR_ACTION_FATAL;
                break;

        case RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED:
        case RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED:
                actions |= RD_KAFKA_ERR_ACTION_PERMANENT;
                break;

        case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
        case RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS:
        case RD_KAFKA_RESP_ERR_CONCURRENT_TRANSACTIONS:
                actions |= RD_KAFKA_ERR_ACTION_RETRY;
                break;

        default:
                /* All unhandled errors are permanent */
                actions |= RD_KAFKA_ERR_ACTION_PERMANENT;
                break;
        }


        /* All unhandled errors are considered permanent */
        if (err && !actions)
                actions |= RD_KAFKA_ERR_ACTION_PERMANENT;

        if (actions & RD_KAFKA_ERR_ACTION_FATAL) {
                rd_kafka_txn_set_fatal_error(rk, RD_DO_LOCK, err,
                                             "Failed to add offsets to "
                                             "transaction: %s",
                                             rd_kafka_err2str(err));

        } else if (actions & RD_KAFKA_ERR_ACTION_RETRY) {
                if (!rd_timeout_expired(remains_ms) &&
                    rd_kafka_buf_retry(rk->rk_eos.txn_coord, request))
                        return;
                actions |= RD_KAFKA_ERR_ACTION_PERMANENT;

        } else if (err) {
                rd_rkb_log(rkb, LOG_ERR, "ADDOFFSETS",
                           "Failed to add offsets to transaction: %s",
                           rd_kafka_err2str(err));
        }

        if (actions & RD_KAFKA_ERR_ACTION_PERMANENT)
                rd_kafka_txn_set_abortable_error(rk, err,
                                                 "Failed to add offsets to "
                                                 "transaction: %s",
                                                 rd_kafka_err2str(err));

        if (!err) {
                /* Step 2: Commit offsets to transaction on the
                 * group coordinator. */

                rd_kafka_coord_req(rk,
                                   RD_KAFKA_COORD_GROUP,
                                   rko->rko_u.txn.group_id,
                                   rd_kafka_txn_send_TxnOffsetCommitRequest,
                                   rko,
                                   rd_timeout_remains_limit0(
                                           remains_ms,
                                           rk->rk_conf.socket_timeout_ms),
                                   RD_KAFKA_REPLYQ(rk->rk_ops, 0),
                                   rd_kafka_txn_handle_TxnOffsetCommit,
                                   rko);

        } else {

                rd_kafka_txn_curr_api_reply(
                        rd_kafka_q_keep(rko->rko_replyq.q), err,
                        "Failed to add offsets to transaction: %s",
                        rd_kafka_err2str(err));

                rd_kafka_op_destroy(rko);
        }
}


/**
 * @brief Async handler for send_offsets_to_transaction()
 *
 * @locks none
 * @locality rdkafka main thread
 */
static rd_kafka_op_res_t
rd_kafka_txn_op_send_offsets_to_transaction (rd_kafka_t *rk,
                                             rd_kafka_q_t *rkq,
                                             rd_kafka_op_t *rko) {
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
        char errstr[512];
        rd_kafka_pid_t pid;

        if (rko->rko_err == RD_KAFKA_RESP_ERR__DESTROY)
                return RD_KAFKA_OP_RES_HANDLED;

        *errstr = '\0';

        rd_kafka_wrlock(rk);

        if ((err = rd_kafka_txn_require_state(
                     rk, errstr, sizeof(errstr),
                     RD_KAFKA_TXN_STATE_IN_TRANSACTION))) {
                rd_kafka_wrunlock(rk);
                goto err;
        }

        rd_kafka_wrunlock(rk);

        pid = rd_kafka_idemp_get_pid0(rk, rd_false/*dont-lock*/);
        if (!rd_kafka_pid_valid(pid)) {
                rd_dassert(!*"BUG: No PID despite proper transaction state");
                err = RD_KAFKA_RESP_ERR__STATE;
                rd_snprintf(errstr, sizeof(errstr),
                            "No PID available (idempotence state %s)",
                            rd_kafka_idemp_state2str(rk->rk_eos.idemp_state));
                goto err;
        }

        /* This is a multi-stage operation, consisting of:
         *  1) send AddOffsetsToTxnRequest to transaction coordinator.
         *  2) send TxnOffsetCommitRequest to group coordinator. */

        rd_kafka_AddOffsetsToTxnRequest(rk->rk_eos.txn_coord,
                                        rk->rk_conf.eos.transactional_id,
                                        pid,
                                        rko->rko_u.txn.group_id,
                                        errstr, sizeof(errstr),
                                        RD_KAFKA_REPLYQ(rk->rk_ops, 0),
                                        rd_kafka_txn_handle_AddOffsetsToTxn,
                                        rko);

        return RD_KAFKA_OP_RES_KEEP; /* the rko is passed to AddOffsetsToTxn */

 err:
        rd_kafka_txn_curr_api_reply(rd_kafka_q_keep(rko->rko_replyq.q),
                                    err, "%s", errstr);

        return RD_KAFKA_OP_RES_HANDLED;
}

/**
 * error returns:
 *   ERR__TRANSPORT - retryable
 */
rd_kafka_resp_err_t
rd_kafka_send_offsets_to_transaction (
        rd_kafka_t *rk,
        const rd_kafka_topic_partition_list_t *offsets,
        const char *consumer_group_id,
        int timeout_ms,
        char *errstr, size_t errstr_size) {
        rd_kafka_op_t *rko;
        rd_kafka_resp_err_t err;
        rd_kafka_topic_partition_list_t *valid_offsets;

        if ((err = rd_kafka_ensure_transactional(rk, errstr, errstr_size)))
                return err;

        if (!consumer_group_id || !*consumer_group_id || !offsets) {
                rd_snprintf(errstr, errstr_size,
                            "consumer_group_id and offsets "
                            "are required parameters");
                return RD_KAFKA_RESP_ERR__INVALID_ARG;
        }

        valid_offsets = rd_kafka_topic_partition_list_match(
                offsets, rd_kafka_topic_partition_match_valid_offset, NULL);

        if (valid_offsets->cnt == 0) {
                /* No valid offsets, e.g., nothing was consumed,
                 * this is not an error, do nothing. */
                rd_kafka_topic_partition_list_destroy(valid_offsets);
                return RD_KAFKA_RESP_ERR_NO_ERROR;
        }

        rd_kafka_topic_partition_list_sort_by_topic(valid_offsets);

        rko = rd_kafka_op_new_cb(rk, RD_KAFKA_OP_TXN,
                                 rd_kafka_txn_op_send_offsets_to_transaction);
        rko->rko_u.txn.offsets = valid_offsets;
        rko->rko_u.txn.group_id = rd_strdup(consumer_group_id);
        if (timeout_ms > rk->rk_conf.eos.transaction_timeout_ms)
                timeout_ms = rk->rk_conf.eos.transaction_timeout_ms;
        rko->rko_u.txn.abs_timeout = rd_timeout_init(timeout_ms);

        return rd_kafka_txn_curr_api_req(
                rk, __FUNCTION__, rko,
                RD_POLL_INFINITE, /* rely on background code to time out */
                0,
                errstr, errstr_size);
}





/**
 * @brief Successfully complete the transaction.
 *
 * @locality rdkafka main thread
 * @locks rd_kafka_wrlock(rk) MUST be held
 */
static void rd_kafka_txn_complete (rd_kafka_t *rk) {

        rd_kafka_dbg(rk, EOS, "TXNCOMPLETE",
                     "Transaction successfully %s",
                     rk->rk_eos.txn_state ==
                     RD_KAFKA_TXN_STATE_COMMITTING_TRANSACTION ?
                     "committed" : "aborted");

        /* Clear all transaction partition state */
        mtx_lock(&rk->rk_eos.txn_pending_lock);
        rd_assert(TAILQ_EMPTY(&rk->rk_eos.txn_pending_rktps));
        mtx_unlock(&rk->rk_eos.txn_pending_lock);

        rd_kafka_txn_clear_partitions(rk);

        rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_READY);
}



/**
 * @brief Handle EndTxnResponse (commit or abort)
 *
 * @locality rdkafka main thread
 * @locks none
 */
static void rd_kafka_txn_handle_EndTxn (rd_kafka_t *rk,
                                        rd_kafka_broker_t *rkb,
                                        rd_kafka_resp_err_t err,
                                        rd_kafka_buf_t *rkbuf,
                                        rd_kafka_buf_t *request,
                                        void *opaque) {
        const int log_decode_errors = LOG_ERR;
        rd_kafka_q_t *rkq = opaque;
        int16_t ErrorCode;
        int actions = 0;
        rd_bool_t is_commit = rd_false;

        if (err == RD_KAFKA_RESP_ERR__DESTROY) {
                rd_kafka_q_destroy(rkq);
                return;
        }

        if (err)
                goto err;

        rd_kafka_buf_read_throttle_time(rkbuf);
        rd_kafka_buf_read_i16(rkbuf, &ErrorCode);

        err = ErrorCode;
        /* FALLTHRU */

 err_parse:
        err = rkbuf->rkbuf_err;
 err:
        rd_kafka_wrlock(rk);
        if (rk->rk_eos.txn_state == RD_KAFKA_TXN_STATE_COMMITTING_TRANSACTION)
                is_commit = rd_true;
        else if (rk->rk_eos.txn_state ==
                 RD_KAFKA_TXN_STATE_ABORTING_TRANSACTION)
                is_commit = rd_false;
        else
                err = RD_KAFKA_RESP_ERR__OUTDATED;
        rd_kafka_wrunlock(rk);

        switch (err)
        {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
                /* EndTxn successful: complete the transaction */
                rd_kafka_txn_complete(rk);
                break;

        case RD_KAFKA_RESP_ERR__OUTDATED:
        case RD_KAFKA_RESP_ERR__DESTROY:
                /* Producer is being terminated, ignore the response. */
                break;

        case RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_NOT_COORDINATOR:
        case RD_KAFKA_RESP_ERR__TRANSPORT:
                rd_kafka_wrlock(rk);
                rd_kafka_txn_coord_set(rk, NULL,
                                       "EndTxn failed: %s",
                                       rd_kafka_err2str(err));
                rd_kafka_wrunlock(rk);
                actions |= RD_KAFKA_ERR_ACTION_RETRY;
                break;

        case RD_KAFKA_RESP_ERR_INVALID_PRODUCER_EPOCH:
        case RD_KAFKA_RESP_ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED:
        case RD_KAFKA_RESP_ERR_INVALID_TXN_STATE:
                actions |= RD_KAFKA_ERR_ACTION_FATAL;
                break;

        default:
                /* All unhandled errors are permanent */
                actions |= RD_KAFKA_ERR_ACTION_PERMANENT;
        }


        if (actions & RD_KAFKA_ERR_ACTION_FATAL) {
                rd_kafka_txn_set_fatal_error(rk, RD_DO_LOCK, err,
                                             "Failed to end transaction: %s",
                                             rd_kafka_err2str(err));

        } else if (actions & RD_KAFKA_ERR_ACTION_RETRY) {
                if (rd_kafka_buf_retry(rkb, request))
                        return;
                actions |= RD_KAFKA_ERR_ACTION_PERMANENT;
        }

        if (actions & RD_KAFKA_ERR_ACTION_PERMANENT)
                rd_kafka_txn_set_abortable_error(rk, err,
                                                 "Failed to end transaction: "
                                                 "%s",
                                                 rd_kafka_err2str(err));

        if (err)
                rd_kafka_txn_curr_api_reply(
                        rkq, err,
                        "EndTxn %s failed: %s", is_commit ? "commit" : "abort",
                        rd_kafka_err2str(err));
        else
                rd_kafka_txn_curr_api_reply(rkq, RD_KAFKA_RESP_ERR_NO_ERROR,
                                            "");
}



/**
 * @brief Handler for commit_transaction()
 *
 * @locks none
 * @locality rdkafka main thread
 */
static rd_kafka_op_res_t
rd_kafka_txn_op_commit_transaction (rd_kafka_t *rk,
                                    rd_kafka_q_t *rkq,
                                    rd_kafka_op_t *rko) {
        rd_kafka_resp_err_t err;
        char errstr[512];
        rd_kafka_pid_t pid;

        if (rko->rko_err == RD_KAFKA_RESP_ERR__DESTROY)
                return RD_KAFKA_OP_RES_HANDLED;

        *errstr = '\0';

        rd_kafka_wrlock(rk);

        if ((err = rd_kafka_txn_require_state(
                     rk, errstr, sizeof(errstr),
                     RD_KAFKA_TXN_STATE_BEGIN_COMMIT)))
                goto err;

        pid = rd_kafka_idemp_get_pid0(rk, rd_false/*dont-lock*/);
        if (!rd_kafka_pid_valid(pid)) {
                rd_dassert(!*"BUG: No PID despite proper transaction state");
                err = RD_KAFKA_RESP_ERR__STATE;
                rd_snprintf(errstr, sizeof(errstr),
                            "No PID available (idempotence state %s)",
                            rd_kafka_idemp_state2str(rk->rk_eos.idemp_state));
                goto err;
        }

        err = rd_kafka_EndTxnRequest(rk->rk_eos.txn_coord,
                                     rk->rk_conf.eos.transactional_id,
                                     pid,
                                     rd_true /* commit */,
                                     errstr, sizeof(errstr),
                                     RD_KAFKA_REPLYQ(rk->rk_ops, 0),
                                     rd_kafka_txn_handle_EndTxn,
                                     rd_kafka_q_keep(rko->rko_replyq.q));
        if (err)
                goto err;

        rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_COMMITTING_TRANSACTION);

        rd_kafka_wrunlock(rk);

        return RD_KAFKA_OP_RES_HANDLED;

 err:
        rd_kafka_wrunlock(rk);

        rd_kafka_txn_curr_api_reply(rd_kafka_q_keep(rko->rko_replyq.q),
                                    err, "%s", errstr);

        return RD_KAFKA_OP_RES_HANDLED;
}


/**
 * @brief Handler for commit_transaction()'s first phase: begin commit
 *
 * @locks none
 * @locality rdkafka main thread
 */
static rd_kafka_op_res_t
rd_kafka_txn_op_begin_commit (rd_kafka_t *rk,
                              rd_kafka_q_t *rkq,
                              rd_kafka_op_t *rko) {
        rd_kafka_resp_err_t err;
        char errstr[512];

        if (rko->rko_err == RD_KAFKA_RESP_ERR__DESTROY)
                return RD_KAFKA_OP_RES_HANDLED;

        *errstr = '\0';

        if ((err = rd_kafka_txn_require_state(
                     rk, errstr, sizeof(errstr),
                     RD_KAFKA_TXN_STATE_IN_TRANSACTION)))
                goto done;

        rd_kafka_wrlock(rk);
        rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_BEGIN_COMMIT);
        rd_kafka_wrunlock(rk);

        /* FALLTHRU */
 done:
        rd_kafka_txn_curr_api_reply(rd_kafka_q_keep(rko->rko_replyq.q),
                                    err, "%s", errstr);

        return RD_KAFKA_OP_RES_HANDLED;
}


rd_kafka_resp_err_t
rd_kafka_commit_transaction (rd_kafka_t *rk, int timeout_ms,
                             char *errstr, size_t errstr_size) {
        rd_kafka_resp_err_t err;
        rd_ts_t abs_timeout;

        if ((err = rd_kafka_ensure_transactional(rk, errstr, errstr_size)))
                return err;

        /* The commit is in two phases:
         *   - begin commit: wait for outstanding messages to be produced,
         *                   disallow new messages from being produced
         *                   by application.
         *   - commit: commit transaction.
         */

        abs_timeout = rd_timeout_init(timeout_ms);

        /* Begin commit */
        err = rd_kafka_txn_curr_api_req(
                rk, "commit_transaction (begin)",
                rd_kafka_op_new_cb(rk, RD_KAFKA_OP_TXN,
                                   rd_kafka_txn_op_begin_commit),
                rd_timeout_remains(abs_timeout),
                RD_KAFKA_TXN_CURR_API_F_FOR_REUSE|
                RD_KAFKA_TXN_CURR_API_F_ABORT_ON_TIMEOUT,
                errstr, errstr_size);
        if (err)
                return err;

        rd_kafka_dbg(rk, EOS, "TXNCOMMIT",
                     "Flushing %d outstanding message(s) prior to commit",
                     rd_kafka_outq_len(rk));

        /* Wait for queued messages to be delivered, limited by
         * the remaining transaction lifetime. */
        err = rd_kafka_flush(rk, rd_timeout_remains(abs_timeout));
        if (err) {
                if (err == RD_KAFKA_RESP_ERR__TIMED_OUT)
                        rd_snprintf(errstr, errstr_size,
                                    "Failed to flush all outstanding messages "
                                    "within the transaction timeout: "
                                    "%d message(s) remaining",
                                    rd_kafka_outq_len(rk));
                else
                        rd_snprintf(errstr, errstr_size,
                                    "Failed to flush outstanding messages: %s",
                                    rd_kafka_err2str(err));

                rd_kafka_txn_curr_api_reset(rk);

                /* FIXME: What to do here? Add test case */
                return err;
        }


        /* Commit transaction */
        err = rd_kafka_txn_curr_api_req(
                rk, "commit_transaction",
                rd_kafka_op_new_cb(rk, RD_KAFKA_OP_TXN,
                                   rd_kafka_txn_op_commit_transaction),
                rd_timeout_remains(abs_timeout),
                RD_KAFKA_TXN_CURR_API_F_REUSE|
                RD_KAFKA_TXN_CURR_API_F_ABORT_ON_TIMEOUT,
                errstr, errstr_size);

        return err;
}



/**
 * @brief Handler for abort_transaction()'s first phase: begin abort
 *
 * @locks none
 * @locality rdkafka main thread
 */
static rd_kafka_op_res_t
rd_kafka_txn_op_begin_abort (rd_kafka_t *rk,
                              rd_kafka_q_t *rkq,
                              rd_kafka_op_t *rko) {
        rd_kafka_resp_err_t err;
        char errstr[512];

        if (rko->rko_err == RD_KAFKA_RESP_ERR__DESTROY)
                return RD_KAFKA_OP_RES_HANDLED;

        *errstr = '\0';

        if ((err = rd_kafka_txn_require_state(
                     rk, errstr, sizeof(errstr),
                     RD_KAFKA_TXN_STATE_IN_TRANSACTION,
                     RD_KAFKA_TXN_STATE_ABORTABLE_ERROR)))
                goto done;

        rd_kafka_wrlock(rk);
        rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_ABORTING_TRANSACTION);
        rd_kafka_wrunlock(rk);

        mtx_lock(&rk->rk_eos.txn_pending_lock);
        rd_kafka_txn_clear_pending_partitions(rk);
        mtx_unlock(&rk->rk_eos.txn_pending_lock);


        /* FALLTHRU */
 done:
        rd_kafka_txn_curr_api_reply(rd_kafka_q_keep(rko->rko_replyq.q),
                                    err, "%s", errstr);

        return RD_KAFKA_OP_RES_HANDLED;
}


/**
 * @brief Handler for abort_transaction()
 *
 * @locks none
 * @locality rdkafka main thread
 */
static rd_kafka_op_res_t
rd_kafka_txn_op_abort_transaction (rd_kafka_t *rk,
                                   rd_kafka_q_t *rkq,
                                   rd_kafka_op_t *rko) {
        rd_kafka_resp_err_t err;
        char errstr[512];
        rd_kafka_pid_t pid;

        if (rko->rko_err == RD_KAFKA_RESP_ERR__DESTROY)
                return RD_KAFKA_OP_RES_HANDLED;

        *errstr = '\0';

        rd_kafka_wrlock(rk);

        if ((err = rd_kafka_txn_require_state(
                     rk, errstr, sizeof(errstr),
                     RD_KAFKA_TXN_STATE_ABORTING_TRANSACTION)))
                goto err;

        pid = rd_kafka_idemp_get_pid0(rk, rd_false/*dont-lock*/);
        if (!rd_kafka_pid_valid(pid)) {
                rd_dassert(!*"BUG: No PID despite proper transaction state");
                err = RD_KAFKA_RESP_ERR__STATE;
                rd_snprintf(errstr, sizeof(errstr),
                            "No PID available (idempotence state %s)",
                            rd_kafka_idemp_state2str(rk->rk_eos.idemp_state));
                goto err;
        }

        if (!rk->rk_eos.txn_req_cnt) {
                rd_kafka_dbg(rk, EOS, "TXNABORT",
                             "No partitions registered: not sending EndTxn");
                rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_READY);
                goto err;
        }

        err = rd_kafka_EndTxnRequest(rk->rk_eos.txn_coord,
                                     rk->rk_conf.eos.transactional_id,
                                     pid,
                                     rd_false /* abort */,
                                     errstr, sizeof(errstr),
                                     RD_KAFKA_REPLYQ(rk->rk_ops, 0),
                                     rd_kafka_txn_handle_EndTxn,
                                     rd_kafka_q_keep(rko->rko_replyq.q));
        if (err)
                goto err;

        rd_kafka_wrunlock(rk);

        return RD_KAFKA_OP_RES_HANDLED;

 err:
        rd_kafka_wrunlock(rk);

        rd_kafka_txn_curr_api_reply(rd_kafka_q_keep(rko->rko_replyq.q),
                                    err, "%s", errstr);

        // FIXME: What state do we transition to? READY? FATAL?

        return RD_KAFKA_OP_RES_HANDLED;
}


rd_kafka_resp_err_t
rd_kafka_abort_transaction (rd_kafka_t *rk, int timeout_ms,
                            char *errstr, size_t errstr_size) {
        rd_kafka_resp_err_t err;
        rd_ts_t abs_timeout = rd_timeout_init(timeout_ms);

        if ((err = rd_kafka_ensure_transactional(rk, errstr, errstr_size)))
                return err;

        /* The abort is multi-phase:
         * - set state to ABORTING_TRANSACTION
         * - flush() outstanding messages
         * - send EndTxn
         *
         * The curr_api must be reused during all these steps to avoid
         * a race condition where another application thread calls a
         * txn API inbetween the steps.
         */

        err = rd_kafka_txn_curr_api_req(
                rk, "abort_transaction (begin)",
                rd_kafka_op_new_cb(rk, RD_KAFKA_OP_TXN,
                                   rd_kafka_txn_op_begin_abort),
                timeout_ms,
                RD_KAFKA_TXN_CURR_API_F_FOR_REUSE|
                RD_KAFKA_TXN_CURR_API_F_ABORT_ON_TIMEOUT,
                errstr, errstr_size);
        if (err)
                return err;

        rd_kafka_dbg(rk, EOS, "TXNABORT",
                     "Purging and flushing %d outstanding message(s) prior "
                     "to abort",
                     rd_kafka_outq_len(rk));

        /* Purge all queued messages.
         * Will need to wait for messages in-flight since purging these
         * messages may lead to gaps in the idempotent producer sequences. */
        err = rd_kafka_purge(rk,
                             RD_KAFKA_PURGE_F_QUEUE|
                             RD_KAFKA_PURGE_F_ABORT_TXN);

        /* Serve delivery reports for the purged messages */
        err = rd_kafka_flush(rk, rd_timeout_remains(abs_timeout));
        if (err) {
                /* FIXME: Not sure these errors matter that much */
                if (err == RD_KAFKA_RESP_ERR__TIMED_OUT)
                        rd_snprintf(errstr, errstr_size,
                                    "Failed to flush all outstanding messages "
                                    "within the transaction timeout: "
                                    "%d message(s) remaining",
                                    rd_kafka_outq_len(rk));
                else
                        rd_snprintf(errstr, errstr_size,
                                    "Failed to flush outstanding messages: %s",
                                    rd_kafka_err2str(err));

                /* FIXME: What to do here? */

                rd_kafka_txn_curr_api_reset(rk);
                return err;
        }


        return rd_kafka_txn_curr_api_req(
                rk, "abort_transaction",
                rd_kafka_op_new_cb(rk, RD_KAFKA_OP_TXN,
                                   rd_kafka_txn_op_abort_transaction),
                0,
                RD_KAFKA_TXN_CURR_API_F_REUSE,
                errstr, errstr_size);
}



/**
 * @brief Coordinator query timer
 *
 * @locality rdkafka main thread
 * @locks none
 */

static void rd_kafka_txn_coord_timer_cb (rd_kafka_timers_t *rkts, void *arg) {
        rd_kafka_t *rk = arg;

        rd_kafka_wrlock(rk);
        rd_kafka_txn_coord_query(rk, "Coordinator query timer");
        rd_kafka_wrunlock(rk);
}

/**
 * @brief (Re-)Start coord query timer
 *
 * @locality rdkafka main thread
 * @locks none
 */
static void rd_kafka_txn_coord_timer_restart (rd_kafka_t *rk, int timeout_ms) {
        rd_assert(rd_kafka_is_transactional(rk));
        rd_kafka_timer_start_oneshot(&rk->rk_timers,
                                     &rk->rk_eos.txn_coord_tmr, rd_true,
                                     1000 * timeout_ms,
                                     rd_kafka_txn_coord_timer_cb, rk);
}


/**
 * @brief Parses and handles a FindCoordinator response.
 *
 * @locality rdkafka main thread
 * @locks none
 */
static void
rd_kafka_txn_handle_FindCoordinator (rd_kafka_t *rk,
                                     rd_kafka_broker_t *rkb,
                                     rd_kafka_resp_err_t err,
                                     rd_kafka_buf_t *rkbuf,
                                     rd_kafka_buf_t *request,
                                     void *opaque) {
        const int log_decode_errors = LOG_ERR;
        int16_t ErrorCode;
        rd_kafkap_str_t Host;
        int32_t NodeId, Port;
        char errstr[512];

        *errstr = '\0';

        rk->rk_eos.txn_wait_coord = rd_false;

        if (err)
                goto err;

        if (request->rkbuf_reqhdr.ApiVersion >= 1)
                rd_kafka_buf_read_throttle_time(rkbuf);

        rd_kafka_buf_read_i16(rkbuf, &ErrorCode);

        if (request->rkbuf_reqhdr.ApiVersion >= 1) {
                rd_kafkap_str_t ErrorMsg;
                rd_kafka_buf_read_str(rkbuf, &ErrorMsg);
                if (ErrorCode)
                        rd_snprintf(errstr, sizeof(errstr),
                                    "%.*s", RD_KAFKAP_STR_PR(&ErrorMsg));
        }

        if ((err = ErrorCode))
                goto err;

        rd_kafka_buf_read_i32(rkbuf, &NodeId);
        rd_kafka_buf_read_str(rkbuf, &Host);
        rd_kafka_buf_read_i32(rkbuf, &Port);

        rd_rkb_dbg(rkb, EOS, "TXNCOORD",
                   "FindCoordinator response: "
                   "Transaction coordinator is broker %"PRId32" (%.*s:%d)",
                   NodeId, RD_KAFKAP_STR_PR(&Host), (int)Port);

        rd_kafka_rdlock(rk);
        if (NodeId == -1)
                err = RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE;
        else if (!(rkb = rd_kafka_broker_find_by_nodeid(rk, NodeId))) {
                rd_snprintf(errstr, sizeof(errstr),
                            "Transaction coordinator %"PRId32" is unknown",
                            NodeId);
                err = RD_KAFKA_RESP_ERR__UNKNOWN_BROKER;
        }
        rd_kafka_rdunlock(rk);

        if (err)
                goto err;

        rd_kafka_wrlock(rk);
        rd_kafka_txn_coord_set(rk, rkb, "FindCoordinator response");
        rd_kafka_wrunlock(rk);

        rd_kafka_broker_destroy(rkb);

        return;

 err_parse:
        err = rkbuf->rkbuf_err;
 err:

        switch (err)
        {
        case RD_KAFKA_RESP_ERR__DESTROY:
                return;

        case RD_KAFKA_RESP_ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED:
                rd_kafka_wrlock(rk);
                rd_kafka_txn_set_fatal_error(
                        rkb->rkb_rk, RD_DONT_LOCK, err,
                        "Failed to find transaction coordinator: %s: %s%s%s",
                        rd_kafka_broker_name(rkb),
                        rd_kafka_err2str(err),
                        *errstr ? ": " : "", errstr);

                rd_kafka_idemp_set_state(rk, RD_KAFKA_IDEMP_STATE_FATAL_ERROR);
                rd_kafka_wrunlock(rk);
                return;

        case RD_KAFKA_RESP_ERR__UNKNOWN_BROKER:
                rd_kafka_metadata_refresh_brokers(rk, NULL, errstr);
                break;

        default:
                break;
        }

        rd_kafka_wrlock(rk);
        rd_kafka_txn_coord_set(rk, NULL,
                               "Failed to find transaction coordinator: %s: %s",
                               rd_kafka_err2name(err),
                               *errstr ? errstr : rd_kafka_err2str(err));
        rd_kafka_wrunlock(rk);
}




/**
 * @brief Query for the transaction coordinator.
 *
 * @returns true if a fatal error was raised, else false.
 *
 * @locality rdkafka main thread
 * @locks rd_kafka_wrlock(rk) MUST be held.
 */
rd_bool_t rd_kafka_txn_coord_query (rd_kafka_t *rk, const char *reason) {
        rd_kafka_resp_err_t err;
        char errstr[512];
        rd_kafka_broker_t *rkb;

        rd_assert(rd_kafka_is_transactional(rk));

        if (rk->rk_eos.txn_wait_coord) {
                rd_kafka_dbg(rk, EOS, "TXNCOORD",
                             "Not sending coordinator query (%s): "
                             "waiting for previous query to finish",
                             reason);
                return rd_false;
        }

        /* Find usable broker to query for the txn coordinator */
        rkb = rd_kafka_idemp_broker_any(rk, &err,
                                        errstr, sizeof(errstr));
        if (!rkb) {
                rd_kafka_dbg(rk, EOS, "TXNCOORD",
                             "Unable to query for transaction coordinator: %s",
                             errstr);

                if (rd_kafka_idemp_check_error(rk, err, errstr))
                        return rd_true;

                rd_kafka_txn_coord_timer_restart(rk, 500);

                return rd_false;
        }

        /* Send FindCoordinator request */
        err = rd_kafka_FindCoordinatorRequest(
                rkb, RD_KAFKA_COORD_TXN,
                rk->rk_conf.eos.transactional_id,
                RD_KAFKA_REPLYQ(rk->rk_ops, 0),
                rd_kafka_txn_handle_FindCoordinator, NULL);

        if (err) {
                rd_snprintf(errstr, sizeof(errstr),
                            "Failed to send coordinator query to %s: "
                            "%s",
                            rd_kafka_broker_name(rkb),
                            rd_kafka_err2str(err));

                rd_kafka_broker_destroy(rkb);

                if (rd_kafka_idemp_check_error(rk, err, errstr))
                        return rd_true; /* Fatal error */

                rd_kafka_txn_coord_timer_restart(rk, 500);

                return rd_false;
        }

        rd_kafka_broker_destroy(rkb);

        rk->rk_eos.txn_wait_coord = rd_true;

        return rd_false;
}

/**
 * @brief Sets or clears the current coordinator address.
 *
 * @returns true if the coordinator was changed, else false.
 *
 * @locality rd_kafka_main_thread
 * @locks rd_kafka_wrlock(rk) MUST be held
 */
rd_bool_t rd_kafka_txn_coord_set (rd_kafka_t *rk, rd_kafka_broker_t *rkb,
                                  const char *fmt, ...) {
        char buf[256];
        va_list ap;

        if (rk->rk_eos.txn_curr_coord == rkb) {
                if (!rkb) {
                        /* Keep querying for the coordinator */
                        rd_kafka_txn_coord_timer_restart(rk, 500);
                }
                return rd_false;
        }

        va_start(ap, fmt);
        vsnprintf(buf, sizeof(buf), fmt, ap);
        va_end(ap);


        rd_kafka_dbg(rk, EOS, "TXNCOORD",
                     "Transaction coordinator changed from %s -> %s: %s",
                     rk->rk_eos.txn_curr_coord ?
                     rd_kafka_broker_name(rk->rk_eos.txn_curr_coord) :
                     "(none)",
                     rkb ? rd_kafka_broker_name(rkb) : "(none)",
                     buf);

        if (rk->rk_eos.txn_curr_coord)
                rd_kafka_broker_destroy(rk->rk_eos.txn_curr_coord);

        rk->rk_eos.txn_curr_coord = rkb;
        if (rkb)
                rd_kafka_broker_keep(rkb);

        rd_kafka_broker_set_nodename(rk->rk_eos.txn_coord,
                                     rk->rk_eos.txn_curr_coord);

        if (!rkb) {
                /* Lost the current coordinator, query for new coordinator */
                rd_kafka_txn_coord_timer_restart(rk, 500);
        } else {
                /* Trigger PID state machine */
                rd_kafka_idemp_pid_fsm(rk);
        }

        return rd_true;
}


/**
 * @brief Coordinator state monitor callback.
 *
 * @locality rdkafka main thread
 * @locks none
 */
void rd_kafka_txn_coord_monitor_cb (rd_kafka_broker_t *rkb) {
        rd_kafka_t *rk = rkb->rkb_rk;
        rd_kafka_broker_state_t state = rd_kafka_broker_get_state(rkb);
        rd_bool_t is_up;

        rd_assert(rk->rk_eos.txn_coord == rkb);

        is_up = rd_kafka_broker_state_is_up(state);
        rd_rkb_dbg(rkb, EOS, "COORD",
                   "Transaction coordinator is now %s",
                   is_up ? "up" : "down");

        if (!is_up) {
                /* Coordinator is down, the connection will be re-established
                 * automatically, but we also trigger a coordinator query
                 * to pick up on coordinator change. */
                rd_kafka_txn_coord_timer_restart(rk, 500);

        } else {
                /* Coordinator is up. */

                rd_kafka_wrlock(rk);
                if (rk->rk_eos.idemp_state < RD_KAFKA_IDEMP_STATE_ASSIGNED) {
                        /* See if a idempotence state change is warranted. */
                        rd_kafka_idemp_pid_fsm(rk);

                } else if (rk->rk_eos.idemp_state ==
                           RD_KAFKA_IDEMP_STATE_ASSIGNED) {
                        /* PID is already valid, continue transactional
                         * operations by checking for partitions to register */
                        rd_kafka_txn_schedule_register_partitions(rk,
                                                                  1/*ASAP*/);
                }

                rd_kafka_wrunlock(rk);
        }
}



/**
 * @brief Transactions manager destructor
 *
 * @locality rdkafka main thread
 * @locks none
 */
void rd_kafka_txns_term (rd_kafka_t *rk) {
        RD_IF_FREE(rk->rk_eos.txn_init_rkq, rd_kafka_q_destroy);

        RD_IF_FREE(rk->rk_eos.txn_errstr, rd_free);

        rd_kafka_timer_stop(&rk->rk_timers,
                            &rk->rk_eos.txn_coord_tmr, 1);
        rd_kafka_timer_stop(&rk->rk_timers,
                            &rk->rk_eos.txn_register_parts_tmr, 1);

        if (rk->rk_eos.txn_curr_coord)
                rd_kafka_broker_destroy(rk->rk_eos.txn_curr_coord);

        /* Logical coordinator */
        rd_kafka_broker_persistent_connection_del(
                rk->rk_eos.txn_coord,
                &rk->rk_eos.txn_coord->rkb_persistconn.coord);
        rd_kafka_broker_monitor_del(&rk->rk_eos.txn_coord_mon);
        rd_kafka_broker_destroy(rk->rk_eos.txn_coord);
        rk->rk_eos.txn_coord = NULL;

        mtx_lock(&rk->rk_eos.txn_pending_lock);
        rd_kafka_txn_clear_pending_partitions(rk);
        mtx_unlock(&rk->rk_eos.txn_pending_lock);
        mtx_destroy(&rk->rk_eos.txn_pending_lock);

        rd_kafka_txn_clear_partitions(rk);
}


/**
 * @brief Initialize transactions manager.
 *
 * @locality application thread
 * @locks none
 */
void rd_kafka_txns_init (rd_kafka_t *rk) {
        rd_atomic32_init(&rk->rk_eos.txn_may_enq, 0);
        mtx_init(&rk->rk_eos.txn_pending_lock, mtx_plain);
        TAILQ_INIT(&rk->rk_eos.txn_pending_rktps);
        TAILQ_INIT(&rk->rk_eos.txn_waitresp_rktps);
        TAILQ_INIT(&rk->rk_eos.txn_rktps);

        /* Logical coordinator */
        rk->rk_eos.txn_coord =
                rd_kafka_broker_add_logical(rk, "TxnCoordinator");

        rd_kafka_broker_monitor_add(&rk->rk_eos.txn_coord_mon,
                                    rk->rk_eos.txn_coord,
                                    rk->rk_ops,
                                    rd_kafka_txn_coord_monitor_cb);

        rd_kafka_broker_persistent_connection_add(
                rk->rk_eos.txn_coord,
                &rk->rk_eos.txn_coord->rkb_persistconn.coord);
}

