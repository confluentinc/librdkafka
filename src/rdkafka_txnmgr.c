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
 * FIXME: how to propagate Abortable errors
 *
 */

#include "rd.h"
#include "rdkafka_int.h"
#include "rdkafka_txnmgr.h"
#include "rdkafka_idempotence.h"
#include "rdkafka_request.h"
#include "rdunittest.h"
#include "rdrand.h"


static void rd_kafka_txn_cancel_op_timeout (rd_kafka_t *rk);


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
 * @locks rd_kafka_*lock() MUST be held
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
 * @returns true if the state transition is valid, else false.
 */
static rd_bool_t
rd_kafka_txn_state_transition_is_valid (rd_kafka_txn_state_t curr,
                                        rd_kafka_txn_state_t new_state) {
        switch (new_state)
        {
        case RD_KAFKA_TXN_STATE_INIT:
                /* This is the initialized value and this transition will
                 * never happen. */
                return rd_false;

        case RD_KAFKA_TXN_STATE_WAIT_PID:
                return curr == RD_KAFKA_TXN_STATE_INIT;

        case RD_KAFKA_TXN_STATE_READY:
                return curr == RD_KAFKA_TXN_STATE_WAIT_PID ||
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
                return curr == RD_KAFKA_TXN_STATE_IN_TRANSACTION ||
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
 * @locks rd_kafka_wrlock MUST be held FIXME
 */
static void rd_kafka_txn_set_state (rd_kafka_t *rk,
                                    rd_kafka_txn_state_t new_state) {
        if (rk->rk_eos.txn_state == new_state)
                return;

        /* Check if state transition is valid */
        if (!rd_kafka_txn_state_transition_is_valid(rk->rk_eos.txn_state,
                                                    new_state)) {
                /* FIXME: This isn't safe for non-rdkmain-thread set states */
                rd_kafka_log(rk, LOG_CRIT, "TXNSTATE",
                             "BUG: Invalid transaction state transition "
                             "attempted: %s -> %s",
                             rd_kafka_txn_state2str(rk->rk_eos.txn_state),
                             rd_kafka_txn_state2str(new_state));

                rd_assert(!*"BUG: Invalid transaction state transition");
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
 * @locality any
 * @locks rd_kafka_wrlock MUST NOT be held
 */
void rd_kafka_txn_set_fatal_error (rd_kafka_t *rk,
                                   rd_kafka_resp_err_t err,
                                   const char *fmt, ...) {
        char errstr[512];
        va_list ap;

        if (rd_kafka_fatal_error(rk, NULL, 0)) {
                rd_kafka_dbg(rk, EOS, "FATAL",
                             "Not propagating fatal transactional error (%s) "
                             "since previous fatal error already raised",
                             rd_kafka_err2name(err));
                return;
        }

        va_start(ap, fmt);
        vsnprintf(errstr, sizeof(errstr), fmt, ap);
        va_end(ap);

        rd_kafka_log(rk, LOG_ALERT, "TXNERR",
                     "Fatal transaction error: %s (%s)",
                     errstr, rd_kafka_err2name(err));

         rd_kafka_set_fatal_error(rk, err, "%s", errstr);

        rd_kafka_wrlock(rk);
        rk->rk_eos.txn_err = err;
        if (rk->rk_eos.txn_errstr)
                rd_free(rk->rk_eos.txn_errstr);
        rk->rk_eos.txn_errstr = rd_strdup(errstr);

        rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_FATAL_ERROR);
        rd_kafka_wrunlock(rk);
}


/**
 * @brief An abortable/recoverable transactional error has occured.
 *
 * FIXME: must be callable from any thread (ProduceResponse rdkafka_request.c)
 *
 * FIXME: How to raise this error to the application?
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

        rd_kafka_log(rk, LOG_ERR, "TXNERR",
                     "Abortable transaction error: %s (%s)",
                     errstr, rd_kafka_err2name(err));

        rd_kafka_wrlock(rk);
        rk->rk_eos.txn_err = err;
        if (rk->rk_eos.txn_errstr)
                rd_free(rk->rk_eos.txn_errstr);
        rk->rk_eos.txn_errstr = rd_strdup(errstr);

        rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_ABORTABLE_ERROR);
        rd_kafka_wrunlock(rk);
}



/**
 * @brief Send op reply to the application which is blocking
 *        on one of the transaction APIs.
 *
 * @locality rdkafka main thread
 * @locks none needed
 */
static void rd_kafka_txn_reply_app (rd_kafka_t *rk, rd_kafka_resp_err_t err,
                                    const char *errstr_fmt, ...) {
        rd_kafka_op_t *rko;

        if (!(rko = rk->rk_eos.txn_curr_op))
                return;

        rd_kafka_txn_cancel_op_timeout(rk);

        rk->rk_eos.txn_curr_op = NULL;

        rko->rko_err = err;
        if (errstr_fmt && *errstr_fmt) {
                va_list ap;
                char errstr[512];
                va_start(ap, errstr_fmt);
                rd_vsnprintf(errstr, sizeof(errstr), errstr_fmt, ap);
                va_end(ap);
                rko->rko_u.txn.errstr = rd_strdup(errstr);
        }

        rd_kafka_replyq_enq(&rko->rko_replyq, rko, 0);
}


/**
 * @brief The underlying idempotent producer state changed,
 *        see if this affects the transactional operations.
 *
 * @locality rdkafka main thread
 * @locks rd_kafka_wrlock() MUST be held
 */
void rd_kafka_txn_idemp_state_change (rd_kafka_t *rk,
                                      rd_kafka_idemp_state_t idemp_state) {

        /* Make sure idempo state does not overflow into txn state bits */
        rd_dassert(!(idemp_state & ~((1<<16)-1)));

#define _COMBINED(TXNSTATE,IDEMPSTATE)          \
        ((TXNSTATE) << 16 | (IDEMPSTATE))


        switch (_COMBINED(rk->rk_eos.txn_state, idemp_state))
        {
        case _COMBINED(RD_KAFKA_TXN_STATE_WAIT_PID,
                       RD_KAFKA_IDEMP_STATE_ASSIGNED):
                RD_UT_COVERAGE(1);
                rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_READY);
                rd_kafka_txn_reply_app(rk, RD_KAFKA_RESP_ERR_NO_ERROR, NULL);
                break;

        default:
                rd_kafka_dbg(rk, EOS, "IDEMPSTATE",
                             "Ignored idempotent producer state change "
                             "Idemp=%s Txn=%s",
                             rd_kafka_idemp_state2str(idemp_state),
                             rd_kafka_txn_state2str(rk->rk_eos.txn_state));
        }

        if (idemp_state == RD_KAFKA_IDEMP_STATE_FATAL_ERROR &&
            rk->rk_eos.txn_state != RD_KAFKA_TXN_STATE_FATAL_ERROR) {
                /* A fatal error has been raised. */

                rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_FATAL_ERROR);

                rd_kafka_txn_reply_app(rk, rd_atomic32_get(&rk->rk_fatal.err),
                                       "%s", rk->rk_fatal.errstr);
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

                        if (ErrorCode) {
                                errcnt++;
                                rd_rkb_dbg(rkb, EOS,
                                           "ADDPARTS",
                                           "AddPartitionsToTxn response: "
                                           "partition \"%.*s\": "
                                           "[%"PRId32"]: %s",
                                           RD_KAFKAP_STR_PR(&Topic),
                                           Partition,
                                           rd_kafka_err2name(ErrorCode));
                        } else {
                                okcnt++;
                        }

                        switch (ErrorCode)
                        {
                        case RD_KAFKA_RESP_ERR_NO_ERROR:
                                /* Move rktp from pending to proper list */
                                rd_kafka_txn_partition_registered(rktp);
                                break;

                        case RD_KAFKA_RESP_ERR_NOT_COORDINATOR:
                                rd_kafka_coord_cache_evict(&rk->rk_coord_cache,
                                                           rkb);
                                /* FALLTHRU */
                        case RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE:
                                actions |= RD_KAFKA_ERR_ACTION_REFRESH;
                                break;

                        case RD_KAFKA_RESP_ERR_CONCURRENT_TRANSACTIONS:
                                retry_backoff_ms = 20;
                                /* FALLTHRU */
                        case RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS:
                        case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
                                actions |= RD_KAFKA_ERR_ACTION_RETRY;
                                break;

                        case RD_KAFKA_RESP_ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED:
                        case RD_KAFKA_RESP_ERR_INVALID_PRODUCER_ID_MAPPING:
                        case RD_KAFKA_RESP_ERR_INVALID_PRODUCER_EPOCH:
                        case RD_KAFKA_RESP_ERR_INVALID_TXN_STATE:
                                actions |= RD_KAFKA_ERR_ACTION_FATAL;
                                err = ErrorCode;
                                break;

                        case RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED:
                                // FIXME
                                break;

                        case RD_KAFKA_RESP_ERR_OPERATION_NOT_ATTEMPTED:
                                /* Partition skipped due to other partition's
                                 * errors */
                                break;
                        default:
                                /* Unhandled error, retry later */
                                actions |= RD_KAFKA_ERR_ACTION_RETRY;
                                break;
                        }

                        rd_kafka_toppar_destroy(s_rktp);
                }

                if (rkt) {
                        rd_kafka_topic_rdunlock(rkt);
                        rd_kafka_topic_destroy0(rkt);
                }
        }

        /* Since these partitions are now allowed to produce
         * we wake up all broker threads. */
        rd_kafka_all_brokers_wakeup(rk, RD_KAFKA_BROKER_STATE_INIT);

        goto done;

 err_parse:
        err = rkbuf->rkbuf_err;

 done:
        if (err)
                rk->rk_eos.txn_req_cnt--;

        if (err == RD_KAFKA_RESP_ERR__OUTDATED)
                return;

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


        rd_rkb_dbg(rkb, EOS, "ADDPARTS", "err %s, actions 0x%x",
                   rd_kafka_err2name(err), actions);

        if (actions & RD_KAFKA_ERR_ACTION_FATAL) {
                rd_kafka_txn_set_fatal_error(rk, err,
                                             "Failed to add partitions to "
                                             "transaction: %s",
                                             rd_kafka_err2str(err));

        } else if (actions & RD_KAFKA_ERR_ACTION_REFRESH) {
                /* Requery for coordinator? */

        } else if (actions & RD_KAFKA_ERR_ACTION_RETRY) {
                rd_kafka_txn_schedule_register_partitions(rk, retry_backoff_ms);

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

        rd_assert(rk->rk_eos.txn_coord);

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

        // FIXME: revisit?
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
 * @locks none
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
 * @locks rd_kafka_wrlock() MUST be held
 */
static void rd_kafka_txn_clear_partitions (rd_kafka_t *rk) {
        rd_kafka_txn_clear_partitions_flag(&rk->rk_eos.txn_rktps,
                                           RD_KAFKA_TOPPAR_F_IN_TXN);
        TAILQ_INIT(&rk->rk_eos.txn_rktps);
}




/**
 * @brief Op timeout callback.
 *
 * @locality rdkafka main thread
 * @locks none
 */
static void rd_kafka_txn_op_timeout_cb (rd_kafka_timers_t *rkts, void *arg) {
        rd_kafka_t *rk = arg;
        rd_kafka_txn_state_t state, new_state;
        // rd_kafka_op_cb_t *handler;

        rd_assert(rk->rk_eos.txn_curr_op);

        // handler = rk->rk_eos.txn_curr_op->rko_op_cb;

        rd_kafka_rdlock(rk);
        new_state = state = rk->rk_eos.txn_state;
        rd_kafka_rdunlock(rk);

        rd_kafka_txn_reply_app(rk, RD_KAFKA_RESP_ERR__TIMED_OUT,
                               "Operation timed out in state %s",
                               rd_kafka_txn_state2str(state));

        /* Properly handle the timeout based on what the operation was
         * (identified by handler), and the current state. */

#if FIXME
        if (handler == rd_kafka_txn_op_init_transactions) {
                /* init_transactions() timeout are ignored by the
                 * internal logic itself:
                 * the PID acquisition of the PID will continue in the
                 * background.
                 * This allows a sub-sequent call to init_transactions()
                 * to continue the wait for completion. */


        } else {


        }
#endif

        if (new_state != state) {
                rd_kafka_wrlock(rk);
                rd_kafka_txn_set_state(rk, new_state);
                rd_kafka_wrunlock(rk);
        }
}




/**
 * @brief Cancel the current op timeout timer.
 *
 * @locality rdkafka main thread
 * @locks none
 */
static void rd_kafka_txn_cancel_op_timeout (rd_kafka_t *rk) {
        rd_kafka_timer_stop(&rk->rk_timers, &rk->rk_eos.txn_curr_op_tmr,
                            RD_DO_LOCK);
}


/**
 * @brief Sets the current op (representing a blocking application API call)
 *        and a timeout for the same.
 *
 * If the timeout expires the rko will fail with ERR__TIMED_OUT
 * and the txnmgr state will be adjusted reasonably.
 *
 * Use rd_kafka_txn_clear_op() when operation finishes prior to the timeout.
 *
 * @locality rdkafka main thread
 * @locks none
 */
static void rd_kafka_txn_set_op_and_timeout (rd_kafka_t *rk,
                                             rd_kafka_op_t *rko,
                                             int timeout_ms) {
        rd_assert(!rk->rk_eos.txn_curr_op);
        rd_assert(rko);

        rk->rk_eos.txn_curr_op = rko;

        rd_kafka_timer_start_oneshot(&rk->rk_timers,
                                     &rk->rk_eos.txn_curr_op_tmr, rd_false,
                                     timeout_ms * 1000,
                                     rd_kafka_txn_op_timeout_cb, rk);
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
        rd_bool_t do_start = rd_true;
        char errstr[512];

        *errstr = '\0';

        rd_kafka_op_reuse(rko);

        if (rk->rk_eos.txn_curr_op) {
                /* This might happen if application is calling conflicting
                 * transactional APIs simultaneously from different threads. */
                rd_snprintf(errstr, sizeof(errstr),
                            "Conflicting transactional call "
                            "already in progress");
                err = RD_KAFKA_RESP_ERR__CONFLICT;
                goto done;
        }

        rd_kafka_wrlock(rk);

        if (rk->rk_eos.txn_state == RD_KAFKA_TXN_STATE_READY) {
                /* Application previously called init_transaction() and
                 * is now calling it again, typically because the previous
                 * call timed out. In the meantime the init has finished
                 * in the background, so just return successfully. */
                rd_kafka_wrunlock(rk);
                goto done;

        } else if (rk->rk_eos.txn_state == RD_KAFKA_TXN_STATE_WAIT_PID) {
                /* Application previously called init_transaction() and
                 * is now calling it again, typically because the previous
                 * call timed out. The PID is still being acquired,
                 * so block until timeout or PID is acquired. */
                do_start = rd_false;

        } else  if (rk->rk_eos.txn_state != RD_KAFKA_TXN_STATE_INIT) {
                if (rk->rk_eos.txn_err) {
                        rd_snprintf(errstr, sizeof(errstr),
                                    "%s", rk->rk_eos.txn_errstr);
                        err = rk->rk_eos.txn_err;
                } else {
                        rd_snprintf(errstr, sizeof(errstr),
                                    "Unable to initialize transactions in "
                                    "state %s: already initialized",
                                    rd_kafka_txn_state2str(rk->rk_eos.
                                                           txn_state));
                        err = RD_KAFKA_RESP_ERR__STATE;
                }
                rd_kafka_wrunlock(rk);
                goto done;
        }

        if (do_start)
                rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_WAIT_PID);

        rd_kafka_wrunlock(rk);

        rd_kafka_txn_set_op_and_timeout(rk, rko, rko->rko_u.txn.timeout_ms);

        /* Start idempotent producer to acquire PID */
        if (do_start)
                rd_kafka_idemp_start(rk, rd_true/*immediately*/);

        return RD_KAFKA_OP_RES_KEEP; /* input rko is used for reply */

 done:
        rko->rko_err = err;
        if (*errstr)
                rko->rko_u.txn.errstr = rd_strdup(errstr);

        rd_kafka_replyq_enq(&rko->rko_replyq, rko, 0);

        return RD_KAFKA_OP_RES_KEEP; /* input rko was used for reply */
}


rd_kafka_resp_err_t
rd_kafka_init_transactions (rd_kafka_t *rk, int timeout_ms,
                            char *errstr, size_t errstr_size) {
        rd_kafka_op_t *rko, *reply;
        rd_kafka_resp_err_t err;

        if (timeout_ms < 1) {
                rd_snprintf(errstr, errstr_size, "Invalid timeout");
                return RD_KAFKA_RESP_ERR__INVALID_ARG;
        }

        if ((err = rd_kafka_ensure_transactional(rk, errstr, errstr_size)))
                return err;

        rko = rd_kafka_op_new_cb(rk, RD_KAFKA_OP_TXN,
                                 rd_kafka_txn_op_init_transactions);
        rko->rko_u.txn.timeout_ms = timeout_ms;

        reply = rd_kafka_op_req(rk->rk_ops, rko, RD_POLL_INFINITE);

        if ((err = reply->rko_err))
                rd_snprintf(errstr, errstr_size, "%s",
                            reply->rko_u.txn.errstr);

        rd_kafka_op_destroy(reply);

        return err;
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

        rd_kafka_op_reuse(rko);

        rd_kafka_wrlock(rk);
        if ((err = rd_kafka_txn_require_state(rk, errstr, sizeof(errstr),
                                              RD_KAFKA_TXN_STATE_READY))) {
                rko->rko_err = err;
                rko->rko_u.txn.errstr = rd_strdup(errstr);
        } else {
                rd_assert(TAILQ_EMPTY(&rk->rk_eos.txn_rktps));

                rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_IN_TRANSACTION);

                rk->rk_eos.txn_req_cnt = 0;

                /* Wake up all broker threads (that may have messages to send
                 * that were waiting for this transaction state.
                 * But needs to be done below with no lock held. */
                wakeup_brokers = rd_true;

        }
        rd_kafka_wrunlock(rk);

        if (wakeup_brokers)
                rd_kafka_all_brokers_wakeup(rk, RD_KAFKA_BROKER_STATE_INIT);

        rd_kafka_replyq_enq(&rko->rko_replyq, rko, 0);

        return RD_KAFKA_OP_RES_KEEP;  /* input rko was used for reply */
}


/**
 * Should be called before the start of each new transaction. Note that prior to the first invocation
 * of this method, you must invoke {@link #initTransactions()} exactly one time.
 *
 * @throws IllegalStateException if no transactional.id has been configured or if {@link #initTransactions()}
 *         has not yet been invoked
 * @throws ProducerFencedException if another producer with the same transactional.id is active
 * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
 *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
 * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
 *         transactional.id is not authorized. See the exception for more details
 * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
 */
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


/**
 * Sends a list of specified offsets to the consumer group coordinator, and also marks
 * those offsets as part of the current transaction. These offsets will be considered
 * committed only if the transaction is committed successfully. The committed offset should
 * be the next message your application will consume, i.e. lastProcessedMessageOffset + 1.
 * <p>
 * This method should be used when you need to batch consumed and produced messages
 * together, typically in a consume-transform-produce pattern. Thus, the specified
 * {@code consumerGroupId} should be the same as config parameter {@code group.id} of the used
 * {@link KafkaConsumer consumer}. Note, that the consumer should have {@code enable.auto.commit=false}
 * and should also not commit offsets manually (via {@link KafkaConsumer#commitSync(Map) sync} or
 * {@link KafkaConsumer#commitAsync(Map, OffsetCommitCallback) async} commits).
 *
 * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
 * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
 * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
 *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
 * @throws org.apache.kafka.common.errors.UnsupportedForMessageFormatException  fatal error indicating the message
 *         format used for the offsets topic on the broker does not support transactions
 * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
 *         transactional.id is not authorized. See the exception for more details
 * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any
 *         other unexpected error
 */


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

        if (err)
                goto done;

        rd_kafka_buf_read_throttle_time(rkbuf);

        partitions = rd_kafka_buf_read_topic_partitions(
                rkbuf, rko->rko_u.txn.offsets->cnt);
        if (!partitions)
                goto err_parse;

        /* FIXME, handle partitions */
        rd_kafka_topic_partition_list_log(rk, "TXNOFFSRESP", RD_KAFKA_DBG_EOS,
                                          partitions);

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

        actions = rd_kafka_err_action(
                rkb, err, request,

                RD_KAFKA_ERR_ACTION_RETRY,
                RD_KAFKA_RESP_ERR__TRANSPORT,

                /* FIXME: Why retry? */
                RD_KAFKA_ERR_ACTION_RETRY,
                RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART,

                RD_KAFKA_ERR_ACTION_RETRY,
                RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS,

                // FIXME

                RD_KAFKA_ERR_ACTION_END);

        rd_rkb_dbg(rkb, EOS, "TXNOFFSETS", "err %s, actions 0x%x",
                   rd_kafka_err2name(err), actions);

        if (partitions)
                rd_kafka_topic_partition_list_destroy(partitions);

        if (actions & RD_KAFKA_ERR_ACTION_FATAL) {
                rd_kafka_txn_set_fatal_error(rk, err,
                                             "Failed to commit offsets to "
                                             "transaction: %s",
                                             rd_kafka_err2str(err));

        } else if (actions & RD_KAFKA_ERR_ACTION_REFRESH) {
                /* Requery for coordinator? */
                /* FIXME */
                // rd_kafka_buf_retry_on_coordinator(rk, _GROUP, group_id, req);
        } else if (actions & RD_KAFKA_ERR_ACTION_RETRY) {
                if (rd_kafka_buf_retry(rkb, request))
                        return;
        }


        rko->rko_err = err;
        if (!*errstr)
                rd_snprintf(errstr, sizeof(errstr),
                            "Failed to commit offsets to transaction: %s",
                            rd_kafka_err2str(err));
        rko->rko_u.txn.errstr = rd_strdup(errstr);
        rd_kafka_replyq_enq(&rko->rko_replyq, rko, 0);
}



/**
 * @brief Construct and send TxnOffsetCommitRequest.
 *        Triggered (asynchronously) by coord_req().
 *
 * @locality rdkafka main thread
 * @locks none
 */
static rd_kafka_resp_err_t
rd_kafka_TxnOffsetCommitRequest_op (rd_kafka_broker_t *rkb,
                                    rd_kafka_op_t *rko,
                                    rd_kafka_replyq_t replyq,
                                    rd_kafka_resp_cb_t *resp_cb,
                                    void *reply_opaque) {
        rd_kafka_t *rk = rkb->rkb_rk;
        rd_kafka_buf_t *rkbuf;
        int16_t ApiVersion;
        rd_kafka_topic_partition_list_t *offsets = rko->rko_u.txn.offsets;
        rd_kafka_pid_t pid;
        int cnt;

        rd_assert(rk->rk_eos.txn_state == RD_KAFKA_TXN_STATE_IN_TRANSACTION);

        pid = rd_kafka_idemp_get_pid0(rk, RD_DO_LOCK);
        if (!rd_kafka_pid_valid(pid))
                return RD_KAFKA_RESP_ERR__STATE; // FIXME> replyq destroy?

        ApiVersion = rd_kafka_broker_ApiVersion_supported(
                rkb, RD_KAFKAP_TxnOffsetCommit, 0, 0, NULL);
        if (ApiVersion == -1) // FIXME: destroy replyq?
                return RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE;

        rkbuf = rd_kafka_buf_new_request(rkb, RD_KAFKAP_TxnOffsetCommit, 1,
                                         offsets->cnt * 50);

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
                return RD_KAFKA_RESP_ERR__NO_OFFSET;
        }

        rd_kafka_topic_partition_list_log(rk, "TXNOFFSET", RD_KAFKA_DBG_EOS,
                                          rko->rko_u.txn.offsets);

        rd_kafka_buf_ApiVersion_set(rkbuf, ApiVersion, 0);

        rkbuf->rkbuf_max_retries = 3;

        rd_kafka_broker_buf_enq_replyq(rkb, rkbuf, replyq, resp_cb,
                                       reply_opaque);

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

        actions = rd_kafka_err_action(
                rkb, err, request,

                RD_KAFKA_ERR_ACTION_RETRY,
                RD_KAFKA_RESP_ERR__TRANSPORT,

                /* FIXME: Why retry? */
                RD_KAFKA_ERR_ACTION_RETRY,
                RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART,

                RD_KAFKA_ERR_ACTION_RETRY,
                RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS,

                RD_KAFKA_ERR_ACTION_REFRESH,
                RD_KAFKA_RESP_ERR_NOT_COORDINATOR,

                RD_KAFKA_ERR_ACTION_RETRY,
                RD_KAFKA_RESP_ERR_CONCURRENT_TRANSACTIONS,
                /* FIXME: smaller retry backoff */

                /* FIXME: convert to __FENCE ? */
                RD_KAFKA_ERR_ACTION_FATAL,
                RD_KAFKA_RESP_ERR_INVALID_PRODUCER_EPOCH,

                RD_KAFKA_ERR_ACTION_FATAL,
                RD_KAFKA_RESP_ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED,

                RD_KAFKA_ERR_ACTION_PERMANENT,
                RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED,

                // FIXME: everything else is FATAL

                RD_KAFKA_ERR_ACTION_END);

        rd_rkb_dbg(rkb, EOS, "ADDOFFSETS", "err %s, actions 0x%x",
                   rd_kafka_err2name(err), actions);

        if (actions & RD_KAFKA_ERR_ACTION_FATAL) {
                rd_kafka_txn_set_fatal_error(rk, err,
                                             "Failed to add offsets to "
                                             "transaction: %s",
                                             rd_kafka_err2str(err));

        } else if (actions & RD_KAFKA_ERR_ACTION_REFRESH) {
                /* Requery for coordinator? */
                /* FIXME */
                // rd_kafka_buf_retry_on_coordinator(rk, _GROUP, group_id, req);
        } else if (actions & RD_KAFKA_ERR_ACTION_RETRY) {
                if (rd_kafka_buf_retry(rk->rk_eos.txn_coord, request))
                        return;
        }

        if (!err) {
                /* Step 3: Commit offsets to transaction on the
                 *         group coordinator. */
                rd_kafka_coord_req(rk,
                                   RD_KAFKA_COORD_GROUP,
                                   rko->rko_u.txn.group_id,
                                   rd_kafka_TxnOffsetCommitRequest_op,
                                   rko,
                                   60 * 1000 /*FIXME*/,
                                   RD_KAFKA_REPLYQ(rk->rk_ops, 0),
                                   rd_kafka_txn_handle_TxnOffsetCommit,
                                   rko);
        } else {
                rko->rko_err = err;
                // FIXME rko->rko_u.txn.errstr = rd_strdup(errstr);
                rd_kafka_replyq_enq(&rko->rko_replyq, rko, 0);
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

        rd_kafka_op_reuse(rko);

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
         *  2) look up group coordinator for the provided group.
         *  3) send TxnOffsetCommitRequest to group coordinator. */

        rd_kafka_AddOffsetsToTxnRequest(rk->rk_eos.txn_coord,
                                        rk->rk_conf.eos.transactional_id,
                                        pid,
                                        rko->rko_u.txn.group_id,
                                        errstr, sizeof(errstr),
                                        RD_KAFKA_REPLYQ(rk->rk_ops, 0),
                                        rd_kafka_txn_handle_AddOffsetsToTxn,
                                        rko);

        return RD_KAFKA_OP_RES_KEEP; /* input rko is used for reply */

 err:
        rko->rko_err = err;
        rko->rko_u.txn.errstr = rd_strdup(errstr);

        rd_kafka_replyq_enq(&rko->rko_replyq, rko, 0);

        return RD_KAFKA_OP_RES_KEEP; /* input rko was used for reply */
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
        char *errstr, size_t errstr_size) {
        rd_kafka_op_t *rko;
        rd_kafka_op_t *reply;
        rd_kafka_resp_err_t err;
        rd_kafka_topic_partition_list_t *valid_offsets;

        if ((err = rd_kafka_ensure_transactional(rk, errstr, errstr_size)))
                return err;

        if (!consumer_group_id || !*consumer_group_id ||
            !offsets || offsets->cnt == 0) {
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
        rd_kafka_topic_partition_list_log(rk, "SENDOFFSETS", RD_KAFKA_DBG_EOS,
                                          rko->rko_u.txn.offsets);
        rko->rko_u.txn.group_id = rd_strdup(consumer_group_id);

        reply = rd_kafka_op_req(rk->rk_ops, rko, RD_POLL_INFINITE);

        if ((err = reply->rko_err))
                rd_snprintf(errstr, errstr_size, "%s",
                            reply->rko_u.txn.errstr ?
                            reply->rko_u.txn.errstr :
                            rd_kafka_err2str(err));

        rd_kafka_op_destroy(reply);

        return err;
}





/**
 * @brief Successfully complete the transaction.
 *
 * @locality rdkafka main thread
 * @locks rd_kafka_wrlock() MUST be held
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

        rd_kafka_txn_reply_app(rk, RD_KAFKA_RESP_ERR_NO_ERROR, NULL);
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
        int16_t ErrorCode;
        int actions = 0;
        rd_bool_t is_commit;

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
                actions |= RD_KAFKA_ERR_ACTION_REFRESH;
                break;

        case RD_KAFKA_RESP_ERR_INVALID_PRODUCER_EPOCH:
        case RD_KAFKA_RESP_ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED:
        case RD_KAFKA_RESP_ERR_INVALID_TXN_STATE:
        default:
                actions |= RD_KAFKA_ERR_ACTION_FATAL;
                break;
        }


        if (actions & RD_KAFKA_ERR_ACTION_FATAL) {
                // FIXME
                // RD_UT_COVERAGE();

        } else if (actions & RD_KAFKA_ERR_ACTION_REFRESH) {
                rd_rkb_dbg(rkb, EOS, "COMMITTXN",
                           "EndTxn %s failed: %s: refreshing coordinator",
                           is_commit ? "commit" : "abort",
                           rd_kafka_err2str(err));
                // FIXME
                //RD_UT_COVERAGE();
        }

        rd_kafka_wrunlock(rk);
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

        rd_kafka_op_reuse(rko);

        rd_kafka_wrlock(rk);

        if ((err = rd_kafka_txn_require_state(
                     rk, errstr, sizeof(errstr),
                     RD_KAFKA_TXN_STATE_BEGIN_COMMIT)))
                goto err;

        if (rk->rk_eos.txn_curr_op) {
                /* This might happen if application is calling conflicting
                 * transactional APIs simultaneously from different threads. */
                rd_snprintf(errstr, sizeof(errstr),
                            "Conflicting transactional call "
                            "already in progress");
                err = RD_KAFKA_RESP_ERR__CONFLICT;
                goto err;
        }

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
                                     rd_kafka_txn_handle_EndTxn, NULL);
        if (err)
                goto err;

        rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_COMMITTING_TRANSACTION);

        /* Store the op for later reply when we've received a response
         * for the EndTxn request. */
        rk->rk_eos.txn_curr_op = rko;

        rd_kafka_wrunlock(rk);

        return RD_KAFKA_OP_RES_KEEP; /* input rko is used for reply */

 err:
        rd_kafka_wrunlock(rk);

        rko->rko_err = err;
        rko->rko_u.txn.errstr = rd_strdup(errstr);

        rd_kafka_replyq_enq(&rko->rko_replyq, rko, 0);

        return RD_KAFKA_OP_RES_KEEP; /* input rko was used for reply */
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

        rd_kafka_op_reuse(rko);

        if ((err = rd_kafka_txn_require_state(
                     rk, errstr, sizeof(errstr),
                     RD_KAFKA_TXN_STATE_IN_TRANSACTION)))
                goto done;

        if (rk->rk_eos.txn_curr_op) {
                /* This might happen if application is calling conflicting
                 * transactional APIs simultaneously from different threads. */
                rd_snprintf(errstr, sizeof(errstr),
                            "Conflicting transactional call "
                            "already in progress");
                err = RD_KAFKA_RESP_ERR__CONFLICT;
                goto done;
        }

        rd_kafka_wrlock(rk);
        rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_BEGIN_COMMIT);
        rd_kafka_wrunlock(rk);

        /* FALLTHRU */
 done:

        rko->rko_err = err;
        if (err)
                rko->rko_u.txn.errstr = rd_strdup(errstr);

        rd_kafka_replyq_enq(&rko->rko_replyq, rko, 0);

        return RD_KAFKA_OP_RES_KEEP; /* input rko was used for reply */
}


/**
 * Commits the ongoing transaction. This method will flush any unsent records before actually committing the transaction.
 *
 * Further, if any of the {@link #send(ProducerRecord)} calls which were part of the transaction hit irrecoverable
 * errors, this method will throw the last received exception immediately and the transaction will not be committed.
 * So all {@link #send(ProducerRecord)} calls in a transaction must succeed in order for this method to succeed.
 *
 * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
 * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
 * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
 *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
 * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
 *         transactional.id is not authorized. See the exception for more details
 * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any
 *         other unexpected error
 */
rd_kafka_resp_err_t
rd_kafka_commit_transaction (rd_kafka_t *rk, int timeout_ms /*FIXME:handle*/,
                             char *errstr, size_t errstr_size) {
        rd_kafka_op_t *reply;
        rd_kafka_resp_err_t err;
        int txn_remains_ms;

        if ((err = rd_kafka_ensure_transactional(rk, errstr, errstr_size)))
                return err;

        /* The commit is in two phases:
         *   - begin commit: wait for outstanding messages to be produced,
         *                   disallow new messages from being produced
         *                   by application.
         *   - commit: commit transaction.
         */

        /* Begin commit */
        reply = rd_kafka_op_req(
                rk->rk_ops,
                rd_kafka_op_new_cb(rk, RD_KAFKA_OP_TXN,
                                   rd_kafka_txn_op_begin_commit),
                RD_POLL_INFINITE);

        if ((err = reply->rko_err))
                rd_snprintf(errstr, errstr_size, "%s",
                            reply->rko_u.txn.errstr);

        rd_kafka_op_destroy(reply);


        txn_remains_ms = rk->rk_conf.eos.transaction_timeout_ms; // FIXME

        /* Wait for queued messages to be delivered, limited by
         * the remaining transaction lifetime. */
        err = rd_kafka_flush(rk, txn_remains_ms);
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

                /* FIXME: What to do here? */
                return err;
        }


        /* Commit transaction */
        reply = rd_kafka_op_req(
                rk->rk_ops,
                rd_kafka_op_new_cb(rk, RD_KAFKA_OP_TXN,
                                   rd_kafka_txn_op_commit_transaction),
                RD_POLL_INFINITE);

        if ((err = reply->rko_err))
                rd_snprintf(errstr, errstr_size, "%s",
                            reply->rko_u.txn.errstr);

        rd_kafka_op_destroy(reply);

        return err;
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

        *errstr = '\0';

        rd_kafka_op_reuse(rko);

        rd_kafka_wrlock(rk);

        if ((err = rd_kafka_txn_require_state(
                     rk, errstr, sizeof(errstr),
                     RD_KAFKA_TXN_STATE_ABORTING_TRANSACTION)))
                goto err;

        if (rk->rk_eos.txn_curr_op) {
                /* This might happen if application is calling conflicting
                 * transactional APIs simultaneously from different threads. */
                rd_snprintf(errstr, sizeof(errstr),
                            "Conflicting transactional call "
                            "already in progress");
                err = RD_KAFKA_RESP_ERR__CONFLICT;
                goto err;
        }

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
                rd_kafka_dbg(rk, EOS, "ABORT",
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
                                     rd_kafka_txn_handle_EndTxn, NULL);
        if (err)
                goto err;

        /* Store the op for later reply when we've received a response
         * for the EndTxn request. */
        rk->rk_eos.txn_curr_op = rko;

        rd_kafka_wrunlock(rk);

        return RD_KAFKA_OP_RES_KEEP; /* input rko is used for reply */

 err:
        rd_kafka_wrunlock(rk);

        rko->rko_err = err;
        if (*errstr)
                rko->rko_u.txn.errstr = rd_strdup(errstr);

        rd_kafka_replyq_enq(&rko->rko_replyq, rko, 0);

        // FIXME: What state do we transition to?

        return RD_KAFKA_OP_RES_KEEP; /* input rko was used for reply */
}


rd_kafka_resp_err_t
rd_kafka_abort_transaction (rd_kafka_t *rk, int timeout_ms /* FIXME:handle */,
                            char *errstr, size_t errstr_size) {
        rd_kafka_op_t *reply;
        rd_kafka_resp_err_t err;
        int txn_remains_ms;

        if ((err = rd_kafka_ensure_transactional(rk, errstr, errstr_size)))
                return err;

        rd_kafka_wrlock(rk);
        if ((err = rd_kafka_txn_require_state(
                     rk, errstr, errstr_size,
                     RD_KAFKA_TXN_STATE_IN_TRANSACTION))) {
                rd_kafka_wrunlock(rk);
                return err;
        }

        txn_remains_ms = rk->rk_conf.eos.transaction_timeout_ms; // FIXME

        rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_ABORTING_TRANSACTION);

        rd_kafka_wrunlock(rk);

        mtx_lock(&rk->rk_eos.txn_pending_lock);
        rd_kafka_txn_clear_pending_partitions(rk);
        mtx_unlock(&rk->rk_eos.txn_pending_lock);

        /* Purge all queued messages.
         * Will need to wait for messages in-flight since purging these
         * messages may lead to gaps in the idempotent producer sequences. */
        err = rd_kafka_purge(rk,
                             RD_KAFKA_PURGE_F_QUEUE|
                             RD_KAFKA_PURGE_F_ABORT_TXN);

        /* Serve delivery reports for the purged messages */
        err = rd_kafka_flush(rk, txn_remains_ms);
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
                return err;
        }


        reply = rd_kafka_op_req(
                rk->rk_ops,
                rd_kafka_op_new_cb(rk, RD_KAFKA_OP_TXN,
                                   rd_kafka_txn_op_abort_transaction),
                RD_POLL_INFINITE);

        if ((err = reply->rko_err))
                rd_snprintf(errstr, errstr_size, "%s",
                            reply->rko_u.txn.errstr);

        rd_kafka_op_destroy(reply);

        return err;
}




/**
 * @brief Transactions manager destructor
 *
 * @locality rdkafka main thread
 * @locks none
 */
void rd_kafka_txns_term (rd_kafka_t *rk) {
        RD_IF_FREE(rk->rk_eos.txn_errstr, rd_free);

        rd_kafka_timer_stop(&rk->rk_timers,
                            &rk->rk_eos.txn_register_parts_tmr, 1);

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
}

