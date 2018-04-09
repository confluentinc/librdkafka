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
 * The underlying handling of the request is also completely asynchronous
 * inside librdkafka, for two reasons:
 *  - everything is async in librdkafka so adding something new that isn't
 *    would mean that existing functionality will need to be changed if
 *    it should be able to work simultaneously (such as statistics, timers,
 *    etc). There is no functional value to making the admin API
 *    synchronous internally, even if it would simplify its implementation.
 *    So making it async allows the Admin API to be used with existing
 *    client types in existing applications without breakage.
 *  - the async approach allows multiple outstanding Admin API requests
 *    simultaneously.
 *
 * The internal async implementation relies on the following concepts:
 *  - it uses a single rko (rd_kafka_op_t) to maintain state.
 *  - the rko has a callback attached - called the worker callback.
 *  - the worker callback is a small state machine that triggers
 *    async operations (be it controller lookups, timeout timers,
 *    protocol transmits, etc).
 *  - the worker callback is only called on the rdkafka main thread.
 *  - the callback is triggered by different events and sources by enqueuing
 *    the rko on the rdkafka man ops queue.
 *
 *
 * Let's illustrate this with a DeleteTopics example. This might look
 * daunting, but it boils down to an asynchronous state machine being
 * triggered by enqueuing the rko op.
 *
 *  1. [app thread] The user constructs the input arguments,
 *     including a response rkqu queue and then calls DeleteTopics().
 *
 *  2. [app thread] DeleteTopics() creates a new internal op (rko) of type
 *     RD_KAFKA_OP_CREATETOPICS, makes a **copy** on the rko of all the
 *     input arguments (which allows the caller to free the originals
 *     whenever she likes). The rko op worker callback is set to the
 *     DeleteTopics asynchronous worker callback rd_kafka_DeleteTopics_worker().
 *
 *  3. [app thread] DeleteTopics() enqueues the rko on librdkafka's main ops
 *     queue that is served by the rdkafka main thread in rd_kafka_thread_main()
 *
 *  4. [rdkafka main thread] The rko is dequeued by rd_kafka_q_serve and
 *     the rd_kafka_poll_cb() is called.
 *
 *  5. [rdkafka main thread] The rko_type switch case identifies the rko
 *     as an RD_KAFKA_OP_DELETETOPICS which is served by the op callback
 *     set in step 2.
 *
 *  6. [rdkafka main thread] rd_kafka_DeleteTopics_worker() (the worker cb)
 *     is called. After some initial checking of err==ERR__DESTROY events
 *     (which is used to clean up outstanding ops (etc) on termination),
 *     the code hits a state machine using rko_u.admin.request_state.
 *
 *  7. [rdkafka main thread] The initial state is RD_KAFKA_ADMIN_STATE_INIT
 *     where the worker validates the user input.
 *     An enqueue once (eonce) object is created - the use of this object
 *     allows having multiple outstanding async functions referencing the
 *     same underlying rko object, but only allowing the first one
 *     to trigger an event.
 *     A timeout timer is set up to trigger the eonce object when the
 *     full options.request_timeout has elapsed.
 *
 *  8. [rdkafka main thread] After initialization the state is updated
 *     to WAIT_CONTROLLER and the code falls through to looking up
 *     the controller broker and waiting for an active connection.
 *     Both the lookup and the waiting for an active connection are
 *     fully asynchronous, and the same eonce used for the timer is passed
 *     to the rd_kafka_broker_controller_async() function which will
 *     trigger the eonce when a broker state change occurs.
 *     If the controller is already known (from metadata) and the connection
 *     is up a rkb broker object is returned and the eonce is not used,
 *     skip to step 11.
 *
 *  9. [rdkafka main thread] Upon metadata retrieval (which is triggered
 *     automatically by other parts of the code) the controller_id may be
 *     updated in which case the eonce is triggered.
 *     The eonce triggering enqueues the original rko on the rdkafka main
 *     ops queue again and we go to step 8 which will check if the controller
 *     connection is up.
 *
 * 10. [broker thread] If the controller_id is now known we wait for
 *     the corresponding broker's connection to come up. This signaling
 *     is performed from the broker thread upon broker state changes
 *     and uses the same eonce. The eonce triggering enqueues the original
 *     rko on the rdkafka main ops queue again we go to back to step 8
 *     to check if broker is now available.
 *
 * 11. [rdkafka main thread] Back in the worker callback we now have an
 *     rkb broker pointer (with reference count increased) for the controller
 *     with the connection up (it might go down while we're referencing it,
 *     but that does not stop us from enqueuing a protocol request).
 *
 * 12. [rdkafka main thread] A DeleteTopics protocol request buffer is
 *     constructed using the input parameters saved on the rko and the
 *     buffer is enqueued on the broker's transmit queue.
 *     The buffer is set up to provide the reply buffer on the rdkafka main
 *     ops queue (the same queue we are operating from) with a handler
 *     callback of rd_kafka_admin_DeleteTopics_handle_response().
 *     The state is updated to the RD_KAFKA_ADMIN_STATE_WAIT_RESPONSE.
 *
 * 13. [broker thread] If the request times out, a response with error code
 *     (ERR__TIMED_OUT) is enqueued. Go to 16.
 *
 * 14. [broker thread] If a response is received, the response buffer
 *     is enqueued. Go to 16.
 *
 * 15. [rdkafka main thread] The buffer callback (..handle_response())
 *     is called, which attempts to extra the original rko from the eonce,
 *     but if the eonce has already been triggered by some other source
 *     (the timeout timer) the buffer callback simply returns and does nothing
 *     since the admin request is over and a result (probably a timeout)
 *     has been enqueued for the application.
 *     If the rko was still intact we temporarily set the reply buffer
 *     in the rko struct and call the worker callback. Go to 17.
 *
 * 16. [rdkafka main thread] The worker callback is called in state
 *     RD_KAFKA_ADMIN_STATE_WAIT_RESPONSE without a response but with an error.
 *     An error result op is created and enqueued on the application's
 *     provided response rkqu queue.
 *     
 * 17. [rdkafka main thread] The worker callback is called din state
 *     RD_KAFKA_ADMIN_STATE_WAIT_RESPONSE with a response buffer with no
 *     error set.
 *     The worker calls DeleteTopics_parse_response() to parse the response
 *     buffer and populates a result op (rko_result) with the response
 *     information (such as per-topic error codes, etc).
 *     The result op is returned to the worker.
 *
 * 18. [rdkafka main thread] The worker enqueues the result up (rko_result)
 *     on the application's provided response rkqu queue.
 *
 * 19. [app thread] The application calls rd_kafka_queue_poll() to
 *     receive the result of the operation. The result may have been
 *     enqueued in step 18 thanks to succesful completion, or in any
 *     of the earlier stages when an error was encountered.
 *
 * 20. [app thread] The application uses rd_kafka_event_DeleteTopics_result()
 *     to retrieve the request-specific result type.
 *
 * 21. Done, so easy.
 *     
 */



static void rd_kafka_AdminOptions_init (rd_kafka_t *rk,
                                        rd_kafka_AdminOptions_t *options);


/**
 * @name Common admin request code
 * @{
 *
 *
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



/**
 * @brief Helper for rd_kafka_<..>_result_error() method.
 */
static rd_kafka_resp_err_t
rd_kafka_admin_result_ret_error (const rd_kafka_op_t *rko,
                                 const char **errstrp) {

        if (errstrp) {
                if (rko->rko_err)
                        *errstrp = rko->rko_u.admin_result.errstr;
                else
                        *errstrp = NULL;
        }

        return rko->rko_err;
}


static const rd_kafka_topic_result_t **
rd_kafka_admin_result_ret_topics (const rd_kafka_op_t *rko,
                                  size_t *cntp) {
        *cntp = rd_list_cnt(&rko->rko_u.admin_result.topics);
        return (const rd_kafka_topic_result_t **)rko->rko_u.admin_result.
                topics.rl_elems;
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
 * @brief Common worker state machine handling regardless of request type.
 *
 * Tasks:
 *  - Sets up timeout on first call.
 *  - Checks for timeout.
 *  - Checks for and fails on errors.
 *
 * @remark All errors are handled by this function and it will
 *         never return != -1 if an rko_err was set. Thus there is no need
 *         for error checking in the caller.
 *
 * @returns -1 if worker must `goto destroy;` label, else 0.
 */
static int rd_kafka_admin_common_worker (rd_kafka_t *rk, rd_kafka_op_t *rko) {
        const char *name = rd_kafka_op2str(rko->rko_type);
        rd_ts_t timeout_in;

        rd_kafka_dbg(rk, ADMIN, name,
                     "%s worker called in state %s: %s",
                     name,
                     rd_kafka_admin_state_desc[rko->rko_u.admin_request.state],
                     rd_kafka_err2str(rko->rko_err));

        if (rko->rko_err == RD_KAFKA_RESP_ERR__DESTROY)
                return -1; /* Terminating */

        rd_assert(thrd_is_current(rko->rko_rk->rk_thread));

        if (rko->rko_err) {
                rd_kafka_admin_result_fail(
                        rko, rko->rko_err,
                        "Failed while %s: %s",
                        rd_kafka_admin_state_desc[rko->rko_u.
                                                  admin_request.state],
                        rd_kafka_err2str(rko->rko_err));
                return -1;
        }

        /* Check for timeout */
        timeout_in = rd_timeout_remains_us(rko->rko_u.admin_request.
                                           abs_timeout);

        if (timeout_in <= 0) {
                rd_kafka_admin_result_fail(
                        rko, RD_KAFKA_RESP_ERR__TIMED_OUT,
                        "Timed out %s",
                        rd_kafka_admin_state_desc[rko->rko_u.
                                                  admin_request.state]);
                return -1;
        }

        switch (rko->rko_u.admin_request.state)
        {
        case RD_KAFKA_ADMIN_STATE_INIT:
                /* First call.
                 * Set up timeout timer. */
                rd_kafka_enq_once_add_source(rko->rko_u.admin_request.eonce,
                                             "timeout timer");
                rd_kafka_timer_start_oneshot(&rk->rk_timers,
                                             &rko->rko_u.admin_request.tmr,
                                             timeout_in,
                                             rd_kafka_admin_eonce_timeout_cb,
                                             rko->rko_u.admin_request.eonce);
                break;

        default:
                break;
        }

        return 0;
}


/**
 * @brief Common worker destroy to be called in destroy: label
 *        in worker.
 */
static void rd_kafka_admin_common_worker_destroy (rd_kafka_t *rk,
                                                  rd_kafka_op_t *rko) {
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
}



/**
 * @brief Asynchronously look up the controller.
 *        To be called repeatedly from each invocation of the worker
 *        when in state RD_KAFKA_ADMIN_STATE_WAIT_CONTROLLER until
 *        a valid rkb is returned.
 *
 * @returns the controller rkb with refcount increased, or NULL if not yet
 *          available.
 */
static rd_kafka_broker_t *
rd_kafka_admin_common_get_controller (rd_kafka_t *rk,
                                      rd_kafka_op_t *rko) {
        rd_kafka_broker_t *rkb;

        rd_kafka_dbg(rk, ADMIN, "ADMIN", "%s: looking up controller",
                     rd_kafka_op2str(rko->rko_type));

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
         * When we are called again we perform the controller lookup
         * again and hopefully get an rkb back, otherwise defer a new
         * async wait. Repeat until success or timeout. */
        if (!(rkb = rd_kafka_broker_controller_async(
                      rk, RD_KAFKA_BROKER_STATE_UP,
                      rko->rko_u.admin_request.eonce))) {
                /* Controller not available, wait asynchronously
                 * for controller code to trigger eonce. */
                return NULL;
        }

        rd_kafka_dbg(rk, ADMIN, "ADMIN", "%s: controller %s",
                     rd_kafka_op2str(rko->rko_type), rkb->rkb_name);

        return rkb;
}



/**
 * @brief Handle response from broker by triggering worker callback.
 *
 * @param opaque is the eonce from the worker protocol request call.
 */
static void rd_kafka_admin_handle_response (rd_kafka_t *rk,
                                            rd_kafka_broker_t *rkb,
                                            rd_kafka_resp_err_t err,
                                            rd_kafka_buf_t *reply,
                                            rd_kafka_buf_t *request,
                                            void *opaque) {
        rd_kafka_enq_once_t *eonce = opaque;
        rd_kafka_op_t *rko;

        /* From "send <protocolrequest>" */
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


/**@}*/


/**
 * @name Misc helpers
 * @{
 *
 *
 */


/**@}*/



/**
 * @name Generic AdminOptions
 * @{
 *
 *
 */

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

/**@}*/






/**
 * @name CreateTopics
 * @{
 *
 *
 *
 */



rd_kafka_NewTopic_t *
rd_kafka_NewTopic_new (const char *topic,
                       int num_partitions,
                       int replication_factor) {
        rd_kafka_NewTopic_t *new_topic;

        if (!topic ||
            num_partitions < 1 || num_partitions > RD_KAFKAP_PARTITIONS_MAX ||
            replication_factor < -1 ||
            replication_factor > RD_KAFKAP_BROKERS_MAX)
                return NULL;

        new_topic = rd_calloc(1, sizeof(*new_topic));
        new_topic->topic = rd_strdup(topic);
        new_topic->num_partitions = num_partitions;
        new_topic->replication_factor = replication_factor;

        /* List of int32 lists */
        rd_list_init(&new_topic->replicas, 0, rd_list_destroy_free);
        rd_list_prealloc_elems(&new_topic->replicas, 0,
                               num_partitions, 0/*nozero*/);

        /* List of strtups */
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
 * @brief Allocate a new NewTopic and make a copy of \p src
 */
static rd_kafka_NewTopic_t *
rd_kafka_NewTopic_copy (const rd_kafka_NewTopic_t *src) {
        rd_kafka_NewTopic_t *dst;

        dst = rd_kafka_NewTopic_new(src->topic, src->num_partitions,
                                    src->replication_factor);

        rd_list_destroy(&dst->replicas); /* created in .._new() */
        rd_list_init_copy(&dst->replicas, &src->replicas);
        rd_list_copy_to(&dst->replicas, &src->replicas,
                        rd_list_copy_preallocated, NULL);

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

        /* Do not allow overwrites */
        if (rd_list_elem(&new_topic->replicas, partition))
                return RD_KAFKA_RESP_ERR__CONFLICT;

        rl = rd_list_init_int32(rd_list_alloc(), broker_id_cnt);

        for (i = 0 ; i < (int)broker_id_cnt ; i++)
                rd_list_set_int32(rl, i, broker_ids[i]);

        rd_list_set(&new_topic->replicas, partition, rl);

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
                char *errstr = NULL;
                rd_kafka_topic_result_t *terr;
                rd_kafka_NewTopic_t skel;
                int orig_pos;

                rd_kafka_buf_read_str(reply, &ktopic);
                rd_kafka_buf_read_i16(reply, &error_code);

                if (rd_kafka_buf_ApiVersion(reply) >= 1)
                        rd_kafka_buf_read_str(reply, &error_msg);

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

                if (error_code) {
                        if (RD_KAFKAP_STR_IS_NULL(&error_msg) ||
                            RD_KAFKAP_STR_LEN(&error_msg) == 0)
                                errstr = (char *)rd_kafka_err2str(error_code);
                        else
                                RD_KAFKAP_STR_DUPA(&errstr, &error_msg);

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
        rd_kafka_broker_t *rkb;
        rd_kafka_op_t *rko_result;
        rd_kafka_resp_err_t err;
        char errstr[512];

        /* Common worker handling. */
        if (rd_kafka_admin_common_worker(rk, rko) == -1)
                goto destroy;

        switch (rko->rko_u.admin_request.state)
        {
        case RD_KAFKA_ADMIN_STATE_INIT:
                /* First call, fall thru to controller lookup */
                rko->rko_u.admin_request.state =
                        RD_KAFKA_ADMIN_STATE_WAIT_CONTROLLER;
                /*FALLTHRU*/

        case RD_KAFKA_ADMIN_STATE_WAIT_CONTROLLER:
                if (!(rkb = rd_kafka_admin_common_get_controller(rk, rko))) {
                        /* Waiting for controller. */
                        return RD_KAFKA_OP_RES_KEEP;
                }

                /* Got controller, send protocol request. */

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
                        rd_kafka_admin_handle_response,
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
                 * trigger the eonce and worker to be called again. */
                return RD_KAFKA_OP_RES_KEEP;


        case RD_KAFKA_ADMIN_STATE_WAIT_RESPONSE:
                /* CreateTopics response */

                rd_kafka_enq_once_del_source(rko->rko_u.admin_request.eonce,
                                             "send CreateTopics");

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
        rd_kafka_admin_common_worker_destroy(rk, rko);
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
        return rd_kafka_admin_result_ret_error((const rd_kafka_op_t *)result,
                                               errstrp);
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
        return rd_kafka_admin_result_ret_topics((const rd_kafka_op_t *)result,
                                                cntp);
}

/**@}*/




/**
 * @name Delete topics
 * @{
 *
 *
 *
 *
 */

rd_kafka_DeleteTopic_t *rd_kafka_DeleteTopic_new (const char *topic) {
        size_t tsize = strlen(topic) + 1;
        rd_kafka_DeleteTopic_t *del_topic;

        /* Single allocation */
        del_topic = rd_malloc(sizeof(*del_topic) + tsize);
        del_topic->topic = del_topic->data;
        memcpy(del_topic->topic, topic, tsize);

        return del_topic;
}

void rd_kafka_DeleteTopic_destroy (rd_kafka_DeleteTopic_t *del_topic) {
        rd_free(del_topic);
}

static void rd_kafka_DeleteTopic_free (void *ptr) {
        rd_kafka_DeleteTopic_destroy(ptr);
}


void rd_kafka_DeleteTopic_destroy_array (rd_kafka_DeleteTopic_t **del_topics,
                                         size_t del_topic_cnt) {
        size_t i;
        for (i = 0 ; i < del_topic_cnt ; i++)
                rd_kafka_DeleteTopic_destroy(del_topics[i]);
}


/**
 * @brief Topic name comparator for DeleteTopic_t
 */
static int rd_kafka_DeleteTopic_cmp (const void *_a, const void *_b) {
        const rd_kafka_DeleteTopic_t *a = _a, *b = _b;
        return strcmp(a->topic, b->topic);
}

/**
 * @brief Allocate a new DeleteTopic and make a copy of \p src
 */
static rd_kafka_DeleteTopic_t *
rd_kafka_DeleteTopic_copy (const rd_kafka_DeleteTopic_t *src) {
        return rd_kafka_DeleteTopic_new(src->topic);
}







/**
 * @brief Parse DeleteTopicsResponse and create ADMIN_RESULT op.
 */
static rd_kafka_resp_err_t
rd_kafka_admin_DeleteTopics_parse_response (rd_kafka_op_t *rko_req,
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

        if (rd_kafka_buf_ApiVersion(reply) >= 1) {
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
                rd_kafka_topic_result_t *terr;
                rd_kafka_NewTopic_t skel;
                int orig_pos;

                rd_kafka_buf_read_str(reply, &ktopic);
                rd_kafka_buf_read_i16(reply, &error_code);

                /* For non-blocking DeleteTopicsRequests the broker
                 * will returned REQUEST_TIMED_OUT for topics
                 * that were triggered for creation -
                 * we hide this error code from the application
                 * since the topic creation is in fact in progress. */
                if (error_code == RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT &&
                    rd_kafka_confval_get_int(&rko_req->rko_u.
                                             admin_request.options.
                                             operation_timeout) <= 0) {
                        error_code = RD_KAFKA_RESP_ERR_NO_ERROR;
                }

                terr = rd_kafka_topic_result_new(ktopic.str,
                                                 RD_KAFKAP_STR_LEN(&ktopic),
                                                 error_code,
                                                 error_code ?
                                                 rd_kafka_err2str(error_code) :
                                                 NULL);

                /* As a convenience to the application we insert topic result
                 * in the same order as they were requested. The broker
                 * does not maintain ordering unfortunately. */
                skel.topic = terr->topic;
                orig_pos = rd_list_index(&rko_req->rko_u.admin_request.args,
                                         &skel, rd_kafka_DeleteTopic_cmp);
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
                    "DeleteTopics response protocol parse failure: %s",
                    rd_kafka_err2str(err));

        return err;
}





/**
 * @brief Asynchronous worker for DeleteTopics
 */
static rd_kafka_op_res_t
rd_kafka_admin_DeleteTopics_worker (rd_kafka_t *rk,
                                    rd_kafka_q_t *rkq,
                                    rd_kafka_op_t *rko) {
        rd_kafka_broker_t *rkb;
        rd_kafka_op_t *rko_result;
        rd_kafka_resp_err_t err;
        char errstr[512];

        /* Common worker handling. */
        if (rd_kafka_admin_common_worker(rk, rko) == -1)
                goto destroy;

        switch (rko->rko_u.admin_request.state)
        {
        case RD_KAFKA_ADMIN_STATE_INIT:
                /* First call, fall thru to controller lookup */
                rko->rko_u.admin_request.state =
                        RD_KAFKA_ADMIN_STATE_WAIT_CONTROLLER;
                /*FALLTHRU*/

        case RD_KAFKA_ADMIN_STATE_WAIT_CONTROLLER:
                if (!(rkb = rd_kafka_admin_common_get_controller(rk, rko))) {
                        /* Waiting for controller. */
                        return RD_KAFKA_OP_RES_KEEP;
                }

                /* Got controller, send protocol request. */

                /* Still need to use the eonce since this worker may
                 * time out while waiting for response from broker, in which
                 * case the broker response will hit an empty eonce (ok). */
                rd_kafka_enq_once_add_source(rko->rko_u.admin_request.eonce,
                                             "send DeleteTopics");

                /* Send request (async) */
                err = rd_kafka_DeleteTopicsRequest(
                        rkb,
                        &rko->rko_u.admin_request.args,
                        &rko->rko_u.admin_request.options,
                        errstr, sizeof(errstr),
                        RD_KAFKA_REPLYQ(rk->rk_ops, 0),
                        rd_kafka_admin_handle_response,
                        rko->rko_u.admin_request.eonce);

                /* Loose refcount from controller_async() */
                rd_kafka_broker_destroy(rkb);

                if (err) {
                        rd_kafka_enq_once_del_source(
                                rko->rko_u.admin_request.eonce,
                                "send DeleteTopics");
                        rd_kafka_admin_result_fail(rko, err, "%s", errstr);
                        goto destroy;
                }

                rko->rko_u.admin_request.state =
                        RD_KAFKA_ADMIN_STATE_WAIT_RESPONSE;

                /* Wait asynchronously for broker response, which will
                 * trigger the eonce and worker to be called again. */
                return RD_KAFKA_OP_RES_KEEP;


        case RD_KAFKA_ADMIN_STATE_WAIT_RESPONSE:
                /* DeleteTopics response */

                rd_kafka_enq_once_del_source(rko->rko_u.admin_request.eonce,
                                             "send DeleteTopics");

                /* Parse response and populate result to application */
                err = rd_kafka_admin_DeleteTopics_parse_response(
                        rko, &rko_result,
                        rko->rko_u.admin_request.reply_buf,
                        errstr, sizeof(errstr));
                if (err) {
                        rd_kafka_admin_result_fail(
                                rko, err,
                                "DeleteTopics worker failed parse response: %s",
                                errstr);
                        goto destroy;
                }

                /* Enqueue result on application queue, we're done. */
                rd_kafka_admin_result_enq(rko, rko_result);

                goto destroy;
        }


        return RD_KAFKA_OP_RES_KEEP;

 destroy:
        rd_kafka_admin_common_worker_destroy(rk, rko);
        return RD_KAFKA_OP_RES_HANDLED; /* trigger's op_destroy() */
}


void rd_kafka_admin_DeleteTopics (rd_kafka_t *rk,
                                  rd_kafka_DeleteTopic_t **del_topics,
                                  size_t del_topic_cnt,
                                  const rd_kafka_AdminOptions_t *options,
                                  rd_kafka_queue_t *rkqu) {
        rd_kafka_op_t *rko;
        size_t i;

        rko = rd_kafka_admin_request_op_new(rk,
                                            RD_KAFKA_OP_DELETETOPICS,
                                            RD_KAFKA_EVENT_DELETETOPICS_RESULT,
                                            rd_kafka_admin_DeleteTopics_worker,
                                            options, rkqu);

        rd_list_init(&rko->rko_u.admin_request.args, del_topic_cnt,
                     rd_kafka_DeleteTopic_free);

        for (i = 0 ; i < del_topic_cnt ; i++)
                rd_list_add(&rko->rko_u.admin_request.args,
                            rd_kafka_DeleteTopic_copy(del_topics[i]));

        rd_kafka_q_enq(rk->rk_ops, rko);
}


rd_kafka_resp_err_t
rd_kafka_DeleteTopics_result_error (
        const rd_kafka_DeleteTopics_result_t *result,
        const char **errstrp) {
        return rd_kafka_admin_result_ret_error((const rd_kafka_op_t *)result,
                                               errstrp);
}



/**
 * @brief Get an array of topic results from a DeleteTopics result.
 *
 * The returned \p topics life-time is the same as the \p result object.
 * @param cntp is updated to the number of elements in the array.
 */
const rd_kafka_topic_result_t **
rd_kafka_DeleteTopics_result_topics (
        const rd_kafka_DeleteTopics_result_t *result,
        size_t *cntp) {
        return rd_kafka_admin_result_ret_topics((const rd_kafka_op_t *)result,
                                                cntp);
}




/**
 * @name Create partitions
 * @{
 *
 *
 *
 *
 */

rd_kafka_NewPartitions_t *rd_kafka_NewPartitions_new (const char *topic,
                                                      size_t new_total_cnt) {
        size_t tsize = strlen(topic) + 1;
        rd_kafka_NewPartitions_t *newps;

        /* Single allocation */
        newps = rd_malloc(sizeof(*newps) + tsize);
        newps->total_cnt = new_total_cnt;
        newps->topic = newps->data;
        memcpy(newps->topic, topic, tsize);

        /* List of int32 lists */
        rd_list_init(&newps->replicas, 0, rd_list_destroy_free);
        rd_list_prealloc_elems(&newps->replicas, 0, new_total_cnt, 0/*nozero*/);

        return newps;
}

/**
 * @brief Topic name comparator for NewPartitions_t
 */
static int rd_kafka_NewPartitions_cmp (const void *_a, const void *_b) {
        const rd_kafka_NewPartitions_t *a = _a, *b = _b;
        return strcmp(a->topic, b->topic);
}


/**
 * @brief Allocate a new CreatePartitions and make a copy of \p src
 */
static rd_kafka_NewPartitions_t *
rd_kafka_NewPartitions_copy (const rd_kafka_NewPartitions_t *src) {
        rd_kafka_NewPartitions_t *dst;

        dst = rd_kafka_NewPartitions_new(src->topic, src->total_cnt);

        rd_list_destroy(&dst->replicas); /* created in .._new() */
        rd_list_init_copy(&dst->replicas, &src->replicas);
        rd_list_copy_to(&dst->replicas, &src->replicas,
                        rd_list_copy_preallocated, NULL);

        return dst;
}

void rd_kafka_NewPartitions_destroy (rd_kafka_NewPartitions_t *newps) {
        rd_list_destroy(&newps->replicas);
        rd_free(newps);
}

static void rd_kafka_NewPartitions_free (void *ptr) {
        rd_kafka_NewPartitions_destroy(ptr);
}


void rd_kafka_NewPartitions_destroy_array (rd_kafka_NewPartitions_t **newps,
                                         size_t newps_cnt) {
        size_t i;
        for (i = 0 ; i < newps_cnt ; i++)
                rd_kafka_NewPartitions_destroy(newps[i]);
}





rd_kafka_resp_err_t
rd_kafka_NewPartitions_set_replica_assignment (rd_kafka_NewPartitions_t *newp,
                                               int32_t new_partition_idx,
                                               int32_t *broker_ids,
                                               size_t broker_id_cnt) {
        rd_list_t *rl;
        int i;

        /* Replica partitions must be added consecutively starting from 0. */
        if (new_partition_idx < 0 ||
            new_partition_idx != rd_list_cnt(&newp->replicas) ||
            broker_id_cnt > RD_KAFKAP_BROKERS_MAX)
                return RD_KAFKA_RESP_ERR__INVALID_ARG;

        rl = rd_list_init_int32(rd_list_alloc(), broker_id_cnt);

        for (i = 0 ; i < (int)broker_id_cnt ; i++)
                rd_list_set_int32(rl, i, broker_ids[i]);

        rd_list_add(&newp->replicas, rl);

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}




/**
 * @brief Parse CreatePartitionsResponse and create ADMIN_RESULT op.
 */
static rd_kafka_resp_err_t
rd_kafka_admin_CreatePartitions_parse_response (rd_kafka_op_t *rko_req,
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
        int32_t Throttle_Time;

        rd_kafka_buf_read_i32(reply, &Throttle_Time);
        rd_kafka_op_throttle_time(rkb, rk->rk_rep, Throttle_Time);

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
                char *errstr = NULL;
                rd_kafka_topic_result_t *terr;
                rd_kafka_NewTopic_t skel;
                rd_kafkap_str_t error_msg;
                int orig_pos;

                rd_kafka_buf_read_str(reply, &ktopic);
                rd_kafka_buf_read_i16(reply, &error_code);
                rd_kafka_buf_read_str(reply, &error_msg);

                /* For non-blocking CreatePartitionsRequests the broker
                 * will returned REQUEST_TIMED_OUT for topics
                 * that were triggered for creation -
                 * we hide this error code from the application
                 * since the topic creation is in fact in progress. */
                if (error_code == RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT &&
                    rd_kafka_confval_get_int(&rko_req->rko_u.
                                             admin_request.options.
                                             operation_timeout) <= 0) {
                        error_code = RD_KAFKA_RESP_ERR_NO_ERROR;
                }

                if (error_code) {
                        if (RD_KAFKAP_STR_IS_NULL(&error_msg) ||
                            RD_KAFKAP_STR_LEN(&error_msg) == 0)
                                errstr = (char *)rd_kafka_err2str(error_code);
                        else
                                RD_KAFKAP_STR_DUPA(&errstr, &error_msg);
                }


                terr = rd_kafka_topic_result_new(ktopic.str,
                                                 RD_KAFKAP_STR_LEN(&ktopic),
                                                 error_code,
                                                 error_code ?
                                                 rd_kafka_err2str(error_code) :
                                                 NULL);

                /* As a convenience to the application we insert topic result
                 * in the same order as they were requested. The broker
                 * does not maintain ordering unfortunately. */
                skel.topic = terr->topic;
                orig_pos = rd_list_index(&rko_req->rko_u.admin_request.args,
                                         &skel, rd_kafka_NewPartitions_cmp);
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
                    "CreatePartitions response protocol parse failure: %s",
                    rd_kafka_err2str(err));

        return err;
}





/**
 * @brief Asynchronous worker for CreatePartitions
 */
static rd_kafka_op_res_t
rd_kafka_admin_CreatePartitions_worker (rd_kafka_t *rk,
                                    rd_kafka_q_t *rkq,
                                    rd_kafka_op_t *rko) {
        rd_kafka_broker_t *rkb;
        rd_kafka_op_t *rko_result;
        rd_kafka_resp_err_t err;
        char errstr[512];

        /* Common worker handling. */
        if (rd_kafka_admin_common_worker(rk, rko) == -1)
                goto destroy;

        switch (rko->rko_u.admin_request.state)
        {
        case RD_KAFKA_ADMIN_STATE_INIT:
                /* First call, fall thru to controller lookup */
                rko->rko_u.admin_request.state =
                        RD_KAFKA_ADMIN_STATE_WAIT_CONTROLLER;
                /*FALLTHRU*/

        case RD_KAFKA_ADMIN_STATE_WAIT_CONTROLLER:
                if (!(rkb = rd_kafka_admin_common_get_controller(rk, rko))) {
                        /* Waiting for controller. */
                        return RD_KAFKA_OP_RES_KEEP;
                }

                /* Got controller, send protocol request. */

                /* Still need to use the eonce since this worker may
                 * time out while waiting for response from broker, in which
                 * case the broker response will hit an empty eonce (ok). */
                rd_kafka_enq_once_add_source(rko->rko_u.admin_request.eonce,
                                             "send CreatePartitions");

                /* Send request (async) */
                err = rd_kafka_CreatePartitionsRequest(
                        rkb,
                        &rko->rko_u.admin_request.args,
                        &rko->rko_u.admin_request.options,
                        errstr, sizeof(errstr),
                        RD_KAFKA_REPLYQ(rk->rk_ops, 0),
                        rd_kafka_admin_handle_response,
                        rko->rko_u.admin_request.eonce);

                /* Loose refcount from controller_async() */
                rd_kafka_broker_destroy(rkb);

                if (err) {
                        rd_kafka_enq_once_del_source(
                                rko->rko_u.admin_request.eonce,
                                "send CreatePartitions");
                        rd_kafka_admin_result_fail(rko, err, "%s", errstr);
                        goto destroy;
                }

                rko->rko_u.admin_request.state =
                        RD_KAFKA_ADMIN_STATE_WAIT_RESPONSE;

                /* Wait asynchronously for broker response, which will
                 * trigger the eonce and worker to be called again. */
                return RD_KAFKA_OP_RES_KEEP;


        case RD_KAFKA_ADMIN_STATE_WAIT_RESPONSE:
                /* CreatePartitions response */

                rd_kafka_enq_once_del_source(rko->rko_u.admin_request.eonce,
                                             "send CreatePartitions");

                /* Parse response and populate result to application */
                err = rd_kafka_admin_CreatePartitions_parse_response(
                        rko, &rko_result,
                        rko->rko_u.admin_request.reply_buf,
                        errstr, sizeof(errstr));
                if (err) {
                        rd_kafka_admin_result_fail(
                                rko, err,
                                "CreatePartitions worker failed parse response: %s",
                                errstr);
                        goto destroy;
                }

                /* Enqueue result on application queue, we're done. */
                rd_kafka_admin_result_enq(rko, rko_result);

                goto destroy;
        }


        return RD_KAFKA_OP_RES_KEEP;

 destroy:
        rd_kafka_admin_common_worker_destroy(rk, rko);
        return RD_KAFKA_OP_RES_HANDLED; /* trigger's op_destroy() */
}


void rd_kafka_admin_CreatePartitions (rd_kafka_t *rk,
                                      rd_kafka_NewPartitions_t **newps,
                                      size_t newps_cnt,
                                      const rd_kafka_AdminOptions_t *options,
                                      rd_kafka_queue_t *rkqu) {
        rd_kafka_op_t *rko;
        size_t i;

        rko = rd_kafka_admin_request_op_new(
                rk,
                RD_KAFKA_OP_CREATEPARTITIONS,
                RD_KAFKA_EVENT_CREATEPARTITIONS_RESULT,
                rd_kafka_admin_CreatePartitions_worker,
                options, rkqu);

        rd_list_init(&rko->rko_u.admin_request.args, newps_cnt,
                     rd_kafka_NewPartitions_free);

        for (i = 0 ; i < newps_cnt ; i++)
                rd_list_add(&rko->rko_u.admin_request.args,
                            rd_kafka_NewPartitions_copy(newps[i]));

        rd_kafka_q_enq(rk->rk_ops, rko);
}


rd_kafka_resp_err_t
rd_kafka_CreatePartitions_result_error (
        const rd_kafka_CreatePartitions_result_t *result,
        const char **errstrp) {
        return rd_kafka_admin_result_ret_error((const rd_kafka_op_t *)result,
                                               errstrp);
}



/**
 * @brief Get an array of topic results from a CreatePartitions result.
 *
 * The returned \p topics life-time is the same as the \p result object.
 * @param cntp is updated to the number of elements in the array.
 */
const rd_kafka_topic_result_t **
rd_kafka_CreatePartitions_result_topics (
        const rd_kafka_CreatePartitions_result_t *result,
        size_t *cntp) {
        return rd_kafka_admin_result_ret_topics((const rd_kafka_op_t *)result,
                                                cntp);
}


/**@}*/
