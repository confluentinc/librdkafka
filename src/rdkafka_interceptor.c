/*
 * librdkafka - The Apache Kafka C/C++ library
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

#include "rdkafka_int.h"
#include "rdkafka_interceptor.h"


/**
 * @brief Interceptor methodtion/method reference
 */
typedef struct rd_kafka_interceptor_method_s {
        union {
                rd_kafka_interceptor_f_on_send_t    *on_send;
                rd_kafka_interceptor_f_on_acknowledgement_t *on_acknowledgement;
                rd_kafka_interceptor_f_on_consume_t *on_consume;
                rd_kafka_interceptor_f_on_commit_t  *on_commit;
                void *generic; /* For easy assignment */

        } u;
        char *ic_name;
        void *ic_opaque;
} rd_kafka_interceptor_method_t;

/**
 * @brief Destroy interceptor methodtion reference
 */
static void
rd_kafka_interceptor_method_destroy (void *ptr) {
        rd_kafka_interceptor_method_t *method = ptr;
        rd_free(method->ic_name);
        rd_free(method);
}





/**
 * @brief Handle an interceptor on_... methodtion call failures.
 */
static RD_INLINE void
rd_kafka_interceptor_failed (rd_kafka_t *rk,
                             const rd_kafka_interceptor_method_t *method,
                             const char *method_name, rd_kafka_resp_err_t err,
                             const rd_kafka_message_t *rkmessage) {

        /* FIXME: Suppress log messages, eventually */
        if (rkmessage)
                rd_kafka_log(rk, LOG_WARNING, "ICFAIL",
                             "Interceptor %s failed %s for "
                             "message on %s [%"PRId32"] @ %"PRId64
                             ": %s",
                             method->ic_name, method_name,
                             rd_kafka_topic_a2i(rkmessage->rkt)->rkt_topic->str,
                             rkmessage->partition,
                             rkmessage->offset,
                             rd_kafka_err2str(err));
        else
                rd_kafka_log(rk, LOG_WARNING, "ICFAIL",
                             "Interceptor %s failed %s: %s",
                             method->ic_name, method_name,
                             rd_kafka_err2str(err));
}


/**
 * @brief Call interceptor on_commit methods.
 * @locality application thread calling poll(), consume() or similar,
 *           or rdkafka main thread if no commit_cb or handler registered.
 */
void
rd_kafka_interceptors_on_commit (rd_kafka_t *rk,
                                 const rd_kafka_topic_partition_list_t *offsets,
                                 rd_kafka_resp_err_t err) {
        rd_kafka_interceptor_method_t *method;
        int i;

        RD_LIST_FOREACH(method, &rk->rk_conf.interceptors.on_commit, i) {
                rd_kafka_resp_err_t ic_err;

                ic_err = method->u.on_commit(rk, offsets, err,
                                             method->ic_opaque);
                if (unlikely(ic_err))
                        rd_kafka_interceptor_failed(rk, method,
                                                    "on_commit", ic_err, NULL);
        }
}


/**
 * @brief Create interceptor method reference
 */
static rd_kafka_interceptor_method_t *
rd_kafka_interceptor_method_new (const char *ic_name,
                                 void *func, void *ic_opaque) {
        rd_kafka_interceptor_method_t *method;

        method            = rd_calloc(1, sizeof(*method));
        method->ic_name   = rd_strdup(ic_name);
        method->ic_opaque = ic_opaque;
        method->u.generic = func;

        return method;
}

/**
 * @brief Add interceptor method reference
 */
static void
rd_kafka_interceptor_method_add (rd_list_t *list, const char *ic_name,
                                 void *func, void *ic_opaque) {
        rd_kafka_interceptor_method_t *method;

        method = rd_kafka_interceptor_method_new(ic_name, func, ic_opaque);
        rd_list_add(list, method);
}

/**
 * @brief Copy constructor for method
 */
static void *rd_kafka_interceptor_method_copy (const void *psrc, void *opaque) {
        const rd_kafka_interceptor_method_t *src = psrc;

        return (void *)rd_kafka_interceptor_method_new(src->ic_name,
                                                       src->u.generic,
                                                       src->ic_opaque);
}


/**
 * @brief Destroy all interceptors
 * @locality application thread calling rd_kafka_conf_destroy() or 
 *           rd_kafka_destroy()
 */
static void rd_kafka_interceptors_destroy (rd_kafka_conf_t *conf) {
        rd_list_destroy(&conf->interceptors.on_send);
        rd_list_destroy(&conf->interceptors.on_acknowledgement);
        rd_list_destroy(&conf->interceptors.on_consume);
        rd_list_destroy(&conf->interceptors.on_commit);
}


/**
 * @brief Initialize interceptor sub-system for config object.
 * @locality application thread
 */
static void
rd_kafka_interceptors_init (rd_kafka_conf_t *conf) {
        rd_list_init(&conf->interceptors.on_send, 0,
                     rd_kafka_interceptor_method_destroy);
        rd_list_init(&conf->interceptors.on_acknowledgement, 0,
                     rd_kafka_interceptor_method_destroy);
        rd_list_init(&conf->interceptors.on_consume, 0,
                     rd_kafka_interceptor_method_destroy);
        rd_list_init(&conf->interceptors.on_commit, 0,
                     rd_kafka_interceptor_method_destroy);
}

/**
 * @name Configuration backend
 */


/**
 * @brief Constructor called when configuration object is created.
 */
void rd_kafka_conf_interceptor_ctor (int scope, void *pconf) {
        rd_kafka_conf_t *conf = pconf;
        assert(scope == _RK_GLOBAL);
        rd_kafka_interceptors_init(conf);
}

/**
 * @brief Destructor called when configuration object is destroyed.
 */
void rd_kafka_conf_interceptor_dtor (int scope, void *pconf) {
        rd_kafka_conf_t *conf = pconf;
        assert(scope == _RK_GLOBAL);
        rd_kafka_interceptors_destroy(conf);
}

/**
 * @brief Copy-constructor called when configuration object \p psrcp is
 *        duplicated to \p dstp.
 */
void rd_kafka_conf_interceptor_copy (int scope, void *pdst, const void *psrc,
                                     void *dstptr, const void *srcptr) {
        rd_kafka_conf_t *dconf = pdst;
        const rd_kafka_conf_t *sconf = psrc;
        assert(scope == _RK_GLOBAL);

        rd_kafka_dbg0(sconf, INTERCEPTOR, "XX", "copy %p <- %p",
                      dconf, sconf);
        rd_list_copy_to(&dconf->interceptors.on_send,
                        &sconf->interceptors.on_send,
                        rd_kafka_interceptor_method_copy, NULL);

        rd_list_copy_to(&dconf->interceptors.on_acknowledgement,
                        &sconf->interceptors.on_acknowledgement,
                        rd_kafka_interceptor_method_copy, NULL);

        rd_list_copy_to(&dconf->interceptors.on_consume,
                        &sconf->interceptors.on_consume,
                        rd_kafka_interceptor_method_copy, NULL);

        rd_list_copy_to(&dconf->interceptors.on_commit,
                        &sconf->interceptors.on_commit,
                        rd_kafka_interceptor_method_copy, NULL);
}





/**
 * @brief Call interceptor on_send methods.
 * @locality application thread calling produce()
 */
void
rd_kafka_interceptors_on_send (rd_kafka_t *rk, rd_kafka_message_t *rkmessage) {
        rd_kafka_interceptor_method_t *method;
        int i;

        RD_LIST_FOREACH(method, &rk->rk_conf.interceptors.on_send, i) {
                rd_kafka_resp_err_t err;

                err = method->u.on_send(rk, rkmessage, method->ic_opaque);
                if (unlikely(err))
                        rd_kafka_interceptor_failed(rk, method, "on_send", err,
                                                    rkmessage);
        }
}



/**
 * @brief Call interceptor on_acknowledgement methods.
 * @locality application thread calling poll(), or the broker thread if
 *           if dr callback has been set.
 */
void
rd_kafka_interceptors_on_acknowledgement (rd_kafka_t *rk,
                                          rd_kafka_message_t *rkmessage) {
        rd_kafka_interceptor_method_t *method;
        int i;

        RD_LIST_FOREACH(method,
                        &rk->rk_conf.interceptors.on_acknowledgement, i) {
                rd_kafka_resp_err_t err;

                err = method->u.on_acknowledgement(rk, rkmessage,
                                                   method->ic_opaque);
                if (unlikely(err))
                        rd_kafka_interceptor_failed(rk, method,
                                                    "on_acknowledgement", err,
                                                    rkmessage);
        }
}


/**
 * @brief Call on_acknowledgement methods for all messages in queue.
 * @locality broker thread
 */
void
rd_kafka_interceptors_on_acknowledgement_queue (rd_kafka_t *rk,
                                                rd_kafka_msgq_t *rkmq) {
        rd_kafka_msg_t *rkm;

        RD_KAFKA_MSGQ_FOREACH(rkm, rkmq) {
                rd_kafka_interceptors_on_acknowledgement(rk,
                                                         &rkm->rkm_rkmessage);
        }
}


/**
 * @brief Call interceptor on_consume methods.
 * @locality application thread calling poll(), consume() or similar prior to
 *           passing the message to the application.
 */
void
rd_kafka_interceptors_on_consume (rd_kafka_t *rk,
                                  rd_kafka_message_t *rkmessage) {
        rd_kafka_interceptor_method_t *method;
        int i;

        RD_LIST_FOREACH(method, &rk->rk_conf.interceptors.on_consume, i) {
                rd_kafka_resp_err_t err;

                err = method->u.on_consume(rk, rkmessage,
                                                   method->ic_opaque);
                if (unlikely(err))
                        rd_kafka_interceptor_failed(rk, method,
                                                    "on_consume", err,
                                                    rkmessage);
        }
}


/**
 * @name Public API (backend)
 * @{
 */



void
rd_kafka_conf_interceptor_add_on_send (
        rd_kafka_conf_t *conf, const char *ic_name,
        rd_kafka_interceptor_f_on_send_t *on_send,
        void *ic_opaque) {
        rd_kafka_interceptor_method_add(&conf->interceptors.on_send,
                                        ic_name, (void *)on_send, ic_opaque);
}

void
rd_kafka_conf_interceptor_add_on_acknowledgement (
        rd_kafka_conf_t *conf, const char *ic_name,
        rd_kafka_interceptor_f_on_acknowledgement_t *on_acknowledgement,
        void *ic_opaque) {
        rd_kafka_interceptor_method_add(&conf->interceptors.
                                        on_acknowledgement,
                                        ic_name,
                                        (void *)on_acknowledgement, ic_opaque);
}


void
rd_kafka_conf_interceptor_add_on_consume (
        rd_kafka_conf_t *conf, const char *ic_name,
        rd_kafka_interceptor_f_on_consume_t *on_consume,
        void *ic_opaque) {
        rd_kafka_interceptor_method_add(&conf->interceptors.on_consume,
                                        ic_name, (void *)on_consume, ic_opaque);
}


void
rd_kafka_conf_interceptor_add_on_commit (
        rd_kafka_conf_t *conf, const char *ic_name,
        rd_kafka_interceptor_f_on_commit_t *on_commit,
        void *ic_opaque) {
        rd_kafka_interceptor_method_add(&conf->interceptors.on_commit,
                                        ic_name, (void *)on_commit, ic_opaque);
}
