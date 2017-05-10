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


/**
 * @brief Interceptor plugin test library
 *
 * Interceptors can be implemented in the app itself and use
 * the direct API to set the interceptors methods, or be implemented
 * as an external plugin library that uses the direct APIs.
 *
 * This file implements the latter, an interceptor plugin library.
 */

#include <stdio.h>
#include <string.h>
#include <assert.h>

/* typical include path outside tests is <librdkafka/rdkafka.h> */
#include "rdkafka.h"

#ifdef _MSC_VER
#define DLL_EXPORT __declspec(dllexport)
#else
#define DLL_EXPORT
#endif


static char *my_interceptor_plug_opaque = "my_interceptor_plug_opaque";
static char *my_ic_opaque = "my_ic_opaque";



/* Producer methods */
rd_kafka_resp_err_t on_send (rd_kafka_t *rk,
                             rd_kafka_message_t *rkmessage,
                             void *ic_opaque) {
        assert(ic_opaque == my_ic_opaque);
        printf("on_send\n");
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}


rd_kafka_resp_err_t on_acknowledgement (rd_kafka_t *rk,
                                        rd_kafka_message_t *rkmessage,
                                        void *ic_opaque) {
        assert(ic_opaque == my_ic_opaque);
        printf("on_acknowledgement: err %d, partition %"PRId32"\n",
               rkmessage->err, rkmessage->partition);
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}

/* Consumer methods */
rd_kafka_resp_err_t on_consume (rd_kafka_t *rk,
                                rd_kafka_message_t *rkmessage,
                                void *ic_opaque) {
        assert(ic_opaque == my_ic_opaque);
        printf("on_consume partition %"PRId32" @ %"PRId64"\n",
               rkmessage->partition, rkmessage->offset);
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}

rd_kafka_resp_err_t on_commit (rd_kafka_t *rk,
                               const rd_kafka_topic_partition_list_t *offsets,
                               rd_kafka_resp_err_t err, void *ic_opaque) {
        assert(ic_opaque == my_ic_opaque);
        printf("on_commit: err %d\n", err);
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}



/**
 * @brief Called from rd_kafka_new(). We use it to set up interceptors.
 */
static rd_kafka_resp_err_t on_new (rd_kafka_t *rk, void *ic_opaque,
                                   char *errstr, size_t errstr_size) {
        rd_kafka_interceptor_add_on_send(rk, __FILE__, on_send,
                                         my_ic_opaque);
        rd_kafka_interceptor_add_on_acknowledgement(rk, __FILE__,
                                                    on_acknowledgement,
                                                    my_ic_opaque);
        rd_kafka_interceptor_add_on_consume(rk, __FILE__, on_consume,
                                            my_ic_opaque);
        rd_kafka_interceptor_add_on_commit(rk, __FILE__, on_commit,
                                           my_ic_opaque);
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}


/**
 * @brief Configuration set handler
 */
static rd_kafka_conf_res_t on_conf_set (rd_kafka_conf_t *conf,
                                        const char *name, const char *val,
                                        char *errstr, size_t errstr_size,
                                        void *ic_opaque) {
        if (!strcmp(name, "interceptor_test.good"))
                return RD_KAFKA_CONF_OK;
        else if (!strcmp(name, "interceptor_test.bad")) {
                snprintf(errstr, errstr_size,
                         "on_conf_set failed deliberately for %s=%s",
                         name, val);
                return RD_KAFKA_CONF_INVALID;
        }

        return RD_KAFKA_CONF_UNKNOWN;
}

static void conf_init0 (rd_kafka_conf_t *conf);


/**
 * @brief Set up new configuration on copy.
 */
static rd_kafka_resp_err_t on_conf_dup (rd_kafka_conf_t *new_conf,
                                        const rd_kafka_conf_t *old_conf,
                                        void *ic_opaque) {
        conf_init0(new_conf);
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}


static rd_kafka_resp_err_t on_conf_destroy (void *ic_opaque) {
        printf("conf_destroy called (opaque %p vs %p)\n",
               ic_opaque, my_interceptor_plug_opaque);
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}



/**
 * @brief Configuration init is intercepted both from plugin.library.paths
 *        as well as rd_kafka_conf_dup().
 *        This internal method serves both cases.
 */
static void conf_init0 (rd_kafka_conf_t *conf) {
        /* Add interceptor methods */
        rd_kafka_conf_interceptor_add_on_new(conf, __FILE__, on_new,
                                             my_ic_opaque);
        rd_kafka_conf_interceptor_add_on_conf_set(conf, __FILE__, on_conf_set,
                                                  NULL);
        rd_kafka_conf_interceptor_add_on_conf_dup(conf, __FILE__, on_conf_dup,
                                                  NULL);
        rd_kafka_conf_interceptor_add_on_conf_destroy(conf, __FILE__,
                                                      on_conf_destroy,
                                                      NULL);
}

/**
 * @brief Plugin conf initializer called when plugin.library.paths is set.
 */
DLL_EXPORT
rd_kafka_resp_err_t conf_init (rd_kafka_conf_t *conf,
                               void **plug_opaquep,
                               char *errstr, size_t errstr_size) {
        *plug_opaquep = (void *)my_interceptor_plug_opaque;

        printf("conf_init called (setting opaque to %p)\n", *plug_opaquep);

        conf_init0(conf);

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}


