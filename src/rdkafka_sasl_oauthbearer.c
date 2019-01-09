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
 * Builtin SASL OAUTHBEARER support
 */
#include "rdkafka_int.h"
#include "rdkafka_transport.h"
#include "rdkafka_transport_int.h"
#include "rdkafka_sasl.h"
#include "rdkafka_sasl_int.h"
#include "rdrand.h"

#if WITH_SSL
#include <openssl/hmac.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#else
#error "WITH_SSL (OpenSSL) is required for SASL OAUTHBEARER"
#endif


/**
 * @brief Per-connection state
 */
struct rd_kafka_sasl_oauthbearer_state {
        enum {
                RD_KAFKA_SASL_OAUTHBEARER_STATE_SEND_CLIENT_FIRST_MESSAGE,
                RD_KAFKA_SASL_OAUTHBEARER_STATE_RECEIVE_SERVER_FIRST_MESSAGE,
                RD_KAFKA_SASL_OAUTHBEARER_STATE_RECEIVE_SERVER_MESSAGE_AFTER_FAILURE,
        } state;
        rd_chariov_t server_error_msg;
};


/**
 * @brief Close and free authentication state
 */
static void rd_kafka_sasl_oauthbearer_close (rd_kafka_transport_t *rktrans) {
        struct rd_kafka_sasl_oauthbearer_state *state = rktrans->rktrans_sasl.state;

        if (!state)
                return;

        RD_IF_FREE(state->server_error_msg.ptr, rd_free);
        rd_free(state);
}



/**
 * @brief Build client-first-message
 */
static void
rd_kafka_sasl_oauthbearer_build_client_first_message (
        rd_kafka_transport_t *rktrans,
        rd_chariov_t *out) {
        const rd_kafka_t *rk = rktrans->rktrans_rkb->rkb_rk;
        const char *token_value = rk->rk_oauthbearer->token_value;

        // TODO: support extensions

        /* "#"" is a stand-in for any CTRL-A character
         * that is part of the client response
         */
        out->size = strlen("n,,#auth=Bearer ") + strlen(token_value)
                + 2; // strlen("##")
        out->ptr = rd_malloc(out->size+1);

        rd_snprintf(out->ptr, out->size+1,
                    "n,,%sauth=Bearer %s%s%s",
                    "\x01",
                    token_value,
                    "\x01",
                    "\x01");

        rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY, "SASLOAUTHBEARER",
                   "sent client first message");
}



/**
 * @brief SASL OAUTHBEARER client state machine
 * @returns -1 on failure (errstr set), else 0.
 */
static int rd_kafka_sasl_oauthbearer_fsm (rd_kafka_transport_t *rktrans,
                                    const rd_chariov_t *in,
                                    char *errstr, size_t errstr_size) {
        static const char *state_names[] = {
                "client-first-message",
                "server-first-message",
                "server-failure-message",
        };
        struct rd_kafka_sasl_oauthbearer_state *state = rktrans->rktrans_sasl.state;
        rd_chariov_t out = RD_ZERO_INIT;
        int r = -1;
        rd_ts_t ts_start = rd_clock();
        int prev_state = state->state;

        rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY, "SASLOAUTHBEARER",
                   "SASL OAUTHBEARER client in state %s",
                   state_names[state->state]);

        if (!rktrans->rktrans_rkb->rkb_rk->rk_oauthbearer->token_value) {
                rd_snprintf(errstr, errstr_size, "OUTHBEARER cannot log in because there is no token available");
                return 0;
        }

        switch (state->state)
        {
        case RD_KAFKA_SASL_OAUTHBEARER_STATE_SEND_CLIENT_FIRST_MESSAGE:
                rd_dassert(!in); /* Not expecting any server-input */

                rd_kafka_sasl_oauthbearer_build_client_first_message(rktrans, &out);
                state->state = RD_KAFKA_SASL_OAUTHBEARER_STATE_RECEIVE_SERVER_FIRST_MESSAGE;
                break;


        case RD_KAFKA_SASL_OAUTHBEARER_STATE_RECEIVE_SERVER_FIRST_MESSAGE:
                if (!in || in->ptr[0] == '\0') {
                        /* Success */
                        rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY | RD_KAFKA_DBG_BROKER,
                                "OAUTHBEARERAUTH",
                                "SASL OAUTHBEARER authentication successful");
                        rd_kafka_sasl_auth_done(rktrans);
                        r = 0;
                        break;
                }

                /* Failure; save error message for later */
                state->server_error_msg.size = in->size;
                state->server_error_msg.ptr  = rd_memdup(in->ptr,
                                                state->server_error_msg.size);

                /* Send final CTRL-A character */
                out.size = 1;
                out.ptr = rd_malloc(out.size + 1);
                rd_snprintf(out.ptr, out.size+1, "\x01");
                state->state = RD_KAFKA_SASL_OAUTHBEARER_STATE_RECEIVE_SERVER_MESSAGE_AFTER_FAILURE;
                r = 0;
                break;

        case RD_KAFKA_SASL_OAUTHBEARER_STATE_RECEIVE_SERVER_MESSAGE_AFTER_FAILURE:
                rd_dassert(!in); /* Not expecting any server-input */

                /* Failure as previosuly communicated by server first message */
                rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY | RD_KAFKA_DBG_BROKER,
                        "OAUTHBEARERAUTH",
                        "SASL OAUTHBEARER authentication failed: %s",
                        state->server_error_msg.ptr);
                r = -1;
                break;
        }

        if (out.ptr) {
                r = rd_kafka_sasl_send(rktrans, out.ptr, (int)out.size,
                                       errstr, errstr_size);
                rd_free(out.ptr);
        }

        ts_start = (rd_clock() - ts_start) / 1000;
        if (ts_start >= 100)
                rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY, "OAUTHBEARER",
                           "SASL OAUTHBEARER state %s handled in %"PRId64"ms",
                           state_names[prev_state], ts_start);


        return r;
}


/**
 * @brief Handle received frame from broker.
 */
static int rd_kafka_sasl_oauthbearer_recv (rd_kafka_transport_t *rktrans,
                                     const void *buf, size_t size,
                                     char *errstr, size_t errstr_size) {
        const rd_chariov_t in = { .ptr = (char *)buf, .size = size };
        return rd_kafka_sasl_oauthbearer_fsm(rktrans, &in, errstr, errstr_size);
}


/**
 * @brief Initialize and start SASL OAUTHBEARER (builtin) authentication.
 *
 * Returns 0 on successful init and -1 on error.
 *
 * @locality broker thread
 */
static int rd_kafka_sasl_oauthbearer_client_new (rd_kafka_transport_t *rktrans,
                                    const char *hostname,
                                    char *errstr, size_t errstr_size) {
        struct rd_kafka_sasl_oauthbearer_state *state;

        state = rd_calloc(1, sizeof(*state));
        state->state = RD_KAFKA_SASL_OAUTHBEARER_STATE_SEND_CLIENT_FIRST_MESSAGE;
        rktrans->rktrans_sasl.state = state;

        /* Kick off the FSM */
        return rd_kafka_sasl_oauthbearer_fsm(rktrans, NULL, errstr, errstr_size);
}



/**
 * @brief Validate OAUTHBEARER config, which is a no-op (we rely on initial token retrieval)
 */
static int rd_kafka_sasl_oauthbearer_conf_validate (rd_kafka_t *rk,
                                              char *errstr,
                                              size_t errstr_size) {
        /*
         * We will rely on the initial token retrieval as a proxy
         * for configuration validation.
         */
        return 0;
}




const struct rd_kafka_sasl_provider rd_kafka_sasl_oauthbearer_provider = {
        .name          = "OAUTHBEARER (builtin)",
        .client_new    = rd_kafka_sasl_oauthbearer_client_new,
        .recv          = rd_kafka_sasl_oauthbearer_recv,
        .close         = rd_kafka_sasl_oauthbearer_close,
        .conf_validate = rd_kafka_sasl_oauthbearer_conf_validate,
};
