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
#include "rdkafka_transport_int.h"
#include "rdkafka_sasl_int.h"


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
        /*
         * A place to store a consistent view of the token and extensions
         * throughout the authentication process -- even if it is refreshed
         * midway through this particular authentication.
         */
        char *token_value;
        char *md_principal_name;
        rd_list_t extensions; /* rd_strtup_t list */
};


/**
 * @brief Close and free authentication state
 */
static void rd_kafka_sasl_oauthbearer_close (rd_kafka_transport_t *rktrans) {
        struct rd_kafka_sasl_oauthbearer_state *state = rktrans->rktrans_sasl.state;

        if (!state)
                return;

        RD_IF_FREE(state->server_error_msg.ptr, rd_free);
        rd_free(state->token_value);
        rd_free(state->md_principal_name);
        rd_list_destroy(&state->extensions);
        rd_free(state);
}



/**
 * @brief Build client-first-message
 */
static void
rd_kafka_sasl_oauthbearer_build_client_first_message (
        rd_kafka_transport_t *rktrans,
        rd_chariov_t *out) {
        struct rd_kafka_sasl_oauthbearer_state *state = rktrans->rktrans_sasl.state;

        /*
         * https://tools.ietf.org/html/rfc7628#section-3.1
         * kvsep          = %x01
         * key            = 1*(ALPHA)
         * value          = *(VCHAR / SP / HTAB / CR / LF )
         * kvpair         = key "=" value kvsep
         * ;;gs2-header     = See RFC 5801
         * client-resp    = (gs2-header kvsep *kvpair kvsep) / kvsep
         */

        const char *gs2_header = "n,,";
        const char *kvsep = "\x01";
        const int kvsep_size = strlen(kvsep);
        int extension_size = 0;
        int i;
        for (i = 0 ; i < rd_list_cnt(&state->extensions) ; i++) {
                rd_strtup_t *extension = rd_list_elem(&state->extensions, i);
                // kvpair         = key "=" value kvsep
                extension_size += strlen(extension->name) + 1 // "="
                        + strlen(extension->value) + kvsep_size;
        }

        // client-resp    = (gs2-header kvsep *kvpair kvsep) / kvsep
        out->size = strlen(gs2_header) + kvsep_size
                + strlen("auth=Bearer ") + strlen(state->token_value) + kvsep_size
                + extension_size + kvsep_size;
        out->ptr = rd_malloc(out->size+1);

        char *buf = out->ptr;
        int size_written = 0;
        size_written += rd_snprintf(buf, out->size+1 - size_written,
                "%s%sauth=Bearer %s%s", gs2_header, kvsep, state->token_value, kvsep);
        buf = out->ptr + size_written;

        for (i = 0 ; i < rd_list_cnt(&state->extensions) ; i++) {
                rd_strtup_t *extension = rd_list_elem(&state->extensions, i);
                size_written += rd_snprintf(buf, out->size+1 - size_written,
                        "%s=%s%s", extension->name, extension->value, kvsep);
                buf = out->ptr + size_written;
        }

        rd_snprintf(buf, out->size+1 - size_written, "%s", kvsep);

        rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY, "SASLOAUTHBEARER",
                   "Built client first message");
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

        rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY, "SASLOAUTHBEARER",
                   "SASL OAUTHBEARER client in state %s",
                   state_names[state->state]);

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
                                "SASL OAUTHBEARER authentication successful (principal=%s)",
                                state->md_principal_name);
                        rd_kafka_sasl_auth_done(rktrans);
                        r = 0;
                        break;
                }

                /* Failure; save error message for later */
                state->server_error_msg.size = in->size;
                state->server_error_msg.ptr  = rd_memdup(in->ptr,
                                                state->server_error_msg.size);

                /*
                 * https://tools.ietf.org/html/rfc7628#section-3.1
                 * kvsep          = %x01
                 * client-resp    = (gs2-header kvsep *kvpair kvsep) / kvsep
                 *
                 * Send final kvsep (CTRL-A) character
                 */
                out.size = 1;
                out.ptr = rd_malloc(out.size + 1);
                rd_snprintf(out.ptr, out.size+1, "\x01");
                state->state = RD_KAFKA_SASL_OAUTHBEARER_STATE_RECEIVE_SERVER_MESSAGE_AFTER_FAILURE;
                r = 0; // will fail later in next state after sending current response
                break;

        case RD_KAFKA_SASL_OAUTHBEARER_STATE_RECEIVE_SERVER_MESSAGE_AFTER_FAILURE:
                rd_dassert(!in); /* Not expecting any server-input */

                /* Failure as previosuly communicated by server first message */
                rd_snprintf(errstr, errstr_size, "SASL OAUTHBEARER authentication failed (principal=%s): %s",
                        state->md_principal_name,
                        state->server_error_msg.ptr);
                rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY | RD_KAFKA_DBG_BROKER,
                        "OAUTHBEARERAUTH",
                        "SASL OAUTHBEARER authentication failed (principal=%s): %s",
                        state->md_principal_name,
                        state->server_error_msg.ptr);
                r = -1;
                break;
        }

        if (out.ptr) {
                r = rd_kafka_sasl_send(rktrans, out.ptr, (int)out.size,
                                       errstr, errstr_size);
                rd_free(out.ptr);
        }

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
        /*
         * Save off the state structure now, before any possibility of
         * returning, so that we will always free up the allocated memory in
         * rd_kafka_sasl_oauthbearer_close().
         */
        rktrans->rktrans_sasl.state = state;
        /*
         * Make sure we have a consistent view of the token and extensions
         * throughout the authentication process -- even if it is refreshed
         * midway through this particular authentication.
         */
        rwlock_rdlock(&rktrans->rktrans_rkb->rkb_rk->rk_oauthbearer->refresh_lock);
        if (!rktrans->rktrans_rkb->rkb_rk->rk_oauthbearer->token_value) {
                rwlock_rdunlock(&rktrans->rktrans_rkb->rkb_rk->rk_oauthbearer->refresh_lock);
                rd_snprintf(errstr, errstr_size, "OUTHBEARER cannot log in because there is no token available");
                return -1;
        }
        state->token_value = strdup(rktrans->rktrans_rkb->rkb_rk->rk_oauthbearer->token_value);
        state->md_principal_name = strdup(rktrans->rktrans_rkb->rkb_rk->rk_oauthbearer->md_principal_name);
        rd_list_copy_to(&state->extensions, &rktrans->rktrans_rkb->rkb_rk->rk_oauthbearer->extensions,
                rd_strtup_list_copy, NULL);
        rwlock_rdunlock(&rktrans->rktrans_rkb->rkb_rk->rk_oauthbearer->refresh_lock);

        /* Confirm there is no explicit "auth" extension */
        int i;
        for (i = 0 ; i < rd_list_cnt(&state->extensions) ; i++) {
                rd_strtup_t *extension = rd_list_elem(&state->extensions, i);
                if (!strcmp(extension->name, "auth")) {
                        rd_snprintf(errstr, errstr_size,
                                "OUTHBEARER config must not provide explicit \"auth\" extension");
                        return -1;
                }
        }


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
