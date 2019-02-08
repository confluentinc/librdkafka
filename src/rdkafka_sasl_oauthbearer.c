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
#include <openssl/evp.h>


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
 * @brief Parse a config value from the string pointed to by \p loc and starting
 * with the given \p prefix and ending with the given \c value_end_char, storing
 * the newly-allocated memory result in the string pointed to by \p value.
 * @returns -1 if string pointed to by \p value is non-zero (errstr set, no
 * memory allocated), else 0 (caller must free allocated memory).
 */
static int parse_unsecured_jws_config_value_for_prefix(char **loc,
        const char *prefix, const char value_end_char, char **value,
        char *errstr, size_t errstr_size) {
        if (*value) {
                rd_snprintf(errstr, errstr_size,
                        "Invalid sasl.oauthbearer.config: "
                        "multiple '%s' entries",
                        prefix);
                return -1;
        }
        *loc += strlen(prefix);
        *value = *loc;
        while (**loc != '\0' && **loc != value_end_char) {
                ++*loc;
        }
        if (**loc == value_end_char) {
                // end the string and skip the character
                **loc = '\0';
                ++*loc;
        }
        // return new allocated memory
        *value = strdup(*value);

        return 0;
}

/*
 * @brief Parse Unsecured JWS config, allocates strings that must be freed
 * @returns -1 on failure (errstr set), else 0.
 */
static int parse_unsecured_jws_config(const char *cfg,
                char **principal_claim_name,
                char **principal,
                char **scope_claim_name,
                char **scope_csv_text,
                int *life_seconds,
                rd_list_t *extensions, /* rd_strtup_t list */
                char *errstr, size_t errstr_size) {
        /*
         * Extensions:
         * 
         * https://tools.ietf.org/html/rfc7628#section-3.1
         * key            = 1*(ALPHA)
         * value          = *(VCHAR / SP / HTAB / CR / LF )
         * 
         * https://tools.ietf.org/html/rfc5234#appendix-B.1
         * ALPHA          =  %x41-5A / %x61-7A   ; A-Z / a-z
         * VCHAR          =  %x21-7E  ; visible (printing) characters
         * SP             =  %x20     ; space
         * HTAB           =  %x09     ; horizontal tab
         * CR             =  %x0D     ; carriage return
         * LF             =  %x0A     ; linefeed
         */

        static const char *prefix_principal_claim_name = "principalClaimName=";
        static const char *prefix_principal = "principal=";
        static const char *prefix_scope_claim_name = "scopeClaimName=";
        static const char *prefix_scope = "scope=";
        static const char *prefix_life_seconds = "lifeSeconds=";
        static const char *prefix_extension = "extension_";

        char *life_seconds_text = NULL;

        char *cfg_copy = rd_strdup(cfg);
        char *loc = cfg_copy;

        int r = 0;

        *principal_claim_name = NULL;
        *principal = NULL;
        *scope_claim_name = NULL;
        *scope_csv_text = NULL;
        *life_seconds = 0;

        while (*loc != '\0' && !r) {
                if (*loc == ' ') {
                        ++loc;
                } else if (!strncmp(prefix_principal_claim_name, loc,
                        strlen(prefix_principal_claim_name))) {
                        r = parse_unsecured_jws_config_value_for_prefix(&loc,
                                prefix_principal_claim_name, ' ',
                                principal_claim_name, errstr, errstr_size);
                        if (!r && **principal_claim_name == '\0') {
                                rd_snprintf(errstr, errstr_size,
                                        "Invalid sasl.oauthbearer.config: "
                                        "empty '%s'",
                                        prefix_principal_claim_name);
                                r = -1;
                        }
                } else if (!strncmp(prefix_principal, loc,
                        strlen(prefix_principal))) {
                        r = parse_unsecured_jws_config_value_for_prefix(&loc,
                                prefix_principal, ' ', principal,
                                errstr, errstr_size);
                        if (!r && **principal == '\0') {
                                rd_snprintf(errstr, errstr_size,
                                        "Invalid sasl.oauthbearer.config: "
                                        "empty '%s'",
                                        prefix_principal);
                                r = -1;
                        }
                } else if (!strncmp(prefix_scope_claim_name, loc,
                        strlen(prefix_scope_claim_name))) {
                        r = parse_unsecured_jws_config_value_for_prefix(&loc,
                                prefix_scope_claim_name, ' ', scope_claim_name,
                                errstr, errstr_size);
                        if (!r && **scope_claim_name == '\0') {
                                rd_snprintf(errstr, errstr_size,
                                        "Invalid sasl.oauthbearer.config: "
                                        "empty '%s'",
                                        prefix_scope_claim_name);
                                r = -1;
                        }
                } else if (!strncmp(prefix_scope, loc, strlen(prefix_scope))) {
                        r = parse_unsecured_jws_config_value_for_prefix(&loc,
                                prefix_scope, ' ', scope_csv_text, errstr,
                                errstr_size);
                        if (!r && **scope_csv_text == '\0') {
                                rd_snprintf(errstr, errstr_size,
                                        "Invalid sasl.oauthbearer.config: "
                                        "empty '%s'",
                                        prefix_scope);
                                r = -1;
                        }
                } else if (!strncmp(prefix_life_seconds, loc,
                        strlen(prefix_life_seconds))) {
                        r = parse_unsecured_jws_config_value_for_prefix(&loc,
                                prefix_life_seconds, ' ', &life_seconds_text,
                                errstr, errstr_size);
                        if (!r) {
                                if (*life_seconds_text == '\0') {
                                        rd_snprintf(errstr, errstr_size,
                                                "Invalid "
                                                "sasl.oauthbearer.config: "
                                                "empty '%s'",
                                                prefix_life_seconds);
                                        r = -1;
                                } else {
                                        long long life_seconds_long;
                                        char *end_ptr;
                                        life_seconds_long = strtoll(life_seconds_text, &end_ptr, 10);
                                        if (*end_ptr != '\0') {
                                                rd_snprintf(errstr, errstr_size,
                                                        "Invalid "
                                                        "sasl.oauthbearer.config: "
                                                        "non-integral '%s': %s",
                                                        prefix_life_seconds,
                                                        life_seconds_text);
                                                r = -1;
                                        } else if (life_seconds_long <= 0 ||
                                                life_seconds_long > INT_MAX) {
                                                rd_snprintf(errstr, errstr_size,
                                                        "Invalid "
                                                        "sasl.oauthbearer.config: "
                                                        "value out of range of "
                                                        "positive int '%s': %s",
                                                        prefix_life_seconds,
                                                        life_seconds_text);
                                                r = -1;
                                        } else {
                                                *life_seconds = life_seconds_long;
                                        }
                                }
                                rd_free(life_seconds_text);
                        }
                } else if (!strncmp(prefix_extension, loc,
                        strlen(prefix_extension))) {
                        char *extension_key = NULL;
                        r = parse_unsecured_jws_config_value_for_prefix(&loc,
                                prefix_extension, '=', &extension_key, errstr,
                                errstr_size);
                        if (!r) {
                                if (*extension_key == '\0') {
                                        rd_snprintf(errstr, errstr_size,
                                                "Invalid "
                                                "sasl.oauthbearer.config: "
                                                "empty '%s' key",
                                                prefix_extension);
                                        r = -1;
                                } else {
                                        char *extension_value = NULL;
                                        r = parse_unsecured_jws_config_value_for_prefix(
                                                &loc, "", ' ', &extension_value,
                                                errstr, errstr_size);
                                        if (!r) {
                                                rd_list_add(extensions,
                                                        rd_strtup_new(
                                                                extension_key,
                                                                extension_value));
                                                rd_free(extension_value);
                                        }
                                }
                                rd_free(extension_key);
                        }
                } else {
                        rd_snprintf(errstr, errstr_size,
                                "Unrecognized sasl.oauthbearer.config "
                                "beginning at: %s",
                                loc);
                        r = -1;
                }
        }

        rd_free(cfg_copy);

        return r;
}

void rd_kafka_oauthbearer_unsecured_token(rd_kafka_t *rk, void *opaque) {
        char *principal_claim_name = NULL;
        char *principal = NULL;
        char *scope_claim_name = NULL;
        char *scope_csv = NULL;
        int life_seconds = 0;
        rd_list_t extensions; /* rd_strtup_t list */
        rd_list_init(&extensions, 0, (void (*)(void *))rd_strtup_destroy);
        rd_list_t scope;
        rd_list_init(&scope, 0, rd_free);
        int scope_json_length = 0;

        char errstr[512] = "\0";
        if (parse_unsecured_jws_config(rk->rk_conf.sasl.oauthbearer_config,
                &principal_claim_name, &principal, &scope_claim_name,
                &scope_csv, &life_seconds, &extensions, errstr,
                sizeof(errstr)) == -1) {
                        rd_kafka_set_fatal_error(rk,
                                RD_KAFKA_RESP_ERR__INVALID_ARG, "%s", errstr);
        } else {
                // make sure we have required and valid info
                if (!principal_claim_name) {
                        principal_claim_name = strdup("sub");
                }
                if (!scope_claim_name) {
                        scope_claim_name = strdup("scope");
                }
                if (!life_seconds) {
                        life_seconds = 3600;
                }
                if (!principal) {
                        rd_kafka_set_fatal_error(rk,
                                RD_KAFKA_RESP_ERR__INVALID_ARG,
                                "Invalid sasl.oauthbearer.config: no principal=<value>");
                } else if (strchr(principal, '"')) {
                        rd_kafka_set_fatal_error(rk,
                                RD_KAFKA_RESP_ERR__INVALID_ARG,
                                "Invalid sasl.oauthbearer.config: principal cannot contain a '\"' character: %s", principal);
                } else if (strchr(principal_claim_name, '"')) {
                        rd_kafka_set_fatal_error(rk,
                                RD_KAFKA_RESP_ERR__INVALID_ARG,
                                "Invalid sasl.oauthbearer.config: principalClaimName cannot contain a '\"' character: %s", principal_claim_name);
                } else if (strchr(scope_claim_name, '"')) {
                        rd_kafka_set_fatal_error(rk,
                                RD_KAFKA_RESP_ERR__INVALID_ARG,
                                "Invalid sasl.oauthbearer.config: scopeClaimName cannot contain a '\"' character: %s", scope_claim_name);
                } else if (scope_csv && strchr(scope_csv, '"')) {
                        rd_kafka_set_fatal_error(rk,
                                RD_KAFKA_RESP_ERR__INVALID_ARG,
                                "Invalid sasl.oauthbearer.config: scope cannot contain a '\"' character: %s", scope_csv);
                } else {
                        if (scope_csv) {
                                // convert from csv to rd_list_t and
                                // calculate json length
                                char *start = scope_csv;
                                char *curr = start;
                                while(*curr != '\0') {
                                        // ignore empty elements (e.g. ",,")
                                        while(*curr != '\0' && *curr == ',') {
                                                ++curr;
                                                ++start;
                                        }
                                        while(*curr != '\0' && *curr != ',') {
                                                ++curr;
                                        }
                                        if (curr != start) {
                                                if (*curr == ',') {
                                                       *curr = '\0';
                                                       ++curr;
                                                }
                                                if (!rd_list_find(&scope, start,
                                                        (void *)strcmp)) {
                                                        rd_list_add(&scope,
                                                                rd_strdup(start));
                                                }
                                                if (scope_json_length == 0) {
                                                        scope_json_length = 2 + // ,"
                                                                strlen(scope_claim_name) +
                                                                4 + // ":["
                                                                strlen(start) +
                                                                1 + // "
                                                                1; // trailing ]
                                                } else {
                                                        scope_json_length += 2; // ,"
                                                        scope_json_length += strlen(start);
                                                        scope_json_length += 1; // "
                                                }
                                                start = curr;
                                        }
                                }
                        }
                        const char *jose_header_encoded = "eyJhbGciOiJub25lIn0"; // {"alg":"none"}
                        int max_json_length;
                        rd_ts_t now_usec = rd_clock();
                        rd_ts_t now_ms = now_usec / 1000;
                        double now_sec = now_ms / 1000.0;
                        // generate json
                        max_json_length = 2 + // {"
                                strlen(principal_claim_name) +
                                3 + // ":"
                                strlen(principal) +
                                8 + // ","iat":
                                14 + // iat NumericDate (e.g. 1549251467.546)
                                7 + // ,"exp":
                                14 + // exp NumericDate (e.g. 1549252067.546)
                                scope_json_length +
                                1; // }
                        // generate scope portion of json
                        char *scope_json;
                        scope_json = rd_malloc(scope_json_length + 1);
                        *scope_json = '\0';
                        char *scope_curr = scope_json;
                        int i;
                        for (i = 0; i < rd_list_cnt(&scope); i++) {
                                if (i == 0) {
                                        scope_curr += sprintf(scope_curr,
                                                ",\"%s\":[\"", scope_claim_name);
                                } else {
                                        scope_curr += sprintf(scope_curr, "%s",
                                                ",\"");
                                }
                                scope_curr += sprintf(scope_curr, "%s\"",
                                        rd_list_elem(&scope, i));
                                if (i == rd_list_cnt(&scope) - 1) {
                                        scope_curr += sprintf(scope_curr, "%s",
                                                "]");
                                }
                        }
                        char *claims_json = rd_malloc(max_json_length + 1);
                        rd_snprintf(claims_json, max_json_length + 1,
                                "{\"%s\":\"%s\",\"iat\":%.3f,\"exp\":%.3f%s}",
                                principal_claim_name, principal, now_sec,
                                now_sec + life_seconds, scope_json);
                        rd_free(scope_json);
                        // convert to base64URL format, first to base64, then to
                        // base64URL
                        char *jws = rd_malloc(strlen(jose_header_encoded) + 1 +
                                (((max_json_length + 2) / 3) * 4) + 1 + 1);
                        sprintf(jws, "%s.", jose_header_encoded);
                        char *jws_claims = jws + strlen(jws);
                        size_t encode_len;
                        encode_len = EVP_EncodeBlock((uint8_t*)jws_claims,
                                (uint8_t*)claims_json, strlen(claims_json));
                        rd_free(claims_json);
                        char *jws_last_char = jws_claims + encode_len - 1;
                        // convert from padded base64 to unpadded base64URL
                        // eliminate any padding
                        while (*jws_last_char == '=') {
                                --jws_last_char;
                        }
                        *(++jws_last_char) = '.';
                        *(jws_last_char + 1) = '\0';
                        // convert the 2 differing encode characters
                        char *jws_maybe_non_url_char = jws;
                        while (*jws_maybe_non_url_char != '\0') {
                                if (*jws_maybe_non_url_char == '+') {
                                        *jws_maybe_non_url_char = '-';
                                } else if (*jws_maybe_non_url_char == '/') {
                                        *jws_maybe_non_url_char = '_';
                                }
                                ++jws_maybe_non_url_char;
                        }
                        const char **extensionv = NULL;
                        int extension_pair_count = rd_list_cnt(&extensions);
                        extensionv = rd_malloc(sizeof(*extensionv) * 2 *
                                extension_pair_count);
                        for (i = 0; i < extension_pair_count; ++i) {
                                rd_strtup_t *strtup = (rd_strtup_t *)
                                        rd_list_elem(&extensions, i);
                                extensionv[2 * i] = strtup->name;
                                extensionv[2 * i + 1] = strtup->value;
                        }
                        rd_kafka_oauthbearer_set_token(rk, jws,
                                now_ms + life_seconds * 1000, principal,
                                extensionv, 2 * extension_pair_count, errstr,
                                sizeof(errstr));
                        rd_free(jws);
                        rd_free(extensionv);
                }
        }
        rd_free(principal_claim_name);
        rd_free(principal);
        rd_free(scope_claim_name);
        rd_free(scope_csv);
        rd_list_destroy(&scope);
        rd_list_destroy(&extensions);
}

/**
 * @brief Close and free authentication state
 */
static void rd_kafka_sasl_oauthbearer_close (rd_kafka_transport_t *rktrans) {
        struct rd_kafka_sasl_oauthbearer_state *state =
                rktrans->rktrans_sasl.state;

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
        struct rd_kafka_sasl_oauthbearer_state *state =
                rktrans->rktrans_sasl.state;

        /*
         * https://tools.ietf.org/html/rfc7628#section-3.1
         * kvsep          = %x01
         * key            = 1*(ALPHA)
         * value          = *(VCHAR / SP / HTAB / CR / LF )
         * kvpair         = key "=" value kvsep
         * ;;gs2-header     = See RFC 5801
         * client-resp    = (gs2-header kvsep *kvpair kvsep) / kvsep
         */

        static const char *gs2_header = "n,,";
        static const char *kvsep = "\x01";
        const int kvsep_size = strlen(kvsep);
        int extension_size = 0;
        int i;
        char *buf;
        int size_written;

        for (i = 0 ; i < rd_list_cnt(&state->extensions) ; i++) {
                rd_strtup_t *extension = rd_list_elem(&state->extensions, i);
                // kvpair         = key "=" value kvsep
                extension_size += strlen(extension->name) + 1 // "="
                        + strlen(extension->value) + kvsep_size;
        }

        // client-resp    = (gs2-header kvsep *kvpair kvsep) / kvsep
        out->size = strlen(gs2_header) + kvsep_size
                + strlen("auth=Bearer ") + strlen(state->token_value)
                + kvsep_size + extension_size + kvsep_size;
        out->ptr = rd_malloc(out->size+1);

        buf = out->ptr;
        size_written = 0;
        size_written += rd_snprintf(buf, out->size+1 - size_written,
                "%s%sauth=Bearer %s%s", gs2_header, kvsep, state->token_value,
                kvsep);
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
        struct rd_kafka_sasl_oauthbearer_state *state =
                rktrans->rktrans_sasl.state;
        rd_chariov_t out = RD_ZERO_INIT;
        int r = -1;

        rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY, "SASLOAUTHBEARER",
                   "SASL OAUTHBEARER client in state %s",
                   state_names[state->state]);

        switch (state->state)
        {
        case RD_KAFKA_SASL_OAUTHBEARER_STATE_SEND_CLIENT_FIRST_MESSAGE:
                rd_dassert(!in); /* Not expecting any server-input */

                rd_kafka_sasl_oauthbearer_build_client_first_message(rktrans,
                        &out);
                state->state = RD_KAFKA_SASL_OAUTHBEARER_STATE_RECEIVE_SERVER_FIRST_MESSAGE;
                break;


        case RD_KAFKA_SASL_OAUTHBEARER_STATE_RECEIVE_SERVER_FIRST_MESSAGE:
                if (!in || in->ptr[0] == '\0') {
                        /* Success */
                        rd_rkb_dbg(rktrans->rktrans_rkb,
                                SECURITY | RD_KAFKA_DBG_BROKER,
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
                r = 0; // will fail later in next state after sending response
                break;

        case RD_KAFKA_SASL_OAUTHBEARER_STATE_RECEIVE_SERVER_MESSAGE_AFTER_FAILURE:
                rd_dassert(!in); /* Not expecting any server-input */

                /* Failure as previosuly communicated by server first message */
                rd_snprintf(errstr, errstr_size,
                        "SASL OAUTHBEARER authentication failed (principal=%s): %s",
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
                rd_snprintf(errstr, errstr_size,
                "OUTHBEARER cannot log in because there is no token available");
                return -1;
        }
        state->token_value = strdup(
                rktrans->rktrans_rkb->rkb_rk->rk_oauthbearer->token_value);
        state->md_principal_name = strdup(
                rktrans->rktrans_rkb->rkb_rk->rk_oauthbearer->md_principal_name);
        rd_list_copy_to(&state->extensions,
                &rktrans->rktrans_rkb->rkb_rk->rk_oauthbearer->extensions,
                rd_strtup_list_copy, NULL);
        rwlock_rdunlock(
                &rktrans->rktrans_rkb->rkb_rk->rk_oauthbearer->refresh_lock);

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
 * @brief Validate OAUTHBEARER config, which is a no-op
 * (we rely on initial token retrieval)
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
