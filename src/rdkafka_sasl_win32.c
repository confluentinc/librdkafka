/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2016 Magnus Edenhill
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
 * Impelements SASL Kerberos GSSAPI authentication client
 * using the native Win32 SSPI.
 */

#include "rdkafka_int.h"
#include "rdkafka_transport.h"
#include "rdkafka_transport_int.h"
#include "rdkafka_sasl.h"
#include "rdkafka_sasl_int.h"


#include <stdio.h>
#include <windows.h>

#define SECURITY_WIN32
#pragma comment(lib, "Secur32.lib")
#include <Sspi.h>


#define RD_KAFKA_SASL_SSPI_CTX_ATTRS \
 (ISC_REQ_CONFIDENTIALITY | ISC_REQ_REPLAY_DETECT | \
  ISC_REQ_SEQUENCE_DETECT | ISC_REQ_CONNECTION)


/**
 * @brief Per-connection SASL state
 */
struct rd_kafka_sasl_state_s {
        CredHandle *cred;
        CtxtHandle *ctx;
        wchar_t principal[512];
};


/**
 * @returns the string representation of a SECURITY_STATUS error code
 */
static const char *rd_kafka_sasl_sspi_err2str (SECURITY_STATUS sr) {
        switch (sr)
        {
                case SEC_E_INSUFFICIENT_MEMORY:
                        return "Insufficient memory";
                case SEC_E_INTERNAL_ERROR:
                        return "Internal error";
                case SEC_E_INVALID_HANDLE:
                        return "Invalid handle";
                case SEC_E_INVALID_TOKEN:
                        return "Invalid token";
                case SEC_E_LOGON_DENIED:
                        return "Logon denied";
                case SEC_E_NO_AUTHENTICATING_AUTHORITY:
                        return "No authority could be contacted for authentication.";
                case SEC_E_NO_CREDENTIALS:
                        return "No credentials";
                case SEC_E_TARGET_UNKNOWN:
                        return "Target unknown";
                case SEC_E_UNSUPPORTED_FUNCTION:
                        return "Unsupported functionality";
                case SEC_E_WRONG_CREDENTIAL_HANDLE:
                        return "The principal that received the authentication "
                                "request is not the same as the one passed "
                                "into  the pszTargetName parameter. "
                                "This indicates a failure in mutual "
                                "authentication.";
                default:
                        return "(no string representation)";
        }
}


/**
 * @brief Create new CredHandle
 */
static CredHandle *
rd_kafka_sasl_sspi_cred_new (rd_kafka_transport_t *rktrans,
                              char *errstr, size_t errstr_size) {
        TimeStamp expiry = { 0, 0 };
        SECURITY_STATUS sr;
        CredHandle *cred = rd_calloc(1, sizeof(*cred));

        sr = AcquireCredentialsHandle(
                NULL, __TEXT("Kerberos"), SECPKG_CRED_OUTBOUND,
                NULL, NULL, NULL, NULL, cred, &expiry);

        if (sr != SEC_E_OK) {
                rd_free(cred);
                rd_snprintf(errstr, errstr_size,
                            "Failed to acquire CredentialsHandle: "
                            "error code %d", sr);
                return NULL;
        }

        rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY, "SASL",
                   "Acquired Kerberos credentials handle (expiry in %d.%ds)",
                   expiry.u.HighPart, expiry.u.LowPart);

        return cred;
}


/**
  * @brief Start or continue SSPI-based authentication processing.
  */
static int rd_kafka_sasl_sspi_continue (rd_kafka_transport_t *rktrans,
                                        const void *inbuf, size_t insize,
                                        char *errstr, size_t errstr_size) {
        rd_kafka_sasl_state_t *state = rktrans->rktrans_sasl.state;
        SecBufferDesc outbufdesc, inbufdesc;
        SecBuffer outsecbuf, insecbuf;
        BYTE outbuf[12288];
        TimeStamp lifespan = { 0, 0 };
        ULONG ret_ctxattrs;
        CtxtHandle *ctx;
        SECURITY_STATUS sr;

        if (inbuf) {
                inbufdesc.ulVersion = SECBUFFER_VERSION;
                inbufdesc.cBuffers = 1;
                inbufdesc.pBuffers  = &insecbuf;

                insecbuf.cbBuffer   = insize;
                insecbuf.BufferType = SECBUFFER_TOKEN;
                insecbuf.pvBuffer   = (void *)inbuf;
        }

        outbufdesc.ulVersion = SECBUFFER_VERSION;
        outbufdesc.cBuffers  = 1;
        outbufdesc.pBuffers  = &outsecbuf;

        outsecbuf.cbBuffer   = sizeof(outbuf);
        outsecbuf.BufferType = SECBUFFER_TOKEN;
        outsecbuf.pvBuffer   = outbuf;

        if (!(ctx = state->ctx)) {
                /* First time: allocate context handle
                 * which will be filled in by Initialize..() */
                ctx = rd_calloc(1, sizeof(*ctx));
        }

        sr = InitializeSecurityContext(
                state->cred, state->ctx, state->principal,
                RD_KAFKA_SASL_SSPI_CTX_ATTRS |
                (state->ctx ? 0 : ISC_REQ_MUTUAL_AUTH | ISC_REQ_IDENTIFY),
                0, SECURITY_NATIVE_DREP,
                inbuf ? &inbufdesc : NULL,
                0, ctx, &outbufdesc, &ret_ctxattrs, &lifespan);

        if (!state->ctx)
                state->ctx = ctx;

        switch (sr)
        {
                case SEC_E_OK:
                        rktrans->rktrans_sasl.complete = 1;
                        break;
                case SEC_I_CONTINUE_NEEDED:
                        break;
                case SEC_I_COMPLETE_NEEDED:
                case SEC_I_COMPLETE_AND_CONTINUE:
                        rd_snprintf(errstr, errstr_size,
                                    "CompleteAuthToken (Digest auth, %d) "
                                    "not implemented", sr);
                        return -1;
                case SEC_I_INCOMPLETE_CREDENTIALS:
                        rd_snprintf(errstr, errstr_size,
                                    "Incomplete credentials: "
                                    "invalid or untrusted certificate");
                        return -1;
                default:
                        rd_snprintf(errstr, errstr_size,
                                    "InitializeSecurityContext "
                                    "failed: %s (0x%x)",
                                    rd_kafka_sasl_sspi_err2str(sr), sr);
                        return -1;
        }

        if (!rktrans->rktrans_sasl.complete) {
                if (rd_kafka_sasl_send(rktrans,
                                       outsecbuf.pvBuffer, outsecbuf.cbBuffer,
                                       errstr, errstr_size) == -1)
                        return -1;
        }

        return 0;
}



/**
* @brief Handle SASL frame received from broker.
*/
static int rd_kafka_sasl_win32_recv (struct rd_kafka_transport_s *rktrans,
                                     const void *buf, size_t size,
                                     char *errstr, size_t errstr_size) {
        rd_kafka_sasl_state_t *state = rktrans->rktrans_sasl.state;

        if (rktrans->rktrans_sasl.complete) {
                if (size != 0) {
                        rd_snprintf(errstr, errstr_size,
                                    "Received %"PRIdsz" bytes after (local) "
                                    "authenticator indicated completion", size);
                        return -1;
                }

                /* Final ack from broker. */
                rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY, "SASLAUTH",
                           "Authenticated");
                rd_kafka_sasl_auth_done(rktrans);
                return 0;
        }

        return rd_kafka_sasl_sspi_continue(rktrans, buf, size,
                                           errstr, errstr_size);
}


/**
* @brief Decommission SSPI state
*/
static void rd_kafka_sasl_win32_close (rd_kafka_transport_t *rktrans) {
        rd_kafka_sasl_state_t *state = rktrans->rktrans_sasl.state;
        if (state->ctx) {
                DeleteSecurityContext(state->ctx);
                rd_free(state->ctx);
        }
        if (state->cred) {
                FreeCredentialsHandle(state->cred);
                rd_free(state->cred);
        }
        rd_free(state);
}


int rd_kafka_sasl_win32_client_new (rd_kafka_transport_t *rktrans,
                                    const char *hostname,
                                    char *errstr, size_t errstr_size) {
        rd_kafka_t *rk = rktrans->rktrans_rkb->rkb_rk;
        rd_kafka_sasl_state_t *state;

        if (strcmp(rk->rk_conf.sasl.mechanisms, "GSSAPI")) {
                rd_snprintf(errstr, errstr_size,
                            "SASL mechanism \"%s\" not supported on platform",
                            rk->rk_conf.sasl.mechanisms);
                return -1;
        }

        state = rd_calloc(1, sizeof(*state));
        rktrans->rktrans_sasl.state = state;
        rktrans->rktrans_sasl.recv = rd_kafka_sasl_win32_recv;
        rktrans->rktrans_sasl.close = rd_kafka_sasl_win32_close;

        _snwprintf(state->principal, RD_ARRAYSIZE(state->principal),
                   L"%hs/%hs",
                   rktrans->rktrans_rkb->rkb_rk->rk_conf.sasl.principal,
                   hostname);

        state->cred = rd_kafka_sasl_sspi_cred_new(rktrans, errstr, errstr_size);
        if (!state->cred)
                return -1;

        if (rd_kafka_sasl_send(rktrans, NULL, 0, errstr, errstr_size) == -1)
                return -1;

#if 0
        if (rd_kafka_sasl_sspi_continue(rktrans, NULL, 0,
                                        errstr, errstr_size) == -1)
                return -1;
#endif
        return 0;
}
