/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2021 Magnus Edenhill
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


#ifndef _RDHTTP_H_
#define _RDHTTP_H_

#define CJSON_HIDE_SYMBOLS
#include "cJSON.h"


typedef struct rd_http_error_s {
        int code;
        char *errstr;
        char data[1];  /**< This is where the error string begins. */
} rd_http_error_t;

void rd_http_error_destroy (rd_http_error_t *herr);

rd_http_error_t *rd_http_get (const char *url, rd_buf_t **rbufp);
rd_http_error_t *rd_http_get_json (const char *url, cJSON **jsonp);

void rd_http_global_init (void);




#ifdef LIBCURL_VERSION
/* Advanced API that exposes the underlying CURL handle.
 * Requires caller to have included curl.h prior to this file. */


typedef struct rd_http_req_s {
        CURL *hreq_curl;     /**< CURL handle */
        rd_buf_t *hreq_buf;  /**< Response buffer */
        int hreq_code;       /**< HTTP response code */
        char hreq_curl_errstr[CURL_ERROR_SIZE]; /**< Error string for curl to
                                                 *   write to. */
} rd_http_req_t;

static void rd_http_req_destroy (rd_http_req_t *hreq);
rd_http_error_t *rd_http_req_init (rd_http_req_t *hreq, const char *url);
rd_http_error_t *rd_http_req_perform_sync (rd_http_req_t *hreq);
#endif



#endif /* _RDHTTP_H_ */
