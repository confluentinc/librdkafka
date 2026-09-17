/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2026, Magnus Edenhill
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

#include "rd.h"
#include "rdkafka_int.h"
#include "rdkafka_msg.h"
#include <stdlib.h>
#include <string.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
        rd_kafka_msg_t rkm;
        memset(&rkm, 0, sizeof(rkm));
        
        /* Initialize a consumer message with binary headers from fuzzer data */
        rkm.rkm_u.consumer.binhdrs.data = (char *)data;
        rkm.rkm_u.consumer.binhdrs.len = size;
        
        /* The header parser will be triggered by rd_kafka_message_headers() */
        rd_kafka_headers_t *hdrs = NULL;
        rd_kafka_message_headers(&rkm.rkm_rkmessage, &hdrs);
        
        /* We don't need to destroy hdrs because they are owned by rkm
         * and would be destroyed by rd_kafka_msg_destroy(&rkm),
         * but since rkm is on stack and we didn't use RD_KAFKA_MSG_F_FREE_RKM,
         * we only need to clean up the headers if they were allocated. */
        if (rkm.rkm_headers)
                rd_kafka_headers_destroy(rkm.rkm_headers);

        return 0;
}
