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

#ifndef _RDKAFKA_INTERCEPTOR_H
#define _RDKAFKA_INTERCEPTOR_H

void
rd_kafka_interceptors_on_send (rd_kafka_t *rk, rd_kafka_message_t *rkmessage);
void
rd_kafka_interceptors_on_acknowledgement (rd_kafka_t *rk,
                                          rd_kafka_message_t *rkmessage);
void
rd_kafka_interceptors_on_acknowledgement_queue (rd_kafka_t *rk,
                                                rd_kafka_msgq_t *rkmq);

void rd_kafka_interceptors_on_consume (rd_kafka_t *rk,
                                       rd_kafka_message_t *rkmessage);
void
rd_kafka_interceptors_on_commit (rd_kafka_t *rk,
                                 const rd_kafka_topic_partition_list_t *offsets,
                                 rd_kafka_resp_err_t err);


void rd_kafka_conf_interceptor_ctor (int scope, void *pconf);
void rd_kafka_conf_interceptor_dtor (int scope, void *pconf);
void rd_kafka_conf_interceptor_copy (int scope, void *pdst, const void *psrc,
                                     void *dstptr, const void *srcptr);

#endif /* _RDKAFKA_INTERCEPTOR_H */
