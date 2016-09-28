/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012-2015, Magnus Edenhill
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

#pragma once


rd_kafka_resp_err_t rd_kafka_metadata0 (rd_kafka_t *rk,
					int all_topics,
					rd_kafka_itopic_t *only_rkt,
					rd_kafka_replyq_t replyq,
					const char *reason);

struct rd_kafka_metadata *
rd_kafka_parse_Metadata (rd_kafka_broker_t *rkb,
                         rd_kafka_itopic_t *rkt, rd_kafka_buf_t *rkbuf,
			 int all_topics);

struct rd_kafka_metadata *
rd_kafka_metadata_copy (const struct rd_kafka_metadata *md, size_t size);

size_t
rd_kafka_metadata_topic_match (rd_kafka_t *rk,
			       rd_list_t *list,
			       const struct rd_kafka_metadata *metadata,
			       const rd_kafka_topic_partition_list_t *match);
