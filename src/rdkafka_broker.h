/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012,2013 Magnus Edenhill
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

extern const char *rd_kafka_broker_state_names[];

#define rd_kafka_buf_keep(rkbuf) (void)rd_atomic_add(&(rkbuf)->rkbuf_refcnt, 1)
void rd_kafka_buf_destroy (rd_kafka_buf_t *rkbuf);

rd_kafka_broker_t *rd_kafka_broker_find_by_nodeid (rd_kafka_t *rk,
						   int32_t nodeid);

rd_kafka_broker_t *rd_kafka_broker_any (rd_kafka_t *rk, int state);

void rd_kafka_topic_leader_query0 (rd_kafka_t *rk, rd_kafka_topic_t *rkt,
				   int do_rk_lock);
#define rd_kafka_topic_leader_query(rk,rkt) \
	rd_kafka_topic_leader_query0(rk,rkt,1)
void rd_kafka_broker_destroy (rd_kafka_broker_t *rkb);

void rd_kafka_dr_msgq (rd_kafka_t *rk,
		       rd_kafka_msgq_t *rkmq, rd_kafka_resp_err_t err);


void rd_kafka_broker_metadata_req (rd_kafka_broker_t *rkb,
                                   int all_topics,
                                   rd_kafka_topic_t *only_rkt,
                                   rd_kafka_q_t *replyq,
                                   const char *reason);

rd_kafka_resp_err_t rd_kafka_toppar_offset_commit (rd_kafka_toppar_t *rktp,
                                                   int64_t offset);
