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

extern const char *rd_kafka_fetch_states[];

void rd_kafka_toppar_destroy0 (rd_kafka_toppar_t *rktp);
void rd_kafka_toppar_insert_msg (rd_kafka_toppar_t *rktp, rd_kafka_msg_t *rkm);
void rd_kafka_toppar_enq_msg (rd_kafka_toppar_t *rktp, rd_kafka_msg_t *rkm);
void rd_kafka_toppar_deq_msg (rd_kafka_toppar_t *rktp, rd_kafka_msg_t *rkm);
void rd_kafka_toppar_insert_msgq (rd_kafka_toppar_t *rktp,
				  rd_kafka_msgq_t *rkmq);

#define rd_kafka_topic_keep(rkt) (void)rd_atomic_add(&(rkt->rkt_refcnt), 1)
void rd_kafka_topic_destroy0 (rd_kafka_topic_t *rkt);

rd_kafka_toppar_t *rd_kafka_toppar_get (const rd_kafka_topic_t *rkt,
					int32_t partition,
					int ua_on_miss);
rd_kafka_toppar_t *rd_kafka_toppar_get2 (rd_kafka_t *rk,
					 const rd_kafkap_str_t *topic,
					 int32_t partition,
					 int ua_on_miss);

rd_kafka_toppar_t *rd_kafka_toppar_desired_get (rd_kafka_topic_t *rkt,
						int32_t partition);
rd_kafka_toppar_t *rd_kafka_toppar_desired_add (rd_kafka_topic_t *rkt,
						int32_t partition);
void rd_kafka_toppar_desired_del (rd_kafka_toppar_t *rktp);

rd_kafka_topic_t *rd_kafka_topic_find (rd_kafka_t *rk,
				       const char *topic);
rd_kafka_topic_t *rd_kafka_topic_find0 (rd_kafka_t *rk,
					const rd_kafkap_str_t *topic);

int rd_kafka_toppar_ua_move (rd_kafka_topic_t *rkt, rd_kafka_msgq_t *rkmq);

void rd_kafka_toppar_broker_delegate (rd_kafka_toppar_t *rktp,
				      rd_kafka_broker_t *rkb);

void rd_kafka_topic_partitions_remove (rd_kafka_topic_t *rkt);

void rd_kafka_topic_metadata_none (rd_kafka_topic_t *rkt);

int rd_kafka_topic_metadata_update (rd_kafka_broker_t *rkb,
				    const struct rd_kafka_TopicMetadata *tm);
int rd_kafka_topic_scan_all (rd_kafka_t *rk, rd_ts_t now);
