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

#include "rdkafka_cgrp.h"

void rd_kafka_GroupCoordinatorRequest (rd_kafka_broker_t *rkb,
                                       const rd_kafkap_str_t *cgrp,
                                       rd_kafka_q_t *replyq,
                                       rd_kafka_resp_cb_t *resp_cb,
                                       void *opaque);


void rd_kafka_toppar_handle_Offset (rd_kafka_broker_t *rkb,
                                    rd_kafka_resp_err_t err,
                                    rd_kafka_buf_t *rkbuf,
                                    rd_kafka_buf_t *request,
                                    void *opaque);
void rd_kafka_toppar_lag_handle_Offset (rd_kafka_broker_t *rkb,
                                        rd_kafka_resp_err_t err,
                                        rd_kafka_buf_t *rkbuf,
                                        rd_kafka_buf_t *request,
                                        void *opaque);
void rd_kafka_OffsetRequest (rd_kafka_broker_t *rkb,
                             rd_kafka_toppar_t *rktp,
                             int64_t query_offset,
                             rd_kafka_q_t *replyq,
                             rd_kafka_resp_cb_t *resp_cb,
                             void *opaque);

void rd_kafka_op_handle_OffsetFetch (rd_kafka_broker_t *rkb,
                                     rd_kafka_resp_err_t err,
                                     rd_kafka_buf_t *rkbuf,
                                     rd_kafka_buf_t *request,
                                     void *opaque);

void rd_kafka_OffsetFetchRequest (rd_kafka_broker_t *rkb,
                                  int16_t api_version,
                                  const rd_kafka_topic_partition_list_t *parts,
                                  rd_kafka_q_t *replyq,
                                  rd_kafka_resp_cb_t *resp_cb,
                                  void *opaque);



void rd_kafka_op_handle_OffsetCommit (rd_kafka_broker_t *rkb,
                                      rd_kafka_resp_err_t err,
                                      rd_kafka_buf_t *rkbuf,
                                      rd_kafka_buf_t *request,
                                      void *opaque);
void rd_kafka_OffsetCommitRequest (rd_kafka_broker_t *rkb,
                                   rd_kafka_cgrp_t *rkcg,
                                   int16_t api_version,
                                   rd_kafka_topic_partition_list_t *offsets,
                                   rd_kafka_q_t *replyq,
                                   rd_kafka_resp_cb_t *resp_cb,
                                   void *opaque);



void rd_kafka_JoinGroupRequest (rd_kafka_broker_t *rkb,
                                const rd_kafkap_str_t *group_id,
                                const rd_kafkap_str_t *member_id,
                                const rd_kafkap_str_t *protocol_type,
                                const rd_kafka_topic_partition_list_t
                                *subscription,
                                rd_kafka_q_t *replyq,
                                rd_kafka_resp_cb_t *resp_cb,
                                void *opaque);
void rd_kafka_cgrp_handle_JoinGroup (rd_kafka_broker_t *rkb,
                                     rd_kafka_resp_err_t err,
                                     rd_kafka_buf_t *rkbuf,
                                     rd_kafka_buf_t *request,
                                     void *opaque);


void rd_kafka_LeaveGroupRequest (rd_kafka_broker_t *rkb,
                                 const rd_kafkap_str_t *group_id,
                                 const rd_kafkap_str_t *member_id,
                                 rd_kafka_q_t *replyq,
                                 rd_kafka_resp_cb_t *resp_cb,
                                 void *opaque);
void rd_kafka_handle_LeaveGroup (rd_kafka_broker_t *rkb,
                                 rd_kafka_resp_err_t err,
                                 rd_kafka_buf_t *rkbuf,
                                 rd_kafka_buf_t *request,
                                 void *opaque);

void rd_kafka_SyncGroupRequest (rd_kafka_broker_t *rkb,
                                const rd_kafkap_str_t *group_id,
                                int32_t generation_id,
                                const rd_kafkap_str_t *member_id,
                                const rd_kafka_group_member_t
                                *assignments,
                                int assignment_cnt,
                                rd_kafka_q_t *replyq,
                                rd_kafka_resp_cb_t *resp_cb,
                                void *opaque);
void rd_kafka_handle_SyncGroup (rd_kafka_broker_t *rkb,
                                rd_kafka_resp_err_t err,
                                rd_kafka_buf_t *rkbuf,
                                rd_kafka_buf_t *request,
                                void *opaque);

void rd_kafka_ListGroupsRequest (rd_kafka_broker_t *rkb,
                                 rd_kafka_q_t *replyq,
                                 rd_kafka_resp_cb_t *resp_cb,
                                 void *opaque);

void rd_kafka_DescribeGroupsRequest (rd_kafka_broker_t *rkb,
                                     const char **groups, int group_cnt,
                                     rd_kafka_q_t *replyq,
                                     rd_kafka_resp_cb_t *resp_cb,
                                     void *opaque);


void rd_kafka_HeartbeatRequest (rd_kafka_broker_t *rkb,
                                const rd_kafkap_str_t *group_id,
                                int32_t generation_id,
                                const rd_kafkap_str_t *member_id,
                                rd_kafka_q_t *replyq,
                                rd_kafka_resp_cb_t *resp_cb,
                                void *opaque);
void rd_kafka_cgrp_handle_Heartbeat (rd_kafka_broker_t *rkb,
                                     rd_kafka_resp_err_t err,
                                     rd_kafka_buf_t *rkbuf,
                                     rd_kafka_buf_t *request,
                                     void *opaque);

rd_kafka_buf_t *rd_kafka_MetadataRequest0 (rd_kafka_broker_t *rkb,
                                           int all_topics,
                                           rd_kafka_itopic_t *only_rkt,
                                           const char *reason);
void rd_kafka_MetadataRequest (rd_kafka_broker_t *rkb,
                               int all_topics,
                               rd_kafka_itopic_t *only_rkt,
                               const char *reason,
                               rd_kafka_q_t *replyq,
                               rd_kafka_resp_cb_t *resp_cb,
                               void *opaque);
void rd_kafka_op_handle_Metadata (rd_kafka_broker_t *rkb,
                                  rd_kafka_resp_err_t err,
                                  rd_kafka_buf_t *rkbuf,
                                  rd_kafka_buf_t *request,
                                  void *opaque);
