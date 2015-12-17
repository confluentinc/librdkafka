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

#include "rdkafka_int.h"
#include "rdkafka_request.h"
#include "rdkafka_broker.h"
#include "rdkafka_offset.h"
#include "rdkafka_topic.h"
#include "rdkafka_partition.h"


/**
 * Kafka protocol request and response handling.
 * All of this code runs in the broker thread and uses op queues for
 * propagating results back to the various sub-systems operating in
 * other threads.
 */

#define RD_KAFKA_ERR_ACTION_PERMANENT   0x1 /* Permanent error */
#define RD_KAFKA_ERR_ACTION_IGNORE      0x2 /* Error can be ignored */
#define RD_KAFKA_ERR_ACTION_REFRESH     0x4 /* Refresh state (e.g., metadata) */
#define RD_KAFKA_ERR_ACTION_INFORM      0x8 /* Inform application about err */

static int rd_kafka_err_action (rd_kafka_broker_t *rkb,
                                rd_kafka_resp_err_t err,
                                rd_kafka_buf_t *rkbuf,
                                rd_kafka_buf_t *request) {
        int actions = 0;

        if (err && rkb && request)
                rd_rkb_dbg(rkb, BROKER, "REQERR",
                           "%sRequest failed: %s",
                           rd_kafka_ApiKey2str(request->rkbuf_reqhdr.ApiKey),
                           rd_kafka_err2str(err));

        // FIXME: ILLEGAL_GENERATION
        switch (err)
        {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
                break;
        case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
        case RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION:
        case RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_GROUP_COORDINATOR_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_NOT_COORDINATOR_FOR_GROUP:
                /* Request metadata information update */
                actions |= RD_KAFKA_ERR_ACTION_REFRESH;
                break;
        case RD_KAFKA_RESP_ERR__DESTROY:
        default:
                actions |= RD_KAFKA_ERR_ACTION_PERMANENT;
                break;
        }

        return actions;
}


static void rd_kafka_assignor_handle_Metadata (rd_kafka_broker_t *rkb,
                                               rd_kafka_resp_err_t err,
                                               rd_kafka_buf_t *rkbuf,
                                               rd_kafka_buf_t *request,
                                               void *opaque);

/**
 * Send GroupCoordinatorRequest
 */
void rd_kafka_GroupCoordinatorRequest (rd_kafka_broker_t *rkb,
                                       const rd_kafkap_str_t *cgrp,
                                       rd_kafka_q_t *replyq,
                                       rd_kafka_resp_cb_t *resp_cb,
                                       void *opaque) {
        rd_kafka_buf_t *rkbuf;

        rkbuf = rd_kafka_buf_new(rkb->rkb_rk, 1, 0);
        rd_kafka_buf_push_kstr(rkbuf, cgrp);

        rd_kafka_broker_buf_enq_replyq(rkb, RD_KAFKAP_GroupCoordinator,
                                       rkbuf, replyq, resp_cb, opaque);
}







/**
 * Parses and handles Offset replies.
 * Returns the parsed Offset in '*Offsetp'.
 * Returns 0 on success, else an error.
 */
static rd_kafka_resp_err_t rd_kafka_handle_Offset (rd_kafka_broker_t *rkb,
                                                   rd_kafka_resp_err_t err,
                                                   rd_kafka_buf_t *rkbuf,
                                                   rd_kafka_buf_t *request,
                                                   rd_kafka_toppar_t *rktp,
                                                   int64_t *Offsetp) {
        const int log_decode_errors = 1;
        int16_t ErrorCode = 0;
        int32_t TopicArrayCnt;
        int64_t Offset = -1;
        int hit = 0;
        int actions;

        if (err) {
                ErrorCode = err;
                goto err;
        }

        rd_kafka_buf_read_i32(rkbuf, &TopicArrayCnt);
        while (TopicArrayCnt-- > 0) {
                rd_kafkap_str_t topic;
                int32_t PartArrayCnt;

                rd_kafka_buf_read_str(rkbuf, &topic);
                rd_kafka_buf_read_i32(rkbuf, &PartArrayCnt);

                while (PartArrayCnt-- > 0) {
                        int32_t partition;
                        int32_t OffsetArrayCnt;
                        int correct_partition;

                        rd_kafka_buf_read_i32(rkbuf, &partition);
                        rd_kafka_buf_read_i16(rkbuf, &ErrorCode);
                        rd_kafka_buf_read_i32(rkbuf, &OffsetArrayCnt);

                        correct_partition =
                                !rd_kafkap_str_cmp(&topic,
                                                   rktp->rktp_rkt->rkt_topic) &&
                                partition == rktp->rktp_partition;

                        while (OffsetArrayCnt-- > 0) {
                                rd_kafka_buf_read_i64(rkbuf, &Offset);

                                if (correct_partition) {
                                        hit = 1;
                                        goto done;
                                }
                        }
                }
        }

err:
        actions = rd_kafka_err_action(rkb, ErrorCode, rkbuf, request);
        if (actions & RD_KAFKA_ERR_ACTION_REFRESH) {
                /* Re-query for coordinator */
                rd_kafka_toppar_lock(rktp);
                rd_kafka_broker_metadata_req(rktp->rktp_leader, 0,
                                             rktp->rktp_rkt, NULL,
                                             "Offset request failed");
                rd_kafka_toppar_unlock(rktp);
                /* Schedule a retry */
                rd_kafka_buf_keep(request);
                rd_kafka_broker_buf_retry(request->rkbuf_rkb, request);
                return RD_KAFKA_RESP_ERR__IN_PROGRESS;
        }

done:
        if (!ErrorCode && !hit)
                ErrorCode = RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION;

        if (!ErrorCode)
                *Offsetp = Offset;

        return ErrorCode;
}


/**
 * Toppar based OffsetResponse handling.
 * This is used for finding the next offset to Fetch.
 */
void rd_kafka_toppar_handle_Offset (rd_kafka_broker_t *rkb,
                                    rd_kafka_resp_err_t err,
                                    rd_kafka_buf_t *rkbuf,
                                    rd_kafka_buf_t *request,
                                    void *opaque) {
        shptr_rd_kafka_toppar_t *s_rktp = opaque;
        rd_kafka_toppar_t *rktp = rd_kafka_toppar_s2i(s_rktp);
        int64_t Offset;

        /* Parse and return Offset */
        err = rd_kafka_handle_Offset(rkb, err, rkbuf, request, rktp, &Offset);

        if (err) {
                rd_kafka_op_t *rko;

                rd_rkb_dbg(rkb, TOPIC, "OFFSET",
                           "Offset reply error for "
                           "topic %.*s [%"PRId32"]: %s",
                           RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                           rktp->rktp_partition, rd_kafka_err2str(err));

                if (err == RD_KAFKA_RESP_ERR__DESTROY) {
                        /* Termination, quick cleanup. */

                        /* from request.opaque */
                        rd_kafka_toppar_destroy(s_rktp);

                        return;
                }
		rd_kafka_toppar_lock(rktp);
                rd_kafka_offset_reset(rktp, rktp->rktp_query_offset,
                                      err, "failed to query logical offset");
		rd_kafka_toppar_unlock(rktp);

                /* Signal error back to application,
                 * unless this is an intermittent problem
                 * (e.g.,connection lost) */
                if (err != RD_KAFKA_RESP_ERR__TRANSPORT) {
                        rko = rd_kafka_op_new(RD_KAFKA_OP_CONSUMER_ERR);
                        rko->rko_err = err;
                        if (rktp->rktp_query_offset <=
                            RD_KAFKA_OFFSET_TAIL_BASE)
                                rko->rko_rkmessage.offset =
                                        rktp->rktp_query_offset -
                                        RD_KAFKA_OFFSET_TAIL_BASE;
                        else
                                rko->rko_rkmessage.offset =
                                        rktp->rktp_query_offset;
                        rko->rko_rkmessage.rkt =
                                rd_kafka_topic_keep_a(rktp->rktp_rkt);
                        rko->rko_rkmessage.partition = rktp->rktp_partition;

                        rd_kafka_q_enq(&rktp->rktp_fetchq, rko);
                }

                rd_kafka_toppar_destroy(s_rktp); /* from request.opaque */
                return;
        }

        rd_kafka_dbg(rktp->rktp_rkt->rkt_rk, TOPIC, "OFFSET",
                     "Offset %"PRId64" request for %.*s [%"PRId32"] "
                     "returned offset %s (%"PRId64")",
                     rktp->rktp_query_offset,
                     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                     rktp->rktp_partition, rd_kafka_offset2str(Offset), Offset);

	rd_kafka_toppar_lock(rktp);
        rd_kafka_toppar_next_offset_handle(rktp, Offset);
	rd_kafka_toppar_unlock(rktp);

        rd_kafka_toppar_destroy(s_rktp); /* from request.opaque */
}


/**
 * Toppar based OffsetResponse handling.
 * This is used for updating the low water mark for consumer lag.
 */
void rd_kafka_toppar_lag_handle_Offset (rd_kafka_broker_t *rkb,
                                        rd_kafka_resp_err_t err,
                                        rd_kafka_buf_t *rkbuf,
                                        rd_kafka_buf_t *request,
                                        void *opaque) {
        shptr_rd_kafka_toppar_t *s_rktp = opaque;
        rd_kafka_toppar_t *rktp = rd_kafka_toppar_s2i(s_rktp);
        int64_t Offset;

        /* Parse and return Offset */
        err = rd_kafka_handle_Offset(rkb, err, rkbuf, request, rktp, &Offset);


        if (!err)
                rktp->rktp_lo_offset = Offset;

        rd_kafka_toppar_destroy(s_rktp); /* from request.opaque */
}



/**
 * Send OffsetRequest for toppar 'rktp'.
 */
void rd_kafka_OffsetRequest (rd_kafka_broker_t *rkb,
                             rd_kafka_toppar_t *rktp,
                             int64_t query_offset,
                             rd_kafka_q_t *replyq,
                             rd_kafka_resp_cb_t *resp_cb,
                             void *opaque) {
	rd_kafka_buf_t *rkbuf;

	rkbuf = rd_kafka_buf_new(rkb->rkb_rk,
                                 3,/*Header,topic,subpart*/
				 /* ReplicaId+TopicArrayCnt */
				 4+4 +
				 /* Topic (pushed) */
				 /* PartArrayCnt+Partition+Time+MaxNumOffs */
				 4+4+8+4);

	/* ReplicaId */
	rd_kafka_buf_write_i32(rkbuf, -1);
	/* TopicArrayCnt */
	rd_kafka_buf_write_i32(rkbuf, 1);
	rd_kafka_buf_autopush(rkbuf);

	/* Topic */
	rd_kafka_buf_push_kstr(rkbuf, rktp->rktp_rkt->rkt_topic);

	/* PartitionArrayCnt */
	rd_kafka_buf_write_i32(rkbuf, 1);
	/* Partition */
	rd_kafka_buf_write_i32(rkbuf, rktp->rktp_partition);
	/* Time/Offset */
	rd_kafka_buf_write_i64(rkbuf, query_offset);
	/* MaxNumberOfOffsets */
	rd_kafka_buf_write_i32(rkbuf, 1);
	rd_kafka_buf_autopush(rkbuf);

	rd_rkb_dbg(rkb, TOPIC, "OFFSET",
		   "OffsetRequest (%"PRId64") for topic %s [%"PRId32"]",
                   query_offset,
                   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition);

	rd_kafka_broker_buf_enq_replyq(rkb, RD_KAFKAP_Offset,
                                       rkbuf, replyq, resp_cb, opaque);
}


/**
 * Generic handler for OffsetFetch responses.
 * Offsets for included partitions will be propagated through the passed
 * 'offsets' list.
 */
static void rd_kafka_handle_OffsetFetch (rd_kafka_broker_t *rkb,
                                         rd_kafka_resp_err_t err,
                                         rd_kafka_buf_t *rkbuf,
                                         rd_kafka_buf_t *request,
                                         rd_kafka_topic_partition_list_t
                                         *offsets) {
        const int log_decode_errors = 1;
        int32_t TopicArrayCnt;
        int64_t offset = -1;
        int16_t ErrorCode = 0;
        rd_kafkap_str_t metadata;
        int i;
        int actions;

        if (err) {
                ErrorCode = err;
                goto err;
        }

        /* Set default offset for all partitions. */
        rd_kafka_topic_partition_list_set_offsets(rkb->rkb_rk, offsets, 0,
                                                  RD_KAFKA_OFFSET_ERROR,
						  0 /* !is commit */);

        rd_kafka_buf_read_i32(rkbuf, &TopicArrayCnt);
        for (i = 0 ; i < TopicArrayCnt ; i++) {
                rd_kafkap_str_t topic;
                int32_t PartArrayCnt;
                char *topic_name;
                int j;

                rd_kafka_buf_read_str(rkbuf, &topic);
                rd_kafka_buf_read_i32(rkbuf, &PartArrayCnt);

                RD_KAFKAP_STR_DUPA(&topic_name, &topic);

                for (j = 0 ; j < PartArrayCnt ; j++) {
                        int32_t partition;
                        shptr_rd_kafka_toppar_t *s_rktp;
                        rd_kafka_topic_partition_t *rktpar;

                        rd_kafka_buf_read_i32(rkbuf, &partition);
                        rd_kafka_buf_read_i64(rkbuf, &offset);
                        rd_kafka_buf_read_str(rkbuf, &metadata);
                        rd_kafka_buf_read_i16(rkbuf, &ErrorCode);

                        rktpar = rd_kafka_topic_partition_list_find(offsets,
                                                                    topic_name,
                                                                    partition,
                                                                    NULL);

                        if (!rktpar)
                                continue;


                        s_rktp = rd_kafka_toppar_get2(rkb->rkb_rk, topic_name,
                                                      partition, 0, 1);
                        if (!s_rktp)
                                continue;

			if (offset == -1)
				rktpar->offset = RD_KAFKA_OFFSET_ERROR;
			else
				rktpar->offset = offset;
                        rktpar->err = ErrorCode;

                        if (rktpar->metadata)
                                rd_free(rktpar->metadata);

                        if (RD_KAFKAP_STR_IS_NULL(&metadata)) {
                                rktpar->metadata = NULL;
                                rktpar->metadata_size = 0;
                        } else {
                                rktpar->metadata = RD_KAFKAP_STR_DUP(&metadata);
                                rktpar->metadata_size =
                                        RD_KAFKAP_STR_LEN(&metadata);
                        }

                        if (!rktpar->_private)
                                rktpar->_private = s_rktp;
                        else
                                rd_kafka_toppar_destroy(s_rktp);

                }
        }


err:
        actions = rd_kafka_err_action(rkb, ErrorCode, rkbuf, request);

        if (actions & RD_KAFKA_ERR_ACTION_REFRESH) {
                /* Re-query for coordinator */
                rd_kafka_cgrp_op(rkb->rkb_rk->rk_cgrp, NULL,
                                 NULL, RD_KAFKA_OP_COORD_QUERY, ErrorCode);
                /* Schedule a retry */
                rd_kafka_buf_keep(request);
                rd_kafka_broker_buf_retry(request->rkbuf_rkb, request);
                return;
        }
}



/**
 * opaque=rko wrapper for handle_OffsetFetch.
 * rko->rko_payload MUST be a `rd_kafka_topic_partition_list_t *` which will
 * be filled in with fetch offsets.
 *
 * A reply will be sent on 'rko->rko_replyq' with type RD_KAFKA_OP_COMMIT.
 *
 * Locality: cgrp's broker thread
 */
void rd_kafka_op_handle_OffsetFetch (rd_kafka_broker_t *rkb,
                                     rd_kafka_resp_err_t err,
                                     rd_kafka_buf_t *rkbuf,
                                     rd_kafka_buf_t *request,
                                     void *opaque) {
        rd_kafka_op_t *rko = opaque;
        rd_kafka_op_t *rko_reply;
        rd_kafka_topic_partition_list_t *offsets = rko->rko_payload;

        if (err == RD_KAFKA_RESP_ERR__DESTROY) {
                /* Termination, quick cleanup. */
                rd_kafka_op_destroy(rko);
                return;
        }

        rd_kafka_assert(NULL, offsets != NULL);

        rko_reply = rd_kafka_op_new(RD_KAFKA_OP_OFFSET_FETCH|RD_KAFKA_OP_REPLY);
        rko_reply->rko_version = rko->rko_version;
        rd_kafka_op_payload_move(rko_reply, rko); /* move 'offsets' */

        rd_kafka_handle_OffsetFetch(rkb, err, rkbuf, request, offsets);

        rd_kafka_q_enq(rko->rko_replyq, rko_reply);

        rd_kafka_op_destroy(rko);
}






/**
 * Send OffsetFetchRequest for toppar.
 *
 * 'parts' must be a sorted list of topic+partitions.
 */
void rd_kafka_OffsetFetchRequest (rd_kafka_broker_t *rkb,
                                  int16_t api_version,
                                  const rd_kafka_topic_partition_list_t *parts,
                                  rd_kafka_q_t *replyq,
                                  rd_kafka_resp_cb_t *resp_cb,
                                  void *opaque) {
	rd_kafka_buf_t *rkbuf;
        int of_TopicCnt;
        int TopicCnt = 0;
        int of_PartCnt = -1;
        const char *last_topic = NULL;
        int PartCnt = 0;
        int i;

	rkbuf = rd_kafka_buf_new_growable(
                rkb->rkb_rk, 1,
                RD_KAFKAP_STR_SIZE(rkb->rkb_rk->rk_conf.group_id) +
                4 +
                (parts->cnt * 32));


        /* ConsumerGroup */
        rd_kafka_buf_write_kstr(rkbuf, rkb->rkb_rk->
                                rk_conf.group_id);
        /* TopicArrayCnt */
        of_TopicCnt = rd_kafka_buf_write_i32(rkbuf, 0); /* Updated later */

        for (i = 0 ; i < parts->cnt ; i++) {
                rd_kafka_topic_partition_t *rktpar = &parts->elems[i];

                if (last_topic == NULL || strcmp(last_topic, rktpar->topic)) {
                        /* New topic */

                        /* Finalize previous PartitionCnt */
                        if (PartCnt > 0)
                                rd_kafka_buf_update_u32(rkbuf, of_PartCnt,
                                                        PartCnt);

                        /* TopicName */
                        rd_kafka_buf_write_str(rkbuf, rktpar->topic, -1);
                        /* PartitionCnt, finalized later */
                        of_PartCnt = rd_kafka_buf_write_i32(rkbuf, 0);
                        PartCnt = 0;
                        TopicCnt++;
                }

                /* Partition */
                rd_kafka_buf_write_i32(rkbuf,  rktpar->partition);
                PartCnt++;
        }

        /* Finalize previous PartitionCnt */
        if (PartCnt > 0)
                rd_kafka_buf_update_u32(rkbuf, of_PartCnt,  PartCnt);

        /* Finalize TopicCnt */
        rd_kafka_buf_update_u32(rkbuf, of_TopicCnt, TopicCnt);


        /* Push write-buffer onto iovec stack */
        rd_kafka_buf_autopush(rkbuf);

        rd_kafka_buf_version_set(rkbuf, api_version);

	rd_rkb_dbg(rkb, TOPIC, "OFFSET",
		   "OffsetFetchRequest(v%d) for %d partition(s)",
                   api_version, parts->cnt);

	rd_kafka_broker_buf_enq_replyq(rkb, RD_KAFKAP_OffsetFetch, rkbuf,
                                       replyq, resp_cb, opaque);
}



/**
 * Handle OffsetCommitResponse
 * Takes the original 'rko' as opaque argument.
 */
void rd_kafka_op_handle_OffsetCommit (rd_kafka_broker_t *rkb,
                                      rd_kafka_resp_err_t err,
                                      rd_kafka_buf_t *rkbuf,
                                      rd_kafka_buf_t *request,
                                      void *opaque) {
        rd_kafka_op_t *rko_orig = opaque;
        const int log_decode_errors = 1;
        int32_t TopicArrayCnt;
        int16_t ErrorCode = 0;
        rd_kafka_q_t *replyq;
        rd_kafka_topic_partition_list_t *offsets = rko_orig->rko_payload;
        int i;
        int oi = 0;  /* index of offsets->elems */

        if (err) {
                ErrorCode = err;
                goto err;
        }

        rd_kafka_buf_read_i32(rkbuf, &TopicArrayCnt);
        for (i = 0 ; i < TopicArrayCnt ; i++) {
                rd_kafkap_str_t topic;
                char *topic_str;
                int32_t PartArrayCnt;
                int j;

                rd_kafka_buf_read_str(rkbuf, &topic);
                rd_kafka_buf_read_i32(rkbuf, &PartArrayCnt);

                RD_KAFKAP_STR_DUPA(&topic_str, &topic);

                for (j = 0 ; j < PartArrayCnt ; j++) {
                        int32_t partition;
                        rd_kafka_topic_partition_t *rktpar;

                        rd_kafka_buf_read_i32(rkbuf, &partition);
                        rd_kafka_buf_read_i16(rkbuf, &ErrorCode);

                        rktpar = rd_kafka_topic_partition_list_find(
                                offsets, topic_str, partition, &oi);

                        if (!rktpar) {
                                /* Received offset for topic/partition we didn't
                                 * ask for, this shouldn't really happen. */
                                continue;
                        }

                        rktpar->err = ErrorCode;
                }
        }


err:
        rd_kafka_err_action(rkb, ErrorCode, rkbuf, request);


        if (ErrorCode != RD_KAFKA_RESP_ERR__DESTROY &&
            (replyq = rko_orig->rko_replyq)) {
                rd_kafka_op_t *rko_reply = rd_kafka_op_new_reply(rko_orig);
                rd_kafka_op_payload_move(rko_reply, rko_orig);
                rko_reply->rko_err = ErrorCode;
                rd_kafka_q_enq(replyq, rko_reply);
        } else {
                rd_kafka_topic_partition_list_destroy(offsets);
        }

        rd_kafka_op_destroy(rko_orig);
}


/**
 * Send OffsetCommitRequest for toppar.
 */
void rd_kafka_OffsetCommitRequest (rd_kafka_broker_t *rkb,
                                   rd_kafka_cgrp_t *rkcg,
                                   int16_t api_version,
                                   rd_kafka_topic_partition_list_t *offsets,
                                   rd_kafka_q_t *replyq,
                                   rd_kafka_resp_cb_t *resp_cb,
                                   void *opaque) {
	rd_kafka_buf_t *rkbuf;
        int of_TopicCnt = -1;
        int TopicCnt = 0;
        const char *last_topic = NULL;
        int of_PartCnt = -1;
        int PartCnt = 0;
        int i;

        rd_kafka_assert(NULL, offsets != NULL);

        rkbuf = rd_kafka_buf_new_growable(rkb->rkb_rk, 1,
                                          100 + (offsets->cnt * 128));

        /* ConsumerGroup */
        rd_kafka_buf_write_kstr(rkbuf, rkcg->rkcg_group_id);

        /* v1,v2 */
        if (api_version >= 1) {
                /* ConsumerGroupGenerationId */
                rd_kafka_buf_write_i32(rkbuf, rkcg->rkcg_generation_id);
                /* ConsumerId */
                rd_kafka_buf_write_kstr(rkbuf, rkcg->rkcg_member_id);
                /* v2: RetentionTime */
                if (api_version == 2)
                        rd_kafka_buf_write_i64(rkbuf, 0);
        }

        /* Sort offsets by topic */
        rd_kafka_topic_partition_list_sort_by_topic(offsets);

        /* TopicArrayCnt: Will be updated when we know the number of topics. */
        of_TopicCnt = rd_kafka_buf_write_i32(rkbuf, 0);

        for (i = 0 ; i < offsets->cnt ; i++) {
                rd_kafka_topic_partition_t *rktpar = &offsets->elems[i];

                if (last_topic == NULL || strcmp(last_topic, rktpar->topic)) {
                        /* New topic */

                        /* Finalize previous PartitionCnt */
                        if (PartCnt > 0)
                                rd_kafka_buf_update_u32(rkbuf, of_PartCnt,
                                                        PartCnt);

                        /* TopicName */
                        rd_kafka_buf_write_str(rkbuf, rktpar->topic, -1);
                        /* PartitionCnt, finalized later */
                        of_PartCnt = rd_kafka_buf_write_i32(rkbuf, 0);
                        PartCnt = 0;
                        TopicCnt++;
                }

                /* Partition */
                rd_kafka_buf_write_i32(rkbuf,  rktpar->partition);
                PartCnt++;

                /* Offset */
                rd_kafka_buf_write_i64(rkbuf, rktpar->offset);

                /* v1: TimeStamp */
                if (api_version == 1)
                        rd_kafka_buf_write_i64(rkbuf, -1);// FIXME: retention time

                /* Metadata */
                rd_kafka_buf_write_str(rkbuf,
                                       rktpar->metadata, rktpar->metadata_size);
        }

        /* Finalize previous PartitionCnt */
        if (PartCnt > 0)
                rd_kafka_buf_update_u32(rkbuf, of_PartCnt,  PartCnt);

        /* Finalize TopicCnt */
        rd_kafka_buf_update_u32(rkbuf, of_TopicCnt, TopicCnt);


        /* Push write-buffer onto iovec stack */
        rd_kafka_buf_autopush(rkbuf);

        rd_kafka_buf_version_set(rkbuf, api_version);

	rd_rkb_dbg(rkb, TOPIC, "OFFSET",
		   "OffsetCommitRequest(v%d, %d partition(s)))",
                   api_version, offsets->cnt);

	rd_kafka_broker_buf_enq_replyq(rkb, RD_KAFKAP_OffsetCommit, rkbuf,
                                       replyq, resp_cb, opaque);

}



/**
 * Write "consumer" protocol type MemberState for SyncGroupRequest.
 */
static rd_kafkap_bytes_t *rd_kafka_group_MemberState_consumer_write (
        const rd_kafka_group_member_t *rkgm) {
        rd_kafka_buf_t *rkbuf;
        int i;
        const char *last_topic = NULL;
        int of_TopicCnt;
        int of_PartCnt = -1;
        int TopicCnt = 0;
        int PartCnt = 0;
        rd_kafkap_bytes_t *MemberState;

        rkbuf = rd_kafka_buf_new_growable(NULL, 1, 100);
        rd_kafka_buf_write_i16(rkbuf, 0); /* Version */
        of_TopicCnt = rd_kafka_buf_write_i32(rkbuf, 0); /* Updated later */
        for (i = 0 ; i < rkgm->rkgm_assignment->cnt ; i++) {
                const rd_kafka_topic_partition_t *rktpar;

                rktpar = &rkgm->rkgm_assignment->elems[i];

                if (!last_topic || strcmp(last_topic,
                                          rktpar->topic)) {
                        if (last_topic)
                                /* Finalize previous PartitionCnt */
                                rd_kafka_buf_update_i32(rkbuf, of_PartCnt,
                                                        PartCnt);
                        rd_kafka_buf_write_str(rkbuf, rktpar->topic, -1);
                        /* Updated later */
                        of_PartCnt = rd_kafka_buf_write_i32(rkbuf, 0);
                        PartCnt = 0;
                        last_topic = rktpar->topic;
                        TopicCnt++;
                }

                rd_kafka_buf_write_i32(rkbuf, rktpar->partition);
                PartCnt++;
        }

        if (of_PartCnt != -1)
                rd_kafka_buf_update_i32(rkbuf, of_PartCnt, PartCnt);
        rd_kafka_buf_update_i32(rkbuf, of_TopicCnt, TopicCnt);

        rd_kafka_buf_write_kbytes(rkbuf, rkgm->rkgm_userdata);

        rd_kafka_buf_autopush(rkbuf);
        MemberState = rd_kafkap_bytes_from_buf(rkbuf);
        rd_kafka_buf_destroy(rkbuf);

        return MemberState;
}

/**
 * Send SyncGroupRequest
 */
void rd_kafka_SyncGroupRequest (rd_kafka_broker_t *rkb,
                                const rd_kafkap_str_t *group_id,
                                int32_t generation_id,
                                const rd_kafkap_str_t *member_id,
                                const rd_kafka_group_member_t
                                *assignments,
                                int assignment_cnt,
                                rd_kafka_q_t *replyq,
                                rd_kafka_resp_cb_t *resp_cb,
                                void *opaque) {
        rd_kafka_buf_t *rkbuf;
        int i;

        rkbuf = rd_kafka_buf_new_growable(rkb->rkb_rk,
                                          1,
                                          RD_KAFKAP_STR_SIZE(group_id) +
                                          4 /* GenerationId */ +
                                          RD_KAFKAP_STR_SIZE(member_id) +
                                          4 /* array size group_assignment */ +
                                          (assignment_cnt * 100/*guess*/));
        rd_kafka_buf_write_kstr(rkbuf, group_id);
        rd_kafka_buf_write_i32(rkbuf, generation_id);
        rd_kafka_buf_write_kstr(rkbuf, member_id);
        rd_kafka_buf_write_i32(rkbuf, assignment_cnt);

        for (i = 0 ; i < assignment_cnt ; i++) {
                const rd_kafka_group_member_t *rkgm = &assignments[i];
                rd_kafkap_bytes_t *MemberState;
                rd_kafka_buf_write_kstr(rkbuf, rkgm->rkgm_member_id);

                MemberState =
                        rd_kafka_group_MemberState_consumer_write(rkgm);
                rd_kafka_buf_write_kbytes(rkbuf, MemberState);
                rd_kafkap_bytes_destroy(MemberState);
        }

        /* Push write-buffer onto iovec stack */
        rd_kafka_buf_autopush(rkbuf);

        /* This is a blocking request */
        rkbuf->rkbuf_flags |= RD_KAFKA_OP_F_BLOCKING;
        rkbuf->rkbuf_ts_timeout = rd_clock() +
                (rkb->rkb_rk->rk_conf.group_session_timeout_ms * 1000) +
                (3*1000*1000/* 3s grace period*/);

        rd_kafka_broker_buf_enq_replyq(rkb, RD_KAFKAP_SyncGroup,
                                       rkbuf, replyq, resp_cb, opaque);
}

/**
 * Handler for SyncGroup responses
 * opaque must be the cgrp handle.
 */
void rd_kafka_handle_SyncGroup (rd_kafka_broker_t *rkb,
                                rd_kafka_resp_err_t err,
                                rd_kafka_buf_t *rkbuf,
                                rd_kafka_buf_t *request,
                                void *opaque) {
        rd_kafka_cgrp_t *rkcg = opaque;
        const int log_decode_errors = 1;
        int16_t ErrorCode = 0;
        rd_kafkap_bytes_t MemberState = RD_ZERO_INIT;
        int actions;

        if (err) {
                ErrorCode = err;
                goto err;
        }

        rd_kafka_buf_read_i16(rkbuf, &ErrorCode);
        rd_kafka_buf_read_bytes(rkbuf, &MemberState);

err:
        actions = rd_kafka_err_action(rkb, ErrorCode, rkbuf, request);

        if (actions & RD_KAFKA_ERR_ACTION_REFRESH) {
                /* Re-query for coordinator */
                rd_kafka_cgrp_op(rkcg, NULL, NULL, RD_KAFKA_OP_COORD_QUERY,
                                 ErrorCode);
                /* FALLTHRU */
        }

        rd_kafka_dbg(rkb->rkb_rk, CGRP, "SYNCGROUP",
                     "SyncGroup response: %s (%d bytes of MemberState data)",
                     rd_kafka_err2str(ErrorCode),
                     RD_KAFKAP_BYTES_LEN(&MemberState));

        if (ErrorCode == RD_KAFKA_RESP_ERR__DESTROY)
                return; /* Termination */

        rd_kafka_cgrp_handle_SyncGroup(rkcg, ErrorCode, &MemberState);
}


/**
 * Send JoinGroupRequest
 */
void rd_kafka_JoinGroupRequest (rd_kafka_broker_t *rkb,
                                const rd_kafkap_str_t *group_id,
                                const rd_kafkap_str_t *member_id,
                                const rd_kafkap_str_t *protocol_type,
                                const rd_kafka_topic_partition_list_t
                                *subscription,
                                rd_kafka_q_t *replyq,
                                rd_kafka_resp_cb_t *resp_cb,
                                void *opaque) {
        rd_kafka_buf_t *rkbuf;
        rd_kafka_t *rk = rkb->rkb_rk;
        rd_kafka_assignor_t *rkas;
        int i;

        rkbuf = rd_kafka_buf_new_growable(rkb->rkb_rk,
                                          1,
                                          RD_KAFKAP_STR_SIZE(group_id) +
                                          4 /* sessionTimeoutMs */ +
                                          RD_KAFKAP_STR_SIZE(member_id) +
                                          RD_KAFKAP_STR_SIZE(protocol_type) +
                                          4 /* array count GroupProtocols */ +
                                          (subscription->cnt * 100));
        rd_kafka_buf_write_kstr(rkbuf, group_id);
        rd_kafka_buf_write_i32(rkbuf, rk->rk_conf.group_session_timeout_ms);
        rd_kafka_buf_write_kstr(rkbuf, member_id);
        rd_kafka_buf_write_kstr(rkbuf, protocol_type);
        rd_kafka_buf_write_i32(rkbuf, rk->rk_conf.enabled_assignor_cnt);

        RD_LIST_FOREACH(rkas, &rk->rk_conf.partition_assignors, i) {
                rd_kafkap_bytes_t *member_metadata;
		if (!rkas->rkas_enabled)
			continue;
                rd_kafka_buf_write_kstr(rkbuf, rkas->rkas_protocol_name);
                member_metadata = rkas->rkas_get_metadata_cb(rkas,subscription);
                rd_kafka_buf_write_kbytes(rkbuf, member_metadata);
                rd_kafkap_bytes_destroy(member_metadata);
        }

        /* Push write-buffer onto iovec stack */
        rd_kafka_buf_autopush(rkbuf);


        /* This is a blocking request */
        rkbuf->rkbuf_flags |= RD_KAFKA_OP_F_BLOCKING;
        rkbuf->rkbuf_ts_timeout = rd_clock() +
                (rk->rk_conf.group_session_timeout_ms * 1000) +
                (3*1000*1000/* 3s grace period*/);

        rd_kafka_broker_buf_enq_replyq(rkb, RD_KAFKAP_JoinGroup,
                                       rkbuf, replyq, resp_cb, opaque);
}




/**
 * Parse single JoinGroup.Members.MemberMetadata for "consumer" ProtocolType
 *
 * Protocol definition:
 * https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal
 *
 * Returns 0 on success or -1 on error.
 */
static int
rd_kafka_group_MemberMetadata_consumer_read (
        rd_kafka_broker_t *rkb, rd_kafka_group_member_t *rkgm,
        const rd_kafkap_str_t *GroupProtocol,
        const rd_kafkap_bytes_t *MemberMetadata) {

        rd_kafka_buf_t *rkbuf;
        int16_t Version;
        int32_t subscription_cnt;
        rd_kafkap_bytes_t UserData;
        const int log_decode_errors = 1;
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR__BAD_MSG;

        /* Create a shadow-buffer pointing to the metadata to ease parsing. */
        rkbuf = rd_kafka_buf_new_shadow(MemberMetadata->data,
                                        RD_KAFKAP_BYTES_LEN(MemberMetadata));

        rd_kafka_buf_read_i16(rkbuf, &Version);
        rd_kafka_buf_read_i32(rkbuf, &subscription_cnt);

        if (subscription_cnt > 10000 || subscription_cnt <= 0)
                goto err;

        rkgm->rkgm_subscription =
                rd_kafka_topic_partition_list_new(subscription_cnt);

        while (subscription_cnt-- > 0) {
                rd_kafkap_str_t Topic;
				char *topic_name;
                rd_kafka_buf_read_str(rkbuf, &Topic);
				RD_KAFKAP_STR_DUPA(&topic_name, &Topic);
                rd_kafka_topic_partition_list_add(rkgm->rkgm_subscription,
                                                  topic_name,
                                                  RD_KAFKA_PARTITION_UA);
        }

        rd_kafka_buf_read_bytes(rkbuf, &UserData);
        rkgm->rkgm_userdata = rd_kafkap_bytes_copy(&UserData);

        rkbuf->rkbuf_buf2 = NULL;  /* Avoid freeing payload */
        rd_kafka_buf_destroy(rkbuf);

        return 0;

err:
        rd_rkb_dbg(rkb, CGRP, "MEMBERMETA",
                   "Failed to parse MemberMetadata for \"%.*s\": %s",
                   RD_KAFKAP_STR_PR(rkgm->rkgm_member_id),
                   rd_kafka_err2str(err));
        if (rkgm->rkgm_subscription) {
                rd_kafka_topic_partition_list_destroy(rkgm->
                                                      rkgm_subscription);
                rkgm->rkgm_subscription = NULL;
        }

        rkbuf->rkbuf_buf2 = NULL;  /* Avoid freeing payload */
        rd_kafka_buf_destroy(rkbuf);
        return -1;
}



/**
 * cgrp handler for JoinGroup responses
 * opaque must be the cgrp handle.
 *
 * Locality: cgrp broker thread
 */
void rd_kafka_cgrp_handle_JoinGroup (rd_kafka_broker_t *rkb,
                                     rd_kafka_resp_err_t err,
                                     rd_kafka_buf_t *rkbuf,
                                     rd_kafka_buf_t *request,
                                     void *opaque) {
        rd_kafka_cgrp_t *rkcg = opaque;
        const int log_decode_errors = 1;
        int16_t ErrorCode = 0;
        int32_t GenerationId;
        rd_kafkap_str_t Protocol, LeaderId, MyMemberId;
        int32_t member_cnt;
        int actions;
        int i_am_leader = 0;

        if (err) {
                ErrorCode = err;
                goto err;
        }

        rd_kafka_buf_read_i16(rkbuf, &ErrorCode);
        rd_kafka_buf_read_i32(rkbuf, &GenerationId);
        rd_kafka_buf_read_str(rkbuf, &Protocol);
        rd_kafka_buf_read_str(rkbuf, &LeaderId);
        rd_kafka_buf_read_str(rkbuf, &MyMemberId);
        rd_kafka_buf_read_i32(rkbuf, &member_cnt);

        rd_kafka_dbg(rkb->rkb_rk, CGRP, "JOINGROUP",
                     "JoinGroup response: GenerationId %"PRId32", "
                     "Protocol %.*s, LeaderId %.*s%s, my MemberId %.*s, "
                     "%"PRId32" members in group: %s",
                     GenerationId,
                     RD_KAFKAP_STR_PR(&Protocol),
                     RD_KAFKAP_STR_PR(&LeaderId),
                     !rd_kafkap_str_cmp(&LeaderId, &MyMemberId) ? " (me)" : "",
                     RD_KAFKAP_STR_PR(&MyMemberId),
                     member_cnt,
                     ErrorCode ? rd_kafka_err2str(ErrorCode) : "(no error)");

        if (!ErrorCode) {
			    char *my_member_id;
				RD_KAFKAP_STR_DUPA(&my_member_id, &MyMemberId);
                rkcg->rkcg_generation_id = GenerationId;
				rd_kafka_cgrp_set_member_id(rkcg, my_member_id);
                i_am_leader = !rd_kafkap_str_cmp(&LeaderId, &MyMemberId);
        } else {
                rd_interval_backoff(&rkcg->rkcg_join_intvl, 1000*1000);
                goto err;
        }

        if (i_am_leader) {
                rd_kafka_group_member_t *members;
                int i;
                int sub_cnt = 0;

                rd_kafka_dbg(rkb->rkb_rk, CGRP, "JOINGROUP",
                             "Elected leader for group \"%s\" "
                             "with %"PRId32" member(s)",
                             rkcg->rkcg_group_id->str, member_cnt);

                if (member_cnt > 100000) {
                        err = RD_KAFKA_RESP_ERR__BAD_MSG;
                        goto err;
                }

                members = rd_calloc(member_cnt, sizeof(*members));

                for (i = 0 ; i < member_cnt ; i++) {
                        rd_kafkap_str_t MemberId;
                        rd_kafkap_bytes_t MemberMetadata;
                        rd_kafka_group_member_t *rkgm;

                        rd_kafka_buf_read_str(rkbuf, &MemberId);
                        rd_kafka_buf_read_bytes(rkbuf, &MemberMetadata);

                        rkgm = &members[sub_cnt];
                        rkgm->rkgm_member_id = rd_kafkap_str_copy(&MemberId);
                        rd_list_init(&rkgm->rkgm_eligible, 0);

                        if (rd_kafka_group_MemberMetadata_consumer_read(
                                    rkb, rkgm, &Protocol, &MemberMetadata)) {
                                /* Failed to parse this member's metadata,
                                 * ignore it. */
                        } else {
                                sub_cnt++;
                                rkgm->rkgm_assignment =
                                        rd_kafka_topic_partition_list_new(
                                                rkgm->rkgm_subscription->size);
                        }

                }

                /* FIXME: What to do if parsing failed for some/all members?
                 *        It is a sign of incompatibility. */


                rd_kafka_cgrp_group_leader_reset(rkcg);

                rkcg->rkcg_group_leader.protocol = RD_KAFKAP_STR_DUP(&Protocol);
                rd_kafka_assert(NULL, rkcg->rkcg_group_leader.members == NULL);
                rkcg->rkcg_group_leader.members    = members;
                rkcg->rkcg_group_leader.member_cnt = sub_cnt;

                rd_kafka_cgrp_set_join_state(
			rkcg, RD_KAFKA_CGRP_JOIN_STATE_WAIT_METADATA);

                /* The assignor will need metadata so fetch it asynchronously
                 * and run the assignor when we get a reply. */
                rd_kafka_MetadataRequest(rkb, 1 /* all topics */, NULL,
                                         "partition assignor",
                                         &rkcg->rkcg_ops,
                                         rd_kafka_assignor_handle_Metadata,
                                         rkcg);
        } else {
                rd_kafka_cgrp_set_join_state(
			rkcg, RD_KAFKA_CGRP_JOIN_STATE_WAIT_SYNC);

                rd_kafka_SyncGroupRequest(rkb, rkcg->rkcg_group_id,
                                          rkcg->rkcg_generation_id,
                                          rkcg->rkcg_member_id,
                                          NULL, 0,
                                          &rkcg->rkcg_ops,
                                          rd_kafka_handle_SyncGroup, rkcg);

        }

err:
        actions = rd_kafka_err_action(rkb, ErrorCode, rkbuf, request);

        if (actions & RD_KAFKA_ERR_ACTION_REFRESH) {
                /* Re-query for coordinator */
                rd_kafka_cgrp_op(rkcg, NULL, NULL, RD_KAFKA_OP_COORD_QUERY,
                                 ErrorCode);
        }

        if (ErrorCode) {
                if (ErrorCode == RD_KAFKA_RESP_ERR__DESTROY)
                        return; /* Termination */

                if (ErrorCode == RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID)
                        rd_kafka_cgrp_set_member_id(rkcg, "");
                rd_kafka_cgrp_set_join_state(rkcg,
                                             RD_KAFKA_CGRP_JOIN_STATE_INIT);
        }
}



/**
 * Send LeaveGroupRequest
 */
void rd_kafka_LeaveGroupRequest (rd_kafka_broker_t *rkb,
                                 const rd_kafkap_str_t *group_id,
                                 const rd_kafkap_str_t *member_id,
                                 rd_kafka_q_t *replyq,
                                 rd_kafka_resp_cb_t *resp_cb,
                                 void *opaque) {
        rd_kafka_buf_t *rkbuf;

        rkbuf = rd_kafka_buf_new(rkb->rkb_rk,
                                 1,
                                 RD_KAFKAP_STR_SIZE(group_id) +
                                 RD_KAFKAP_STR_SIZE(member_id));
        rd_kafka_buf_write_kstr(rkbuf, group_id);
        rd_kafka_buf_write_kstr(rkbuf, member_id);

        /* Push write-buffer onto iovec stack */
        rd_kafka_buf_autopush(rkbuf);

        rd_kafka_broker_buf_enq_replyq(rkb, RD_KAFKAP_LeaveGroup,
                                       rkbuf, replyq, resp_cb, opaque);
}


/**
 * Handler for LeaveGroup responses
 * opaque must be the cgrp handle.
 */
void rd_kafka_handle_LeaveGroup (rd_kafka_broker_t *rkb,
                                 rd_kafka_resp_err_t err,
                                 rd_kafka_buf_t *rkbuf,
                                 rd_kafka_buf_t *request,
                                 void *opaque) {
        rd_kafka_cgrp_t *rkcg = opaque;
        const int log_decode_errors = 1;
        int16_t ErrorCode = 0;
        int actions;

        if (err) {
                ErrorCode = err;
                goto err;
        }

        rd_kafka_buf_read_i16(rkbuf, &ErrorCode);


err:
        actions = rd_kafka_err_action(rkb, ErrorCode, rkbuf, request);

        if (actions & RD_KAFKA_ERR_ACTION_REFRESH) {
                /* Re-query for coordinator */
                rd_kafka_cgrp_op(rkcg, NULL, NULL, RD_KAFKA_OP_COORD_QUERY,
                                 ErrorCode);
                /* Schedule a retry */
                rd_kafka_buf_keep(request);
                rd_kafka_broker_buf_retry(request->rkbuf_rkb, request);
                return;
        }

        if (ErrorCode)
                rd_kafka_dbg(rkb->rkb_rk, CGRP, "LEAVEGROUP",
                             "LeaveGroup response: %s",
                             rd_kafka_err2str(ErrorCode));
}






/**
 * Send HeartbeatRequest
 */
void rd_kafka_HeartbeatRequest (rd_kafka_broker_t *rkb,
                                const rd_kafkap_str_t *group_id,
                                int32_t generation_id,
                                const rd_kafkap_str_t *member_id,
                                rd_kafka_q_t *replyq,
                                rd_kafka_resp_cb_t *resp_cb,
                                void *opaque) {
        rd_kafka_buf_t *rkbuf;

        rd_rkb_dbg(rkb, CGRP, "HEARTBEAT",
                   "Heartbeat for group \"%s\" generation id %"PRId32,
                   group_id->str, generation_id);

        rkbuf = rd_kafka_buf_new(rkb->rkb_rk,
                                 1,
                                 RD_KAFKAP_STR_SIZE(group_id) +
                                 4 /* GenerationId */ +
                                 RD_KAFKAP_STR_SIZE(member_id));

        rd_kafka_buf_write_kstr(rkbuf, group_id);
        rd_kafka_buf_write_i32(rkbuf, generation_id);
        rd_kafka_buf_write_kstr(rkbuf, member_id);

        /* Push write-buffer onto iovec stack */
        rd_kafka_buf_autopush(rkbuf);

        rd_kafka_broker_buf_enq_replyq(rkb, RD_KAFKAP_Heartbeat,
                                       rkbuf, replyq, resp_cb, opaque);
}


/**
 * Generic handler for Heartbeat responses.
 * opaque must be the cgrp handle.
 */
void rd_kafka_cgrp_handle_Heartbeat (rd_kafka_broker_t *rkb,
                                     rd_kafka_resp_err_t err,
                                     rd_kafka_buf_t *rkbuf,
                                     rd_kafka_buf_t *request,
                                     void *opaque) {
        rd_kafka_cgrp_t *rkcg = opaque;
        const int log_decode_errors = 1;
        int16_t ErrorCode = 0;
        int actions;

        if (err) {
                ErrorCode = err;
                goto err;
        }

        rd_kafka_buf_read_i16(rkbuf, &ErrorCode);

err:
        actions = rd_kafka_err_action(rkb, ErrorCode, rkbuf, request);

        if (actions & RD_KAFKA_ERR_ACTION_REFRESH) {
                /* Re-query for coordinator */
                rd_kafka_cgrp_op(rkcg, NULL, NULL, RD_KAFKA_OP_COORD_QUERY,
                                 ErrorCode);
                /* Schedule a retry */
                rd_kafka_buf_keep(request);
                rd_kafka_broker_buf_retry(request->rkbuf_rkb, request);
                return;
        }

        if (ErrorCode != 0 && ErrorCode != RD_KAFKA_RESP_ERR__DESTROY)
		rd_kafka_cgrp_handle_heartbeat_error(rkcg, ErrorCode);
}



/**
 * Construct MetadataRequest (does not send)
 *
 * all_topics - if true, all topics in cluster will be requested, else only
 *              the ones known locally.
 * only_rkt   - only request this specific topic (optional)
 * reason     - metadata request reason
 *
 */
rd_kafka_buf_t *rd_kafka_MetadataRequest0 (rd_kafka_broker_t *rkb,
                                           int all_topics,
                                           rd_kafka_itopic_t *only_rkt,
                                           const char *reason) {
	rd_kafka_buf_t *rkbuf;
	int32_t arrsize = 0;
	size_t tnamelen = 0;
	rd_kafka_itopic_t *rkt;

	rd_rkb_dbg(rkb, METADATA, "METADATA",
		   "Request metadata for %s: %s",
                   only_rkt ? only_rkt->rkt_topic->str :
		   (all_topics ? "all topics":"locally known topics"),
                   reason ? reason : "");

	if (only_rkt || all_topics) {
		rd_kafka_rdlock(rkb->rkb_rk);

		/* Calculate size to hold requested topics */
		TAILQ_FOREACH(rkt, &rkb->rkb_rk->rk_topics, rkt_link) {
			if (only_rkt && only_rkt != rkt)
				continue;

			arrsize++;
			tnamelen += RD_KAFKAP_STR_SIZE(rkt->rkt_topic);
		}
	}

	rkbuf = rd_kafka_buf_new(rkb->rkb_rk, 1, sizeof(arrsize) + tnamelen);
	rd_kafka_buf_write_i32(rkbuf, arrsize);

	if (only_rkt || all_topics) {
		/* Just our locally known topics */

		TAILQ_FOREACH(rkt, &rkb->rkb_rk->rk_topics, rkt_link) {
                        if (only_rkt && only_rkt != rkt)
				continue;
			rd_kafka_buf_write_kstr(rkbuf, rkt->rkt_topic);
		}
		rd_kafka_rdunlock(rkb->rkb_rk);
	}


	rd_kafka_buf_autopush(rkbuf);


	return rkbuf;
}


void rd_kafka_MetadataRequest (rd_kafka_broker_t *rkb,
                               int all_topics,
                               rd_kafka_itopic_t *only_rkt,
                               const char *reason,
                               rd_kafka_q_t *replyq,
                               rd_kafka_resp_cb_t *resp_cb,
                               void *opaque) {
        rd_kafka_buf_t *rkbuf = rd_kafka_MetadataRequest0(rkb, all_topics,
                                                          only_rkt, reason);

        rd_kafka_broker_buf_enq_replyq(rkb, RD_KAFKAP_Metadata,
                                       rkbuf, replyq, resp_cb, opaque);
}



/**
 * Handle a Metadata response message.
 * If 'rkt' is non-NULL the metadata originated from a topic-specific request.
 *
 * The metadata will be marshalled into 'struct rd_kafka_metadata*' structs.
 *
 * Returns the marshalled metadata, or NULL on parse error.
 *
 * Locality: broker thread
 */
static struct rd_kafka_metadata *
rd_kafka_parse_Metadata (rd_kafka_broker_t *rkb,
                         rd_kafka_itopic_t *rkt, rd_kafka_buf_t *rkbuf) {
	int i, j, k;
	int req_rkt_seen = 0;
        char *msh_buf = NULL;
        int   msh_of  = 0;
        int   msh_size;
        struct rd_kafka_metadata *md = NULL;
        int rkb_namelen;
        const int log_decode_errors = 1;

        rd_kafka_broker_lock(rkb);
        rkb_namelen = strlen(rkb->rkb_name)+1;
        /* We assume that the marshalled representation is
         * no more than 4 times larger than the wire representation. */
        msh_size = sizeof(*md) + rkb_namelen + (rkbuf->rkbuf_len * 4);
        msh_buf = rd_malloc(msh_size);

        _MSH_ALLOC(rkbuf, md, sizeof(*md));
        md->orig_broker_id = rkb->rkb_nodeid;
        _MSH_ALLOC(rkbuf, md->orig_broker_name, rkb_namelen);
        memcpy(md->orig_broker_name, rkb->rkb_name, rkb_namelen);
        rd_kafka_broker_unlock(rkb);

	/* Read Brokers */
	rd_kafka_buf_read_i32a(rkbuf, md->broker_cnt);
	if (md->broker_cnt > RD_KAFKAP_BROKERS_MAX)
		rd_kafka_buf_parse_fail(rkbuf, "Broker_cnt %i > BROKERS_MAX %i",
					md->broker_cnt, RD_KAFKAP_BROKERS_MAX);

        _MSH_ALLOC(rkbuf, md->brokers, md->broker_cnt * sizeof(*md->brokers));

	for (i = 0 ; i < md->broker_cnt ; i++) {
                rd_kafka_buf_read_i32a(rkbuf, md->brokers[i].id);
                rd_kafka_buf_read_str_msh(rkbuf, md->brokers[i].host);
		rd_kafka_buf_read_i32a(rkbuf, md->brokers[i].port);
	}


	/* Read TopicMetadata */
	rd_kafka_buf_read_i32a(rkbuf, md->topic_cnt);
	rd_rkb_dbg(rkb, METADATA, "METADATA", "%i brokers, %i topics",
                   md->broker_cnt, md->topic_cnt);

	if (md->topic_cnt > RD_KAFKAP_TOPICS_MAX)
		rd_kafka_buf_parse_fail(rkbuf, "TopicMetadata_cnt %"PRId32
					" > TOPICS_MAX %i",
					md->topic_cnt, RD_KAFKAP_TOPICS_MAX);

        _MSH_ALLOC(rkbuf, md->topics, md->topic_cnt * sizeof(*md->topics));

	for (i = 0 ; i < md->topic_cnt ; i++) {

		rd_kafka_buf_read_i16a(rkbuf, md->topics[i].err);
		rd_kafka_buf_read_str_msh(rkbuf, md->topics[i].topic);

		/* PartitionMetadata */
                rd_kafka_buf_read_i32a(rkbuf, md->topics[i].partition_cnt);
		if (md->topics[i].partition_cnt > RD_KAFKAP_PARTITIONS_MAX)
			rd_kafka_buf_parse_fail(rkbuf,
						"TopicMetadata[%i]."
						"PartitionMetadata_cnt %i "
						"> PARTITIONS_MAX %i",
						i, md->topics[i].partition_cnt,
						RD_KAFKAP_PARTITIONS_MAX);

                _MSH_ALLOC(rkbuf, md->topics[i].partitions,
                           md->topics[i].partition_cnt *
                           sizeof(*md->topics[i].partitions));

		for (j = 0 ; j < md->topics[i].partition_cnt ; j++) {
			rd_kafka_buf_read_i16a(rkbuf, md->topics[i].partitions[j].err);
			rd_kafka_buf_read_i32a(rkbuf, md->topics[i].partitions[j].id);
			rd_kafka_buf_read_i32a(rkbuf, md->topics[i].partitions[j].leader);

			/* Replicas */
			rd_kafka_buf_read_i32a(rkbuf, md->topics[i].partitions[j].replica_cnt);
			if (md->topics[i].partitions[j].replica_cnt >
			    RD_KAFKAP_BROKERS_MAX)
				rd_kafka_buf_parse_fail(rkbuf,
							"TopicMetadata[%i]."
							"PartitionMetadata[%i]."
							"Replica_cnt "
							"%i > BROKERS_MAX %i",
							i, j,
							md->topics[i].
							partitions[j].
							replica_cnt,
							RD_KAFKAP_BROKERS_MAX);

                        _MSH_ALLOC(rkbuf, md->topics[i].partitions[j].replicas,
                                   md->topics[i].partitions[j].replica_cnt *
                                   sizeof(*md->topics[i].partitions[j].
                                          replicas));

                        for (k = 0 ;
                             k < md->topics[i].partitions[j].replica_cnt; k++)
				rd_kafka_buf_read_i32a(rkbuf, md->topics[i].partitions[j].
                                           replicas[k]);

			/* Isrs */
			rd_kafka_buf_read_i32a(rkbuf, md->topics[i].partitions[j].isr_cnt);
			if (md->topics[i].partitions[j].isr_cnt >
			    RD_KAFKAP_BROKERS_MAX)
				rd_kafka_buf_parse_fail(rkbuf,
							"TopicMetadata[%i]."
							"PartitionMetadata[%i]."
							"Isr_cnt "
							"%i > BROKERS_MAX %i",
							i, j,
							md->topics[i].
							partitions[j].isr_cnt,
							RD_KAFKAP_BROKERS_MAX);

                        _MSH_ALLOC(rkbuf, md->topics[i].partitions[j].isrs,
                                   md->topics[i].partitions[j].isr_cnt *
                                   sizeof(*md->topics[i].partitions[j].isrs));
                        for (k = 0 ;
                             k < md->topics[i].partitions[j].isr_cnt; k++)
				rd_kafka_buf_read_i32a(rkbuf, md->topics[i].
						       partitions[j].isrs[k]);

		}
	}

        /* Entire Metadata response now parsed without errors:
         * now update our internal state according to the response. */

        /* Avoid metadata updates when we're terminating. */
	if (rd_kafka_terminating(rkb->rkb_rk))
                goto done;

	/* Update our list of brokers. */
	for (i = 0 ; i < md->broker_cnt ; i++) {
		rd_rkb_dbg(rkb, METADATA, "METADATA",
			   "  Broker #%i/%i: %s:%i NodeId %"PRId32,
			   i, md->broker_cnt,
                           md->brokers[i].host,
                           md->brokers[i].port,
                           md->brokers[i].id);
		rd_kafka_broker_update(rkb->rkb_rk, rkb->rkb_proto,
				       &md->brokers[i]);
	}

	/* Update partition count and leader for each topic we know about */
	for (i = 0 ; i < md->topic_cnt ; i++) {
                rd_rkb_dbg(rkb, METADATA, "METADATA",
                           "  Topic #%i/%i: %s with %i partitions%s%s",
                           i, md->topic_cnt, md->topics[i].topic,
                           md->topics[i].partition_cnt,
                           md->topics[i].err ? ": " : "",
                           md->topics[i].err ?
                           rd_kafka_err2str(md->topics[i].err) : "");


		if (rkt && !rd_kafkap_str_cmp_str(rkt->rkt_topic,
                                                  md->topics[i].topic))
			req_rkt_seen++;

		rd_kafka_topic_metadata_update(rkb, &md->topics[i]);
	}


	/* Requested topics not seen in metadata? Propogate to topic code. */
	if (rkt) {
		rd_rkb_dbg(rkb, TOPIC, "METADATA",
			   "Requested topic %s %sseen in metadata",
			   rkt->rkt_topic->str,
                           req_rkt_seen ? "" : "not ");
		if (!req_rkt_seen)
			rd_kafka_topic_metadata_none(rkt);
	}


done:
        /* This metadata request was triggered by someone wanting
         * the metadata information back as a reply, so send that reply now.
         * In this case we must not rd_free the metadata memory here,
         * the requestee will do. */
        return md;

err:
        rd_free(msh_buf);
        return NULL;
}


/**
 * Generic op-based handler for Metadata responses
 *
 * Locality: rdkafka main thread
 */
void rd_kafka_op_handle_Metadata (rd_kafka_broker_t *rkb,
                                  rd_kafka_resp_err_t err,
                                  rd_kafka_buf_t *rkbuf,
                                  rd_kafka_buf_t *request,
                                  void *opaque) {
        rd_kafka_op_t *rko = opaque;
        struct rd_kafka_metadata *md = NULL;
        rd_kafka_q_t *replyq;

	rd_rkb_dbg(rkb, METADATA, "METADATA",
		   "===== Received metadata from %s =====",
		   rkb->rkb_name);

	/* Avoid metadata updates when we're terminating. */
	if (rd_kafka_terminating(rkb->rkb_rk))
                err = RD_KAFKA_RESP_ERR__DESTROY;

	if (unlikely(err)) {
		/* FIXME: handle error */
                if (err == RD_KAFKA_RESP_ERR__DESTROY) {
                        rd_kafka_op_destroy(rko);
                        return; /* Terminating */
                }

                rd_rkb_log(rkb, LOG_WARNING, "METADATA",
                           "Metadata request failed: %s",
                           rd_kafka_err2str(err));
	} else {
		md = rd_kafka_parse_Metadata(rkb,
                                             rko->rko_rkt ?
                                             rd_kafka_topic_a2i(rko->rko_rkt):
                                             NULL,
                                             rkbuf);
        }

        if (rko->rko_rkt) {
                rd_kafka_itopic_t *rkt = rd_kafka_topic_a2i(rko->rko_rkt);
                rd_kafka_topic_wrlock(rkt);
                rkt->rkt_flags &= ~RD_KAFKA_TOPIC_F_LEADER_QUERY;
                rd_kafka_topic_wrunlock(rkt);
        }

        if ((replyq = rko->rko_replyq)) {
                /* Reply to metadata requester, passing on the metadata.
                 * Reuse requesting rko for the reply. */
                rko->rko_replyq = NULL;
                rko->rko_err = err;
                rko->rko_metadata = md;
                rd_kafka_q_enq(replyq, rko);
                /* Drop refcount to queue */
                rd_kafka_q_destroy(replyq);
        } else {
                if (md)
                        rd_free(md);
                rd_kafka_op_destroy(rko);
        }
}

static void rd_kafka_assignor_handle_Metadata (rd_kafka_broker_t *rkb,
                                               rd_kafka_resp_err_t err,
                                               rd_kafka_buf_t *rkbuf,
                                               rd_kafka_buf_t *request,
                                               void *opaque) {
        rd_kafka_cgrp_t *rkcg = opaque;
        struct rd_kafka_metadata *md = NULL;

        if (err == RD_KAFKA_RESP_ERR__DESTROY)
                return; /* Terminating */

        if (!err) {
                md = rd_kafka_parse_Metadata(rkb, NULL, rkbuf);
                if (!md)
                        err = RD_KAFKA_RESP_ERR__BAD_MSG;
        }

        rd_kafka_cgrp_handle_Metadata(rkcg, err, md);

        if (md)
                rd_free(md);
}


