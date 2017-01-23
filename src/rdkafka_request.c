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

#include <stdarg.h>

#include "rdkafka_int.h"
#include "rdkafka_request.h"
#include "rdkafka_broker.h"
#include "rdkafka_offset.h"
#include "rdkafka_topic.h"
#include "rdkafka_partition.h"
#include "rdkafka_metadata.h"

#include "rdrand.h"

/**
 * Kafka protocol request and response handling.
 * All of this code runs in the broker thread and uses op queues for
 * propagating results back to the various sub-systems operating in
 * other threads.
 */


/**
 * @brief Decide action(s) to take based on the returned error code.
 *
 * The optional var-args is a .._ACTION_END terminated list
 * of action,error tuples which overrides the general behaviour.
 * It is to be read as: for \p error, return \p action(s).
 */
int rd_kafka_err_action (rd_kafka_broker_t *rkb,
			 rd_kafka_resp_err_t err,
			 rd_kafka_buf_t *rkbuf,
			 rd_kafka_buf_t *request, ...) {
	va_list ap;
        int actions = 0;
	int exp_act;

	/* Match explicitly defined error mappings first. */
	va_start(ap, request);
	while ((exp_act = va_arg(ap, int))) {
		int exp_err = va_arg(ap, int);

		if (err == exp_err)
			actions |= exp_act;
	}
	va_end(ap);

	if (err && rkb && request)
                rd_rkb_dbg(rkb, BROKER, "REQERR",
                           "%sRequest failed: %s: explicit actions 0x%x",
                           rd_kafka_ApiKey2str(request->rkbuf_reqhdr.ApiKey),
                           rd_kafka_err2str(err), actions);

	/* Explicit error match. */
	if (actions)
		return actions;

	/* Default error matching */
        switch (err)
        {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
                break;
        case RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION:
        case RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_GROUP_COORDINATOR_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_NOT_COORDINATOR_FOR_GROUP:
        case RD_KAFKA_RESP_ERR__WAIT_COORD:
                /* Request metadata information update */
                actions |= RD_KAFKA_ERR_ACTION_REFRESH;
                break;
	case RD_KAFKA_RESP_ERR__TRANSPORT:
		/* Broker connection down */
		actions |= RD_KAFKA_ERR_ACTION_RETRY;
		break;
        case RD_KAFKA_RESP_ERR__DESTROY:
	case RD_KAFKA_RESP_ERR_INVALID_SESSION_TIMEOUT:
        default:
                actions |= RD_KAFKA_ERR_ACTION_PERMANENT;
                break;
        }

        return actions;
}


static void rd_kafka_assignor_handle_Metadata (rd_kafka_t *rk,
					       rd_kafka_broker_t *rkb,
                                               rd_kafka_resp_err_t err,
                                               rd_kafka_buf_t *rkbuf,
                                               rd_kafka_buf_t *request,
                                               void *opaque);

/**
 * Send GroupCoordinatorRequest
 */
void rd_kafka_GroupCoordinatorRequest (rd_kafka_broker_t *rkb,
                                       const rd_kafkap_str_t *cgrp,
                                       rd_kafka_replyq_t replyq,
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
rd_kafka_resp_err_t rd_kafka_handle_Offset (rd_kafka_t *rk,
					    rd_kafka_broker_t *rkb,
					    rd_kafka_resp_err_t err,
					    rd_kafka_buf_t *rkbuf,
					    rd_kafka_buf_t *request,
					    const char *topic,
					    int32_t partition,
					    int64_t *offsets,
					    size_t *offset_cntp) {

        const int log_decode_errors = 1;
        int16_t ErrorCode = 0;
        int32_t TopicArrayCnt;
        int actions;
	size_t offsets_written = 0;

        if (err) {
                ErrorCode = err;
                goto err;
        }

	/* NOTE:
	 * Broker may return offsets in a different constellation than
	 * in the original request .*/

        rd_kafka_buf_read_i32(rkbuf, &TopicArrayCnt);
        while (TopicArrayCnt-- > 0) {
                rd_kafkap_str_t ktopic;
                int32_t PartArrayCnt;

                rd_kafka_buf_read_str(rkbuf, &ktopic);
                rd_kafka_buf_read_i32(rkbuf, &PartArrayCnt);

                while (PartArrayCnt-- > 0) {
                        int32_t kpartition;
                        int32_t OffsetArrayCnt;
                        int correct_partition;

                        rd_kafka_buf_read_i32(rkbuf, &kpartition);
                        rd_kafka_buf_read_i16(rkbuf, &ErrorCode);
                        rd_kafka_buf_read_i32(rkbuf, &OffsetArrayCnt);

                        correct_partition =
                                !rd_kafkap_str_cmp_str(&ktopic, topic) &&
                                kpartition == partition;

                        while (OffsetArrayCnt-- > 0) {
				int64_t Offset;
                                rd_kafka_buf_read_i64(rkbuf, &Offset);

                                if (correct_partition &&
				    offsets_written < *offset_cntp)
					offsets[offsets_written++] = Offset;
                        }
                }
        }

	*offset_cntp = offsets_written;
	goto done;

 err:
        actions = rd_kafka_err_action(
		rkb, ErrorCode, rkbuf, request,
		RD_KAFKA_ERR_ACTION_PERMANENT,
		RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART,

		RD_KAFKA_ERR_ACTION_REFRESH|RD_KAFKA_ERR_ACTION_RETRY,
		RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION,

		RD_KAFKA_ERR_ACTION_END);

        if (actions & RD_KAFKA_ERR_ACTION_REFRESH) {
                /* Re-query for leader */
		shptr_rd_kafka_itopic_t *s_rkt;

		if ((s_rkt = rd_kafka_topic_find(rk, topic, 1/*lock*/))) {
			rd_kafka_topic_leader_query(rk,
						    rd_kafka_topic_s2i(s_rkt));
			rd_kafka_topic_destroy0(s_rkt);
		}
	}
	if (actions & RD_KAFKA_ERR_ACTION_RETRY) {
		if (rd_kafka_buf_retry(rkb, request))
			return RD_KAFKA_RESP_ERR__IN_PROGRESS;
		/* FALLTHRU */
	}

done:
        if (!ErrorCode && offsets_written == 0)
                ErrorCode = RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION;

        return ErrorCode;
}






/**
 * Send OffsetRequest for toppar 'rktp'.
 */
void rd_kafka_OffsetRequest (rd_kafka_broker_t *rkb,
                             const char *topic, int32_t partition,
                             const int64_t *query_offsets, size_t offset_cnt,
                             rd_kafka_replyq_t replyq,
                             rd_kafka_resp_cb_t *resp_cb,
                             void *opaque) {
	rd_kafka_buf_t *rkbuf;
	size_t i;

	rkbuf = rd_kafka_buf_new(rkb->rkb_rk, 1,
				 /* ReplicaId+TopicArrayCnt+Topic */
				 4+4+RD_KAFKAP_STR_SIZE0((int)strlen(topic)) +
				 /* PartArrayCnt */
				 4 +
				 /* offset_cnt * Partition+Time+MaxNumOffs */
				 (offset_cnt * (4+8+4)));

	/* ReplicaId */
	rd_kafka_buf_write_i32(rkbuf, -1);
	/* TopicArrayCnt */
	rd_kafka_buf_write_i32(rkbuf, 1);
	/* Topic */
	rd_kafka_buf_write_str(rkbuf, topic, -1);
	/* PartitionArrayCnt */
	rd_kafka_buf_write_i32(rkbuf, (int32_t)offset_cnt);

	for (i = 0 ; i < offset_cnt ; i++) {
		/* Partition */
		rd_kafka_buf_write_i32(rkbuf, partition);

		/* Time/Offset */
		rd_kafka_buf_write_i64(rkbuf, query_offsets[i]);

		/* MaxNumberOfOffsets */
		rd_kafka_buf_write_i32(rkbuf, (int32_t)offset_cnt);
	}

	rd_kafka_buf_autopush(rkbuf);

	rd_rkb_dbg(rkb, TOPIC, "OFFSET",
		   "OffsetRequest (%"PRIdsz" offsets) for "
		   "topic %s [%"PRId32"] (v%d)",
                   offset_cnt, topic, partition, rkbuf->rkbuf_replyq.version);

	rd_kafka_broker_buf_enq_replyq(rkb, RD_KAFKAP_Offset,
                                       rkbuf, replyq, resp_cb, opaque);
}


/**
 * Generic handler for OffsetFetch responses.
 * Offsets for included partitions will be propagated through the passed
 * 'offsets' list.
 *
 * \p update_toppar: update toppar's committed_offset
 */
rd_kafka_resp_err_t
rd_kafka_handle_OffsetFetch (rd_kafka_t *rk,
			     rd_kafka_broker_t *rkb,
			     rd_kafka_resp_err_t err,
			     rd_kafka_buf_t *rkbuf,
			     rd_kafka_buf_t *request,
			     rd_kafka_topic_partition_list_t *offsets,
			     int update_toppar) {
        const int log_decode_errors = 1;
        int32_t TopicArrayCnt;
        int64_t offset = RD_KAFKA_OFFSET_INVALID;
        rd_kafkap_str_t metadata;
        int i;
        int actions;
        int seen_cnt = 0;

        if (err)
                goto err;

        /* Set default offset for all partitions. */
        rd_kafka_topic_partition_list_set_offsets(rkb->rkb_rk, offsets, 0,
                                                  RD_KAFKA_OFFSET_INVALID,
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
                        int16_t err2;

                        rd_kafka_buf_read_i32(rkbuf, &partition);
                        rd_kafka_buf_read_i64(rkbuf, &offset);
                        rd_kafka_buf_read_str(rkbuf, &metadata);
                        rd_kafka_buf_read_i16(rkbuf, &err2);

                        rktpar = rd_kafka_topic_partition_list_find(offsets,
                                                                    topic_name,
                                                                    partition);
                        if (!rktpar) {
				rd_rkb_dbg(rkb, TOPIC, "OFFSETFETCH",
					   "OffsetFetchResponse: %s [%"PRId32"] "
					   "not found in local list: ignoring",
					   topic_name, partition);
                                continue;
			}

                        seen_cnt++;

			if (!(s_rktp = rktpar->_private)) {
				s_rktp = rd_kafka_toppar_get2(rkb->rkb_rk,
							      topic_name,
							      partition, 0, 0);
				/* May be NULL if topic is not locally known */
				rktpar->_private = s_rktp;
			}

			/* broker reports invalid offset as -1 */
			if (offset == -1)
				rktpar->offset = RD_KAFKA_OFFSET_INVALID;
			else
				rktpar->offset = offset;
                        rktpar->err = err2;

			rd_rkb_dbg(rkb, TOPIC, "OFFSETFETCH",
				   "OffsetFetchResponse: %s [%"PRId32"] offset %"PRId64,
				   topic_name, partition, offset);

			if (update_toppar && !err2 && s_rktp) {
				rd_kafka_toppar_t *rktp = rd_kafka_toppar_s2i(s_rktp);
				/* Update toppar's committed offset */
				rd_kafka_toppar_lock(rktp);
				rktp->rktp_committed_offset = rktpar->offset;
				rd_kafka_toppar_unlock(rktp);
			}


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
                }
        }


err:
        rd_rkb_dbg(rkb, TOPIC, "OFFFETCH",
                   "OffsetFetch for %d/%d partition(s) returned %s",
                   seen_cnt,
                   offsets ? offsets->cnt : -1, rd_kafka_err2str(err));

        actions = rd_kafka_err_action(rkb, err, rkbuf, request,
				      RD_KAFKA_ERR_ACTION_END);

        if (actions & RD_KAFKA_ERR_ACTION_REFRESH) {
                /* Re-query for coordinator */
                rd_kafka_cgrp_op(rkb->rkb_rk->rk_cgrp, NULL,
                                 RD_KAFKA_NO_REPLYQ,
				 RD_KAFKA_OP_COORD_QUERY, err);
                if (request) {
                        /* Schedule a retry */
                        rd_kafka_buf_keep(request);
                        rd_kafka_broker_buf_retry(request->rkbuf_rkb, request);
                }
        }

	return err;
}



/**
 * opaque=rko wrapper for handle_OffsetFetch.
 * rko->rko_payload MUST be a `rd_kafka_topic_partition_list_t *` which will
 * be filled in with fetch offsets.
 *
 * A reply will be sent on 'rko->rko_replyq' with type RD_KAFKA_OP_OFFSET_FETCH.
 *
 * Locality: cgrp's broker thread
 */
void rd_kafka_op_handle_OffsetFetch (rd_kafka_t *rk,
				     rd_kafka_broker_t *rkb,
                                     rd_kafka_resp_err_t err,
                                     rd_kafka_buf_t *rkbuf,
                                     rd_kafka_buf_t *request,
                                     void *opaque) {
        rd_kafka_op_t *rko = opaque;
        rd_kafka_op_t *rko_reply;
        rd_kafka_topic_partition_list_t *offsets;

	RD_KAFKA_OP_TYPE_ASSERT(rko, RD_KAFKA_OP_OFFSET_FETCH);

        if (err == RD_KAFKA_RESP_ERR__DESTROY) {
                /* Termination, quick cleanup. */
                rd_kafka_op_destroy(rko);
                return;
        }

        offsets = rd_kafka_topic_partition_list_copy(
                rko->rko_u.offset_fetch.partitions);

        rko_reply = rd_kafka_op_new(RD_KAFKA_OP_OFFSET_FETCH|RD_KAFKA_OP_REPLY);
        rko_reply->rko_err = err;
        rko_reply->rko_u.offset_fetch.partitions = offsets;
        rko_reply->rko_u.offset_fetch.do_free = 1;
	if (rko->rko_rktp)
		rko_reply->rko_rktp = rd_kafka_toppar_keep(
			rd_kafka_toppar_s2i(rko->rko_rktp));

	/* If all partitions already had usable offsets then there
	 * was no request sent and thus no reply, the offsets list is
	 * good to go. */
	if (rkbuf)
		rd_kafka_handle_OffsetFetch(rkb->rkb_rk, rkb, err, rkbuf,
					    request, offsets, 0);

	rd_kafka_replyq_enq(&rko->rko_replyq, rko_reply, 0);

        rd_kafka_op_destroy(rko);
}






/**
 * Send OffsetFetchRequest for toppar.
 *
 * Any partition with a usable offset will be ignored, if all partitions
 * have usable offsets then no request is sent at all but an empty
 * reply is enqueued on the replyq.
 */
void rd_kafka_OffsetFetchRequest (rd_kafka_broker_t *rkb,
                                  int16_t api_version,
                                  rd_kafka_topic_partition_list_t *parts,
				  rd_kafka_replyq_t replyq,
                                  rd_kafka_resp_cb_t *resp_cb,
                                  void *opaque) {
	rd_kafka_buf_t *rkbuf;
        size_t of_TopicCnt;
        int TopicCnt = 0;
        ssize_t of_PartCnt = -1;
        const char *last_topic = NULL;
        int PartCnt = 0;
	int tot_PartCnt = 0;
        int i;

	rkbuf = rd_kafka_buf_new_growable(
                rkb->rkb_rk, 1,
                RD_KAFKAP_STR_SIZE(rkb->rkb_rk->rk_conf.group_id) +
                4 +
                (parts->cnt * 32));


        /* ConsumerGroup */
        rd_kafka_buf_write_kstr(rkbuf, rkb->rkb_rk->
                                rk_conf.group_id);

        /* Sort partitions by topic */
        rd_kafka_topic_partition_list_sort_by_topic(parts);

	/* TopicArrayCnt */
        of_TopicCnt = rd_kafka_buf_write_i32(rkbuf, 0); /* Updated later */

        for (i = 0 ; i < parts->cnt ; i++) {
                rd_kafka_topic_partition_t *rktpar = &parts->elems[i];

		/* Ignore partitions with a usable offset. */
		if (rktpar->offset != RD_KAFKA_OFFSET_INVALID &&
		    rktpar->offset != RD_KAFKA_OFFSET_STORED) {
			rd_rkb_dbg(rkb, TOPIC, "OFFSET",
				   "OffsetFetchRequest: skipping %s [%"PRId32"] "
				   "with valid offset %s",
				   rktpar->topic, rktpar->partition,
				   rd_kafka_offset2str(rktpar->offset));
			continue;
		}

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
			last_topic = rktpar->topic;
                        TopicCnt++;
                }

                /* Partition */
                rd_kafka_buf_write_i32(rkbuf,  rktpar->partition);
                PartCnt++;
		tot_PartCnt++;
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
		   "OffsetFetchRequest(v%d) for %d/%d partition(s)",
                   api_version, tot_PartCnt, parts->cnt);

	if (tot_PartCnt == 0) {
		/* No partitions needs OffsetFetch, enqueue empty
		 * response right away. */
		rkbuf->rkbuf_rkb = rkb;
		rd_kafka_broker_keep(rkb);
                rkbuf->rkbuf_replyq = replyq;
                rkbuf->rkbuf_cb     = resp_cb;
                rkbuf->rkbuf_opaque = opaque;
		rd_kafka_buf_callback(rkb->rkb_rk, rkb, 0, NULL, rkbuf);
		return;
	}



	rd_kafka_broker_buf_enq_replyq(rkb, RD_KAFKAP_OffsetFetch, rkbuf,
                                       replyq, resp_cb, opaque);
}


/**
 * @remark \p offsets may be NULL if \p err is set
 */
rd_kafka_resp_err_t
rd_kafka_handle_OffsetCommit (rd_kafka_t *rk,
			      rd_kafka_broker_t *rkb,
			      rd_kafka_resp_err_t err,
			      rd_kafka_buf_t *rkbuf,
			      rd_kafka_buf_t *request,
			      rd_kafka_topic_partition_list_t *offsets) {
        const int log_decode_errors = 1;
        int32_t TopicArrayCnt;
        int16_t ErrorCode = 0;
        int i;
	int actions;

        if (err)
		goto err;

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
                                offsets, topic_str, partition);

                        if (!rktpar) {
                                /* Received offset for topic/partition we didn't
                                 * ask for, this shouldn't really happen. */
                                continue;
                        }

                        rktpar->err = ErrorCode;
                }
        }
	goto done;

err:
        actions = rd_kafka_err_action(
		rkb, err, rkbuf, request,

		RD_KAFKA_ERR_ACTION_PERMANENT,
		RD_KAFKA_RESP_ERR_OFFSET_METADATA_TOO_LARGE,

		RD_KAFKA_ERR_ACTION_RETRY,
		RD_KAFKA_RESP_ERR_GROUP_LOAD_IN_PROGRESS,

		RD_KAFKA_ERR_ACTION_REFRESH|RD_KAFKA_ERR_ACTION_RETRY,
		RD_KAFKA_RESP_ERR_GROUP_COORDINATOR_NOT_AVAILABLE,

		RD_KAFKA_ERR_ACTION_REFRESH|RD_KAFKA_ERR_ACTION_RETRY,
		RD_KAFKA_RESP_ERR_NOT_COORDINATOR_FOR_GROUP,

		RD_KAFKA_ERR_ACTION_REFRESH|RD_KAFKA_ERR_ACTION_RETRY,
		RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION,

		RD_KAFKA_ERR_ACTION_REFRESH|RD_KAFKA_ERR_ACTION_RETRY,
		RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID,

		RD_KAFKA_ERR_ACTION_RETRY,
		RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS,

		RD_KAFKA_ERR_ACTION_PERMANENT,
		RD_KAFKA_RESP_ERR_INVALID_COMMIT_OFFSET_SIZE,

		RD_KAFKA_ERR_ACTION_PERMANENT,
		RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED,

		RD_KAFKA_ERR_ACTION_PERMANENT,
		RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED,

		RD_KAFKA_ERR_ACTION_END);

	if (actions & RD_KAFKA_ERR_ACTION_REFRESH && rk->rk_cgrp) {
		/* Re-query for coordinator */
		rd_kafka_cgrp_coord_query(rk->rk_cgrp,
					  "OffsetCommitRequest failed");
	}
	if (actions & RD_KAFKA_ERR_ACTION_RETRY) {
		if (rd_kafka_buf_retry(rkb, request))
			return RD_KAFKA_RESP_ERR__IN_PROGRESS;
		/* FALLTHRU */
	}

 done:
	return err;
}




/**
 * @brief Send OffsetCommitRequest for a list of partitions.
 *
 * @returns 0 if none of the partitions in \p offsets had valid offsets,
 *          else 1.
 */
int rd_kafka_OffsetCommitRequest (rd_kafka_broker_t *rkb,
                                   rd_kafka_cgrp_t *rkcg,
                                   int16_t api_version,
                                   rd_kafka_topic_partition_list_t *offsets,
                                   rd_kafka_replyq_t replyq,
                                   rd_kafka_resp_cb_t *resp_cb,
                                   void *opaque) {
	rd_kafka_buf_t *rkbuf;
        ssize_t of_TopicCnt = -1;
        int TopicCnt = 0;
        const char *last_topic = NULL;
        ssize_t of_PartCnt = -1;
        int PartCnt = 0;
	int tot_PartCnt = 0;
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
                        rd_kafka_buf_write_i64(rkbuf, -1);
        }

        /* Sort offsets by topic */
        rd_kafka_topic_partition_list_sort_by_topic(offsets);

        /* TopicArrayCnt: Will be updated when we know the number of topics. */
        of_TopicCnt = rd_kafka_buf_write_i32(rkbuf, 0);

        for (i = 0 ; i < offsets->cnt ; i++) {
                rd_kafka_topic_partition_t *rktpar = &offsets->elems[i];

		/* Skip partitions with invalid offset. */
		if (rktpar->offset < 0)
			continue;

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
			last_topic = rktpar->topic;
                        TopicCnt++;
                }

                /* Partition */
                rd_kafka_buf_write_i32(rkbuf,  rktpar->partition);
                PartCnt++;
		tot_PartCnt++;

                /* Offset */
                rd_kafka_buf_write_i64(rkbuf, rktpar->offset);

                /* v1: TimeStamp */
                if (api_version == 1)
                        rd_kafka_buf_write_i64(rkbuf, -1);// FIXME: retention time

                /* Metadata */
		/* Java client 0.9.0 and broker <0.10.0 can't parse
		 * Null metadata fields, so as a workaround we send an
		 * empty string if it's Null. */
		if (!rktpar->metadata)
			rd_kafka_buf_write_str(rkbuf, "", 0);
		else
			rd_kafka_buf_write_str(rkbuf,
					       rktpar->metadata,
					       rktpar->metadata_size);
        }

	if (tot_PartCnt == 0) {
		/* No topic+partitions had valid offsets to commit. */
		rd_kafka_replyq_destroy(&replyq);
		rd_kafka_buf_destroy(rkbuf);
		return 0;
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
		   "Enqueue OffsetCommitRequest(v%d, %d/%d partition(s)))",
                   api_version, tot_PartCnt, offsets->cnt);

	rd_kafka_broker_buf_enq_replyq(rkb, RD_KAFKAP_OffsetCommit, rkbuf,
                                       replyq, resp_cb, opaque);

	return 1;

}



/**
 * Write "consumer" protocol type MemberState for SyncGroupRequest.
 */
static rd_kafkap_bytes_t *rd_kafka_group_MemberState_consumer_write (
        const rd_kafka_group_member_t *rkgm) {
        rd_kafka_buf_t *rkbuf;
        int i;
        const char *last_topic = NULL;
        size_t of_TopicCnt;
        ssize_t of_PartCnt = -1;
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
                                rd_kafka_replyq_t replyq,
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
void rd_kafka_handle_SyncGroup (rd_kafka_t *rk,
				rd_kafka_broker_t *rkb,
                                rd_kafka_resp_err_t err,
                                rd_kafka_buf_t *rkbuf,
                                rd_kafka_buf_t *request,
                                void *opaque) {
        rd_kafka_cgrp_t *rkcg = opaque;
        const int log_decode_errors = 1;
        int16_t ErrorCode = 0;
        rd_kafkap_bytes_t MemberState = RD_ZERO_INIT;
        int actions;

	if (rkcg->rkcg_join_state != RD_KAFKA_CGRP_JOIN_STATE_WAIT_SYNC) {
		rd_kafka_dbg(rkb->rkb_rk, CGRP, "SYNCGROUP",
			     "SyncGroup response: discarding outdated request "
			     "(now in join-state %s)",
			     rd_kafka_cgrp_join_state_names[rkcg->
							    rkcg_join_state]);
		return;
	}

        if (err) {
                ErrorCode = err;
                goto err;
        }

        rd_kafka_buf_read_i16(rkbuf, &ErrorCode);
        rd_kafka_buf_read_bytes(rkbuf, &MemberState);

err:
        actions = rd_kafka_err_action(rkb, ErrorCode, rkbuf, request,
				      RD_KAFKA_ERR_ACTION_END);

        if (actions & RD_KAFKA_ERR_ACTION_REFRESH) {
                /* Re-query for coordinator */
                rd_kafka_cgrp_op(rkcg, NULL, RD_KAFKA_NO_REPLYQ,
				 RD_KAFKA_OP_COORD_QUERY,
                                 ErrorCode);
                /* FALLTHRU */
        }

        rd_kafka_dbg(rkb->rkb_rk, CGRP, "SYNCGROUP",
                     "SyncGroup response: %s (%d bytes of MemberState data)",
                     rd_kafka_err2str(ErrorCode),
                     RD_KAFKAP_BYTES_LEN(&MemberState));

        if (ErrorCode == RD_KAFKA_RESP_ERR__DESTROY)
                return; /* Termination */

        rd_kafka_cgrp_handle_SyncGroup(rkcg, rkb, ErrorCode, &MemberState);
}


/**
 * Send JoinGroupRequest
 */
void rd_kafka_JoinGroupRequest (rd_kafka_broker_t *rkb,
                                const rd_kafkap_str_t *group_id,
                                const rd_kafkap_str_t *member_id,
                                const rd_kafkap_str_t *protocol_type,
				const rd_list_t *topics,
                                rd_kafka_replyq_t replyq,
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
                                          (rd_list_cnt(topics) * 100));
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
                member_metadata = rkas->rkas_get_metadata_cb(rkas, topics);
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
void rd_kafka_cgrp_handle_JoinGroup (rd_kafka_t *rk,
				     rd_kafka_broker_t *rkb,
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

	if (rkcg->rkcg_join_state != RD_KAFKA_CGRP_JOIN_STATE_WAIT_JOIN) {
		rd_kafka_dbg(rkb->rkb_rk, CGRP, "JOINGROUP",
			     "JoinGroup response: discarding outdated request "
			     "(now in join-state %s)",
			     rd_kafka_cgrp_join_state_names[rkcg->
							    rkcg_join_state]);
		return;
	}

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
                                         RD_KAFKA_REPLYQ(rkcg->rkcg_ops, 0),
                                         rd_kafka_assignor_handle_Metadata,
                                         rkcg);
        } else {
                rd_kafka_cgrp_set_join_state(
			rkcg, RD_KAFKA_CGRP_JOIN_STATE_WAIT_SYNC);

                rd_kafka_SyncGroupRequest(rkb, rkcg->rkcg_group_id,
                                          rkcg->rkcg_generation_id,
                                          rkcg->rkcg_member_id,
                                          NULL, 0,
                                          RD_KAFKA_REPLYQ(rkcg->rkcg_ops, 0),
                                          rd_kafka_handle_SyncGroup, rkcg);

        }

err:
        actions = rd_kafka_err_action(rkb, ErrorCode, rkbuf, request,
				      RD_KAFKA_ERR_ACTION_IGNORE,
				      RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID,

				      RD_KAFKA_ERR_ACTION_END);

        if (actions & RD_KAFKA_ERR_ACTION_REFRESH) {
                /* Re-query for coordinator */
                rd_kafka_cgrp_op(rkcg, NULL, RD_KAFKA_NO_REPLYQ,
				 RD_KAFKA_OP_COORD_QUERY, ErrorCode);
        }

        if (ErrorCode) {
                if (ErrorCode == RD_KAFKA_RESP_ERR__DESTROY)
                        return; /* Termination */

		if (actions & RD_KAFKA_ERR_ACTION_PERMANENT)
			rd_kafka_q_op_err(rkcg->rkcg_q,
					  RD_KAFKA_OP_CONSUMER_ERR,
					  ErrorCode, 0, NULL, 0,
					  "JoinGroup failed: %s",
					  rd_kafka_err2str(ErrorCode));

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
                                 rd_kafka_replyq_t replyq,
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
void rd_kafka_handle_LeaveGroup (rd_kafka_t *rk,
				 rd_kafka_broker_t *rkb,
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
        actions = rd_kafka_err_action(rkb, ErrorCode, rkbuf, request,
				      RD_KAFKA_ERR_ACTION_END);

        if (actions & RD_KAFKA_ERR_ACTION_REFRESH) {
                /* Re-query for coordinator */
                rd_kafka_cgrp_op(rkcg, NULL, RD_KAFKA_NO_REPLYQ,
				 RD_KAFKA_OP_COORD_QUERY, ErrorCode);
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
                                rd_kafka_replyq_t replyq,
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

        rkbuf->rkbuf_ts_timeout = rd_clock() +
                (rkb->rkb_rk->rk_conf.group_session_timeout_ms * 1000);

        rd_kafka_broker_buf_enq_replyq(rkb, RD_KAFKAP_Heartbeat,
                                       rkbuf, replyq, resp_cb, opaque);
}


/**
 * Generic handler for Heartbeat responses.
 * opaque must be the cgrp handle.
 */
void rd_kafka_cgrp_handle_Heartbeat (rd_kafka_t *rk,
				     rd_kafka_broker_t *rkb,
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
        actions = rd_kafka_err_action(rkb, ErrorCode, rkbuf, request,
				      RD_KAFKA_ERR_ACTION_END);

        if (actions & RD_KAFKA_ERR_ACTION_REFRESH) {
                /* Re-query for coordinator */
                rd_kafka_cgrp_op(rkcg, NULL, RD_KAFKA_NO_REPLYQ,
				 RD_KAFKA_OP_COORD_QUERY, ErrorCode);
                /* Schedule a retry */
		if (ErrorCode != RD_KAFKA_RESP_ERR_NOT_COORDINATOR_FOR_GROUP) {
			rd_kafka_buf_keep(request);
			rd_kafka_broker_buf_retry(request->rkbuf_rkb, request);
		}
                return;
        }

        rd_dassert(rkcg->rkcg_flags & RD_KAFKA_CGRP_F_HEARTBEAT_IN_TRANSIT);
        rkcg->rkcg_flags &= ~RD_KAFKA_CGRP_F_HEARTBEAT_IN_TRANSIT;

        if (ErrorCode != 0 && ErrorCode != RD_KAFKA_RESP_ERR__DESTROY)
		rd_kafka_cgrp_handle_heartbeat_error(rkcg, ErrorCode);
}




/**
 * Send ListGroupsRequest
 */
void rd_kafka_ListGroupsRequest (rd_kafka_broker_t *rkb,
                                 rd_kafka_replyq_t replyq,
                                 rd_kafka_resp_cb_t *resp_cb,
                                 void *opaque) {
        rd_kafka_buf_t *rkbuf;

        rkbuf = rd_kafka_buf_new(rkb->rkb_rk, 0, 0);

        rd_kafka_broker_buf_enq_replyq(rkb, RD_KAFKAP_ListGroups,
                                       rkbuf, replyq, resp_cb, opaque);
}


/**
 * Send DescribeGroupsRequest
 */
void rd_kafka_DescribeGroupsRequest (rd_kafka_broker_t *rkb,
                                     const char **groups, int group_cnt,
                                     rd_kafka_replyq_t replyq,
                                     rd_kafka_resp_cb_t *resp_cb,
                                     void *opaque) {
        rd_kafka_buf_t *rkbuf;

        rkbuf = rd_kafka_buf_new_growable(rkb->rkb_rk, 1, 32*group_cnt);

        rd_kafka_buf_write_i32(rkbuf, group_cnt);
        while (group_cnt-- > 0)
                rd_kafka_buf_write_str(rkbuf, groups[group_cnt], -1);

        rd_kafka_buf_autopush(rkbuf);

        rd_kafka_broker_buf_enq_replyq(rkb, RD_KAFKAP_DescribeGroups,
                                       rkbuf, replyq, resp_cb, opaque);
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

	if (only_rkt || !all_topics) {
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

	if (only_rkt || !all_topics) {
		/* Just our locally known topics */

		TAILQ_FOREACH(rkt, &rkb->rkb_rk->rk_topics, rkt_link) {
                        if (only_rkt && only_rkt != rkt)
				continue;
			rd_kafka_buf_write_kstr(rkbuf, rkt->rkt_topic);
		}
		rd_kafka_rdunlock(rkb->rkb_rk);
	}


	rd_kafka_buf_autopush(rkbuf);

	/* Metadata requests are part of the important control plane
	 * and should go before other requests (Produce, Fetch, etc). */
	rkbuf->rkbuf_flags |= RD_KAFKA_OP_F_FLASH;

	return rkbuf;
}


void rd_kafka_MetadataRequest (rd_kafka_broker_t *rkb,
                               int all_topics,
                               rd_kafka_itopic_t *only_rkt,
                               const char *reason,
                               rd_kafka_replyq_t replyq,
                               rd_kafka_resp_cb_t *resp_cb,
                               void *opaque) {
        rd_kafka_buf_t *rkbuf = rd_kafka_MetadataRequest0(rkb, all_topics,
                                                          only_rkt, reason);

        rd_kafka_broker_buf_enq_replyq(rkb, RD_KAFKAP_Metadata,
                                       rkbuf, replyq, resp_cb, opaque);
}





/**
 * Generic op-based handler for Metadata responses
 *
 * Locality: rdkafka main thread
 */
void rd_kafka_op_handle_Metadata (rd_kafka_t *rk,
				  rd_kafka_broker_t *rkb,
                                  rd_kafka_resp_err_t err,
                                  rd_kafka_buf_t *rkbuf,
                                  rd_kafka_buf_t *request,
                                  void *opaque) {
        rd_kafka_op_t *rko = opaque;
        struct rd_kafka_metadata *md = NULL;
	rd_kafka_itopic_t *rkt;

	rd_rkb_dbg(rkb, METADATA, "METADATA",
		   "===== Received metadata =====");

	/* Avoid metadata updates when we're terminating. */
	if (rd_kafka_terminating(rkb->rkb_rk))
                err = RD_KAFKA_RESP_ERR__DESTROY;

	if (rko->rko_u.metadata.rkt)
		rkt = rd_kafka_topic_a2i(rko->rko_u.metadata.rkt);
	else
		rkt = NULL;

	if (unlikely(err)) {
		/* FIXME: handle error */
                if (err == RD_KAFKA_RESP_ERR__DESTROY) {
                        rd_kafka_op_destroy(rko);
                        return; /* Terminating */
                }

                rd_rkb_log(rkb, LOG_WARNING, "METADATA",
                           "Metadata request failed: %s (%dms)",
                           rd_kafka_err2str(err),
			   (int)(request->rkbuf_ts_sent/1000));
	} else {
		md = rd_kafka_parse_Metadata(rkb, rkt, rkbuf,
					     rko->rko_u.metadata.all_topics);
		if (!md) {
			if (rd_kafka_buf_retry(rkb, request))
				return;
			err = RD_KAFKA_RESP_ERR__BAD_MSG;
		} else if (rkb->rkb_rk->rk_cgrp &&
			 rko->rko_u.metadata.all_topics)
			rd_kafka_cgrp_metadata_update_check(rkb->rkb_rk->rk_cgrp,
							    md);
        }

        if (rkt) {
                rd_kafka_topic_wrlock(rkt);
                rkt->rkt_flags &= ~RD_KAFKA_TOPIC_F_LEADER_QUERY;
                rd_kafka_topic_wrunlock(rkt);
        }

        if (rko->rko_replyq.q) {
                /* Reply to metadata requester, passing on the metadata.
                 * Reuse requesting rko for the reply. */
                rko->rko_err = err;
                rko->rko_u.metadata.metadata = md;

                rd_kafka_replyq_enq(&rko->rko_replyq, rko, 0);
        } else {
                if (md)
                        rd_free(md);
                rd_kafka_op_destroy(rko);
        }
}

static void rd_kafka_assignor_handle_Metadata (rd_kafka_t *rk,
					       rd_kafka_broker_t *rkb,
                                               rd_kafka_resp_err_t err,
                                               rd_kafka_buf_t *rkbuf,
                                               rd_kafka_buf_t *request,
                                               void *opaque) {
        rd_kafka_cgrp_t *rkcg = opaque;
        struct rd_kafka_metadata *md = NULL;

        if (err == RD_KAFKA_RESP_ERR__DESTROY)
                return; /* Terminating */

        if (!err) {
                md = rd_kafka_parse_Metadata(rkb, NULL, rkbuf, 1/*all_topics*/);
		if (!md) {
			if (rd_kafka_buf_retry(rkb, request))
				return;
                        err = RD_KAFKA_RESP_ERR__BAD_MSG;
		}
        }

        rd_kafka_cgrp_handle_Metadata(rkcg, err, md);

        if (md)
                rd_free(md);
}




/**
 * @brief Parses and handles ApiVersion reply.
 *
 * @param apis will be allocated and populated with broker's supported APIs.
 * @param api_cnt will be set to the number of elements in \p *apis

 * @returns 0 on success, else an error.
 */
rd_kafka_resp_err_t
rd_kafka_handle_ApiVersion (rd_kafka_t *rk,
			    rd_kafka_broker_t *rkb,
			    rd_kafka_resp_err_t err,
			    rd_kafka_buf_t *rkbuf,
			    rd_kafka_buf_t *request,
			    struct rd_kafka_ApiVersion **apis,
			    size_t *api_cnt) {
        const int log_decode_errors = 1;
        int actions;
	int32_t ApiArrayCnt;
	int16_t ErrorCode;
	int i = 0;

	*apis = NULL;

        if (err)
                goto err;

	rd_kafka_buf_read_i16(rkbuf, &ErrorCode);
	if ((err = ErrorCode))
		goto err;

        rd_kafka_buf_read_i32(rkbuf, &ApiArrayCnt);
	if (ApiArrayCnt > 1000)
		rd_kafka_buf_parse_fail(rkbuf,
					"ApiArrayCnt %"PRId32" out of range",
					ApiArrayCnt);

	rd_rkb_dbg(rkb, FEATURE, "APIVERSION",
		   "Broker API support:");

	*apis = malloc(sizeof(**apis) * ApiArrayCnt);

	for (i = 0 ; i < ApiArrayCnt ; i++) {
		struct rd_kafka_ApiVersion *api = &(*apis)[i];

		rd_kafka_buf_read_i16(rkbuf, &api->ApiKey);
		rd_kafka_buf_read_i16(rkbuf, &api->MinVer);
		rd_kafka_buf_read_i16(rkbuf, &api->MaxVer);

		rd_rkb_dbg(rkb, FEATURE, "APIVERSION",
			   "  ApiKey %s (%hd) Versions %hd..%hd",
			   rd_kafka_ApiKey2str(api->ApiKey),
			   api->ApiKey, api->MinVer, api->MaxVer);
        }

	*api_cnt = ApiArrayCnt;
	goto done;

 err:
	if (*apis)
		rd_free(*apis);

        actions = rd_kafka_err_action(
		rkb, err, rkbuf, request,
		RD_KAFKA_ERR_ACTION_END);

	if (actions & RD_KAFKA_ERR_ACTION_RETRY) {
		if (rd_kafka_buf_retry(rkb, request))
			return RD_KAFKA_RESP_ERR__IN_PROGRESS;
		/* FALLTHRU */
	}

done:
        return err;
}



/**
 * Send ApiVersionRequest (KIP-35)
 */
void rd_kafka_ApiVersionRequest (rd_kafka_broker_t *rkb,
				 rd_kafka_replyq_t replyq,
				 rd_kafka_resp_cb_t *resp_cb,
				 void *opaque, int flash_msg) {
        rd_kafka_buf_t *rkbuf;

        rkbuf = rd_kafka_buf_new(rkb->rkb_rk, 1, 4);
	rkbuf->rkbuf_flags |= (flash_msg ? RD_KAFKA_OP_F_FLASH : 0);
	rd_kafka_buf_write_i32(rkbuf, 0); /* Empty array: request all APIs */
	rd_kafka_buf_autopush(rkbuf);

	/* Non-supporting brokers will tear down the conneciton when they
	 * receive an unknown API request, so dont retry request on failure. */
	rkbuf->rkbuf_retries = RD_KAFKA_BUF_NO_RETRIES;

	/* 0.9.0.x brokers will not close the connection on unsupported
	 * API requests, so we minimize the timeout to 10s for the request.
	 * This is a regression on the broker part. */
	if (rkb->rkb_rk->rk_conf.socket_timeout_ms > 10*1000)
		rkbuf->rkbuf_ts_timeout = rd_clock() + (10 * 1000 * 1000);

	if (replyq.q)
		rd_kafka_broker_buf_enq_replyq(rkb, RD_KAFKAP_ApiVersion,
					       rkbuf, replyq, resp_cb, opaque);
	else /* in broker thread */
		rd_kafka_broker_buf_enq1(rkb, RD_KAFKAP_ApiVersion, rkbuf,
					 resp_cb, opaque);
}


/**
 * Send SaslHandshakeRequest (KIP-43)
 */
void rd_kafka_SaslHandshakeRequest (rd_kafka_broker_t *rkb,
				    const char *mechanism,
				    rd_kafka_replyq_t replyq,
				    rd_kafka_resp_cb_t *resp_cb,
				    void *opaque, int flash_msg) {
        rd_kafka_buf_t *rkbuf;
	int mechlen = (int)strlen(mechanism);

        rkbuf = rd_kafka_buf_new(rkb->rkb_rk, 1, RD_KAFKAP_STR_SIZE0(mechlen));
	rkbuf->rkbuf_flags |= (flash_msg ? RD_KAFKA_OP_F_FLASH : 0);
	rd_kafka_buf_write_str(rkbuf, mechanism, mechlen);
	rd_kafka_buf_autopush(rkbuf);

	/* Non-supporting brokers will tear down the conneciton when they
	 * receive an unknown API request or where the SASL GSSAPI
	 * token type is not recognized, so dont retry request on failure. */
	rkbuf->rkbuf_retries = RD_KAFKA_BUF_NO_RETRIES;

	/* 0.9.0.x brokers will not close the connection on unsupported
	 * API requests, so we minimize the timeout of the request.
	 * This is a regression on the broker part. */
	if (rkb->rkb_rk->rk_conf.socket_timeout_ms > 10*1000)
		rkbuf->rkbuf_ts_timeout = rd_clock() + (10 * 1000);

	if (replyq.q)
		rd_kafka_broker_buf_enq_replyq(rkb, RD_KAFKAP_SaslHandshake,
					       rkbuf, replyq, resp_cb, opaque);
	else /* in broker thread */
		rd_kafka_broker_buf_enq1(rkb, RD_KAFKAP_SaslHandshake, rkbuf,
					 resp_cb, opaque);
}
