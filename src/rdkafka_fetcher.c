/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2022, Magnus Edenhill
 *               2023, Confluent Inc.
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
 * @name Fetcher
 *
 */

#include "rdkafka_int.h"
#include "rdkafka_offset.h"
#include "rdkafka_msgset.h"
#include "rdkafka_fetcher.h"
#include "rdkafka_request.h"


/**
 * Backoff the next Fetch request (due to error).
 */
static void rd_kafka_broker_fetch_backoff(rd_kafka_broker_t *rkb,
                                          rd_kafka_resp_err_t err) {
        int backoff_ms            = rkb->rkb_rk->rk_conf.fetch_error_backoff_ms;
        rkb->rkb_ts_fetch_backoff = rd_clock() + (backoff_ms * 1000);
        rd_rkb_dbg(rkb, FETCH, "BACKOFF", "Fetch backoff for %dms: %s",
                   backoff_ms, rd_kafka_err2str(err));
}

/**
 * @brief Backoff the next Fetch for specific partition
 *
 * @returns the absolute backoff time (the current time for no backoff).
 */
static rd_ts_t rd_kafka_toppar_fetch_backoff(rd_kafka_broker_t *rkb,
                                             rd_kafka_toppar_t *rktp,
                                             rd_kafka_resp_err_t err) {
        int backoff_ms;

        /* Don't back off on reaching end of partition */
        if (err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                rktp->rktp_ts_fetch_backoff = 0;
                return rd_clock(); /* Immediate: No practical backoff */
        }

        if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL)
                backoff_ms = rkb->rkb_rk->rk_conf.fetch_queue_backoff_ms;
        else
                backoff_ms = rkb->rkb_rk->rk_conf.fetch_error_backoff_ms;

        if (unlikely(!backoff_ms)) {
                rktp->rktp_ts_fetch_backoff = 0;
                return rd_clock(); /* Immediate: No practical backoff */
        }

        /* Certain errors that may require manual intervention should have
         * a longer backoff time. */
        if (err == RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED)
                backoff_ms = RD_MAX(1000, backoff_ms * 10);

        rktp->rktp_ts_fetch_backoff = rd_clock() + (backoff_ms * 1000);

        rd_rkb_dbg(rkb, FETCH, "BACKOFF",
                   "%s [%" PRId32 "]: Fetch backoff for %dms%s%s",
                   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
                   backoff_ms, err ? ": " : "",
                   err ? rd_kafka_err2str(err) : "");

        return rktp->rktp_ts_fetch_backoff;
}

/**
 * @brief Handle preferred replica in fetch response.
 *
 * @locks rd_kafka_toppar_lock(rktp) and
 *        rd_kafka_rdlock(rk) must NOT be held.
 *
 * @locality broker thread
 */
static void rd_kafka_fetch_preferred_replica_handle(rd_kafka_toppar_t *rktp,
                                                    rd_kafka_buf_t *rkbuf,
                                                    rd_kafka_broker_t *rkb,
                                                    int32_t preferred_id) {
        const rd_ts_t one_minute   = 60 * 1000 * 1000;
        const rd_ts_t five_seconds = 5 * 1000 * 1000;
        rd_kafka_broker_t *preferred_rkb;
        rd_kafka_t *rk = rktp->rktp_rkt->rkt_rk;
        rd_ts_t new_intvl =
            rd_interval_immediate(&rktp->rktp_new_lease_intvl, one_minute, 0);

        if (new_intvl < 0) {
                /* In lieu of KIP-320, the toppar is delegated back to
                 * the leader in the event of an offset out-of-range
                 * error (KIP-392 error case #4) because this scenario
                 * implies the preferred replica is out-of-sync.
                 *
                 * If program execution reaches here, the leader has
                 * relatively quickly instructed the client back to
                 * a preferred replica, quite possibly the same one
                 * as before (possibly resulting from stale metadata),
                 * so we back off the toppar to slow down potential
                 * back-and-forth.
                 */

                if (rd_interval_immediate(&rktp->rktp_new_lease_log_intvl,
                                          one_minute, 0) > 0)
                        rd_rkb_log(rkb, LOG_NOTICE, "FETCH",
                                   "%.*s [%" PRId32
                                   "]: preferred replica "
                                   "(%" PRId32
                                   ") lease changing too quickly "
                                   "(%" PRId64
                                   "s < 60s): possibly due to "
                                   "unavailable replica or stale cluster "
                                   "state: backing off next fetch",
                                   RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                                   rktp->rktp_partition, preferred_id,
                                   (one_minute - -new_intvl) / (1000 * 1000));

                rd_kafka_toppar_fetch_backoff(rkb, rktp,
                                              RD_KAFKA_RESP_ERR_NO_ERROR);
        }

        rd_kafka_rdlock(rk);
        preferred_rkb = rd_kafka_broker_find_by_nodeid(rk, preferred_id);
        rd_kafka_rdunlock(rk);

        if (preferred_rkb) {
                rd_interval_reset_to_now(&rktp->rktp_lease_intvl, 0);
                rd_kafka_toppar_lock(rktp);
                rd_kafka_toppar_broker_update(rktp, preferred_id, preferred_rkb,
                                              "preferred replica updated");
                rd_kafka_toppar_unlock(rktp);
                rd_kafka_broker_destroy(preferred_rkb);
                return;
        }

        if (rd_interval_immediate(&rktp->rktp_metadata_intvl, five_seconds, 0) >
            0) {
                rd_rkb_log(rkb, LOG_NOTICE, "FETCH",
                           "%.*s [%" PRId32 "]: preferred replica (%" PRId32
                           ") "
                           "is unknown: refreshing metadata",
                           RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                           rktp->rktp_partition, preferred_id);

                rd_kafka_metadata_refresh_brokers(
                    rktp->rktp_rkt->rkt_rk, NULL,
                    "preferred replica unavailable");
        }

        rd_kafka_toppar_fetch_backoff(rkb, rktp,
                                      RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE);
}


/**
 * @brief Handle partition-specific Fetch error.
 */
static void rd_kafka_fetch_reply_handle_partition_error(
    rd_kafka_broker_t *rkb,
    rd_kafka_toppar_t *rktp,
    const struct rd_kafka_toppar_ver *tver,
    rd_kafka_resp_err_t err,
    int64_t HighwaterMarkOffset) {

        rd_rkb_dbg(rkb, FETCH, "FETCHERR",
                   "%.*s [%" PRId32 "]: Fetch failed at %s: %s",
                   RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                   rktp->rktp_partition,
                   rd_kafka_fetch_pos2str(rktp->rktp_offsets.fetch_pos),
                   rd_kafka_err2name(err));

        /* Some errors should be passed to the
         * application while some handled by rdkafka */
        switch (err) {
                /* Errors handled by rdkafka */
        case RD_KAFKA_RESP_ERR_OFFSET_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
        case RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_NOT_LEADER_OR_FOLLOWER:
        case RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_KAFKA_STORAGE_ERROR:
        case RD_KAFKA_RESP_ERR_UNKNOWN_LEADER_EPOCH:
        case RD_KAFKA_RESP_ERR_FENCED_LEADER_EPOCH:
        case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_ID:
                if (err == RD_KAFKA_RESP_ERR_OFFSET_NOT_AVAILABLE) {
                        /* Occurs when:
                         *   - Msg exists on broker but
                         *     offset > HWM, or:
                         *   - HWM is >= offset, but msg not
                         *     yet available at that offset
                         *     (replica is out of sync).
                         *   - partition leader is out of sync.
                         *
                         * Handle by requesting metadata update, changing back
                         * to the leader, and then retrying FETCH
                         * (with backoff).
                         */
                        rd_rkb_dbg(rkb, MSG, "FETCH",
                                   "Topic %s [%" PRId32
                                   "]: %s not "
                                   "available on broker %" PRId32
                                   " (leader %" PRId32
                                   "): updating metadata and retrying",
                                   rktp->rktp_rkt->rkt_topic->str,
                                   rktp->rktp_partition,
                                   rd_kafka_fetch_pos2str(
                                       rktp->rktp_offsets.fetch_pos),
                                   rktp->rktp_broker_id, rktp->rktp_leader_id);
                }

                if (err == RD_KAFKA_RESP_ERR_UNKNOWN_LEADER_EPOCH) {
                        rd_rkb_dbg(rkb, MSG | RD_KAFKA_DBG_CONSUMER, "FETCH",
                                   "Topic %s [%" PRId32
                                   "]: Fetch failed at %s: %s: broker %" PRId32
                                   "has not yet caught up on latest metadata: "
                                   "retrying",
                                   rktp->rktp_rkt->rkt_topic->str,
                                   rktp->rktp_partition,
                                   rd_kafka_fetch_pos2str(
                                       rktp->rktp_offsets.fetch_pos),
                                   rd_kafka_err2str(err), rktp->rktp_broker_id);
                }

                if (rktp->rktp_broker_id != rktp->rktp_leader_id) {
                        rd_kafka_toppar_delegate_to_leader(rktp);
                }
                /* Request metadata information update*/
                rd_kafka_toppar_leader_unavailable(rktp, "fetch", err);
                break;

        case RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE: {
                rd_kafka_fetch_pos_t err_pos;

                if (rktp->rktp_broker_id != rktp->rktp_leader_id &&
                    rktp->rktp_offsets.fetch_pos.offset > HighwaterMarkOffset) {
                        rd_kafka_log(rkb->rkb_rk, LOG_WARNING, "FETCH",
                                     "Topic %s [%" PRId32
                                     "]: %s "
                                     " out of range (HighwaterMark %" PRId64
                                     " fetching from "
                                     "broker %" PRId32 " (leader %" PRId32
                                     "): reverting to leader",
                                     rktp->rktp_rkt->rkt_topic->str,
                                     rktp->rktp_partition,
                                     rd_kafka_fetch_pos2str(
                                         rktp->rktp_offsets.fetch_pos),
                                     HighwaterMarkOffset, rktp->rktp_broker_id,
                                     rktp->rktp_leader_id);

                        /* Out of range error cannot be taken as definitive
                         * when fetching from follower.
                         * Revert back to the leader in lieu of KIP-320.
                         */
                        rd_kafka_toppar_delegate_to_leader(rktp);
                        break;
                }

                /* Application error */
                err_pos = rktp->rktp_offsets.fetch_pos;
                rktp->rktp_offsets.fetch_pos.offset = RD_KAFKA_OFFSET_INVALID;
                rktp->rktp_offsets.fetch_pos.leader_epoch = -1;
                rd_kafka_offset_reset(rktp, rd_kafka_broker_id(rkb), err_pos,
                                      err,
                                      "fetch failed due to requested offset "
                                      "not available on the broker");
        } break;

        case RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED:
                /* If we're not authorized to access the
                 * topic mark it as errored to deny
                 * further Fetch requests. */
                if (rktp->rktp_last_error != err) {
                        rd_kafka_consumer_err(
                            rktp->rktp_fetchq, rd_kafka_broker_id(rkb), err,
                            tver->version, NULL, rktp,
                            rktp->rktp_offsets.fetch_pos.offset,
                            "Fetch from broker %" PRId32 " failed: %s",
                            rd_kafka_broker_id(rkb), rd_kafka_err2str(err));
                        rktp->rktp_last_error = err;
                }
                break;


                /* Application errors */
        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                if (rkb->rkb_rk->rk_conf.enable_partition_eof)
                        rd_kafka_consumer_err(
                            rktp->rktp_fetchq, rd_kafka_broker_id(rkb), err,
                            tver->version, NULL, rktp,
                            rktp->rktp_offsets.fetch_pos.offset,
                            "Fetch from broker %" PRId32
                            " reached end of "
                            "partition at offset %" PRId64
                            " (HighwaterMark %" PRId64 ")",
                            rd_kafka_broker_id(rkb),
                            rktp->rktp_offsets.fetch_pos.offset,
                            HighwaterMarkOffset);
                break;

        case RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE:
        default: /* and all other errors */
                rd_dassert(tver->version > 0);
                rd_kafka_consumer_err(
                    rktp->rktp_fetchq, rd_kafka_broker_id(rkb), err,
                    tver->version, NULL, rktp,
                    rktp->rktp_offsets.fetch_pos.offset,
                    "Fetch from broker %" PRId32 " failed at %s: %s",
                    rd_kafka_broker_id(rkb),
                    rd_kafka_fetch_pos2str(rktp->rktp_offsets.fetch_pos),
                    rd_kafka_err2str(err));
                break;
        }

        /* Back off the next fetch for this partition */
        rd_kafka_toppar_fetch_backoff(rkb, rktp, err);
}

static void rd_kafkap_Fetch_reply_tags_set_topic_cnt(
    rd_kafkap_Fetch_reply_tags_t *reply_tags,
    int32_t TopicCnt) {
        reply_tags->TopicCnt = TopicCnt;
        rd_dassert(!reply_tags->Topics);
        reply_tags->Topics = rd_calloc(TopicCnt, sizeof(*reply_tags->Topics));
}

static void
rd_kafkap_Fetch_reply_tags_set_topic(rd_kafkap_Fetch_reply_tags_t *reply_tags,
                                     int TopicIdx,
                                     rd_kafka_Uuid_t TopicId,
                                     int32_t PartitionCnt) {
        reply_tags->Topics[TopicIdx].TopicId      = TopicId;
        reply_tags->Topics[TopicIdx].PartitionCnt = PartitionCnt;
        rd_dassert(!reply_tags->Topics[TopicIdx].Partitions);
        reply_tags->Topics[TopicIdx].Partitions = rd_calloc(
            PartitionCnt, sizeof(*reply_tags->Topics[TopicIdx].Partitions));
}


static void
rd_kafkap_Fetch_reply_tags_destroy(rd_kafkap_Fetch_reply_tags_t *reply_tags) {
        int i;
        for (i = 0; i < reply_tags->TopicCnt; i++) {
                RD_IF_FREE(reply_tags->Topics[i].Partitions, rd_free);
        }
        RD_IF_FREE(reply_tags->Topics, rd_free);
        RD_IF_FREE(reply_tags->NodeEndpoints.NodeEndpoints, rd_free);
}

static int rd_kafkap_Fetch_reply_tags_partition_parse(
    rd_kafka_buf_t *rkbuf,
    uint64_t tagtype,
    uint64_t taglen,
    rd_kafkap_Fetch_reply_tags_Topic_t *TopicTags,
    rd_kafkap_Fetch_reply_tags_Partition_t *PartitionTags) {
        switch (tagtype) {
        case 1: /* CurrentLeader */
                if (rd_kafka_buf_read_CurrentLeader(
                        rkbuf, &PartitionTags->CurrentLeader) == -1)
                        goto err_parse;
                TopicTags->partitions_with_leader_change_cnt++;
                return 1;
        default:
                return 0;
        }
err_parse:
        return -1;
}

static int
rd_kafkap_Fetch_reply_tags_parse(rd_kafka_buf_t *rkbuf,
                                 uint64_t tagtype,
                                 uint64_t taglen,
                                 rd_kafkap_Fetch_reply_tags_t *tags) {
        switch (tagtype) {
        case 0: /* NodeEndpoints */
                if (rd_kafka_buf_read_NodeEndpoints(rkbuf,
                                                    &tags->NodeEndpoints) == -1)
                        goto err_parse;
                return 1;
        default:
                return 0;
        }
err_parse:
        return -1;
}

static void
rd_kafka_handle_Fetch_metadata_update(rd_kafka_broker_t *rkb,
                                      rd_kafkap_Fetch_reply_tags_t *FetchTags) {
        if (FetchTags->topics_with_leader_change_cnt &&
            FetchTags->NodeEndpoints.NodeEndpoints) {
                rd_kafka_metadata_t *md           = NULL;
                rd_kafka_metadata_internal_t *mdi = NULL;
                rd_tmpabuf_t tbuf;
                int32_t nodeid;
                rd_kafka_op_t *rko;
                int i, changed_topic, changed_partition;

                rd_kafka_broker_lock(rkb);
                nodeid = rkb->rkb_nodeid;
                rd_kafka_broker_unlock(rkb);

                rd_tmpabuf_new(&tbuf, 0, rd_true /*assert on fail*/);
                rd_tmpabuf_add_alloc(&tbuf, sizeof(*mdi));
                rd_kafkap_leader_discovery_tmpabuf_add_alloc_brokers(
                    &tbuf, &FetchTags->NodeEndpoints);
                rd_kafkap_leader_discovery_tmpabuf_add_alloc_topics(
                    &tbuf, FetchTags->topics_with_leader_change_cnt);
                for (i = 0; i < FetchTags->TopicCnt; i++) {
                        if (!FetchTags->Topics[i]
                                 .partitions_with_leader_change_cnt)
                                continue;
                        rd_kafkap_leader_discovery_tmpabuf_add_alloc_topic(
                            &tbuf, NULL,
                            FetchTags->Topics[i]
                                .partitions_with_leader_change_cnt);
                }
                rd_tmpabuf_finalize(&tbuf);

                mdi = rd_tmpabuf_alloc(&tbuf, sizeof(*mdi));
                md  = &mdi->metadata;

                rd_kafkap_leader_discovery_metadata_init(mdi, nodeid);

                rd_kafkap_leader_discovery_set_brokers(
                    &tbuf, mdi, &FetchTags->NodeEndpoints);

                rd_kafkap_leader_discovery_set_topic_cnt(
                    &tbuf, mdi, FetchTags->topics_with_leader_change_cnt);

                changed_topic = 0;
                for (i = 0; i < FetchTags->TopicCnt; i++) {
                        int j;
                        if (!FetchTags->Topics[i]
                                 .partitions_with_leader_change_cnt)
                                continue;

                        rd_kafkap_leader_discovery_set_topic(
                            &tbuf, mdi, changed_topic,
                            FetchTags->Topics[i].TopicId, NULL,
                            FetchTags->Topics[i]
                                .partitions_with_leader_change_cnt);

                        changed_partition = 0;
                        for (j = 0; j < FetchTags->Topics[i].PartitionCnt;
                             j++) {
                                if (FetchTags->Topics[i]
                                        .Partitions[j]
                                        .CurrentLeader.LeaderId < 0)
                                        continue;

                                rd_kafkap_Fetch_reply_tags_Partition_t
                                    *Partition =
                                        &FetchTags->Topics[i].Partitions[j];
                                rd_kafkap_leader_discovery_set_CurrentLeader(
                                    &tbuf, mdi, changed_topic,
                                    changed_partition, Partition->Partition,
                                    &Partition->CurrentLeader);
                                changed_partition++;
                        }
                        changed_topic++;
                }

                rko = rd_kafka_op_new(RD_KAFKA_OP_METADATA_UPDATE);
                rko->rko_u.metadata.md  = md;
                rko->rko_u.metadata.mdi = mdi;
                rd_kafka_q_enq(rkb->rkb_rk->rk_ops, rko);
        }
}

/**
 * @brief Per-partition FetchResponse parsing and handling.
 *
 * @returns an error on buffer parse failure, else RD_KAFKA_RESP_ERR_NO_ERROR.
 */
static rd_kafka_resp_err_t rd_kafka_fetch_reply_handle_partition(
    rd_kafka_broker_t *rkb,
    const rd_kafkap_str_t *topic,
    rd_kafka_topic_t *rkt /*possibly NULL*/,
    rd_kafka_buf_t *rkbuf,
    rd_kafka_buf_t *request,
    int16_t ErrorCode,
    rd_kafkap_Fetch_reply_tags_Topic_t *TopicTags,
    rd_kafkap_Fetch_reply_tags_Partition_t *PartitionTags) {
        const int log_decode_errors = LOG_ERR;
        struct rd_kafka_toppar_ver *tver, tver_skel;
        rd_kafka_toppar_t *rktp               = NULL;
        rd_kafka_aborted_txns_t *aborted_txns = NULL;
        rd_slice_t save_slice;
        int32_t fetch_version;
        struct {
                int32_t Partition;
                int16_t ErrorCode;
                int64_t HighwaterMarkOffset;
                int64_t LastStableOffset; /* v4 */
                int64_t LogStartOffset;   /* v5 */
                int32_t MessageSetSize;
                int32_t PreferredReadReplica; /* v11 */
        } hdr;
        rd_kafka_resp_err_t err;
        int64_t end_offset;

        rd_kafka_buf_read_i32(rkbuf, &hdr.Partition);
        rd_kafka_buf_read_i16(rkbuf, &hdr.ErrorCode);
        if (PartitionTags)
                PartitionTags->Partition = hdr.Partition;
        if (ErrorCode)
                hdr.ErrorCode = ErrorCode;
        rd_kafka_buf_read_i64(rkbuf, &hdr.HighwaterMarkOffset);

        end_offset = hdr.HighwaterMarkOffset;

        hdr.LastStableOffset = RD_KAFKA_OFFSET_INVALID;
        hdr.LogStartOffset   = RD_KAFKA_OFFSET_INVALID;
        if (rd_kafka_buf_ApiVersion(request) >= 4) {
                int32_t AbortedTxnCnt;
                int k;
                rd_kafka_buf_read_i64(rkbuf, &hdr.LastStableOffset);
                if (rd_kafka_buf_ApiVersion(request) >= 5)
                        rd_kafka_buf_read_i64(rkbuf, &hdr.LogStartOffset);

                rd_kafka_buf_read_arraycnt(rkbuf, &AbortedTxnCnt,
                                           RD_KAFKAP_ABORTED_TRANSACTIONS_MAX);

                if (rkb->rkb_rk->rk_conf.isolation_level ==
                    RD_KAFKA_READ_UNCOMMITTED) {

                        if (unlikely(AbortedTxnCnt > 0)) {
                                rd_rkb_log(rkb, LOG_ERR, "FETCH",
                                           "%.*s [%" PRId32
                                           "]: "
                                           "%" PRId32
                                           " aborted transaction(s) "
                                           "encountered in READ_UNCOMMITTED "
                                           "fetch response: ignoring.",
                                           RD_KAFKAP_STR_PR(topic),
                                           hdr.Partition, AbortedTxnCnt);
                                for (k = 0; k < AbortedTxnCnt; k++) {
                                        rd_kafka_buf_skip(rkbuf, (8 + 8));
                                        /* AbortedTransaction tags */
                                        rd_kafka_buf_skip_tags(rkbuf);
                                }
                        }
                } else {
                        /* Older brokers may return LSO -1,
                         * in which case we use the HWM. */
                        if (hdr.LastStableOffset >= 0)
                                end_offset = hdr.LastStableOffset;

                        if (AbortedTxnCnt > 0) {
                                aborted_txns =
                                    rd_kafka_aborted_txns_new(AbortedTxnCnt);
                                for (k = 0; k < AbortedTxnCnt; k++) {
                                        int64_t PID;
                                        int64_t FirstOffset;
                                        rd_kafka_buf_read_i64(rkbuf, &PID);
                                        rd_kafka_buf_read_i64(rkbuf,
                                                              &FirstOffset);
                                        /* AbortedTransaction tags */
                                        rd_kafka_buf_skip_tags(rkbuf);
                                        rd_kafka_aborted_txns_add(
                                            aborted_txns, PID, FirstOffset);
                                }
                                rd_kafka_aborted_txns_sort(aborted_txns);
                        }
                }
        }

        if (rd_kafka_buf_ApiVersion(request) >= 11)
                rd_kafka_buf_read_i32(rkbuf, &hdr.PreferredReadReplica);
        else
                hdr.PreferredReadReplica = -1;
        /* Compact Records Array */
        rd_kafka_buf_read_arraycnt(rkbuf, &hdr.MessageSetSize, -1);

        if (unlikely(hdr.MessageSetSize < 0))
                rd_kafka_buf_parse_fail(
                    rkbuf,
                    "%.*s [%" PRId32 "]: invalid MessageSetSize %" PRId32,
                    RD_KAFKAP_STR_PR(topic), hdr.Partition, hdr.MessageSetSize);

        /* Look up topic+partition */
        if (likely(rkt != NULL)) {
                rd_kafka_topic_rdlock(rkt);
                rktp = rd_kafka_toppar_get(rkt, hdr.Partition,
                                           0 /*no ua-on-miss*/);
                rd_kafka_topic_rdunlock(rkt);
        }

        if (unlikely(!rkt || !rktp)) {
                rd_rkb_dbg(rkb, TOPIC, "UNKTOPIC",
                           "Received Fetch response (error %hu) for unknown "
                           "topic %.*s [%" PRId32 "]: ignoring",
                           hdr.ErrorCode, RD_KAFKAP_STR_PR(topic),
                           hdr.Partition);
                rd_kafka_buf_skip(rkbuf, hdr.MessageSetSize);
                goto done;
        }

        rd_kafka_toppar_lock(rktp);
        rktp->rktp_lo_offset = hdr.LogStartOffset;
        rktp->rktp_hi_offset = hdr.HighwaterMarkOffset;
        /* Let the LastStable offset be the effective
         * end_offset based on protocol version, that is:
         * if connected to a broker that does not support
         * LastStableOffset we use the HighwaterMarkOffset. */
        rktp->rktp_ls_offset = end_offset;
        rd_kafka_toppar_unlock(rktp);

        if (hdr.PreferredReadReplica != -1) {

                rd_kafka_fetch_preferred_replica_handle(
                    rktp, rkbuf, rkb, hdr.PreferredReadReplica);

                if (unlikely(hdr.MessageSetSize != 0)) {
                        rd_rkb_log(rkb, LOG_WARNING, "FETCH",
                                   "%.*s [%" PRId32
                                   "]: Fetch response has both preferred read "
                                   "replica and non-zero message set size: "
                                   "%" PRId32 ": skipping messages",
                                   RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                                   rktp->rktp_partition, hdr.MessageSetSize);
                        rd_kafka_buf_skip(rkbuf, hdr.MessageSetSize);
                }
                goto done;
        }

        rd_kafka_toppar_lock(rktp);

        /* Make sure toppar hasn't moved to another broker
         * during the lifetime of the request. */
        if (unlikely(rktp->rktp_broker != rkb)) {
                rd_kafka_toppar_unlock(rktp);
                rd_rkb_dbg(rkb, MSG, "FETCH",
                           "%.*s [%" PRId32
                           "]: partition broker has changed: "
                           "discarding fetch response",
                           RD_KAFKAP_STR_PR(topic), hdr.Partition);
                rd_kafka_buf_skip(rkbuf, hdr.MessageSetSize);
                goto done;
        }

        fetch_version = rktp->rktp_fetch_version;
        rd_kafka_toppar_unlock(rktp);

        /* Check if this Fetch is for an outdated fetch version,
         * or the original rktp was removed and a new one
         * created (due to partition count decreasing and
         * then increasing again, which can happen in
         * desynchronized clusters): if so ignore it. */
        tver_skel.rktp = rktp;
        tver           = rd_list_find(request->rkbuf_rktp_vers, &tver_skel,
                                      rd_kafka_toppar_ver_cmp);
        rd_kafka_assert(NULL, tver);
        if (tver->rktp != rktp || tver->version < fetch_version) {
                rd_rkb_dbg(rkb, MSG, "DROP",
                           "%s [%" PRId32
                           "]: dropping outdated fetch response "
                           "(v%d < %d or old rktp)",
                           rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
                           tver->version, fetch_version);
                rd_atomic64_add(&rktp->rktp_c.rx_ver_drops, 1);
                rd_kafka_buf_skip(rkbuf, hdr.MessageSetSize);
                goto done;
        }

        rd_rkb_dbg(rkb, MSG, "FETCH",
                   "Topic %.*s [%" PRId32 "] MessageSet size %" PRId32
                   ", error \"%s\", MaxOffset %" PRId64 ", LSO %" PRId64
                   ", Ver %" PRId32 "/%" PRId32,
                   RD_KAFKAP_STR_PR(topic), hdr.Partition, hdr.MessageSetSize,
                   rd_kafka_err2str(hdr.ErrorCode), hdr.HighwaterMarkOffset,
                   hdr.LastStableOffset, tver->version, fetch_version);

        /* If this is the last message of the queue,
         * signal EOF back to the application. */
        if (end_offset == rktp->rktp_offsets.fetch_pos.offset &&
            rktp->rktp_offsets.eof_offset != end_offset) {
                hdr.ErrorCode = RD_KAFKA_RESP_ERR__PARTITION_EOF;
                rktp->rktp_offsets.eof_offset = end_offset;
        }

        if (unlikely(hdr.ErrorCode != RD_KAFKA_RESP_ERR_NO_ERROR)) {
                /* Handle partition-level errors. */
                rd_kafka_fetch_reply_handle_partition_error(
                    rkb, rktp, tver, hdr.ErrorCode, hdr.HighwaterMarkOffset);

                rd_kafka_buf_skip(rkbuf, hdr.MessageSetSize);
                goto done;
        }

        /* No error, clear any previous fetch error. */
        rktp->rktp_last_error = RD_KAFKA_RESP_ERR_NO_ERROR;

        if (unlikely(hdr.MessageSetSize <= 0))
                goto done;

        /**
         * Parse MessageSet
         */
        if (!rd_slice_narrow_relative(&rkbuf->rkbuf_reader, &save_slice,
                                      (size_t)hdr.MessageSetSize))
                rd_kafka_buf_check_len(rkbuf, hdr.MessageSetSize);

        /* Parse messages */
        err = rd_kafka_msgset_parse(rkbuf, request, rktp, aborted_txns, tver);


        rd_slice_widen(&rkbuf->rkbuf_reader, &save_slice);
        /* Continue with next partition regardless of
         * parse errors (which are partition-specific) */

        /* On error: back off the fetcher for this partition */
        if (unlikely(err))
                rd_kafka_toppar_fetch_backoff(rkb, rktp, err);

        goto done;

err_parse:
        if (aborted_txns)
                rd_kafka_aborted_txns_destroy(aborted_txns);
        if (rktp)
                rd_kafka_toppar_destroy(rktp); /*from get()*/
        return rkbuf->rkbuf_err;

done:
        if (aborted_txns)
                rd_kafka_aborted_txns_destroy(aborted_txns);
        if (likely(rktp != NULL))
                rd_kafka_toppar_destroy(rktp); /*from get()*/

        if (PartitionTags) {
                /* Set default LeaderId and LeaderEpoch */
                PartitionTags->CurrentLeader.LeaderId    = -1;
                PartitionTags->CurrentLeader.LeaderEpoch = -1;
        }
        rd_kafka_buf_read_tags(rkbuf,
                               rd_kafkap_Fetch_reply_tags_partition_parse,
                               TopicTags, PartitionTags);

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}

/**
 * Parses and handles a Fetch reply.
 * Returns 0 on success or an error code on failure.
 */
static rd_kafka_resp_err_t
rd_kafka_fetch_reply_handle(rd_kafka_broker_t *rkb,
                            rd_kafka_buf_t *rkbuf,
                            rd_kafka_buf_t *request) {
        int32_t TopicArrayCnt;
        int i;
        const int log_decode_errors            = LOG_ERR;
        rd_kafka_topic_t *rkt                  = NULL;
        int16_t ErrorCode                      = RD_KAFKA_RESP_ERR_NO_ERROR;
        rd_kafkap_Fetch_reply_tags_t FetchTags = RD_ZERO_INIT;
        rd_bool_t has_fetch_tags               = rd_false;

        if (rd_kafka_buf_ApiVersion(request) >= 1) {
                int32_t Throttle_Time;
                rd_kafka_buf_read_i32(rkbuf, &Throttle_Time);

                rd_kafka_op_throttle_time(rkb, rkb->rkb_rk->rk_rep,
                                          Throttle_Time);
        }

        if (rd_kafka_buf_ApiVersion(request) >= 7) {
                int32_t SessionId;
                rd_kafka_buf_read_i16(rkbuf, &ErrorCode);
                rd_kafka_buf_read_i32(rkbuf, &SessionId);
        }

        rd_kafka_buf_read_arraycnt(rkbuf, &TopicArrayCnt, RD_KAFKAP_TOPICS_MAX);
        /* Verify that TopicArrayCnt seems to be in line with remaining size */
        rd_kafka_buf_check_len(rkbuf,
                               TopicArrayCnt * (3 /*topic min size*/ +
                                                4 /*PartitionArrayCnt*/ + 4 +
                                                2 + 8 + 4 /*inner header*/));

        if (rd_kafka_buf_ApiVersion(request) >= 12) {
                has_fetch_tags = rd_true;
                rd_kafkap_Fetch_reply_tags_set_topic_cnt(&FetchTags,
                                                         TopicArrayCnt);
        }

        for (i = 0; i < TopicArrayCnt; i++) {
                rd_kafkap_str_t topic    = RD_ZERO_INIT;
                rd_kafka_Uuid_t topic_id = RD_KAFKA_UUID_ZERO;
                int32_t PartitionArrayCnt;
                int j;

                if (rd_kafka_buf_ApiVersion(request) > 12) {
                        rd_kafka_buf_read_uuid(rkbuf, &topic_id);
                        rkt = rd_kafka_topic_find_by_topic_id(rkb->rkb_rk,
                                                              topic_id);
                        if (rkt)
                                topic = *rkt->rkt_topic;
                } else {
                        rd_kafka_buf_read_str(rkbuf, &topic);
                        rkt = rd_kafka_topic_find0(rkb->rkb_rk, &topic);
                }

                rd_kafka_buf_read_arraycnt(rkbuf, &PartitionArrayCnt,
                                           RD_KAFKAP_PARTITIONS_MAX);
                if (rd_kafka_buf_ApiVersion(request) >= 12) {
                        rd_kafkap_Fetch_reply_tags_set_topic(
                            &FetchTags, i, topic_id, PartitionArrayCnt);
                }

                for (j = 0; j < PartitionArrayCnt; j++) {
                        if (rd_kafka_fetch_reply_handle_partition(
                                rkb, &topic, rkt, rkbuf, request, ErrorCode,
                                has_fetch_tags ? &FetchTags.Topics[i] : NULL,
                                has_fetch_tags
                                    ? &FetchTags.Topics[i].Partitions[j]
                                    : NULL))
                                goto err_parse;
                }
                if (has_fetch_tags &&
                    FetchTags.Topics[i].partitions_with_leader_change_cnt) {
                        FetchTags.topics_with_leader_change_cnt++;
                }

                if (rkt) {
                        rd_kafka_topic_destroy0(rkt);
                        rkt = NULL;
                }
                /* Topic Tags */
                rd_kafka_buf_skip_tags(rkbuf);
        }

        /* Top level tags */
        rd_kafka_buf_read_tags(rkbuf, rd_kafkap_Fetch_reply_tags_parse,
                               &FetchTags);

        if (rd_kafka_buf_read_remain(rkbuf) != 0) {
                rd_kafka_buf_parse_fail(rkbuf,
                                        "Remaining data after message set "
                                        "parse: %" PRIusz " bytes",
                                        rd_kafka_buf_read_remain(rkbuf));
                RD_NOTREACHED();
        }
        rd_kafka_handle_Fetch_metadata_update(rkb, &FetchTags);
        rd_kafkap_Fetch_reply_tags_destroy(&FetchTags);

        return 0;

err_parse:
        if (rkt)
                rd_kafka_topic_destroy0(rkt);
        rd_kafkap_Fetch_reply_tags_destroy(&FetchTags);
        rd_rkb_dbg(rkb, MSG, "BADMSG",
                   "Bad message (Fetch v%d): "
                   "is broker.version.fallback incorrectly set?",
                   (int)request->rkbuf_reqhdr.ApiVersion);
        return rkbuf->rkbuf_err;
}


static rd_kafka_resp_err_t rd_kafka_share_fetch_reply_handle_partition(
    rd_kafka_broker_t *rkb,
    const rd_kafkap_str_t *topic,
    rd_kafka_topic_t *rkt /*possibly NULL*/,
    rd_kafka_buf_t *rkbuf,
    rd_kafka_buf_t *request) {

        /* TODO: KIP-932: Check rd_kafka_fetch_reply_handle_partition
                         * and modify as needed for ShareFetch.
                         */
        int32_t PartitionId;
        int16_t PartitionFetchErrorCode;
        rd_kafkap_str_t PartitionFetchErrorStr = RD_KAFKAP_STR_INITIALIZER_EMPTY;
        int16_t AcknowledgementErrorCode;
        rd_kafkap_str_t AcknowledgementErrorStr = RD_KAFKAP_STR_INITIALIZER_EMPTY;
        rd_kafkap_CurrentLeader_t CurrentLeader;
        int32_t MessageSetSize;
        rd_kafka_toppar_t *rktp               = NULL;
        struct rd_kafka_toppar_ver tver;
        rd_slice_t save_slice;
        const int log_decode_errors            = LOG_ERR;
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
        int32_t AcquiredRecordsArrayCnt;
        int64_t FirstOffset;
        int64_t LastOffset;
        int16_t DeliveryCount;
        int i;

        rd_kafka_buf_read_i32(rkbuf, &PartitionId); // Partition
        rd_kafka_buf_read_i16(rkbuf, &PartitionFetchErrorCode); // PartitionFetchError
        rd_kafka_buf_read_str(rkbuf, &PartitionFetchErrorStr); // ErrorString
        /* TODO KIP-932: We should reset (to INVALID) previous acknowledgement information in the reply
           or maybe while sending the request itself? */
        rd_kafka_buf_read_i16(rkbuf, &AcknowledgementErrorCode); // AcknowledgementError
        rd_kafka_buf_read_str(rkbuf, &AcknowledgementErrorStr); // AcknowledgementErrorString
        rd_kafka_buf_read_CurrentLeader(rkbuf, &CurrentLeader); // CurrentLeader
        
        /* Compact Records Array */
        rd_kafka_buf_read_arraycnt(rkbuf, &MessageSetSize, -1);

        if (unlikely(MessageSetSize < 0))
                rd_kafka_buf_parse_fail(
                rkbuf,
                "%.*s [%" PRId32 "]: invalid MessageSetSize %" PRId32,
                RD_KAFKAP_STR_PR(topic), PartitionId, MessageSetSize);

        /* Look up topic+partition */
        if (likely(rkt != NULL)) {
                rd_kafka_topic_rdlock(rkt);
                rktp = rd_kafka_toppar_get(rkt, PartitionId,
                                        0 /*no ua-on-miss*/);
                rd_kafka_topic_rdunlock(rkt);
        }

        if (unlikely(!rkt || !rktp)) {
                rd_rkb_dbg(rkb, TOPIC, "UNKTOPIC",
                        "Received Fetch response (error %hu) for unknown "
                        "topic %.*s [%" PRId32 "]: ignoring",
                        PartitionFetchErrorCode, RD_KAFKAP_STR_PR(topic),
                        PartitionId);
                rd_kafka_buf_skip(rkbuf, MessageSetSize);
                rd_kafka_buf_read_arraycnt(rkbuf, &AcquiredRecordsArrayCnt, -1); // AcquiredRecordsArrayCnt
                for (i = 0; i < AcquiredRecordsArrayCnt; i++) {
                        rd_kafka_buf_read_i64(rkbuf, &FirstOffset); // FirstOffset
                        rd_kafka_buf_read_i64(rkbuf, &LastOffset); // LastOffset
                        rd_kafka_buf_read_i16(rkbuf, &DeliveryCount); // DeliveryCount
                        rd_kafka_buf_skip_tags(rkbuf); // AcquiredRecords tags
                }
                goto done;
        }

        tver.rktp = rktp;
        tver.version = rktp->rktp_fetch_version;

        
        /* No error, clear any previous fetch error. */
        rktp->rktp_last_error = RD_KAFKA_RESP_ERR_NO_ERROR;

        if(MessageSetSize > 0) {
                /**
                 * Parse MessageSet
                 */
                if (!rd_slice_narrow_relative(&rkbuf->rkbuf_reader, &save_slice,
                                        (size_t) MessageSetSize))
                        rd_kafka_buf_check_len(rkbuf, MessageSetSize);

                /* Parse messages 
                TODO KIP-932: This part might raise issue as We are adding messages
                                to the consumer queue in partition by partition manner.
                                The poll returns messages as soon as they are available in the queue,
                                so messages for different partitions in the same fetch request might
                                not be sent at once to the user.
                */
                err = rd_kafka_msgset_parse(rkbuf, request, rktp, NULL, &tver);

                rd_slice_widen(&rkbuf->rkbuf_reader, &save_slice);
                /* Continue with next partition regardless of
                * parse errors (which are partition-specific) */

        }

        rd_kafka_buf_read_arraycnt(rkbuf, &AcquiredRecordsArrayCnt, -1); // AcquiredRecordsArrayCnt
        rd_rkb_dbg(rkb, FETCH, "SHAREFETCH", "%.*s [%" PRId32 "] : Share Acknowledgement Count: %ld, AcquiredRecordsArrayCnt: %d\n",
               RD_KAFKAP_STR_PR(topic), PartitionId,
               rktp->rktp_share_acknowledge_count, AcquiredRecordsArrayCnt);
        rd_dassert(rktp->rktp_share_acknowledge_count == 0);
        rd_dassert(rktp->rktp_share_acknowledge == NULL);
        if(AcquiredRecordsArrayCnt > 0) {
                rktp->rktp_share_acknowledge_count = AcquiredRecordsArrayCnt;
                rktp->rktp_share_acknowledge = rd_calloc(AcquiredRecordsArrayCnt,
                                                        sizeof(*rktp->rktp_share_acknowledge));
                for (i = 0; i < AcquiredRecordsArrayCnt; i++) {
                        rd_kafka_buf_read_i64(rkbuf, &FirstOffset); // FirstOffset
                        rd_kafka_buf_read_i64(rkbuf, &LastOffset); // LastOffset
                        rd_kafka_buf_read_i16(rkbuf, &DeliveryCount); // DeliveryCount
                        rd_kafka_buf_skip_tags(rkbuf); // AcquiredRecords tags
                        rd_rkb_dbg(rkb, FETCH, "SHAREFETCH",
                                "%.*s [%" PRId32 "]: Acquired Records from offset %" PRId64
                                " to %" PRId64 ", DeliveryCount %" PRId16,
                                RD_KAFKAP_STR_PR(topic), PartitionId,
                                FirstOffset, LastOffset, DeliveryCount);
                        rktp->rktp_share_acknowledge[i].first_offset = FirstOffset;
                        rktp->rktp_share_acknowledge[i].last_offset = LastOffset;
                        rktp->rktp_share_acknowledge[i].delivery_count = DeliveryCount;
                }
        }

        rd_kafka_buf_skip_tags(rkbuf); // Partition tags

        goto done;

err_parse:
        if (rktp)
                rd_kafka_toppar_destroy(rktp); /*from get()*/
        return rkbuf->rkbuf_err;

done:
        if (likely(rktp != NULL))
                rd_kafka_toppar_destroy(rktp); /*from get()*/

        return err;

    }


/**
 * Parses and handles a ShareFetch reply.
 * Returns 0 on success or an error code on failure.
 * 
 * TODO KIP-932: Change return type to proper error with message. See `rd_kafka_error_t *`.
 */
static rd_kafka_resp_err_t
rd_kafka_share_fetch_reply_handle(rd_kafka_broker_t *rkb,
                            rd_kafka_buf_t *rkbuf,
                            rd_kafka_buf_t *request) {
        int32_t TopicArrayCnt;
        int i;
        const int log_decode_errors            = LOG_ERR;
        rd_kafka_topic_t *rkt                  = NULL;
        int16_t ErrorCode                      = RD_KAFKA_RESP_ERR_NO_ERROR;
        rd_kafkap_str_t ErrorStr               = RD_KAFKAP_STR_INITIALIZER_EMPTY;
        int32_t AcquisitionLockTimeoutMs       = 0;
        rd_kafkap_NodeEndpoints_t NodeEndpoints;
        NodeEndpoints.NodeEndpoints = NULL;
        NodeEndpoints.NodeEndpointCnt = 0;

        rd_kafka_buf_read_throttle_time(rkbuf);

        rd_kafka_buf_read_i16(rkbuf, &ErrorCode);
        rd_kafka_buf_read_str(rkbuf, &ErrorStr);

        if (ErrorCode) {
                rd_rkb_log(rkb, LOG_ERR, "SHAREFETCH",
                           "ShareFetch response error %d: '%.*s'",
                           ErrorCode,
                           RD_KAFKAP_STR_PR(&ErrorStr));
                return ErrorCode;
        }

        rd_kafka_buf_read_i32(rkbuf, &AcquisitionLockTimeoutMs);

        rd_kafka_buf_read_arraycnt(rkbuf, &TopicArrayCnt, RD_KAFKAP_TOPICS_MAX);
        /* TODO KIP-932: Check if required.
        Verify that TopicArrayCnt seems to be in line with remaining size */
        // rd_kafka_buf_check_len(rkbuf,
        //                        TopicArrayCnt * (3 /*topic min size*/ +
        //                                         4 /*PartitionArrayCnt*/ + 4 +
        //                                         2 + 8 + 4 /*inner header*/));

        for (i = 0; i < TopicArrayCnt; i++) {
                rd_kafkap_str_t topic    = RD_ZERO_INIT;
                rd_kafka_Uuid_t topic_id = RD_KAFKA_UUID_ZERO;
                int32_t PartitionArrayCnt;
                int j;

                rd_kafka_buf_read_uuid(rkbuf, &topic_id);
                rkt = rd_kafka_topic_find_by_topic_id(rkb->rkb_rk,
                                                        topic_id);
                if (rkt)
                        topic = *rkt->rkt_topic;

                rd_kafka_buf_read_arraycnt(rkbuf, &PartitionArrayCnt,
                                           RD_KAFKAP_PARTITIONS_MAX);

                for (j = 0; j < PartitionArrayCnt; j++) {
                        if (rd_kafka_share_fetch_reply_handle_partition(
                                rkb, &topic, rkt, rkbuf, request))
                                goto err_parse;
                }

                if (rkt) {
                        rd_kafka_topic_destroy0(rkt);
                        rkt = NULL;
                }
                /* Topic Tags */
                rd_kafka_buf_skip_tags(rkbuf);
        }

        rd_kafka_buf_read_NodeEndpoints(rkbuf, &NodeEndpoints);

        /* Top level tags */
        rd_kafka_buf_skip_tags(rkbuf);


        if (rd_kafka_buf_read_remain(rkbuf) != 0) {
                rd_kafka_buf_parse_fail(rkbuf,
                                        "Remaining data after message set "
                                        "parse: %" PRIusz " bytes",
                                        rd_kafka_buf_read_remain(rkbuf));
                RD_NOTREACHED();
        }

// done:
        RD_IF_FREE(NodeEndpoints.NodeEndpoints, rd_free);
        RD_IF_FREE(rkt, rd_kafka_topic_destroy0);
        return RD_KAFKA_RESP_ERR_NO_ERROR;

err_parse:
        if (rkt)
                rd_kafka_topic_destroy0(rkt);
        rd_rkb_dbg(rkb, MSG, "BADMSG",
                   "Bad message (Fetch v%d): "
                   "is broker.version.fallback incorrectly set?",
                   (int)request->rkbuf_reqhdr.ApiVersion);
        return rkbuf->rkbuf_err;
}


/**
 * TODO KIP-932: Implement.
 */
// static void rd_kafak_broker_session_reset(rd_kafka_broker_t *rkb) {
// }
static void rd_kafka_broker_session_update_epoch(rd_kafka_broker_t *rkb) {
        if (rkb->rkb_share_fetch_session.epoch == -1) {
                rd_kafka_dbg(rkb->rkb_rk, MSG, "SHAREFETCH",
                                "Not updating next epoch for -1 as it should be -1 again.");
                return;
        }
        if (rkb->rkb_share_fetch_session.epoch == INT32_MAX)
                rkb->rkb_share_fetch_session.epoch = 1;
        else
                rkb->rkb_share_fetch_session.epoch++;
}

static void rd_kafka_broker_session_add_partition_to_toppars_in_session(rd_kafka_broker_t *rkb, rd_kafka_toppar_t *rktp) {
        rd_kafka_toppar_t *session_rktp, *adding_rktp;
        TAILQ_FOREACH(session_rktp, &rkb->rkb_share_fetch_session.toppars_in_session, rktp_rkb_session_link) {
                if(rktp == session_rktp) {
                        rd_kafka_dbg(rkb->rkb_rk, MSG, "SHAREFETCH",
                                        "%s [%" PRId32
                                        "]: already in ShareFetch session",
                                        rktp->rktp_rkt->rkt_topic->str,
                                        rktp->rktp_partition);
                        return;
                }
        }
        rd_kafka_dbg(rkb->rkb_rk, FETCH, "SHAREFETCH",
                        "%s [%" PRId32
                        "]: adding to ShareFetch session",
                        rktp->rktp_rkt->rkt_topic->str,
                        rktp->rktp_partition);
        adding_rktp = rd_kafka_toppar_keep(rktp);
        TAILQ_INSERT_TAIL(&rkb->rkb_share_fetch_session.toppars_in_session, adding_rktp, rktp_rkb_session_link);
        rkb->rkb_share_fetch_session.toppars_in_session_cnt++;
}

static void rd_kafka_broker_session_remove_partition_from_toppars_in_session(rd_kafka_broker_t *rkb, rd_kafka_toppar_t *rktp) {
        rd_kafka_toppar_t *session_rktp, *tmp_rktp;
        TAILQ_FOREACH_SAFE(session_rktp, &rkb->rkb_share_fetch_session.toppars_in_session, rktp_rkb_session_link, tmp_rktp) {
                if(rktp == session_rktp) {
                        TAILQ_REMOVE(&rkb->rkb_share_fetch_session.toppars_in_session, session_rktp, rktp_rkb_session_link);
                        rd_kafka_toppar_destroy(session_rktp); // from session list
                        rkb->rkb_share_fetch_session.toppars_in_session_cnt--;
                        rd_kafka_dbg(rkb->rkb_rk, MSG, "SHAREFETCH",
                                        "%s [%" PRId32
                                        "]: removed from ShareFetch session",
                                        rktp->rktp_rkt->rkt_topic->str,
                                        rktp->rktp_partition);
                        return;
                }
        }
        rd_kafka_dbg(rkb->rkb_rk, MSG, "SHAREFETCH",
                        "%s [%" PRId32
                        "]: not found in ShareFetch session",
                        rktp->rktp_rkt->rkt_topic->str,
                        rktp->rktp_partition);
}

static void rd_kafka_broker_session_update_toppars_in_session(rd_kafka_broker_t *rkb, rd_kafka_toppar_t *rktp, rd_bool_t add) {
        if(add)
                rd_kafka_broker_session_add_partition_to_toppars_in_session(rkb, rktp);
        else
                rd_kafka_broker_session_remove_partition_from_toppars_in_session(rkb, rktp);

}


// static void rd_kafka_broker_session_update_added_partitions(rd_kafka_broker_t *rkb) {
//         size_t i;
//         rd_kafka_toppar_t *rktp, *removed_rktp;
//         rd_list_t *toppars_to_add = rkb->rkb_share_fetch_session.toppars_to_add;
//         rd_list_t *added_toppars = rkb->rkb_share_fetch_session.adding_toppars;

//         if(added_toppars == NULL || rd_list_cnt(added_toppars) == 0)
//                 return;

//         RD_LIST_FOREACH(rktp, added_toppars, i) {
//                 rd_kafka_broker_session_update_toppars_in_session(rkb, rktp, rd_true /* add */);
//                 if(toppars_to_add) {
//                         removed_rktp = rd_list_remove(toppars_to_add, rktp);
//                         if(removed_rktp)
//                                 rd_kafka_toppar_destroy(removed_rktp); // from partitions list
//                 }
//         }
//         rd_list_destroy(added_toppars);
//         rkb->rkb_share_fetch_session.adding_toppars = NULL;
// }

// static void rd_kafka_broker_session_update_removed_partitions(rd_kafka_broker_t *rkb) {
//         size_t i;
//         rd_kafka_toppar_t *rktp, *removed_rktp;
//         rd_list_t *toppars_to_forget = rkb->rkb_share_fetch_session.toppars_to_forget;
//         rd_list_t *forgotten_toppars = rkb->rkb_share_fetch_session.forgetting_toppars;

//         if(forgotten_toppars == NULL || rd_list_cnt(forgotten_toppars) == 0)
//                 return;

//         RD_LIST_FOREACH(rktp, forgotten_toppars, i) {
//                 rd_kafka_broker_session_update_toppars_in_session(rkb, rktp, rd_false /* remove */);
//                 if(toppars_to_forget) {
//                         removed_rktp = rd_list_remove(toppars_to_forget, rktp);
//                         if(removed_rktp)
//                                 rd_kafka_toppar_destroy(removed_rktp); // from partitions list
//                 }
//         }
//         rd_list_destroy(forgotten_toppars);
//         rkb->rkb_share_fetch_session.forgetting_toppars = NULL;
// }

static void rd_kafka_broker_session_update_toppars_list(
    rd_kafka_broker_t *rkb,
    rd_list_t **request_toppars_ptr,
    rd_list_t **toppars_to_remove_from_ptr,
    rd_bool_t add) {
        size_t i;
        rd_kafka_toppar_t *rktp, *removed_rktp;
        rd_list_t *request_toppars = *request_toppars_ptr;
        rd_list_t *toppars_to_remove_from = *toppars_to_remove_from_ptr;

        if (request_toppars == NULL || rd_list_cnt(request_toppars) == 0)
                return;

        rd_kafka_dbg(rkb->rkb_rk, FETCH, "SHAREFETCH","%d toppars being %s the session:",
                        rd_list_cnt(request_toppars),
                        add ? "added to" : "removed from");

        RD_LIST_FOREACH(rktp, request_toppars, i) {
                rd_kafka_dbg(rkb->rkb_rk, FETCH, "SHAREFETCH","    %s [%" PRId32 "]",
                        rktp->rktp_rkt->rkt_topic->str,
                        rktp->rktp_partition);
                rd_kafka_broker_session_update_toppars_in_session(rkb, rktp, add);
                if (toppars_to_remove_from) {
                        removed_rktp = rd_list_remove(toppars_to_remove_from, rktp);
                        if (removed_rktp) {
                                rd_kafka_toppar_destroy(removed_rktp); /* from partitions list */
                                if(rd_list_empty(toppars_to_remove_from)) {
                                        rd_list_destroy(toppars_to_remove_from);
                                        *toppars_to_remove_from_ptr = NULL;
                                }
                        }
                }
        }
        rd_list_destroy(request_toppars);
        *request_toppars_ptr = NULL;
}

static void rd_kafka_broker_session_update_added_partitions(
    rd_kafka_broker_t *rkb) {
        rd_kafka_broker_session_update_toppars_list(
            rkb, &rkb->rkb_share_fetch_session.adding_toppars,
            &rkb->rkb_share_fetch_session.toppars_to_add, rd_true);
}

static void rd_kafka_broker_session_update_removed_partitions(
    rd_kafka_broker_t *rkb) {
        rd_kafka_broker_session_update_toppars_list(
            rkb, &rkb->rkb_share_fetch_session.forgetting_toppars,
            &rkb->rkb_share_fetch_session.toppars_to_forget, rd_false);
}

static void rd_kafka_broker_session_update_partitions(rd_kafka_broker_t *rkb) {
        rd_kafka_broker_session_update_added_partitions(rkb);
        rd_kafka_broker_session_update_removed_partitions(rkb);
}


/**
 * Update ShareFetch session state after a Fetch or ShareFetch response.
 * TODO KIP-932: Improve efficiency of this function.
 */
static void rd_kafka_broker_session_update(rd_kafka_broker_t *rkb) {
        rd_kafka_broker_session_update_epoch(rkb);
        rd_kafka_broker_session_update_partitions(rkb);
}

/**
 * @broker ShareFetchResponse handling.
 *
 * @locality broker thread  (or any thread if err == __DESTROY).
 */
static void rd_kafka_broker_share_fetch_reply(rd_kafka_t *rk,
                                        rd_kafka_broker_t *rkb,
                                        rd_kafka_resp_err_t err,
                                        rd_kafka_buf_t *reply,
                                        rd_kafka_buf_t *request,
                                        void *opaque) {

        rd_kafka_op_t *rko_orig = opaque;

        /**
         * TODO KIP-932: Improve this handling with Error handling and leave flow.
         */
        if (err == RD_KAFKA_RESP_ERR__DESTROY) {
                /* TODO KIP-932: Check what is needed out of the below */
                rd_kafka_broker_session_update(rkb);
                rd_kafka_op_reply(rko_orig, err);
                return; /* Terminating */
        }

        rd_kafka_assert(rkb->rkb_rk, rkb->rkb_fetching > 0);

        /* Parse and handle the messages (unless the request errored) */
        if (!err && reply)
                err = rd_kafka_share_fetch_reply_handle(rkb, reply, request);

        rd_kafka_broker_session_update(rkb);

        /* TODO: Check if this error handling is required here or in the main thread. */
        if (unlikely(err)) {
                char tmp[128];

                rd_rkb_dbg(rkb, MSG, "FETCH", "Fetch reply: %s",
                           rd_kafka_err2str(err));
                switch (err) {
                case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
                case RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE:
                case RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION:
                case RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE:
                case RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE:
                case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_ID:
                        /* Request metadata information update */
                        rd_snprintf(tmp, sizeof(tmp), "FetchRequest failed: %s",
                                    rd_kafka_err2str(err));
                        rd_kafka_metadata_refresh_known_topics(
                            rkb->rkb_rk, NULL, rd_true /*force*/, tmp);
                        /* FALLTHRU */

                case RD_KAFKA_RESP_ERR__TRANSPORT:
                case RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT:
                case RD_KAFKA_RESP_ERR__MSG_TIMED_OUT:
                        /* The fetch is already intervalled from
                         * consumer_serve() so dont retry. */
                        break;

                default:
                        break;
                }

                /*
                 * TODO KIP-932: Check if this needed or not. If yes, check the working.
                 */
                rd_kafka_broker_fetch_backoff(rkb, err);
                /* FALLTHRU */
        }

        if (rko_orig)
                rd_kafka_op_reply(rko_orig, err);
        rkb->rkb_fetching = 0;
}

/**
 * @broker FetchResponse handling.
 *
 * @locality broker thread  (or any thread if err == __DESTROY).
 */
static void rd_kafka_broker_fetch_reply(rd_kafka_t *rk,
                                        rd_kafka_broker_t *rkb,
                                        rd_kafka_resp_err_t err,
                                        rd_kafka_buf_t *reply,
                                        rd_kafka_buf_t *request,
                                        void *opaque) {

        if (err == RD_KAFKA_RESP_ERR__DESTROY)
                return; /* Terminating */

        rd_kafka_assert(rkb->rkb_rk, rkb->rkb_fetching > 0);
        rkb->rkb_fetching = 0;

        /* Parse and handle the messages (unless the request errored) */
        if (!err && reply)
                err = rd_kafka_fetch_reply_handle(rkb, reply, request);

        if (unlikely(err)) {
                char tmp[128];

                rd_rkb_dbg(rkb, MSG, "FETCH", "Fetch reply: %s",
                           rd_kafka_err2str(err));
                switch (err) {
                case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
                case RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE:
                case RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION:
                case RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE:
                case RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE:
                case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_ID:
                        /* Request metadata information update */
                        rd_snprintf(tmp, sizeof(tmp), "FetchRequest failed: %s",
                                    rd_kafka_err2str(err));
                        rd_kafka_metadata_refresh_known_topics(
                            rkb->rkb_rk, NULL, rd_true /*force*/, tmp);
                        /* FALLTHRU */

                case RD_KAFKA_RESP_ERR__TRANSPORT:
                case RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT:
                case RD_KAFKA_RESP_ERR__MSG_TIMED_OUT:
                        /* The fetch is already intervalled from
                         * consumer_serve() so dont retry. */
                        break;

                default:
                        break;
                }

                rd_kafka_broker_fetch_backoff(rkb, err);
                /* FALLTHRU */
        }
}

/**
 * @brief Check if any toppars have a zero topic id.
 *
 */
static rd_bool_t can_use_topic_ids(rd_kafka_broker_t *rkb) {
        rd_kafka_toppar_t *rktp = rkb->rkb_active_toppar_next;
        do {
                if (RD_KAFKA_UUID_IS_ZERO(rktp->rktp_rkt->rkt_topic_id))
                        return rd_false;
        } while ((rktp = CIRCLEQ_LOOP_NEXT(&rkb->rkb_active_toppars, rktp,
                                           rktp_activelink)) !=
                 rkb->rkb_active_toppar_next);

        return rd_true;
}


void rd_kafka_ShareFetchRequest(
    rd_kafka_broker_t *rkb,
    const rd_kafkap_str_t *group_id,
    const rd_kafkap_str_t *member_id,
    int32_t share_session_epoch,
    int32_t wait_max_ms,
    int32_t min_bytes,
    int32_t max_bytes,
    int32_t max_records,
    int32_t batch_size,
    rd_list_t *toppars_to_send,
    rd_list_t *toppars_to_forget,
    rd_bool_t is_leave_request,
    rd_kafka_op_t *rko_orig,
    rd_ts_t now) {
        rd_kafka_toppar_t *rktp;
        rd_kafka_buf_t *rkbuf;
        int cnt                     = 0;
        size_t of_TopicArrayCnt     = 0;
        int TopicArrayCnt           = 0;
        size_t of_PartitionArrayCnt = 0;
        int PartitionArrayCnt       = 0;
        rd_kafka_topic_t *rkt_last  = NULL;
        int16_t ApiVersion          = 0;
        size_t rkbuf_size           = 0;
        int toppars_to_send_cnt    = toppars_to_send ? rd_list_cnt(toppars_to_send) : 0;
        int i;
        size_t j;
        rd_bool_t has_acknowledgements_or_topics_to_add = toppars_to_send && rd_list_cnt(toppars_to_send) > 0 ? rd_true : rd_false;
        rd_bool_t has_toppars_to_forget = toppars_to_forget && rd_list_cnt(toppars_to_forget) > 0 ? rd_true : rd_false;
        rd_bool_t is_fetching_messages = max_records > 0 ? rd_true : rd_false;

        rd_kafka_dbg(rkb->rkb_rk, FETCH, "SHAREFETCH", "toppars_to_send_cnt=%d, has_acknowledgements_or_topics_to_add=%d, has_toppars_to_forget=%d, is_fetching_messages=%d",
               toppars_to_send_cnt, has_acknowledgements_or_topics_to_add, has_toppars_to_forget, is_fetching_messages);
        /*
         * Only sending 1 aknowledgement for each partition. StartOffset + LastOffset + AcknowledgementType (ACCEPT for now).
         * TODO KIP-932: Change this to accommodate explicit acknowledgements.
         */
        size_t acknowledgement_size = 8 + 8 + 1;

        /* Calculate buffer size */
        if (group_id)
                rkbuf_size += RD_KAFKAP_STR_SIZE(group_id);
        if (member_id)
                rkbuf_size += RD_KAFKAP_STR_SIZE(member_id);
        /* ShareSessionEpoch + WaitMaxMs + MinBytes + MaxBytes + MaxRecords + BatchSize + TopicArrayCnt*/
        rkbuf_size += 4 + 4 + 4 + 4 + 4 + 4 + 4;
        /* N x (topic id + partition id + acknowledgement) */
        rkbuf_size += (toppars_to_send_cnt * (32 + 4 + acknowledgement_size));
        if( has_toppars_to_forget) {
            /* M x (topic id + partition id) */
            rkbuf_size += (rd_list_cnt(toppars_to_forget) * (32 + 4));
        }

        ApiVersion = rd_kafka_broker_ApiVersion_supported(rkb, RD_KAFKAP_ShareFetch,
                                                          1, 1, NULL);

        rkbuf = rd_kafka_buf_new_flexver_request(rkb, RD_KAFKAP_ShareFetch, 1,
                                                 rkbuf_size,
                                                 rd_true);

        if (rkb->rkb_features & RD_KAFKA_FEATURE_MSGVER2)
                rd_kafka_buf_ApiVersion_set(rkbuf, ApiVersion,
                                            RD_KAFKA_FEATURE_MSGVER2);
        else if (rkb->rkb_features & RD_KAFKA_FEATURE_MSGVER1)
                rd_kafka_buf_ApiVersion_set(rkbuf, ApiVersion,
                                            RD_KAFKA_FEATURE_MSGVER1);
        else if (rkb->rkb_features & RD_KAFKA_FEATURE_THROTTLETIME)
                rd_kafka_buf_ApiVersion_set(rkbuf, ApiVersion,
                                            RD_KAFKA_FEATURE_THROTTLETIME);

        /* GroupId */
        rd_kafka_buf_write_kstr(rkbuf, group_id);

        /* MemberId */
        rd_kafka_buf_write_kstr(rkbuf, member_id);

        // printf(" --------------------------------------- rd_kafka_ShareFetchRequest: member_id=%.*s\n",
        //        RD_KAFKAP_STR_PR(member_id));

        /* ShareSessionEpoch */
        rd_kafka_buf_write_i32(rkbuf, share_session_epoch);

        /* WaitMaxMs */
        rd_kafka_buf_write_i32(rkbuf, wait_max_ms);

        /* MinBytes */
        rd_kafka_buf_write_i32(rkbuf, min_bytes);

        /* MaxBytes */
        rd_kafka_buf_write_i32(rkbuf, max_bytes);

        /* MaxRecords */
        rd_kafka_buf_write_i32(rkbuf, max_records);

        /* BatchSize */
        rd_kafka_buf_write_i32(rkbuf, batch_size);
        
        /* Write zero TopicArrayCnt but store pointer for later update */
        of_TopicArrayCnt = rd_kafka_buf_write_arraycnt_pos(rkbuf);

        RD_LIST_FOREACH(rktp, toppars_to_send, i) {

                /* TODO KIP-932: This condition will cause partitions of same topics
                   to be inside single instance of the topic as toppars_to_send is not
                   sorted. Eg: T1 0, T1 1, T2 0, T1 3, T1 5, T2 1  will translate to 
                   T1 (0,1), T2 (0), T1 (3, 5), T2 (1) instead it should be
                   T1 (0,1,3,5) T2(0,1) Fix this. */
                if (rkt_last != rktp->rktp_rkt) {
                        if (rkt_last != NULL) {
                                /* Update PartitionArrayCnt */
                                rd_kafka_buf_finalize_arraycnt(
                                rkbuf, of_PartitionArrayCnt,
                                PartitionArrayCnt);
                                /* Topic tags */
                                rd_kafka_buf_write_tags_empty(rkbuf);
                        }

                        rd_kafka_topic_rdlock(rktp->rktp_rkt);
                        /* Topic ID */
                        rd_kafka_buf_write_uuid(
                        rkbuf, &rktp->rktp_rkt->rkt_topic_id);
                        rd_kafka_topic_rdunlock(rktp->rktp_rkt);
                        
                        TopicArrayCnt++;
                        rkt_last = rktp->rktp_rkt;
                        /* Partition count */
                        of_PartitionArrayCnt =
                        rd_kafka_buf_write_arraycnt_pos(rkbuf);
                        PartitionArrayCnt = 0;
                }

                PartitionArrayCnt++;

                /* Partition */
                rd_kafka_buf_write_i32(rkbuf, rktp->rktp_partition);

                // printf(" ------------------------------------------------------------------ AcknowledgementBatches for topic %.*s [%" PRId32 "] : first_offset=%" PRId64 ", last_offset=%" PRId64 "\n",
                //         RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                //         rktp->rktp_partition,
                //         rktp->rktp_share_acknowledge.first_offset,
                //         rktp->rktp_share_acknowledge.last_offset);
                /* AcknowledgementBatches */
                if (rktp->rktp_share_acknowledge_count > 0) {
                        rd_rkb_dbg(rkb, FETCH, "SHAREFETCH", "rd_kafka_ShareFetchRequest: topic %.*s [%" PRId32 "] : sending %ld acknowledgements",
                                        RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                                        rktp->rktp_partition,
                                        rktp->rktp_share_acknowledge_count);
                        /* For now we only support ACCEPT */
                        rd_kafka_buf_write_arraycnt(rkbuf, rktp->rktp_share_acknowledge_count); /* ArrayCnt = 1 */
                        for(j = 0; j < rktp->rktp_share_acknowledge_count; j++) {
                                /* FirstOffset */
                                rd_kafka_buf_write_i64(rkbuf, rktp->rktp_share_acknowledge[j].first_offset);
                                /* LastOffset */
                                rd_kafka_buf_write_i64(rkbuf, rktp->rktp_share_acknowledge[j].last_offset);
                                /* AcknowledgementType */
                                rd_kafka_buf_write_arraycnt(rkbuf, 1); /* ArrayCnt = 1 */
                                rd_kafka_buf_write_i8(rkbuf, 1); /* ACCEPT */
                                /* Acknowledgement tags */
                                rd_kafka_buf_write_tags_empty(rkbuf);
                        }
                        rktp->rktp_share_acknowledge_count = 0;
                        rd_free(rktp->rktp_share_acknowledge);
                        rktp->rktp_share_acknowledge = NULL;
                } else {
                        rd_rkb_dbg(rkb, FETCH, "SHAREFETCH", "rd_kafka_ShareFetchRequest: topic %.*s [%" PRId32 "] : No acknowledgements to send",
                                RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                                rktp->rktp_partition);
                        /* No acknowledgements */
                        rd_kafka_buf_write_arraycnt(rkbuf, 0);
                }

                /* Partition tags */
                rd_kafka_buf_write_tags_empty(rkbuf);

                rd_rkb_dbg(rkb, FETCH, "SHAREFETCH",
                        "Share Fetch topic %.*s [%" PRId32 "]",
                        RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                        rktp->rktp_partition);

                cnt++;
        }

        rd_kafka_dbg(rkb->rkb_rk, FETCH, "SHAREFETCH",
                   "Share Fetch Request with %d toppars on %d topics",
                   cnt, TopicArrayCnt);

        if (rkt_last != NULL) {
                /* Update last topic's PartitionArrayCnt */
                rd_kafka_buf_finalize_arraycnt(rkbuf, of_PartitionArrayCnt,
                                               PartitionArrayCnt);
                /* Topic tags */
                rd_kafka_buf_write_tags_empty(rkbuf);
        }

        /* Update TopicArrayCnt */
        rd_kafka_buf_finalize_arraycnt(rkbuf, of_TopicArrayCnt, TopicArrayCnt);

        if(toppars_to_send) {
                rd_list_destroy(toppars_to_send);
        }

        /*
         * TODO KIP-932: Move this to the caller.
         */
        if(is_leave_request || has_acknowledgements_or_topics_to_add || has_toppars_to_forget || is_fetching_messages) {
                rd_kafka_dbg(rkb->rkb_rk, FETCH, "SHAREFETCH",
                           "Share Fetch Request sent with%s%s%s",
                           has_acknowledgements_or_topics_to_add ? " acknowledgements," : "",
                           has_toppars_to_forget ? " forgotten toppars," : "",
                           is_fetching_messages ? " fetching messages" : "");
        } else {
                rd_kafka_buf_destroy(rkbuf);
                rd_kafka_dbg(rkb->rkb_rk, FETCH, "SHAREFETCH",
                           "Share Fetch Request not sent since there are no "
                           "acknowledgements, forgotten toppars or messages to fetch");
                rd_kafka_op_reply(rko_orig, RD_KAFKA_RESP_ERR__NOOP);
                return;
        }

        if (has_toppars_to_forget) {
                TopicArrayCnt = 0;
                PartitionArrayCnt = 0;
                rkt_last = NULL;
                /* Write zero TopicArrayCnt but store pointer for later update */
                of_TopicArrayCnt = rd_kafka_buf_write_arraycnt_pos(rkbuf);
                rd_kafka_dbg(rkb->rkb_rk, FETCH, "SHAREFETCH",
                           "Forgetting %d toppars", rd_list_cnt(toppars_to_forget));
                RD_LIST_FOREACH(rktp, toppars_to_forget, i) {
                        /* TODO KIP-932: This condition will cause partitions of same topics
                        to be inside single instance of the topic as toppars_to_send is not
                        sorted. Eg: T1 0, T1 1, T2 0, T1 3, T1 5, T2 1  will translate to 
                        T1 (0,1), T2 (0), T1 (3, 5), T2 (1) instead it should be
                        T1 (0,1,3,5) T2(0,1) Fix this. */
                        if (rkt_last != rktp->rktp_rkt) {
                                if (rkt_last != NULL) {
                                        /* Update PartitionArrayCnt */
                                        rd_kafka_buf_finalize_arraycnt(
                                        rkbuf, of_PartitionArrayCnt,
                                        PartitionArrayCnt);
                                        /* Topic tags */
                                        rd_kafka_buf_write_tags_empty(rkbuf);
                                }

                                rd_kafka_topic_rdlock(rktp->rktp_rkt);
                                /* Topic ID */
                                rd_kafka_buf_write_uuid(
                                rkbuf, &rktp->rktp_rkt->rkt_topic_id);
                                rd_kafka_topic_rdunlock(rktp->rktp_rkt);
                                
                                TopicArrayCnt++;
                                rkt_last = rktp->rktp_rkt;
                                /* Partition count */
                                of_PartitionArrayCnt =
                                rd_kafka_buf_write_arraycnt_pos(rkbuf);
                                PartitionArrayCnt = 0;
                        }

                        PartitionArrayCnt++;

                        /* Partition */
                        rd_kafka_buf_write_i32(rkbuf, rktp->rktp_partition);

                        rd_rkb_dbg(rkb, FETCH, "SHAREFETCH",
                                "Forgetting Fetch partition %.*s [%" PRId32 "]",
                                RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                                rktp->rktp_partition);

                }
                if (rkt_last != NULL) {
                        /* Update last topic's PartitionArrayCnt */
                        rd_kafka_buf_finalize_arraycnt(rkbuf, of_PartitionArrayCnt,
                                                PartitionArrayCnt);
                        /* Topic tags */
                        rd_kafka_buf_write_tags_empty(rkbuf);
                }
                /* Update TopicArrayCnt */
                rd_kafka_buf_finalize_arraycnt(rkbuf, of_TopicArrayCnt, TopicArrayCnt);
        } else {
                /* ForgottenToppars */
                rd_kafka_buf_write_arraycnt(rkbuf, 0);
        }

        /* Consider Fetch requests blocking if fetch.wait.max.ms >= 1s */
        if (rkb->rkb_rk->rk_conf.fetch_wait_max_ms >= 1000)
                rkbuf->rkbuf_flags |= RD_KAFKA_OP_F_BLOCKING;

        /* Use configured timeout */
        rd_kafka_buf_set_timeout(rkbuf,
                                 rkb->rkb_rk->rk_conf.socket_timeout_ms +
                                     rkb->rkb_rk->rk_conf.fetch_wait_max_ms,
                                 now);

        rkb->rkb_fetching = 1;
        rd_kafka_dbg(rkb->rkb_rk, MSG, "FETCH",
                   "Issuing ShareFetch request (max wait %dms, min %d bytes, "
                   "max %d bytes, max %d records) with %d toppars to broker %s "
                   "(id %" PRId32 ")",
                   wait_max_ms, min_bytes, max_bytes, max_records, cnt,
                   rkb->rkb_name, rkb->rkb_nodeid);
        rd_kafka_broker_buf_enq1(rkb, rkbuf, rd_kafka_broker_share_fetch_reply, rko_orig);

        return;
}

static rd_list_t *rd_kafka_broker_share_fetch_get_toppars_to_send_on_leave(rd_kafka_broker_t *rkb) {
        /* TODO KIP-932: Implement this properly. Remaining acknowledgements should be sent */

        // TAILQ_FOREACH(rktp, &rkb->rkb_share_fetch_session.toppars_in_session, rktp_rkblink) {
        //         if (rktp->rktp_share_acknowledge.first_offset >= 0) {
        //                 rd_list_add(toppars_to_send, rktp);
        //         }
        // }

        return rd_list_new(0, NULL);
}

static rd_list_t *rd_kafka_broker_share_fetch_get_toppars_to_send(rd_kafka_broker_t *rkb) {
        /* TODO KIP-932: Improve this allocation with Acknowledgement implementation */
        int adding_toppar_cnt = rkb->rkb_share_fetch_session.toppars_to_add ? rd_list_cnt(rkb->rkb_share_fetch_session.toppars_to_add) : 0;
        int initial_toppars_to_send_cnt = rkb->rkb_toppar_cnt + adding_toppar_cnt;
        rd_list_t *toppars_to_send = rd_list_new(initial_toppars_to_send_cnt, NULL);
        rd_kafka_toppar_t *rktp;
        int i;

        TAILQ_FOREACH(rktp, &rkb->rkb_share_fetch_session.toppars_in_session, rktp_rkb_session_link) {
                rd_rkb_dbg(rkb, FETCH, "SHAREFETCH", "rd_kafka_broker_share_fetch_get_toppars_to_send: checking toppar topic %.*s [%" PRId32 "] with %ld acknowledgements",
                       RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                       rktp->rktp_partition,
                       rktp->rktp_share_acknowledge_count);
                if (rktp->rktp_share_acknowledge_count > 0) {
                        rd_rkb_dbg(rkb, FETCH, "SHAREFETCH", "rd_kafka_broker_share_fetch_get_toppars_to_send: adding to toppars_to_send topic %.*s [%" PRId32 "] with %ld acknowledgements",
                               RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                               rktp->rktp_partition,
                               rktp->rktp_share_acknowledge_count);
                        rd_list_add(toppars_to_send, rktp);
                } else {
                        rd_rkb_dbg(rkb, FETCH, "SHAREFETCH", "rd_kafka_broker_share_fetch_get_toppars_to_send: not adding to toppars_to_send topic %.*s [%" PRId32 "] since it has no acknowledgements",
                               RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                               rktp->rktp_partition);
                }
        }

        if (rkb->rkb_share_fetch_session.toppars_to_add) {
                RD_LIST_FOREACH(rktp, rkb->rkb_share_fetch_session.toppars_to_add, i) {
                        rd_rkb_dbg(rkb, FETCH, "SHAREFETCH", "rd_kafka_broker_share_fetch_get_toppars_to_send: adding topic %.*s [%" PRId32 "] to the session",
                                   RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                                   rktp->rktp_partition);
                        rd_list_add(toppars_to_send, rktp);
                }
        }

        return toppars_to_send;
}

void rd_kafka_broker_share_fetch_session_clear(rd_kafka_broker_t *rkb) {
        rd_kafka_toppar_t *rktp, *tmp_rktp;

        rkb->rkb_share_fetch_session.epoch = -1;

        /* Clear toppars in session */
        TAILQ_FOREACH_SAFE(rktp, &rkb->rkb_share_fetch_session.toppars_in_session, rktp_rkb_session_link, tmp_rktp) {
                TAILQ_REMOVE(&rkb->rkb_share_fetch_session.toppars_in_session, rktp, rktp_rkb_session_link);
                if(rktp->rktp_share_acknowledge) {
                        rd_free(rktp->rktp_share_acknowledge);
                        rktp->rktp_share_acknowledge = NULL;
                        rktp->rktp_share_acknowledge_count = 0;
                }
                rd_rkb_dbg(rkb, BROKER, "SHAREFETCH",
                                "%s [%" PRId32
                                "]: removed from ShareFetch session on clear",
                                rktp->rktp_rkt->rkt_topic->str,
                                rktp->rktp_partition);
                rd_kafka_toppar_destroy(rktp); // from session list
        }
        rkb->rkb_share_fetch_session.toppars_in_session_cnt = 0;

        /* Clear toppars to add */
        if(rkb->rkb_share_fetch_session.toppars_to_add) {
                rd_rkb_dbg(rkb, BROKER, "SHAREFETCH",
                                "Clearing %d toppars to add from ShareFetch session on clear",
                                rd_list_cnt(rkb->rkb_share_fetch_session.toppars_to_add));
                rd_list_destroy(rkb->rkb_share_fetch_session.toppars_to_add);
                rkb->rkb_share_fetch_session.toppars_to_add = NULL;
        }

        /* Clear toppars to forget */
        if(rkb->rkb_share_fetch_session.toppars_to_forget) {
                rd_rkb_dbg(rkb, BROKER, "SHAREFETCH",
                                "Clearing %d toppars to forget from ShareFetch session on clear",
                                rd_list_cnt(rkb->rkb_share_fetch_session.toppars_to_forget));
                rd_list_destroy(rkb->rkb_share_fetch_session.toppars_to_forget);
                rkb->rkb_share_fetch_session.toppars_to_forget = NULL;
        }

        /* Clear adding toppars */
        if(rkb->rkb_share_fetch_session.adding_toppars) {
                rd_rkb_dbg(rkb, BROKER, "SHAREFETCH",
                                "Clearing %d adding toppars from ShareFetch session on clear",
                                rd_list_cnt(rkb->rkb_share_fetch_session.adding_toppars));
                rd_list_destroy(rkb->rkb_share_fetch_session.adding_toppars);
                rkb->rkb_share_fetch_session.adding_toppars = NULL;
        }

        /* Clear forgetting toppars */
        if(rkb->rkb_share_fetch_session.forgetting_toppars) {
                rd_rkb_dbg(rkb, BROKER, "SHAREFETCH",
                                "Clearing %d forgetting toppars from ShareFetch session on clear",
                                rd_list_cnt(rkb->rkb_share_fetch_session.forgetting_toppars));
                rd_list_destroy(rkb->rkb_share_fetch_session.forgetting_toppars);
                rkb->rkb_share_fetch_session.forgetting_toppars = NULL;
        }
}

void rd_kafka_broker_share_fetch_leave(rd_kafka_broker_t *rkb, rd_kafka_op_t *rko_orig, rd_ts_t now) {
        rd_kafka_cgrp_t *rkcg = rkb->rkb_rk->rk_cgrp;
         rd_kafka_ShareFetchRequest(
            rkb,
            rkcg->rkcg_group_id, /* group_id */
            rkcg->rkcg_member_id, /* member_id */
            rkb->rkb_share_fetch_session.epoch,   /* share_session_epoch */
            rkb->rkb_rk->rk_conf.fetch_wait_max_ms,
            rkb->rkb_rk->rk_conf.fetch_min_bytes,
            rkb->rkb_rk->rk_conf.fetch_max_bytes,
            0,
            0,
            rd_kafka_broker_share_fetch_get_toppars_to_send_on_leave(rkb), /* toppars to send */
            NULL,    /* forgetting toppars */
            rd_true, /* leave request */
            rko_orig, /* rko */
            now);
        rd_kafka_broker_share_fetch_session_clear(rkb);
}

void rd_kafka_broker_share_fetch(rd_kafka_broker_t *rkb, rd_kafka_op_t *rko_orig, rd_ts_t now) {

        rd_kafka_cgrp_t *rkcg = rkb->rkb_rk->rk_cgrp;
        int32_t max_records = 0;

        /* TODO KIP-932: Check if needed while closing the consumer.*/
        rd_assert(rkb->rkb_rk->rk_cgrp);

        if(!rkcg->rkcg_member_id) {
                rd_kafka_dbg(rkb->rkb_rk, FETCH, "SHAREFETCH",
                           "Share Fetch requested without member_id");
                rd_kafka_op_reply(rko_orig, RD_KAFKA_RESP_ERR__INVALID_ARG);
                return;
        }

        if(rko_orig->rko_u.share_fetch.should_fetch) {
                max_records = rkb->rkb_rk->rk_conf.share.max_poll_records;
        }

        if(rkb->rkb_share_fetch_session.toppars_to_add)
                rkb->rkb_share_fetch_session.adding_toppars = rd_list_copy(rkb->rkb_share_fetch_session.toppars_to_add, rd_kafka_toppar_list_copy, NULL);
        if(rkb->rkb_share_fetch_session.toppars_to_forget)
                rkb->rkb_share_fetch_session.forgetting_toppars = rd_list_copy(rkb->rkb_share_fetch_session.toppars_to_forget, rd_kafka_toppar_list_copy, NULL);

        rd_kafka_ShareFetchRequest(
            rkb,
            rkcg->rkcg_group_id, /* group_id */
            rkcg->rkcg_member_id, /* member_id */
            rkb->rkb_share_fetch_session.epoch,   /* share_session_epoch */
            rkb->rkb_rk->rk_conf.fetch_wait_max_ms,
            rkb->rkb_rk->rk_conf.fetch_min_bytes,
            rkb->rkb_rk->rk_conf.fetch_max_bytes,
            max_records,
            max_records, /* TODO KIP-932: Check if this is correct for batch size or not */
            rd_kafka_broker_share_fetch_get_toppars_to_send(rkb), /* toppars to send */
            rkb->rkb_share_fetch_session.toppars_to_forget,    /* forgetting toppars */
            rd_false, /* not leave request */
            rko_orig, /* rko */
            now);
}

/**
 * @brief Build and send a Fetch request message for all underflowed toppars
 *        for a specific broker.
 *
 * @returns the number of partitions included in the FetchRequest, if any.
 *
 * @locality broker thread
 */
int rd_kafka_broker_fetch_toppars(rd_kafka_broker_t *rkb, rd_ts_t now) {
        rd_kafka_toppar_t *rktp;
        rd_kafka_buf_t *rkbuf;
        int cnt                     = 0;
        size_t of_TopicArrayCnt     = 0;
        int TopicArrayCnt           = 0;
        size_t of_PartitionArrayCnt = 0;
        int PartitionArrayCnt       = 0;
        rd_kafka_topic_t *rkt_last  = NULL;
        int16_t ApiVersion          = 0;

        /* Create buffer and segments:
         *   1 x ReplicaId MaxWaitTime MinBytes TopicArrayCnt
         *   N x topic name
         *   N x PartitionArrayCnt Partition FetchOffset MaxBytes
         * where N = number of toppars.
         * Since we dont keep track of the number of topics served by
         * this broker, only the partition count, we do a worst-case calc
         * when allocating and assume each partition is on its own topic
         */

        if (unlikely(rkb->rkb_active_toppar_cnt == 0))
                return 0;

        ApiVersion = rd_kafka_broker_ApiVersion_supported(rkb, RD_KAFKAP_Fetch,
                                                          0, 16, NULL);

        /* Fallback to version 12 if topic id is null which can happen if
         * inter.broker.protocol.version is < 2.8 */
        if (ApiVersion > 12 && !can_use_topic_ids(rkb))
                ApiVersion = 12;

        rkbuf = rd_kafka_buf_new_flexver_request(
            rkb, RD_KAFKAP_Fetch, 1,
            /* MaxWaitTime+MinBytes+MaxBytes+IsolationLevel+
             *   SessionId+Epoch+TopicCnt */
            4 + 4 + 4 + 1 + 4 + 4 + 4 +
                /* N x PartCnt+Partition+CurrentLeaderEpoch+FetchOffset+
                 * LastFetchedEpoch+LogStartOffset+MaxBytes+?TopicNameLen?*/
                (rkb->rkb_active_toppar_cnt *
                 (4 + 4 + 4 + 8 + 4 + 8 + 4 + 40)) +
                /* ForgottenTopicsCnt */
                4 +
                /* N x ForgottenTopicsData */
                0,
            ApiVersion >= 12);

        if (rkb->rkb_features & RD_KAFKA_FEATURE_MSGVER2)
                rd_kafka_buf_ApiVersion_set(rkbuf, ApiVersion,
                                            RD_KAFKA_FEATURE_MSGVER2);
        else if (rkb->rkb_features & RD_KAFKA_FEATURE_MSGVER1)
                rd_kafka_buf_ApiVersion_set(rkbuf, ApiVersion,
                                            RD_KAFKA_FEATURE_MSGVER1);
        else if (rkb->rkb_features & RD_KAFKA_FEATURE_THROTTLETIME)
                rd_kafka_buf_ApiVersion_set(rkbuf, ApiVersion,
                                            RD_KAFKA_FEATURE_THROTTLETIME);


        /* FetchRequest header */
        if (rd_kafka_buf_ApiVersion(rkbuf) <= 14)
                /* ReplicaId */
                rd_kafka_buf_write_i32(rkbuf, -1);

        /* MaxWaitTime */
        rd_kafka_buf_write_i32(rkbuf, rkb->rkb_rk->rk_conf.fetch_wait_max_ms);
        /* MinBytes */
        rd_kafka_buf_write_i32(rkbuf, rkb->rkb_rk->rk_conf.fetch_min_bytes);

        if (rd_kafka_buf_ApiVersion(rkbuf) >= 3)
                /* MaxBytes */
                rd_kafka_buf_write_i32(rkbuf,
                                       rkb->rkb_rk->rk_conf.fetch_max_bytes);

        if (rd_kafka_buf_ApiVersion(rkbuf) >= 4)
                /* IsolationLevel */
                rd_kafka_buf_write_i8(rkbuf,
                                      rkb->rkb_rk->rk_conf.isolation_level);

        if (rd_kafka_buf_ApiVersion(rkbuf) >= 7) {
                /* SessionId */
                rd_kafka_buf_write_i32(rkbuf, 0);
                /* Epoch */
                rd_kafka_buf_write_i32(rkbuf, -1);
        }

        /* Write zero TopicArrayCnt but store pointer for later update */
        of_TopicArrayCnt = rd_kafka_buf_write_arraycnt_pos(rkbuf);

        /* Prepare map for storing the fetch version for each partition,
         * this will later be checked in Fetch response to purge outdated
         * responses (e.g., after a seek). */
        rkbuf->rkbuf_rktp_vers =
            rd_list_new(0, (void *)rd_kafka_toppar_ver_destroy);
        rd_list_prealloc_elems(rkbuf->rkbuf_rktp_vers,
                               sizeof(struct rd_kafka_toppar_ver),
                               rkb->rkb_active_toppar_cnt, 0);

        /* Round-robin start of the list. */
        rktp = rkb->rkb_active_toppar_next;
        do {
                struct rd_kafka_toppar_ver *tver;

                if (rkt_last != rktp->rktp_rkt) {
                        if (rkt_last != NULL) {
                                /* Update PartitionArrayCnt */
                                rd_kafka_buf_finalize_arraycnt(
                                    rkbuf, of_PartitionArrayCnt,
                                    PartitionArrayCnt);
                                /* Topic tags */
                                rd_kafka_buf_write_tags_empty(rkbuf);
                        }

                        /* TODO: This is not thread safe as topic can
                                 be recreated in which case topic id is
                                 updated from the main thread and we are
                                 sending topic id from broker thread.*/
                        if (rd_kafka_buf_ApiVersion(rkbuf) > 12) {
                                /* Topic id must be non-zero here */
                                rd_dassert(!RD_KAFKA_UUID_IS_ZERO(
                                    rktp->rktp_rkt->rkt_topic_id));
                                /* Topic ID */
                                rd_kafka_buf_write_uuid(
                                    rkbuf, &rktp->rktp_rkt->rkt_topic_id);
                        } else {
                                /* Topic name */
                                rd_kafka_buf_write_kstr(
                                    rkbuf, rktp->rktp_rkt->rkt_topic);
                        }

                        TopicArrayCnt++;
                        rkt_last = rktp->rktp_rkt;
                        /* Partition count */
                        of_PartitionArrayCnt =
                            rd_kafka_buf_write_arraycnt_pos(rkbuf);
                        PartitionArrayCnt = 0;
                }

                PartitionArrayCnt++;

                /* Partition */
                rd_kafka_buf_write_i32(rkbuf, rktp->rktp_partition);

                if (rd_kafka_buf_ApiVersion(rkbuf) >= 9) {
                        /* CurrentLeaderEpoch */
                        if (rktp->rktp_leader_epoch < 0 &&
                            rd_kafka_has_reliable_leader_epochs(rkb)) {
                                /* If current leader epoch is set to -1 and
                                 * the broker has reliable leader epochs,
                                 * send 0 instead, so that epoch is checked
                                 * and optionally metadata is refreshed.
                                 * This can happen if metadata is read initially
                                 * without an existing topic (see
                                 * rd_kafka_topic_metadata_update2).
                                 */
                                rd_kafka_buf_write_i32(rkbuf, 0);
                        } else {
                                rd_kafka_buf_write_i32(rkbuf,
                                                       rktp->rktp_leader_epoch);
                        }
                }
                /* FetchOffset */
                rd_kafka_buf_write_i64(rkbuf,
                                       rktp->rktp_offsets.fetch_pos.offset);
                if (rd_kafka_buf_ApiVersion(rkbuf) >= 12)
                        /* LastFetchedEpoch - only used by follower replica */
                        rd_kafka_buf_write_i32(rkbuf, -1);
                if (rd_kafka_buf_ApiVersion(rkbuf) >= 5)
                        /* LogStartOffset - only used by follower replica */
                        rd_kafka_buf_write_i64(rkbuf, -1);

                /* MaxBytes */
                rd_kafka_buf_write_i32(rkbuf, rktp->rktp_fetch_msg_max_bytes);

                /* Partition tags */
                rd_kafka_buf_write_tags_empty(rkbuf);

                rd_rkb_dbg(rkb, FETCH, "FETCH",
                           "Fetch topic %.*s [%" PRId32 "] at offset %" PRId64
                           " (leader epoch %" PRId32
                           ", current leader epoch %" PRId32 ", v%d)",
                           RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                           rktp->rktp_partition,
                           rktp->rktp_offsets.fetch_pos.offset,
                           rktp->rktp_offsets.fetch_pos.leader_epoch,
                           rktp->rktp_leader_epoch, rktp->rktp_fetch_version);

                /* We must have a valid fetch offset when we get here */
                rd_dassert(rktp->rktp_offsets.fetch_pos.offset >= 0);

                /* Add toppar + op version mapping. */
                tver          = rd_list_add(rkbuf->rkbuf_rktp_vers, NULL);
                tver->rktp    = rd_kafka_toppar_keep(rktp);
                tver->version = rktp->rktp_fetch_version;

                cnt++;
        } while ((rktp = CIRCLEQ_LOOP_NEXT(&rkb->rkb_active_toppars, rktp,
                                           rktp_activelink)) !=
                 rkb->rkb_active_toppar_next);

        /* Update next toppar to fetch in round-robin list. */
        rd_kafka_broker_active_toppar_next(
            rkb, rktp ? CIRCLEQ_LOOP_NEXT(&rkb->rkb_active_toppars, rktp,
                                          rktp_activelink)
                      : NULL);

        rd_rkb_dbg(rkb, FETCH, "FETCH", "Fetch %i/%i/%i toppar(s)", cnt,
                   rkb->rkb_active_toppar_cnt, rkb->rkb_toppar_cnt);
        if (!cnt) {
                rd_kafka_buf_destroy(rkbuf);
                return cnt;
        }

        if (rkt_last != NULL) {
                /* Update last topic's PartitionArrayCnt */
                rd_kafka_buf_finalize_arraycnt(rkbuf, of_PartitionArrayCnt,
                                               PartitionArrayCnt);
                /* Topic tags */
                rd_kafka_buf_write_tags_empty(rkbuf);
        }

        /* Update TopicArrayCnt */
        rd_kafka_buf_finalize_arraycnt(rkbuf, of_TopicArrayCnt, TopicArrayCnt);


        if (rd_kafka_buf_ApiVersion(rkbuf) >= 7)
                /* Length of the ForgottenTopics list (KIP-227). Broker
                 * use only - not used by the consumer. */
                rd_kafka_buf_write_arraycnt(rkbuf, 0);

        if (rd_kafka_buf_ApiVersion(rkbuf) >= 11)
                /* RackId */
                rd_kafka_buf_write_kstr(rkbuf,
                                        rkb->rkb_rk->rk_conf.client_rack);

        /* Consider Fetch requests blocking if fetch.wait.max.ms >= 1s */
        if (rkb->rkb_rk->rk_conf.fetch_wait_max_ms >= 1000)
                rkbuf->rkbuf_flags |= RD_KAFKA_OP_F_BLOCKING;

        /* Use configured timeout */
        rd_kafka_buf_set_timeout(rkbuf,
                                 rkb->rkb_rk->rk_conf.socket_timeout_ms +
                                     rkb->rkb_rk->rk_conf.fetch_wait_max_ms,
                                 now);

        /* Sort toppar versions for quicker lookups in Fetch response. */
        rd_list_sort(rkbuf->rkbuf_rktp_vers, rd_kafka_toppar_ver_cmp);

        rkb->rkb_fetching = 1;
        rd_kafka_broker_buf_enq1(rkb, rkbuf, rd_kafka_broker_fetch_reply, NULL);

        return cnt;
}

/**
 * @brief Decide whether it should start fetching from next fetch start
 *        or continue with current fetch pos.
 *
 * @param rktp the toppar
 *
 * @returns rd_true if it should start fetching from next fetch start,
 *          rd_false otherwise.
 *
 * @locality any
 * @locks toppar_lock() MUST be held
 */
static rd_bool_t rd_kafka_toppar_fetch_decide_start_from_next_fetch_start(
    rd_kafka_toppar_t *rktp) {
        return rktp->rktp_op_version > rktp->rktp_fetch_version ||
               rd_kafka_fetch_pos_cmp(&rktp->rktp_next_fetch_start,
                                      &rktp->rktp_last_next_fetch_start) ||
               rktp->rktp_offsets.fetch_pos.offset == RD_KAFKA_OFFSET_INVALID;
}

/**
 * @brief Return next fetch start position:
 *        if it should start fetching from next fetch start
 *        or continue with current fetch pos.
 *
 * @param rktp The toppar
 *
 * @returns Next fetch start position
 *
 * @locality any
 * @locks toppar_lock() MUST be held
 */
rd_kafka_fetch_pos_t
rd_kafka_toppar_fetch_decide_next_fetch_start_pos(rd_kafka_toppar_t *rktp) {
        if (rd_kafka_toppar_fetch_decide_start_from_next_fetch_start(rktp))
                return rktp->rktp_next_fetch_start;
        else
                return rktp->rktp_offsets.fetch_pos;
}

/**
 * @brief Decide whether this toppar should be on the fetch list or not.
 *
 * Also:
 *  - update toppar's op version (for broker thread's copy)
 *  - finalize statistics (move rktp_offsets to rktp_offsets_fin)
 *
 * @returns the partition's Fetch backoff timestamp, or 0 if no backoff.
 *
 * @locality broker thread
 * @locks none
 */
rd_ts_t rd_kafka_toppar_fetch_decide(rd_kafka_toppar_t *rktp,
                                     rd_kafka_broker_t *rkb,
                                     int force_remove) {
        int should_fetch   = 1;
        const char *reason = "";
        int32_t version;
        rd_ts_t ts_backoff      = 0;
        rd_bool_t lease_expired = rd_false;

        rd_kafka_toppar_lock(rktp);

        /* Check for preferred replica lease expiry */
        lease_expired = rktp->rktp_leader_id != rktp->rktp_broker_id &&
                        rd_interval(&rktp->rktp_lease_intvl,
                                    5 * 60 * 1000 * 1000 /*5 minutes*/, 0) > 0;
        if (lease_expired) {
                /* delegate_to_leader() requires no locks to be held */
                rd_kafka_toppar_unlock(rktp);
                rd_kafka_toppar_delegate_to_leader(rktp);
                rd_kafka_toppar_lock(rktp);

                reason       = "preferred replica lease expired";
                should_fetch = 0;
                goto done;
        }

        /* Forced removal from fetch list */
        if (unlikely(force_remove)) {
                reason       = "forced removal";
                should_fetch = 0;
                goto done;
        }

        if (unlikely((rktp->rktp_flags & RD_KAFKA_TOPPAR_F_REMOVE) != 0)) {
                reason       = "partition removed";
                should_fetch = 0;
                goto done;
        }

        /* Skip toppars not in active fetch state */
        if (rktp->rktp_fetch_state != RD_KAFKA_TOPPAR_FETCH_ACTIVE) {
                reason       = "not in active fetch state";
                should_fetch = 0;
                goto done;
        }

        /* Update broker thread's fetch op version */
        version = rktp->rktp_op_version;
        if (rd_kafka_toppar_fetch_decide_start_from_next_fetch_start(rktp)) {
                /* New version barrier, something was modified from the
                 * control plane. Reset and start over.
                 * Alternatively only the next_offset changed but not the
                 * barrier, which is the case when automatically triggering
                 * offset.reset (such as on PARTITION_EOF or
                 * OFFSET_OUT_OF_RANGE). */

                rd_kafka_dbg(
                    rktp->rktp_rkt->rkt_rk, TOPIC, "FETCHDEC",
                    "Topic %s [%" PRId32
                    "]: fetch decide: "
                    "updating to version %d (was %d) at %s "
                    "(was %s)",
                    rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
                    version, rktp->rktp_fetch_version,
                    rd_kafka_fetch_pos2str(rktp->rktp_next_fetch_start),
                    rd_kafka_fetch_pos2str(rktp->rktp_offsets.fetch_pos));

                rd_kafka_offset_stats_reset(&rktp->rktp_offsets);

                /* New start offset */
                rktp->rktp_offsets.fetch_pos     = rktp->rktp_next_fetch_start;
                rktp->rktp_last_next_fetch_start = rktp->rktp_next_fetch_start;

                rktp->rktp_fetch_version = version;

                /* Clear last error to propagate new fetch
                 * errors if encountered. */
                rktp->rktp_last_error = RD_KAFKA_RESP_ERR_NO_ERROR;

                rd_kafka_q_purge_toppar_version(rktp->rktp_fetchq, rktp,
                                                version);
        }


        if (RD_KAFKA_TOPPAR_IS_PAUSED(rktp)) {
                should_fetch = 0;
                reason       = "paused";

        } else if (RD_KAFKA_OFFSET_IS_LOGICAL(
                       rktp->rktp_next_fetch_start.offset)) {
                should_fetch = 0;
                reason       = "no concrete offset";
        } else if (rktp->rktp_ts_fetch_backoff > rd_clock()) {
                reason       = "fetch backed off";
                ts_backoff   = rktp->rktp_ts_fetch_backoff;
                should_fetch = 0;
        } else if (rd_kafka_q_len(rktp->rktp_fetchq) >=
                   rkb->rkb_rk->rk_conf.queued_min_msgs) {
                /* Skip toppars who's local message queue is already above
                 * the lower threshold. */
                reason     = "queued.min.messages exceeded";
                ts_backoff = rd_kafka_toppar_fetch_backoff(
                    rkb, rktp, RD_KAFKA_RESP_ERR__QUEUE_FULL);
                should_fetch = 0;

        } else if ((int64_t)rd_kafka_q_size(rktp->rktp_fetchq) >=
                   rkb->rkb_rk->rk_conf.queued_max_msg_bytes) {
                reason     = "queued.max.messages.kbytes exceeded";
                ts_backoff = rd_kafka_toppar_fetch_backoff(
                    rkb, rktp, RD_KAFKA_RESP_ERR__QUEUE_FULL);
                should_fetch = 0;
        }

done:
        /* Copy offset stats to finalized place holder. */
        rktp->rktp_offsets_fin = rktp->rktp_offsets;

        if (rktp->rktp_fetch != should_fetch) {
                rd_rkb_dbg(
                    rkb, FETCH, "FETCH",
                    "Topic %s [%" PRId32
                    "] in state %s at %s "
                    "(%d/%d msgs, %" PRId64
                    "/%d kb queued, "
                    "opv %" PRId32 ") is %s%s",
                    rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
                    rd_kafka_fetch_states[rktp->rktp_fetch_state],
                    rd_kafka_fetch_pos2str(rktp->rktp_next_fetch_start),
                    rd_kafka_q_len(rktp->rktp_fetchq),
                    rkb->rkb_rk->rk_conf.queued_min_msgs,
                    rd_kafka_q_size(rktp->rktp_fetchq) / 1024,
                    rkb->rkb_rk->rk_conf.queued_max_msg_kbytes,
                    rktp->rktp_fetch_version,
                    should_fetch ? "fetchable" : "not fetchable: ", reason);

                if (should_fetch) {
                        rd_dassert(rktp->rktp_fetch_version > 0);
                        rd_kafka_broker_active_toppar_add(
                            rkb, rktp, *reason ? reason : "fetchable");
                } else {
                        rd_kafka_broker_active_toppar_del(rkb, rktp, reason);
                }
        }

        rd_kafka_toppar_unlock(rktp);

        /* Non-fetching partitions will have an
         * indefinate backoff, unless explicitly specified. */
        if (!should_fetch && !ts_backoff)
                ts_backoff = RD_TS_MAX;

        return ts_backoff;
}
