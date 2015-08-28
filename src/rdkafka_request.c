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
#include "rdkafka_cgrp.h"
#include "rdkafka_offset.h"


rd_kafka_resp_err_t
rd_kafka_ConsumerMetadataResponse (rd_kafka_broker_t *rkb, rd_kafka_buf_t *rkbuf,
                                   int32_t *coordidp) {
        const int log_decode_errors = 1;
        size_t of = 0;
        int16_t ErrorCode = RD_KAFKA_RESP_ERR_UNKNOWN;
        int32_t CoordId;
        rd_kafkap_str_t *CoordHost;
        int32_t CoordPort;
        struct rd_kafka_metadata_broker mdb = RD_ZERO_INIT;

        _READ_I16(&ErrorCode);
        _READ_I32(&CoordId);
        _READ_STR(CoordHost);
        _READ_I32(&CoordPort);

        printf("CoordId %"PRId32"\n", CoordId);
        if (ErrorCode) {
                rd_rkb_dbg(rkb, CONSUMER, "METADATA",
                           "ConsumerMetadata error: %s", rd_kafka_err2str(ErrorCode));

        } else {
                mdb.id = CoordId;
                mdb.host = RD_KAFKAP_STR_DUPA(CoordHost);
                mdb.port = CoordPort;

                rd_rkb_dbg(rkb, CONSUMER, "METADATA",
                           "ConsumerMetadata: kafka://%s:%i/?id=%"PRId32,
                           mdb.host, mdb.port, mdb.id);
                rd_kafka_broker_update(rkb->rkb_rk, &mdb);
        }

        *coordidp = CoordId;
        return 0;
err:
        return ErrorCode;
}


rd_kafka_buf_t *rd_kafka_ConsumerMetadataRequest (rd_kafka_t *rk,
                                                  const rd_kafkap_str_t *cgrp) {
        rd_kafka_buf_t *rkbuf;

        rkbuf = rd_kafka_buf_new(1, RD_KAFKAP_STR_SIZE(cgrp));
        rd_kafka_buf_write_kstr(rkbuf, cgrp);
        rd_kafka_buf_autopush(rkbuf);

        return rkbuf;
}
#if 0
static void route_reply (rd_kafka_broker_t *rkb,
                         rd_kafka_resp_err_t err,
                         rd_kafka_buf_t *reply,
                         rd_kafka_buf_t *request,
                         void *opaque) {
        rd_kafka_q_t *replyq = opaque;
        rd_kafka_op_t *rko;

        rko = rd_kafka_op_new(RD_KAFKA_OP_REPLY);

        rko->rko_err = err;
        if (reply) {
                rko->rko_rkbuf = reply;
                rd_kafka_buf_keep(reply);
        }

        printf("route reply on %p ref %i\n",
               replyq, rd_atomic32_get(&replyq->rkq_refcnt));
        rd_kafka_q_enq(replyq, rko);
        rd_kafka_q_destroy(replyq);
}
#endif

void rd_kafka_OffsetCommitResponse (rd_kafka_broker_t *rkb, rd_kafka_buf_t *rkbuf) {
        const int log_decode_errors = 1;
        size_t of = 0;
        int32_t TopicCount;
        int i;

        /*
OffsetCommitResponse => [TopicName [Partition ErrorCode]]]
  TopicName => string
  Partition => int32
  ErrorCode => int16
        */

        _READ_I32(&TopicCount);

        for (i = 0 ; i < TopicCount ; i++) {
                rd_kafkap_str_t *Topic;
                int32_t PartitionCount;
                int j;

                _READ_STR(Topic);
                _READ_I32(&PartitionCount);

                for (j = 0 ; j < PartitionCount ; j++) {
                        int32_t Partition;
                        int16_t ErrorCode;
                        _READ_I32(&Partition);
                        _READ_I16(&ErrorCode);

                        printf("OffsetCommitResponse: %.*s[%"PRId32"] error %s\n",
                               RD_KAFKAP_STR_PR(Topic), Partition,
                               rd_kafka_err2str(ErrorCode));
                }
        }
        return;
err:
        return;
}

/**
 * Commit offsets for all toppars handled by this cgrp.
 */
rd_kafka_buf_t *rd_kafka_OffsetCommitRequest (rd_kafka_cgrp_t *rkcg,
                                              int version) {
        rd_kafka_buf_t *rkbuf;
        const rd_kafkap_str_t null_str = RD_KAFKAP_STR_INIT_NULL;
        const rd_kafkap_str_t *metadata = &null_str;
        rd_kafka_topic_t *last_rkt = NULL;
        rd_kafka_offsets_t *rkoff;
        int32_t cgrpgenid = -1; // FIXME
        int64_t timestamp = 0; // FIXME

        rd_kafka_cgrp_lock(rkcg);

        rkbuf = rd_kafka_buf_new_growable(1,
                                          /* Arbitrary */
                                          128 +
                                          (rkcg->rkcg_topic_cnt * 128) +
                                          (rkcg->rkcg_offsets_cnt * 64));

        rd_kafka_buf_version_set(rkbuf, version);

        rd_kafka_buf_write_kstr(rkbuf, rkcg->rkcg_name);
        if (version >= 1) {
                rd_kafka_buf_write_i32(rkbuf, cgrpgenid);
                rd_kafka_buf_write_kstr(rkbuf, rkcg->rkcg_rk->rk_conf.client_id);
        }

        if (version >= 2)
                rd_kafka_buf_write_i64(rkbuf, timestamp); /* RetentionTime */

        rd_kafka_buf_write_i32(rkbuf, rkcg->rkcg_topic_cnt);
        LIST_FOREACH(rkoff, &rkcg->rkcg_offsets, rkoff_link) {
                rd_kafka_topic_t *rkt = rkoff->rkoff_rkt;
                if (rkt != last_rkt) {
                        rd_kafka_buf_write_kstr(rkbuf, rkt->rkt_topic);
                        rd_kafka_buf_write_i32(rkbuf,
                                               rd_atomic32_get(&rkt->rkt_offsets_cnt));
                        last_rkt = rkt;
                }
                rd_kafka_buf_write_i32(rkbuf, rkoff->rkoff_partition);
                rd_kafka_buf_write_i64(rkbuf,
                                       rd_atomic64_get(&rkoff->rkoff_stored));
                if (version == 1)
                        rd_kafka_buf_write_i64(rkbuf, timestamp);
                rd_kafka_buf_write_kstr(rkbuf, metadata);
        }

        rd_kafka_buf_autopush(rkbuf);

        return rkbuf;
}



void rd_kafka_cgrp_OffsetFetchResponse (rd_kafka_buf_t *rkbuf, void *opaque) {
        rd_kafka_broker_t *rkb = rkbuf->rkbuf_rkb;
        rd_kafka_cgrp_t *rkcg = opaque;
        const int log_decode_errors = 1;
        size_t of = 0;
        int32_t TopicCount;
        rd_kafka_resp_err_t err = rkbuf->rkbuf_err;
        int i;

        /*
OffsetFetchResponse => [TopicName [Partition Offset Metadata ErrorCode]]
  TopicName => string
  Partition => int32
  Offset => int64
  Metadata => string
  ErrorCode => int16
        */

        printf("%s: OffsetFetchResp err %s\n", rkb->rkb_name, rd_kafka_err2str(err));
        rd_kafka_cgrp_lock(rkcg);

        if (err)
                goto err;

        _READ_I32(&TopicCount);

        for (i = 0 ; i < TopicCount ; i++) {
                rd_kafkap_str_t *Topic;
                int32_t PartitionCount;
                int j;

                _READ_STR(Topic);
                _READ_I32(&PartitionCount);

                for (j = 0 ; j < PartitionCount ; j++) {
                        int32_t Partition;
                        int64_t Offset;
                        int16_t ErrorCode;
                        rd_kafkap_str_t *Metadata;
                        rd_kafka_offsets_t *rkoff;

                        _READ_I32(&Partition);
                        _READ_I64(&Offset);
                        _READ_STR(Metadata);
                        _READ_I16(&ErrorCode);


                        printf("OffsetFetchResponse: %.*s[%"PRId32"]: Offset %"PRId64
                               ", Metadata \"%.*s\", error %s\n",
                               RD_KAFKAP_STR_PR(Topic), Partition, Offset,
                               RD_KAFKAP_STR_PR(Metadata), rd_kafka_err2str(ErrorCode));

                        rkoff = rd_kafka_cgrp_offsets_find(rkcg,
                                                           Topic, Partition);
                        if (!rkoff)
                                continue;

                        if (ErrorCode) {
                                /* FIXME: What to do here? */
                                rd_kafka_offsets_reset(rkoff, ErrorCode);

                        } else {
                                if (Offset == -1)
                                        rd_kafka_offsets_reset(rkoff, 0);
                                else
                                        rd_kafka_offsets_set_start(rkoff,
                                                                   Offset);
                        }
                }
        }

        /* Serve offsets (since their state might have changed) */
        rd_kafka_cgrp_offsets_serve(rkcg);

        rd_kafka_cgrp_unlock(rkcg);
        return;
err:
        printf("OffsetFetchResponse: Error: %s\n", rd_kafka_err2str(err));
        rd_kafka_cgrp_unlock(rkcg);
        return;
}


/**
 * Fetch offsets for active toppars in cgrp.
 * If 'all' is set then offsets are fetched for all active toppars,
 * else just for those we dont currently have an offset for.
 *
 * NOTE: lock(rkcg) MUST be held.
 */
rd_kafka_buf_t *rd_kafka_OffsetFetchRequest (rd_kafka_cgrp_t *rkcg, int version,
                                             int all) {
        rd_kafka_buf_t *rkbuf;
        rd_kafka_offsets_t *rkoff;
        rd_kafka_topic_t *last_rkt = NULL;
        int topic_cnt_of = -1, part_cnt_of = -1;
        int topic_cnt = 0, part_cnt = 0;

        rd_kafka_dbg(rkcg->rkcg_rk, CONSUMER, "OFFETCH",
                     "Consumer group \"%.*s\": OffsetFetch (v%d, all=%d) for %d toppars",
                     RD_KAFKAP_STR_PR(rkcg->rkcg_name),
                     version, all, rkcg->rkcg_offsets_cnt);

        rkbuf = rd_kafka_buf_new_growable(1,
                                          /* Arbitrary */
                                          RD_KAFKAP_STR_SIZE(rkcg->rkcg_name) +
                                          (rkcg->rkcg_topic_cnt * 64) +
                                          (rkcg->rkcg_offsets_cnt * 4));
        rd_kafka_buf_version_set(rkbuf, version);

        /* ConsumerGroup */
        rd_kafka_buf_write_kstr(rkbuf, rkcg->rkcg_name);

        /* Topic count (will be updated later) */
        topic_cnt_of = rd_kafka_buf_write_i32(rkbuf, 0);

        LIST_FOREACH(rkoff, &rkcg->rkcg_offsets, rkoff_link) {
                if (!all && rd_atomic64_get(&rkoff->rkoff_committed) != -1) {
                        rd_kafka_dbg(rkcg->rkcg_rk, CONSUMER, "OFFETCH",
                                     "Consumer group \"%.*s\": "
                                     "not fetching offset for "
                                     "%.*s [%"PRId32"]: already active "
                                     "(committed offset %"PRId64")",
                                     RD_KAFKAP_STR_PR(rkcg->rkcg_name),
                                     RD_KAFKAP_STR_PR(rkoff->rkoff_rkt->rkt_topic),
                                     rkoff->rkoff_partition,
                                     rd_atomic64_get(&rkoff->rkoff_committed));
                        continue;
                }

                if (last_rkt != rkoff->rkoff_rkt) {
                        if (part_cnt_of != -1) {
                                /* Update latest Partition count */
                                rd_kafka_buf_update_i32(rkbuf, part_cnt_of,
                                                        part_cnt);
                                part_cnt = 0;
                        }
                        topic_cnt++;
                        last_rkt = rkoff->rkoff_rkt;

                        /* Topic name */
                        rd_kafka_buf_write_kstr(rkbuf,
                                                rkoff->rkoff_rkt->rkt_topic);

                        /* Partition count (will be updated later) */
                        part_cnt_of = rd_kafka_buf_write_i32(rkbuf, 0);
                }

                /* Partition */
                rd_kafka_buf_write_i32(rkbuf, rkoff->rkoff_partition);
                part_cnt++;
        }

        printf("OffsetFetchRequest: %i topics, %i partitions\n", topic_cnt, part_cnt);
        if (!topic_cnt) {
                /* No toppars to fetch offsets for. */
                rd_kafka_buf_destroy(rkbuf);
                return NULL;
        }

        /* Update latest Partition count */
        if (part_cnt)
                rd_kafka_buf_update_i32(rkbuf, part_cnt_of, part_cnt);

        /* Update Topic count */
        rd_kafka_buf_update_i32(rkbuf, topic_cnt_of, topic_cnt);

        rd_kafka_buf_autopush(rkbuf);

        return rkbuf;
}

rd_kafka_resp_err_t
rd_kafka_consumer_group_join (rd_kafka_topic_t *rkt, const char *group) {
        #if 0
        rd_kafkap_str_t *cgrp;
        rd_kafka_buf_t *rkbuf;
        rd_kafka_broker_t *rkb;
        rd_kafka_q_t *replyq;
        rd_kafka_op_t *rko;
        const int timeout_ms = 3000;
        rd_kafka_t *rk = rkt->rkt_rk;
        int32_t coordid;
        rd_kafka_resp_err_t err;
        int64_t offset;
        int32_t partition = 0;

        /* Query any broker that is up, and if none are up pick the first one,
         * if we're lucky it will be up before the timeout */
        rd_kafka_rdlock(rk);
        if (!(rkb = rd_kafka_broker_any(rk, RD_KAFKA_BROKER_STATE_UP))) {
                rkb = TAILQ_FIRST(&rk->rk_brokers);
                if (rkb)
                        rd_kafka_broker_keep(rkb);
        }
        rd_kafka_rdunlock(rk);

        if (!rkb)
                return RD_KAFKA_RESP_ERR__TRANSPORT;


        cgrp = rkt->rkt_conf.group_id;

        printf("Using group %.*s\n", RD_KAFKAP_STR_PR(cgrp));

        /* Give one refcount to destination, will be decreased when
         * reply is enqueued on replyq.
         * This ensures the replyq stays alive even after we timeout here. */
        replyq = rd_kafka_q_new();
        rd_kafka_q_keep(replyq);
        rd_kafka_q_keep(replyq);



        /* Ask for coordinator */
        rkbuf = rd_kafka_ConsumerMetadataRequest(rkt->rkt_rk, cgrp);

        rkbuf->rkbuf_ts_timeout = rd_clock() + 
		rkb->rkb_rk->rk_conf.socket_timeout_ms * 1000;

        rd_kafka_broker_buf_enq1(rkb, RD_KAFKAP_ConsumerMetadata, rkbuf,
                                 route_reply, replyq);

        /* Wait for reply (or timeout) */
        rko = rd_kafka_q_pop(replyq, timeout_ms, 0);
        printf("rko1: %p, refcnt %i\n", rko, rd_atomic32_get(&replyq->rkq_refcnt));
        /* Timeout */
        if (!rko)
                return RD_KAFKA_RESP_ERR__TIMED_OUT;

        /* Error */
        if (rko->rko_err) {
                rd_kafka_resp_err_t err = rko->rko_err;
                rd_kafka_op_destroy(rko);
                return err;
        }

        err = rd_kafka_ConsumerMetadataResponse(rkb, rko->rko_rkbuf, &coordid);

        rd_kafka_op_destroy(rko);
        if (err)
                return err;

        rd_kafka_broker_destroy(rkb);



        rd_kafka_rdlock(rk);
        rkb = rd_kafka_broker_find_by_nodeid(rk, coordid);
        if (!rkb) {
                printf("Coordinator %"PRId32" not available\n", coordid);
                return RD_KAFKA_RESP_ERR_UNKNOWN;
        }

        offset = rd_jitter(1,500);
        printf("Commit offset %"PRId64"\n", offset);
        rkbuf = rd_kafka_OffsetCommitRequest(rkt, partition, 1, 1, 1, -1,
                                             rd_kafkap_str_new("boink"));
	rkbuf->rkbuf_ts_timeout = rd_clock() + 
		rkb->rkb_rk->rk_conf.socket_timeout_ms * 1000;

        printf("Send Commit to %s\n", rkb->rkb_name);
        rd_kafka_broker_buf_enq1(rkb, RD_KAFKAP_OffsetCommit, rkbuf,
                                 route_reply, replyq);


        /* Wait for reply (or timeout) */
        rko = rd_kafka_q_pop(replyq, timeout_ms, 0);
        printf("rko %p: %s\n", rko,
               rko ? rd_kafka_err2str(rko->rko_err) : "NONE");
        // FIXME rd_kafka_q_destroy(replyq);

        /* Timeout */
        if (!rko)
                return RD_KAFKA_RESP_ERR__TIMED_OUT;

        /* Error */
        if (rko->rko_err) {
                rd_kafka_resp_err_t err = rko->rko_err;
                rd_kafka_op_destroy(rko);
                return err;
        }

        rd_kafka_OffsetCommitResponse(rkb, rko->rko_rkbuf);

        /* Reply */
        rd_kafka_op_destroy(rko);


        /* Fetch offset */
        rkbuf = rd_kafka_OffsetFetchRequest(rkt, partition, 1, cgrp);
	rkbuf->rkbuf_ts_timeout = rd_clock() +
		rkb->rkb_rk->rk_conf.socket_timeout_ms * 1000;

        printf("Send OffsetFetch to %s\n", rkb->rkb_name);
        rd_kafka_broker_buf_enq1(rkb, RD_KAFKAP_OffsetFetch, rkbuf,
                                 route_reply, replyq);

        rd_kafka_broker_destroy(rkb);

        /* Wait for reply (or timeout) */
        rko = rd_kafka_q_pop(replyq, timeout_ms, 0);
        printf("rko fetch %p: %s\n", rko,
               rko ? rd_kafka_err2str(rko->rko_err) : "NONE");
        // FIXME rd_kafka_q_destroy(replyq);

        /* Timeout */
        if (!rko)
                return RD_KAFKA_RESP_ERR__TIMED_OUT;

        /* Error */
        if (rko->rko_err) {
                rd_kafka_resp_err_t err = rko->rko_err;
                rd_kafka_op_destroy(rko);
                return err;
        }

        rd_kafka_OffsetFetchResponse(rkb, rko->rko_rkbuf);

        /* Reply */
        rd_kafka_op_destroy(rko);
#endif
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}
