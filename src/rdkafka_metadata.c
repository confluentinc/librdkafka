/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012-2013, Magnus Edenhill
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
#include "rdkafka_topic.h"
#include "rdkafka_broker.h"
#include "rdkafka_request.h"
#include "rdkafka_metadata.h"

#include <string.h>



rd_kafka_resp_err_t
rd_kafka_metadata (rd_kafka_t *rk, int all_topics,
                   rd_kafka_topic_t *only_rkt,
                   const struct rd_kafka_metadata **metadatap,
                   int timeout_ms) {
        rd_kafka_q_t *rkq;
        rd_kafka_broker_t *rkb;
        rd_kafka_op_t *rko;
	rd_ts_t ts_end = rd_timeout_init(timeout_ms);

        /* Query any broker that is up, and if none are up pick the first one,
         * if we're lucky it will be up before the timeout */
	rkb = rd_kafka_broker_any_usable(rk, timeout_ms);
	if (!rkb)
		return RD_KAFKA_RESP_ERR__TRANSPORT;

        rkq = rd_kafka_q_new(rk);

        /* Async: request metadata */
        rd_kafka_broker_metadata_req(rkb, all_topics,
                                     only_rkt ?
                                     rd_kafka_topic_a2i(only_rkt) : NULL,
                                     RD_KAFKA_REPLYQ(rkq, 0),
                                     "application requested");

        rd_kafka_broker_destroy(rkb);

        /* Wait for reply (or timeout) */
        rko = rd_kafka_q_pop(rkq, rd_timeout_remains(ts_end), 0);

        rd_kafka_q_destroy(rkq);

        /* Timeout */
        if (!rko)
                return RD_KAFKA_RESP_ERR__TIMED_OUT;

        /* Error */
        if (rko->rko_err) {
                rd_kafka_resp_err_t err = rko->rko_err;
                rd_kafka_op_destroy(rko);
                return err;
        }

        /* Reply: pass metadata pointer to application who now owns it*/
        rd_kafka_assert(rk, rko->rko_u.metadata.metadata);
        *metadatap = rko->rko_u.metadata.metadata;
        rko->rko_u.metadata.metadata = NULL;
        rd_kafka_op_destroy(rko);

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}


/**
 * Initiate metadata request using any available broker.
 *
 * all_topics - if true, all topics in cluster will be requested, else only
 *              the ones known locally.
 * only_rkt   - only request this specific topic (optional)
 * replyq     - enqueue reply op on this queue (optional)
 * reason     - metadata request reason
 *
 * @returns RD_KAFKA_RESP_ERR__TRANSPORT if no broker is available, else NO_ERROR
 * @locality any thread
 */
rd_kafka_resp_err_t rd_kafka_metadata0 (rd_kafka_t *rk,
					int all_topics,
					rd_kafka_itopic_t *only_rkt,
					rd_kafka_replyq_t replyq,
					const char *reason) {
        rd_kafka_op_t *rko;
	rd_kafka_broker_t *rkb;

	rkb = rd_kafka_broker_any_usable(rk, RD_POLL_NOWAIT);
	if (!rkb)
		return RD_KAFKA_RESP_ERR__TRANSPORT;

        rko = rd_kafka_op_new(RD_KAFKA_OP_METADATA_REQ);
        rko->rko_u.metadata.all_topics = all_topics;
        if (only_rkt)
                rko->rko_u.metadata.rkt = rd_kafka_topic_keep_a(only_rkt);

	rko->rko_replyq = replyq;

	strncpy(rko->rko_u.metadata.reason, reason,
		sizeof(rko->rko_u.metadata.reason)-1);

        rd_kafka_broker_metadata_req_op(rkb, rko);

	rd_kafka_broker_destroy(rkb);

	return RD_KAFKA_RESP_ERR_NO_ERROR;
}


void rd_kafka_metadata_destroy (const struct rd_kafka_metadata *metadata) {
        rd_free((void *)metadata);
}


/**
 * @returns a newly allocated copy of metadata \p src of size \p size
 */
struct rd_kafka_metadata *
rd_kafka_metadata_copy (const struct rd_kafka_metadata *src, size_t size) {
	struct rd_kafka_metadata *md;
	rd_tmpabuf_t tbuf;
	int i;

	/* metadata is stored in one contigious buffer where structs and
	 * and pointed-to fields are layed out in a memory aligned fashion.
	 * rd_tmpabuf_t provides the infrastructure to do this.
	 * Because of this we copy all the structs verbatim but
	 * any pointer fields needs to be copied explicitly to update
	 * the pointer address. */
	rd_tmpabuf_new(&tbuf, size, 1/*assert on fail*/);
	md = rd_tmpabuf_write(&tbuf, src, sizeof(*md));

	rd_tmpabuf_write_str(&tbuf, src->orig_broker_name);


	/* Copy Brokers */
	md->brokers = rd_tmpabuf_write(&tbuf, src->brokers,
				      md->broker_cnt * sizeof(*md->brokers));

	for (i = 0 ; i < md->broker_cnt ; i++)
		md->brokers[i].host =
			rd_tmpabuf_write_str(&tbuf, src->brokers[i].host);


	/* Copy TopicMetadata */
        md->topics = rd_tmpabuf_write(&tbuf, src->topics,
				      md->topic_cnt * sizeof(*md->topics));

	for (i = 0 ; i < md->topic_cnt ; i++) {
		int j;

		md->topics[i].topic = rd_tmpabuf_write_str(&tbuf,
							   src->topics[i].topic);


		/* Copy partitions */
		md->topics[i].partitions =
			rd_tmpabuf_write(&tbuf, src->topics[i].partitions,
					 md->topics[i].partition_cnt *
					 sizeof(*md->topics[i].partitions));

		for (j = 0 ; j < md->topics[i].partition_cnt ; j++) {
			/* Copy replicas and ISRs */
			md->topics[i].partitions[j].replicas =
				rd_tmpabuf_write(&tbuf,
						 src->topics[i].partitions[j].
						 replicas,
						 md->topics[i].partitions[j].
						 replica_cnt *
						 sizeof(*md->topics[i].
							partitions[j].
							replicas));

			md->topics[i].partitions[j].isrs =
				rd_tmpabuf_write(&tbuf,
						 src->topics[i].partitions[j].
						 isrs,
						 md->topics[i].partitions[j].
						 isr_cnt *
						 sizeof(*md->topics[i].
							partitions[j].
							isrs));

		}
	}

	/* Check for tmpabuf errors */
	if (rd_tmpabuf_failed(&tbuf))
		rd_kafka_assert(NULL, !*"metadata copy failed");

	/* Delibarely not destroying the tmpabuf since we return
	 * its allocated memory. */

	return md;
}





/**
 * Handle a Metadata response message.
 * If 'rkt' is non-NULL the metadata originated from a topic-specific request.
 * \p all_topics indicate that the request asked for all topics in the cluster.
 *
 * The metadata will be marshalled into 'struct rd_kafka_metadata*' structs.
 *
 * Returns the marshalled metadata, or NULL on parse error.
 *
 * Locality: rdkafka main thread
 */
struct rd_kafka_metadata *
rd_kafka_parse_Metadata (rd_kafka_broker_t *rkb,
                         rd_kafka_itopic_t *rkt, rd_kafka_buf_t *rkbuf,
			 int all_topics) {
	int i, j, k;
	int req_rkt_seen = 0;
	rd_tmpabuf_t tbuf;
        struct rd_kafka_metadata *md;
        size_t rkb_namelen;
        const int log_decode_errors = 1;

        rd_kafka_broker_lock(rkb);
        rkb_namelen = strlen(rkb->rkb_name)+1;
        /* We assume that the marshalled representation is
         * no more than 4 times larger than the wire representation. */
	rd_tmpabuf_new(&tbuf,
		       sizeof(*md) + rkb_namelen + (rkbuf->rkbuf_len * 4),
		       ENABLE_DEVEL/*assert in DEVEL mode */);

	md = rd_tmpabuf_alloc(&tbuf, sizeof(*md));
        md->orig_broker_id = rkb->rkb_nodeid;
	md->orig_broker_name = rd_tmpabuf_write(&tbuf,
						rkb->rkb_name, rkb_namelen);
        rd_kafka_broker_unlock(rkb);

	/* Read Brokers */
	rd_kafka_buf_read_i32a(rkbuf, md->broker_cnt);
	if (md->broker_cnt > RD_KAFKAP_BROKERS_MAX)
		rd_kafka_buf_parse_fail(rkbuf, "Broker_cnt %i > BROKERS_MAX %i",
					md->broker_cnt, RD_KAFKAP_BROKERS_MAX);

	if (!(md->brokers = rd_tmpabuf_alloc(&tbuf, md->broker_cnt *
					     sizeof(*md->brokers))))
		rd_kafka_buf_parse_fail(rkbuf,
					"%d brokers: tmpabuf memory shortage",
					md->broker_cnt);

	for (i = 0 ; i < md->broker_cnt ; i++) {
                rd_kafka_buf_read_i32a(rkbuf, md->brokers[i].id);
                rd_kafka_buf_read_str_tmpabuf(rkbuf, &tbuf, md->brokers[i].host);
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

	if (!(md->topics = rd_tmpabuf_alloc(&tbuf,
					    md->topic_cnt *
					    sizeof(*md->topics))))
		rd_kafka_buf_parse_fail(rkbuf,
					"%d topics: tmpabuf memory shortage",
					md->topic_cnt);

	for (i = 0 ; i < md->topic_cnt ; i++) {
		rd_kafka_buf_read_i16a(rkbuf, md->topics[i].err);
		rd_kafka_buf_read_str_tmpabuf(rkbuf, &tbuf, md->topics[i].topic);

		/* PartitionMetadata */
                rd_kafka_buf_read_i32a(rkbuf, md->topics[i].partition_cnt);
		if (md->topics[i].partition_cnt > RD_KAFKAP_PARTITIONS_MAX)
			rd_kafka_buf_parse_fail(rkbuf,
						"TopicMetadata[%i]."
						"PartitionMetadata_cnt %i "
						"> PARTITIONS_MAX %i",
						i, md->topics[i].partition_cnt,
						RD_KAFKAP_PARTITIONS_MAX);

		if (!(md->topics[i].partitions =
		      rd_tmpabuf_alloc(&tbuf,
				       md->topics[i].partition_cnt *
				       sizeof(*md->topics[i].partitions))))
			rd_kafka_buf_parse_fail(rkbuf,
						"%s: %d partitions: "
						"tmpabuf memory shortage",
						md->topics[i].topic,
						md->topics[i].partition_cnt);

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

			if (!(md->topics[i].partitions[j].replicas =
			      rd_tmpabuf_alloc(&tbuf,
					       md->topics[i].
					       partitions[j].replica_cnt *
					       sizeof(*md->topics[i].
						      partitions[j].replicas))))
				rd_kafka_buf_parse_fail(
					rkbuf,
					"%s [%"PRId32"]: %d replicas: "
					"tmpabuf memory shortage",
					md->topics[i].topic,
					md->topics[i].partitions[j].id,
					md->topics[i].partitions[j].replica_cnt);


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

			if (!(md->topics[i].partitions[j].isrs =
			      rd_tmpabuf_alloc(&tbuf,
					       md->topics[i].
					       partitions[j].isr_cnt *
					       sizeof(*md->topics[i].
						      partitions[j].isrs))))
				rd_kafka_buf_parse_fail(
					rkbuf,
					"%s [%"PRId32"]: %d isrs: "
					"tmpabuf memory shortage",
					md->topics[i].topic,
					md->topics[i].partitions[j].id,
					md->topics[i].partitions[j].isr_cnt);


                        for (k = 0 ;
                             k < md->topics[i].partitions[j].isr_cnt; k++)
				rd_kafka_buf_read_i32a(rkbuf, md->topics[i].
						       partitions[j].isrs[k]);

		}
	}

        /* Entire Metadata response now parsed without errors:
         * update our internal state according to the response. */

        /* Avoid metadata updates when we're terminating. */
	if (rd_kafka_terminating(rkb->rkb_rk))
                goto done;

	if (md->broker_cnt == 0 && md->topic_cnt == 0) {
		rd_rkb_dbg(rkb, METADATA, "METADATA",
			   "No brokers or topics in metadata: retrying");
		goto err;
	}

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


        rd_kafka_wrlock(rkb->rkb_rk);
        rkb->rkb_rk->rk_ts_metadata = rd_clock();

	if (all_topics) {
		if (rkb->rkb_rk->rk_full_metadata)
			rd_kafka_metadata_destroy(rkb->rkb_rk->rk_full_metadata);
		rkb->rkb_rk->rk_full_metadata =
			rd_kafka_metadata_copy(md, tbuf.of);
		rkb->rkb_rk->rk_ts_full_metadata = rkb->rkb_rk->rk_ts_metadata;
	}
        rd_kafka_wrunlock(rkb->rkb_rk);


done:
        /* This metadata request was triggered by someone wanting
         * the metadata information back as a reply, so send that reply now.
         * In this case we must not rd_free the metadata memory here,
         * the requestee will do.
	 * The tbuf is explicitly not destroyed as we return its memory
	 * to the caller. */
        return md;

err:
	rd_tmpabuf_destroy(&tbuf);
        return NULL;
}


/**
 * @brief Add all topics in \p metadata to \p list (rd_kafka_topic_info_t *)
 *        that matches the topics in \p match
 *
 * @returns the number of topics matched and added to \p list
 */
size_t
rd_kafka_metadata_topic_match (rd_kafka_t *rk,
			       rd_list_t *list,
			       const struct rd_kafka_metadata *metadata,
			       const rd_kafka_topic_partition_list_t *match) {
	int ti;
	size_t cnt = 0;

        /* For each topic in the cluster, scan through the match list
	 * to find matching topic. */
        for (ti = 0 ; ti < metadata->topic_cnt ; ti++) {
		const char *topic = metadata->topics[ti].topic;
                int i;

                /* Ignore topics in blacklist */
                if (rk->rk_conf.topic_blacklist &&
		    rd_kafka_pattern_match(rk->rk_conf.topic_blacklist, topic))
			continue;

                /* Scan for matches */
		for (i = 0 ; i < match->cnt ; i++) {
			if (!rd_kafka_topic_match(rk,
						  match->elems[i].topic, topic))
				continue;

			rd_list_add(list,
				    rd_kafka_topic_info_new(
					    topic,
					    metadata->topics[ti].partition_cnt));
			cnt++;
		}
        }

	return cnt;
}
