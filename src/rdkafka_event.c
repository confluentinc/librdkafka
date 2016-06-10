/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2016 Magnus Edenhill
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
#include "rdkafka_event.h"
#include "rd.h"


static const char *rd_kafka_event_names[] = {
	"(NONE)",
	"DeliveryReport",
	"Fetch",
	"Error",
};

rd_kafka_event_type_t rd_kafka_event_type (const rd_kafka_event_t *rkev) {
	return rkev ? rkev->rko_evtype : RD_KAFKA_EVENT_NONE;
}

const char *rd_kafka_event_name (const rd_kafka_event_t *rkev) {
	return rd_kafka_event_names[rkev?rkev->rko_evtype:RD_KAFKA_EVENT_NONE];
}




void rd_kafka_event_destroy (rd_kafka_event_t *rkev) {
	if (unlikely(!rkev))
		return;
	rd_kafka_op_destroy(rkev);
}


/**
 * @brief RD_KAFKA_EVENT_DR
 * @returns the next message from the dr queue
 */
static const rd_kafka_message_t *
rd_kafka_event_dr_message_next (rd_kafka_event_t *rkev) {
	rd_kafka_op_t *rko = rkev;
	rd_kafka_msg_t *rkm;

	/* Delivery report:
	 * call application DR callback for each message. */

	if (unlikely(!(rkm = TAILQ_FIRST(&rko->rko_msgq.rkmq_msgs))))
		return NULL;

	rd_kafka_msgq_deq(&rko->rko_msgq, rkm, 1);

	/* Put rkm on secondary message queue which will be purged later. */
	rd_kafka_msgq_enq(&rko->rko_msgq2, rkm);

	/* Convert private rkm to public rkmessage */
        rko->rko_rkmessage.payload    = rkm->rkm_payload;
        rko->rko_rkmessage.len        = rkm->rkm_len;
        rko->rko_rkmessage.err        = rko->rko_err;
        rko->rko_rkmessage.offset     = rkm->rkm_offset;
        rko->rko_rkmessage.rkt        = rko->rko_rkt;
        rko->rko_rkmessage.partition  = rkm->rkm_partition;
        rko->rko_rkmessage._private   = rkm->rkm_opaque;

	if (rkm->rkm_key && !RD_KAFKAP_BYTES_IS_NULL(rkm->rkm_key)) {
		rko->rko_rkmessage.key = (void *)rkm->rkm_key->data;
		rko->rko_rkmessage.key_len = RD_KAFKAP_BYTES_LEN(rkm->rkm_key);
	} else {
		rko->rko_rkmessage.key = NULL;
		rko->rko_rkmessage.key_len = 0;
	}

	return &rko->rko_rkmessage;
}

const rd_kafka_message_t *rd_kafka_event_message (rd_kafka_event_t *rkev) {
	switch (rkev->rko_evtype)
	{
	case RD_KAFKA_EVENT_DR:
		return rd_kafka_event_dr_message_next(rkev);
	case RD_KAFKA_EVENT_FETCH:
		return rd_kafka_message_get(rkev);
	default:
		return NULL;
	}
}

size_t rd_kafka_event_message_count (rd_kafka_event_t *rkev) {
	switch (rkev->rko_evtype)
	{
	case RD_KAFKA_EVENT_DR:
		return rd_atomic32_get(&rkev->rko_msgq.rkmq_msg_cnt);
	case RD_KAFKA_EVENT_FETCH:
		return 1;
	default:
		return 0;
	}
}


rd_kafka_resp_err_t rd_kafka_event_error (rd_kafka_event_t *rkev) {
	return rkev->rko_err;
}


int rd_kafka_event_log (rd_kafka_event_t *rkev, const char **fac, const char **str,
			int *level) {
	if (unlikely(rkev->rko_evtype != RD_KAFKA_EVENT_LOG))
		return -1;

	if (likely(fac != NULL))
		;//*fac = rkev->rkev_u.log.fac;
	if (likely(str != NULL))
		;//*str = rkev->rkev_u.log.str;
	if (likely(level != NULL))
		;//*level = rkev->rkev_u.log.level;

	return 0;
}


rd_kafka_topic_partition_list_t *
rd_kafka_event_topic_partition_list (rd_kafka_event_t *rkev) {
	if (unlikely(rkev->rko_evtype != RD_KAFKA_EVENT_REBALANCE))
		return NULL;

	return (rd_kafka_topic_partition_list_t *)rkev->rko_payload;
}
