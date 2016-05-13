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


/**
 * This is the high level consumer API which is mutually exclusive
 * with the old legacy simple consumer.
 * Only one of these interfaces may be used on a given rd_kafka_t handle.
 */

#include "rdkafka_int.h"
#include "rdkafka_subscription.h"


/**
 * Checks that a high level consumer can be added (no previous simple consumers)
 * Returns 1 if okay, else 0.
 *
 * A rd_kafka_t handle can never migrate from simple to high-level, or
 * vice versa, so we dont need a ..consumer_del().
 */
static RD_INLINE int rd_kafka_high_level_consumer_add (rd_kafka_t *rk) {
        if (rd_atomic32_get(&rk->rk_simple_cnt) > 0)
                return 0;

        rd_atomic32_sub(&rk->rk_simple_cnt, 1);
        return 1;
}


/* FIXME: Remove these */
rd_kafka_resp_err_t rd_kafka_subscribe_rkt (rd_kafka_itopic_t *rkt) {

        rd_kafka_topic_wrlock(rkt);
        /* FIXME: mark for subscription */
        rd_kafka_topic_wrunlock(rkt);

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}


rd_kafka_resp_err_t rd_kafka_unsubscribe (rd_kafka_t *rk) {
        rd_kafka_cgrp_t *rkcg;

        if (!(rkcg = rd_kafka_cgrp_get(rk)))
                return RD_KAFKA_RESP_ERR__UNKNOWN_GROUP;

        return rd_kafka_op_err_destroy(rd_kafka_op_req2(&rkcg->rkcg_ops,
                                                        RD_KAFKA_OP_SUBSCRIBE));
}


rd_kafka_resp_err_t
rd_kafka_subscribe (rd_kafka_t *rk,
                    const rd_kafka_topic_partition_list_t *topics) {

        rd_kafka_op_t *rko;
        rd_kafka_cgrp_t *rkcg;

        if (!(rkcg = rd_kafka_cgrp_get(rk)))
                return RD_KAFKA_RESP_ERR__UNKNOWN_GROUP;

        rko = rd_kafka_op_new(RD_KAFKA_OP_SUBSCRIBE);
        rd_kafka_op_payload_set(rko,
                                rd_kafka_topic_partition_list_copy(topics),
                                (void *)rd_kafka_topic_partition_list_destroy);

        return rd_kafka_op_err_destroy(
                rd_kafka_op_req(&rkcg->rkcg_ops, rko, RD_POLL_INFINITE));
}


rd_kafka_resp_err_t
rd_kafka_assign (rd_kafka_t *rk,
                 const rd_kafka_topic_partition_list_t *partitions) {
        rd_kafka_op_t *rko;
        rd_kafka_cgrp_t *rkcg;

        if (!(rkcg = rd_kafka_cgrp_get(rk)))
                return RD_KAFKA_RESP_ERR__UNKNOWN_GROUP;

        rko = rd_kafka_op_new(RD_KAFKA_OP_ASSIGN);
	if (partitions)
                rd_kafka_op_payload_set(
                        rko,
                        rd_kafka_topic_partition_list_copy(partitions),
			(void *)rd_kafka_topic_partition_list_destroy);

        return rd_kafka_op_err_destroy(
                rd_kafka_op_req(&rkcg->rkcg_ops, rko, RD_POLL_INFINITE));
}



rd_kafka_resp_err_t
rd_kafka_assignment (rd_kafka_t *rk,
                     rd_kafka_topic_partition_list_t **partitions) {
        rd_kafka_op_t *rko;
        rd_kafka_resp_err_t err;
        rd_kafka_cgrp_t *rkcg;

        if (!(rkcg = rd_kafka_cgrp_get(rk)))
                return RD_KAFKA_RESP_ERR__UNKNOWN_GROUP;

        rko = rd_kafka_op_req2(&rkcg->rkcg_ops, RD_KAFKA_OP_GET_ASSIGNMENT);
	if (!rko)
		return RD_KAFKA_RESP_ERR__TIMED_OUT;

        err = rko->rko_err;

        *partitions = rko->rko_payload;
        rko->rko_payload = NULL;
        rd_kafka_op_destroy(rko);

        if (!*partitions && !err) {
                /* Create an empty list for convenience of the caller */
                *partitions = rd_kafka_topic_partition_list_new(0);
        }

        return err;
}

rd_kafka_resp_err_t
rd_kafka_subscription (rd_kafka_t *rk,
                       rd_kafka_topic_partition_list_t **topics){
	rd_kafka_op_t *rko;
        rd_kafka_resp_err_t err;
        rd_kafka_cgrp_t *rkcg;

        if (!(rkcg = rd_kafka_cgrp_get(rk)))
                return RD_KAFKA_RESP_ERR__UNKNOWN_GROUP;

        rko = rd_kafka_op_req2(&rkcg->rkcg_ops, RD_KAFKA_OP_GET_SUBSCRIPTION);
	if (!rko)
		return RD_KAFKA_RESP_ERR__TIMED_OUT;

        err = rko->rko_err;

        *topics = rko->rko_payload;
        rko->rko_payload = NULL;
        rd_kafka_op_destroy(rko);

        if (!*topics && !err) {
                /* Create an empty list for convenience of the caller */
                *topics = rd_kafka_topic_partition_list_new(0);
        }

        return err;
}


rd_kafka_resp_err_t
rd_kafka_pause_partitions (rd_kafka_t *rk,
			   rd_kafka_topic_partition_list_t *partitions) {
	return rd_kafka_toppars_pause_resume(rk, 1, RD_KAFKA_TOPPAR_F_APP_PAUSE,
					     partitions);
}


rd_kafka_resp_err_t
rd_kafka_resume_partitions (rd_kafka_t *rk,
			   rd_kafka_topic_partition_list_t *partitions) {
	return rd_kafka_toppars_pause_resume(rk, 0, RD_KAFKA_TOPPAR_F_APP_PAUSE,
					     partitions);
}

