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
#include "rdkafka_broker.h"
#include "rdkafka_request.h"
#include "rdkafka_topic.h"
#include "rdkafka_partition.h"
#include "rdkafka_assignor.h"
#include "rdkafka_offset.h"
#include "rdkafka_cgrp.h"

#include "trex.h"


static int rd_kafka_cgrp_reassign_broker (rd_kafka_cgrp_t *rkcg);
static void rd_kafka_cgrp_check_unassign_done (rd_kafka_cgrp_t *rkcg);
static void rd_kafka_cgrp_offset_commit_tmr_cb (rd_kafka_timers_t *rkts,
                                                void *arg);


const char *rd_kafka_cgrp_state_names[] = {
        "init",
        "term",
        "query-coord",
        "wait-coord",
        "wait-broker",
        "wait-broker-transport",
        "up"
};

const char *rd_kafka_cgrp_join_state_names[] = {
        "init",
        "wait-join",
        "wait-metadata",
        "wait-sync",
        "wait-unassign",
        "wait-rebalance_cb",
        "assigned"
};


static void rd_kafka_cgrp_set_state (rd_kafka_cgrp_t *rkcg, int state) {
        if ((int)rkcg->rkcg_state == state)
                return;

        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "CGRPSTATE",
                     "Group \"%.*s\" changed state %s -> %s",
                     RD_KAFKAP_STR_PR(rkcg->rkcg_group_id),
                     rd_kafka_cgrp_state_names[rkcg->rkcg_state],
                     rd_kafka_cgrp_state_names[state]);
        rkcg->rkcg_state = state;
        rkcg->rkcg_ts_statechange = rd_clock();
}


void rd_kafka_cgrp_set_join_state (rd_kafka_cgrp_t *rkcg, int join_state){
        if ((int)rkcg->rkcg_join_state == join_state)
                return;

        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "CGRPJOINSTATE",
                     "Group \"%.*s\" changed join state %s -> %s",
                     RD_KAFKAP_STR_PR(rkcg->rkcg_group_id),
                     rd_kafka_cgrp_join_state_names[rkcg->rkcg_join_state],
                     rd_kafka_cgrp_join_state_names[join_state]);
        rkcg->rkcg_join_state = join_state;
}



void rd_kafka_cgrp_destroy_final (rd_kafka_cgrp_t *rkcg) {
        rd_kafka_assert(rkcg->rkcg_rk, !rkcg->rkcg_assignment);
        rd_kafka_assert(rkcg->rkcg_rk, !rkcg->rkcg_subscription);
        rd_kafka_assert(rkcg->rkcg_rk, !rkcg->rkcg_group_leader.members);
        rd_kafka_cgrp_set_member_id(rkcg, NULL);

        rd_kafka_q_destroy(&rkcg->rkcg_q);
        rd_kafka_q_destroy(&rkcg->rkcg_ops);
        rd_kafka_pattern_list_clear(&rkcg->rkcg_whitelist);
        rd_kafka_assert(rkcg->rkcg_rk, TAILQ_EMPTY(&rkcg->rkcg_topics));
        rd_kafka_assert(rkcg->rkcg_rk, rd_list_empty(&rkcg->rkcg_toppars));
        rd_list_destroy(&rkcg->rkcg_toppars, NULL);

        rd_free(rkcg);
}




rd_kafka_cgrp_t *rd_kafka_cgrp_new (rd_kafka_t *rk,
                                    const rd_kafkap_str_t *group_id,
                                    const rd_kafkap_str_t *client_id) {
        rd_kafka_cgrp_t *rkcg;

        rkcg = rd_calloc(1, sizeof(*rkcg));

        rkcg->rkcg_rk = rk;
        rkcg->rkcg_group_id = group_id;
        rkcg->rkcg_client_id = client_id;
        rkcg->rkcg_coord_id = -1;
        rkcg->rkcg_generation_id = -1;

        mtx_init(&rkcg->rkcg_lock, mtx_plain);
        rd_kafka_q_init(&rkcg->rkcg_ops, rk);
        rd_kafka_q_init(&rkcg->rkcg_q, rk);
        TAILQ_INIT(&rkcg->rkcg_topics);
        rd_list_init(&rkcg->rkcg_toppars, 32);
        rd_kafka_pattern_list_init(&rkcg->rkcg_whitelist, NULL, NULL, 0);
        rd_kafka_cgrp_set_member_id(rkcg, "");
        rd_interval_init(&rkcg->rkcg_coord_query_intvl);
        rd_interval_init(&rkcg->rkcg_heartbeat_intvl);
        rd_interval_init(&rkcg->rkcg_join_intvl);

        if (RD_KAFKAP_STR_IS_NULL(group_id)) {
                /* No group configured: Operate in legacy/SimpleConsumer mode */
                rd_kafka_simple_consumer_add(rk);
                /* no need look up group coordinator (no queries) */
                rd_interval_disable(&rkcg->rkcg_coord_query_intvl);
        }

        if (rk->rk_conf.enable_auto_commit &&
            rk->rk_conf.auto_commit_interval_ms > 0)
                rd_kafka_timer_start(&rk->rk_timers,
                                     &rkcg->rkcg_offset_commit_tmr,
                                     rk->rk_conf.
				     auto_commit_interval_ms * 1000,
                                     rd_kafka_cgrp_offset_commit_tmr_cb,
                                     rkcg);

        /* Assign consumer group to a handler broker. */
        rd_kafka_cgrp_reassign_broker(rkcg);

        return rkcg;
}



/**
 * Select a broker to handle this cgrp.
 * It will prefer the coordinator broker but if that is not available
 * any other broker that is Up will be used, and if that also fails
 * uses the internal broker handle.
 *
 * NOTE: The returned rkb will have had its refcnt increased.
 */
static rd_kafka_broker_t *rd_kafka_cgrp_select_broker (rd_kafka_cgrp_t *rkcg) {
        rd_kafka_broker_t *rkb = NULL;


        /* No need for a managing broker when cgrp is terminated */
        if (rkcg->rkcg_state == RD_KAFKA_CGRP_STATE_TERM)
                return NULL;

        rd_kafka_rdlock(rkcg->rkcg_rk);
        /* Try to find the coordinator broker, if it not found
         * move the cgrp to any other Up broker which will
         * do further coord querying while waiting for the
         * proper broker to materialise.
         * If that also fails, go with the internal broker */
        if (rkcg->rkcg_coord_id != -1)
                rkb = rd_kafka_broker_find_by_nodeid(rkcg->rkcg_rk,
                                                     rkcg->rkcg_coord_id);
        if (!rkb)
                rkb = rd_kafka_broker_prefer(rkcg->rkcg_rk,
                                             rkcg->rkcg_coord_id,
                                             RD_KAFKA_BROKER_STATE_UP);
        if (!rkb)
                rkb = rd_kafka_broker_internal(rkcg->rkcg_rk);

        rd_kafka_rdunlock(rkcg->rkcg_rk);

        /* Dont change managing broker unless warranted.
         * This means do not change to another non-coordinator broker
         * while we are waiting for the proper coordinator broker to
         * become available. */
        if (rkb && rkcg->rkcg_rkb && rkb != rkcg->rkcg_rkb &&
            !RD_KAFKA_CGRP_BROKER_IS_COORD(rkcg, rkb) &&
            !RD_KAFKA_CGRP_BROKER_IS_COORD(rkcg, rkcg->rkcg_rkb) &&
            rkcg->rkcg_rkb->rkb_source != RD_KAFKA_INTERNAL) {
                rd_kafka_broker_destroy(rkb);
                rkb = rkcg->rkcg_rkb;
                rd_kafka_broker_keep(rkb);
        }

        return rkb;
}


/**
 * Delegate cgrp to broker by sending an op to the broker.
 *
 * Locality: broker thread
 */
static void rd_kafka_cgrp_delegate_broker (rd_kafka_cgrp_t *rkcg,
                                           rd_kafka_broker_t *rkb) {
        rd_kafka_op_t *rko;

        rd_kafka_assert(rkcg->rkcg_rk, rkcg->rkcg_rkb == NULL);
        rkcg->rkcg_rkb = rkb;
        rd_kafka_broker_keep(rkb);

        rko = rd_kafka_op_new(RD_KAFKA_OP_CGRP_DELEGATE);
        rko->rko_cgrp = rkcg;

        rd_kafka_q_enq(&rkb->rkb_ops, rko);
}


/**
 * Assign cgrp to broker.
 *
 * NOTE: Must only be called from the current broker's thread.
 *
 * Locality: broker thread
 */
void rd_kafka_cgrp_assign_broker (rd_kafka_cgrp_t *rkcg,
                                  rd_kafka_broker_t *rkb) {
        rd_kafka_assert(rkb->rkb_rk, rkcg->rkcg_rkb == rkb);

        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "BRKASSIGN",
                     "Group \"%.*s\" management assigned to broker %s",
                     RD_KAFKAP_STR_PR(rkcg->rkcg_group_id), rkb->rkb_name);

        rkb->rkb_cgrp = rkcg;

        /* Reset query interval to trigger an immediate
         * coord query if required */
        if (!rd_interval_disabled(&rkcg->rkcg_coord_query_intvl))
                rd_interval_reset(&rkcg->rkcg_coord_query_intvl);

        if (RD_KAFKA_CGRP_BROKER_IS_COORD(rkcg, rkb))
                rd_kafka_cgrp_set_state(rkcg, RD_KAFKA_CGRP_STATE_WAIT_BROKER_TRANSPORT);

}


/**
 * Unassign cgrp from current broker.
 *
 * NOTE: Must only be called from the current broker's thread.
 *
 * Locality: broker thread
 */
static void rd_kafka_cgrp_unassign_broker (rd_kafka_cgrp_t *rkcg) {
        rd_kafka_broker_t *rkb = rkcg->rkcg_rkb;

        rd_kafka_assert(rkb->rkb_rk, thrd_is_current(rkb->rkb_thread));

        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "BRKUNASSIGN",
                     "Group \"%.*s\" management unassigned "
                     "from broker handle %s",
                     RD_KAFKAP_STR_PR(rkcg->rkcg_group_id), rkb->rkb_name);

        rkb->rkb_cgrp = NULL;

        // FIXME: How do we handle ops in the queue when we have no broker?
        rkcg->rkcg_rkb = NULL;
        rd_kafka_broker_destroy(rkb); /* from delegate() */
}


/**
 * Assign cgrp to a broker to handle.
 * It will prefer the coordinator broker but if that is not available
 * any other broker that is Up will be used, and if that also fails
 * uses the internal broker handle.
 *
 * Returns 1 if the cgrp was reassigned, else 0.
 */
static int rd_kafka_cgrp_reassign_broker (rd_kafka_cgrp_t *rkcg) {
        rd_kafka_broker_t *rkb;

        rkb = rd_kafka_cgrp_select_broker(rkcg);

        if (rkb == rkcg->rkcg_rkb) {
                if (rkb && RD_KAFKA_CGRP_BROKER_IS_COORD(rkcg, rkb))
                        rd_kafka_cgrp_set_state(rkcg, RD_KAFKA_CGRP_STATE_WAIT_BROKER_TRANSPORT);
                else
                        rd_kafka_cgrp_set_state(rkcg, RD_KAFKA_CGRP_STATE_WAIT_BROKER);

                if (rkb)
                        rd_kafka_broker_destroy(rkb);
                return 0; /* No change */
        }

        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "BRKREASSIGN",
                     "Group \"%.*s\" management reassigned from "
                     "broker %s to %s",
                     RD_KAFKAP_STR_PR(rkcg->rkcg_group_id),
                     rkcg->rkcg_rkb ? rkcg->rkcg_rkb->rkb_name : "(none)",
                     rkb ? rkb->rkb_name : "(none)");


        if (rkcg->rkcg_rkb)
                rd_kafka_cgrp_unassign_broker(rkcg);

        rd_kafka_cgrp_set_state(rkcg, RD_KAFKA_CGRP_STATE_WAIT_BROKER);

        if (rkb) {
                rd_kafka_cgrp_delegate_broker(rkcg, rkb);
		rd_kafka_broker_destroy(rkb); /* from select_broker() */
	}

        return 1;
}


/**
 * Update the cgrp's coordinator and move it to that broker.
 */
void rd_kafka_cgrp_coord_update (rd_kafka_cgrp_t *rkcg, int32_t coord_id) {

        if (rkcg->rkcg_coord_id == coord_id)
                return;

        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "CGRPCOORD",
                     "Group \"%.*s\" changing coordinator %"PRId32" -> %"PRId32,
                     RD_KAFKAP_STR_PR(rkcg->rkcg_group_id), rkcg->rkcg_coord_id,
                     coord_id);
        rkcg->rkcg_coord_id = coord_id;

        rd_kafka_cgrp_set_state(rkcg, RD_KAFKA_CGRP_STATE_WAIT_BROKER);

        rd_kafka_cgrp_reassign_broker(rkcg);
}






/**
 * Handle GroupCoordinator response
 */
static void rd_kafka_cgrp_handle_GroupCoordinator (rd_kafka_broker_t *rkb,
                                                   rd_kafka_resp_err_t err,
                                                   rd_kafka_buf_t *rkbuf,
                                                   rd_kafka_buf_t *request,
                                                   void *opaque) {
        const int log_decode_errors = 1;
        int16_t ErrorCode = 0;
        int32_t CoordId;
        rd_kafkap_str_t CoordHost;
        int32_t CoordPort;
        rd_kafka_cgrp_t *rkcg = opaque;
        struct rd_kafka_metadata_broker mdb = RD_ZERO_INIT;

        if (likely(!(ErrorCode = err))) {
                rd_kafka_buf_read_i16(rkbuf, &ErrorCode);
                rd_kafka_buf_read_i32(rkbuf, &CoordId);
                rd_kafka_buf_read_str(rkbuf, &CoordHost);
                rd_kafka_buf_read_i32(rkbuf, &CoordPort);
        }

        if (ErrorCode)
                goto err2;


        mdb.id = CoordId;
	RD_KAFKAP_STR_DUPA(&mdb.host, &CoordHost);
	mdb.port = CoordPort;

        rd_rkb_dbg(rkb, CGRP, "CGRPCOORD",
                   "Group \"%.*s\" coordinator is %s:%i id %"PRId32,
                   RD_KAFKAP_STR_PR(rkcg->rkcg_group_id),
                   mdb.host, mdb.port, mdb.id);
        rd_kafka_broker_update(rkb->rkb_rk, rkb->rkb_proto, &mdb);

        rd_kafka_cgrp_coord_update(rkcg, CoordId);
        return;

err: /* Parse error */
        ErrorCode = RD_KAFKA_RESP_ERR__BAD_MSG;
        /* FALLTHRU */

err2:
        rd_rkb_dbg(rkb, CGRP, "CGRPCOORD",
                   "Group \"%.*s\" GroupCoordinator response error: %s",
                   RD_KAFKAP_STR_PR(rkcg->rkcg_group_id),
                   rd_kafka_err2str(ErrorCode));

        /* Suppress repeated errors */
        if (rkcg->rkcg_last_err != ErrorCode) {
                rd_kafka_op_app_fmt(&rkcg->rkcg_q, RD_KAFKA_OP_CONSUMER_ERR,
                                    NULL, ErrorCode,
                                    "GroupCoordinator response error: %s",
                                    rd_kafka_err2str(ErrorCode));
                rkcg->rkcg_last_err = ErrorCode;
        }

        if (ErrorCode == RD_KAFKA_RESP_ERR_GROUP_COORDINATOR_NOT_AVAILABLE ||
            ErrorCode == RD_KAFKA_RESP_ERR_NOT_COORDINATOR_FOR_GROUP)
                rd_kafka_cgrp_coord_update(rkcg, -1);
}


/**
 * Query for coordinator.
 *
 * Locality: broker thread
 */
static void rd_kafka_cgrp_coord_query (rd_kafka_cgrp_t *rkcg,
                                       rd_kafka_broker_t *rkb,
                                       const char *reason) {
        rd_rkb_dbg(rkb, CGRP, "CGRPQUERY",
                   "Group \"%.*s\": querying for coordinator: %s",
                   RD_KAFKAP_STR_PR(rkcg->rkcg_group_id), reason);

        if (rkb->rkb_source == RD_KAFKA_INTERNAL ||
            rkb->rkb_state < RD_KAFKA_BROKER_STATE_UP)
                return;

        rd_kafka_GroupCoordinatorRequest(rkb, rkcg->rkcg_group_id,
                                         &rkcg->rkcg_ops,
                                         rd_kafka_cgrp_handle_GroupCoordinator,
                                         rkcg);

        if (rkcg->rkcg_state == RD_KAFKA_CGRP_STATE_QUERY_COORD)
                rd_kafka_cgrp_set_state(rkcg, RD_KAFKA_CGRP_STATE_WAIT_COORD);

}



static void rd_kafka_cgrp_leave (rd_kafka_cgrp_t *rkcg, int ignore_response) {
        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "LEAVE",
                   "Group \"%.*s\": leave",
                   RD_KAFKAP_STR_PR(rkcg->rkcg_group_id));

        if (rkcg->rkcg_state == RD_KAFKA_CGRP_STATE_UP)
                rd_kafka_LeaveGroupRequest(rkcg->rkcg_rkb, rkcg->rkcg_group_id,
                                           rkcg->rkcg_member_id,
                                           &rkcg->rkcg_ops,
                                           ignore_response ? NULL :
                                           rd_kafka_handle_LeaveGroup, rkcg);
        else if (!ignore_response)
                rd_kafka_handle_LeaveGroup(rkcg->rkcg_rkb,
                                           RD_KAFKA_RESP_ERR__WAIT_COORD,
                                           NULL, NULL, rkcg);
}

static void rd_kafka_cgrp_join (rd_kafka_cgrp_t *rkcg) {

        if (rkcg->rkcg_state != RD_KAFKA_CGRP_STATE_UP ||
            rkcg->rkcg_join_state != RD_KAFKA_CGRP_JOIN_STATE_INIT)
                return;

        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "JOIN",
                     "Group \"%.*s\": join with %d subscribed topic(s)",
                     RD_KAFKAP_STR_PR(rkcg->rkcg_group_id),
                     rkcg->rkcg_subscription->cnt);

        rd_kafka_cgrp_set_join_state(rkcg, RD_KAFKA_CGRP_JOIN_STATE_WAIT_JOIN);
        rd_kafka_JoinGroupRequest(rkcg->rkcg_rkb, rkcg->rkcg_group_id,
                                  rkcg->rkcg_member_id,
                                  rkcg->rkcg_rk->rk_conf.group_protocol_type,
                                  rkcg->rkcg_subscription,
                                  &rkcg->rkcg_ops,
                                  rd_kafka_cgrp_handle_JoinGroup, rkcg);
}


static void rd_kafka_cgrp_heartbeat (rd_kafka_cgrp_t *rkcg,
                                     rd_kafka_broker_t *rkb) {
        rd_kafka_HeartbeatRequest(rkb, rkcg->rkcg_group_id,
                                  rkcg->rkcg_generation_id,
                                  rkcg->rkcg_member_id,
                                  &rkcg->rkcg_ops,
                                  rd_kafka_cgrp_handle_Heartbeat, rkcg);
}

/**
 * Cgrp is now terminated: decommission it and signal back to application.
 */
static void rd_kafka_cgrp_terminated (rd_kafka_cgrp_t *rkcg) {

	rd_kafka_assert(NULL, rkcg->rkcg_wait_unassign_cnt == 0);
	rd_kafka_assert(NULL, !(rkcg->rkcg_flags &
				(RD_KAFKA_CGRP_F_WAIT_COMMIT|
				 RD_KAFKA_CGRP_F_WAIT_UNASSIGN)));
        rd_kafka_cgrp_set_state(rkcg, RD_KAFKA_CGRP_STATE_TERM);

        rd_kafka_timer_stop(&rkcg->rkcg_rk->rk_timers,
                            &rkcg->rkcg_offset_commit_tmr, 1/*lock*/);

	if (rkcg->rkcg_rkb)
		rd_kafka_cgrp_unassign_broker(rkcg);

        if (rkcg->rkcg_reply_rko) {
                rd_kafka_q_t *replyq = rkcg->rkcg_reply_rko->rko_replyq;
                /* Signal back to application. */
                rkcg->rkcg_reply_rko->rko_replyq = NULL;
                rd_kafka_q_enq(replyq, rkcg->rkcg_reply_rko);
                rkcg->rkcg_reply_rko = NULL;
                rd_kafka_q_destroy(replyq);
        }
}


/**
 * If a cgrp is terminating and all outstanding ops are now finished
 * then progress to final termination and return 1.
 * Else returns 0.
 */
static int rd_kafka_cgrp_try_terminate (rd_kafka_cgrp_t *rkcg) {

        if ((rkcg->rkcg_flags & RD_KAFKA_CGRP_F_TERMINATE) &&
            rd_list_empty(&rkcg->rkcg_toppars) &&
	    rkcg->rkcg_wait_unassign_cnt == 0 &&
            !(rkcg->rkcg_flags & (RD_KAFKA_CGRP_F_WAIT_COMMIT|
				  RD_KAFKA_CGRP_F_WAIT_UNASSIGN))) {
                rd_kafka_cgrp_terminated(rkcg);
                return 1;
        } else {
                return 0;
        }
}


/**
 * Add partition to this cgrp management
 */
static void rd_kafka_cgrp_partition_add (rd_kafka_cgrp_t *rkcg,
                                         rd_kafka_toppar_t *rktp) {
        rd_kafka_dbg(rkcg->rkcg_rk, CGRP,"PARTADD",
                     "Group \"%s\": add %s [%"PRId32"]",
                     rkcg->rkcg_group_id->str,
                     rktp->rktp_rkt->rkt_topic->str,
                     rktp->rktp_partition);

        rd_kafka_assert(rkcg->rkcg_rk, !rktp->rktp_s_for_cgrp);
        rktp->rktp_s_for_cgrp = rd_kafka_toppar_keep(rktp);
        rd_list_add(&rkcg->rkcg_toppars, rktp->rktp_s_for_cgrp);
}

/**
 * Remove partition from this cgrp management
 */
static void rd_kafka_cgrp_partition_del (rd_kafka_cgrp_t *rkcg,
                                         rd_kafka_toppar_t *rktp) {
        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "PARTDEL",
                     "Group \"%s\": delete %s [%"PRId32"]",
                     rkcg->rkcg_group_id->str,
                     rktp->rktp_rkt->rkt_topic->str,
                     rktp->rktp_partition);
        rd_kafka_assert(rkcg->rkcg_rk, rktp->rktp_s_for_cgrp);

        rd_list_remove(&rkcg->rkcg_toppars, rktp->rktp_s_for_cgrp);
        rd_kafka_toppar_destroy(rktp->rktp_s_for_cgrp);
        rktp->rktp_s_for_cgrp = NULL;

        rd_kafka_cgrp_try_terminate(rkcg);
}




/**
 * Fetch offsets for a list of partitions
 */
static void
rd_kafka_cgrp_offsets_fetch (rd_kafka_cgrp_t *rkcg, rd_kafka_broker_t *rkb,
                             rd_kafka_topic_partition_list_t *offsets,
                             rd_kafka_q_t *replyq) {

        rd_kafka_op_t *rko = rd_kafka_op_new(RD_KAFKA_OP_OFFSET_FETCH);

        rd_kafka_op_payload_set(rko,
                                rd_kafka_topic_partition_list_copy(offsets),
                                (void *)rd_kafka_topic_partition_list_destroy);

        rd_kafka_q_keep(replyq);
        rko->rko_replyq  = replyq;

        if (rkcg->rkcg_state != RD_KAFKA_CGRP_STATE_UP || !rkb)
                rd_kafka_op_handle_OffsetFetch(rkb,
                                               RD_KAFKA_RESP_ERR__WAIT_COORD,
                                               NULL, NULL, rko);
        else
                rd_kafka_OffsetFetchRequest(
                        rkb, 1, offsets,
                        &rkcg->rkcg_ops, rd_kafka_op_handle_OffsetFetch, rko);

}


/**
 * Start fetching all partitions in 'assignment' (async)
 */
static void
rd_kafka_cgrp_partitions_fetch_start (rd_kafka_cgrp_t *rkcg,
                                      rd_kafka_topic_partition_list_t
                                      *assignment, int usable_offsets) {
        int i;

        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "FETCHSTART",
                     "Group \"%s\": starting fetchers for %d assigned "
                     "partition(s)",
                     rkcg->rkcg_group_id->str, assignment->cnt);

        if (assignment->cnt == 0)
                return;

        if (!usable_offsets &&
            rkcg->rkcg_rk->rk_conf.offset_store_method ==
            RD_KAFKA_OFFSET_METHOD_BROKER) {

                /* Fetch offsets for all assigned partitions */
                rd_kafka_cgrp_offsets_fetch(rkcg, rkcg->rkcg_rkb, assignment,
                                            &rkcg->rkcg_ops);

        } else {
                for (i = 0 ; i < assignment->cnt ; i++) {
                        rd_kafka_topic_partition_t *rktpar =
                                &assignment->elems[i];
                        shptr_rd_kafka_toppar_t *s_rktp = rktpar->_private;
                        rd_kafka_toppar_t *rktp = rd_kafka_toppar_s2i(s_rktp);

                        rd_kafka_assert(rkcg->rkcg_rk, !rktp->rktp_assigned);

			if (!RD_KAFKA_OFFSET_IS_LOGICAL(rktpar->offset))
				rd_kafka_offset_store0(rktp, rktpar->offset, 1);

			rd_kafka_toppar_op_fetch_start(rktp, rktpar->offset,
                                                       &rkcg->rkcg_q, NULL);

                        rktp->rktp_assigned = 1;
			rkcg->rkcg_assigned_cnt++;
                }
        }
}




/**
 * Enqueue a rebalance op (if configured). 'partitions' is copied.
 * This delegates the responsibility of assign() and unassign() to the
 * application.
 *
 * Returns 1 if a rebalance op was enqueued, else 0.
 * Returns 0 if 'assignment' is NULL.
 */
static int
rd_kafka_rebalance_op (rd_kafka_cgrp_t *rkcg,
		       rd_kafka_resp_err_t err,
		       const rd_kafka_topic_partition_list_t *assignment) {

	if (!rkcg->rkcg_rk->rk_conf.rebalance_cb || !assignment)
		return 0;

	rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "ASSIGN",
		     "Group \"%s\": delegating %s of %d partition(s) "
		     "to application rebalance callback",
		     rkcg->rkcg_group_id->str,
		     err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS ?
		     "revoke":"assign", assignment->cnt);

        rd_kafka_cgrp_set_join_state(
                rkcg, RD_KAFKA_CGRP_JOIN_STATE_WAIT_REBALANCE_CB);

	rd_kafka_op_app(&rkcg->rkcg_q, RD_KAFKA_OP_REBALANCE,
			RD_KAFKA_OP_F_FREE, NULL, err,
			rd_kafka_topic_partition_list_copy(assignment), 0,
			(void *)rd_kafka_topic_partition_list_destroy);
	return 1;
}


/**
 * Handler of OffsetCommit response (after parsing).
 *
 * @warning Function takes ownership of 'offsets'.
 */
static void
rd_kafka_cgrp_handle_OffsetCommit (rd_kafka_cgrp_t *rkcg,
                                   rd_kafka_resp_err_t err,
                                   rd_kafka_topic_partition_list_t
                                   *offsets) {

        rd_kafka_assert(rkcg->rkcg_rk,
                        rkcg->rkcg_flags & RD_KAFKA_CGRP_F_WAIT_COMMIT);
        rkcg->rkcg_flags &= ~RD_KAFKA_CGRP_F_WAIT_COMMIT;

	rd_kafka_offset_commit_cb_op(rkcg->rkcg_rk, err, offsets);
        offsets = NULL; /* Ownership delegated to commit_cb_op */

        if (rd_kafka_cgrp_try_terminate(rkcg))
                return; /* terminated */

        if (rkcg->rkcg_join_state == RD_KAFKA_CGRP_JOIN_STATE_WAIT_UNASSIGN)
		rd_kafka_cgrp_check_unassign_done(rkcg);
}


/**
 * Commit a list of offsets.
 * Reuse the orignating 'rko' for the async reply.
 * 'rko->rko_payload' must be set to 'offsets' (or NULL for no-op)
 *
 * Might alter \p offsets but does not hang on to it after return.
 */
static void rd_kafka_cgrp_offsets_commit (rd_kafka_cgrp_t *rkcg,
                                          rd_kafka_broker_t *rkb,
                                          rd_kafka_op_t *rko,
                                          rd_kafka_topic_partition_list_t
                                          *offsets) {
        if (rkcg->rkcg_state != RD_KAFKA_CGRP_STATE_UP || !rkb)
                rd_kafka_op_handle_OffsetCommit(rkb,
                                                RD_KAFKA_RESP_ERR__WAIT_COORD,
                                                NULL, NULL,
                                                rko);
        else if (!offsets)
                rd_kafka_op_handle_OffsetCommit(rkb,
                                                RD_KAFKA_RESP_ERR__NO_OFFSET,
                                                NULL, NULL,
                                                rko);
        else
                rd_kafka_OffsetCommitRequest(
                        rkb, rkcg, 1, offsets,
                        &rkcg->rkcg_ops, rd_kafka_op_handle_OffsetCommit, rko);

}


/**
 * Commit offsets for all assigned partitions.
 */
static void rd_kafka_cgrp_assigned_offsets_commit (rd_kafka_cgrp_t *rkcg) {
        rd_kafka_topic_partition_list_t *offsets;
        rd_kafka_op_t *rko;
	int valid_offset_cnt;

        offsets = rd_kafka_topic_partition_list_copy(rkcg->rkcg_assignment);

        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "COMMIT",
                     "Group \"%s\": committing offsets for %d partition(s)",
                     rkcg->rkcg_group_id->str, offsets->cnt);


	rkcg->rkcg_flags |= RD_KAFKA_CGRP_F_WAIT_COMMIT;

	valid_offset_cnt =
		rd_kafka_topic_partition_list_set_offsets(rkcg->rkcg_rk,
							  offsets, 1, 0,
							  1 /* is commit */);

	if (valid_offset_cnt == 0) {
		rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "COMMIT",
			     "Group \"%s\": no valid offsets to commit",
			     rkcg->rkcg_group_id->str);
                /* Takes ownership of 'offsets' */
		rd_kafka_cgrp_handle_OffsetCommit(rkcg,
						  RD_KAFKA_RESP_ERR__NO_OFFSET,
						  offsets);
		return;
	}

        /* Create op to hold the response */
        rko = rd_kafka_op_new(RD_KAFKA_OP_OFFSET_COMMIT);
        rd_kafka_op_payload_set(rko, offsets,
                                (void *)rd_kafka_topic_partition_list_destroy);
        rko->rko_replyq = &rkcg->rkcg_ops;
        rd_kafka_q_keep(rko->rko_replyq);

        /* Async offsets commit */
        rd_kafka_cgrp_offsets_commit(rkcg, rkcg->rkcg_rkb, rko, offsets);

}


/**
 * auto.commit.interval.ms commit timer callback.
 *
 * Trigger a group offset commit.
 *
 * Locality: rdkafka main thread  (NOTE! NOT cgrp thread!)
 */
static void rd_kafka_cgrp_offset_commit_tmr_cb (rd_kafka_timers_t *rkts,
                                                void *arg) {
        rd_kafka_cgrp_t *rkcg = arg;
        rd_kafka_op_t *rko;

        rko = rd_kafka_op_new(RD_KAFKA_OP_OFFSET_COMMIT);
        /* NULL rko_payload means current assignment */
        /* Use rkcg reply queue so that offset_commit_cb is called, if confd */
        if (rkcg->rkcg_rk->rk_conf.offset_commit_cb) {
                rko->rko_replyq = &rkcg->rkcg_q;
                rd_kafka_q_keep(rko->rko_replyq);
        }

        rd_kafka_q_enq(&rkcg->rkcg_ops, rko);
}




/**
 * Call when all unassign operations are done to transition to the next state
 */
static void rd_kafka_cgrp_unassign_done (rd_kafka_cgrp_t *rkcg) {
	rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "UNASSIGN",
		     "Group \"%s\": unassign done in state %s (join state %s): "
		     "%s",
		     rkcg->rkcg_group_id->str,
		     rd_kafka_cgrp_state_names[rkcg->rkcg_state],
		     rd_kafka_cgrp_join_state_names[rkcg->rkcg_join_state],
		     rkcg->rkcg_assignment ?
		     "with new assignment" : "without new assignment");

	if (rkcg->rkcg_flags & RD_KAFKA_CGRP_F_LEAVE_ON_UNASSIGN) {
		rd_kafka_cgrp_leave(rkcg, 1/*ignore response*/);
		rkcg->rkcg_flags &= ~RD_KAFKA_CGRP_F_LEAVE_ON_UNASSIGN;
	}

        if (rkcg->rkcg_assignment) {
		rd_kafka_cgrp_set_join_state(rkcg,
					     RD_KAFKA_CGRP_JOIN_STATE_ASSIGNED);
                rd_kafka_cgrp_partitions_fetch_start(rkcg,
                                                     rkcg->rkcg_assignment, 0);
	} else {
		rd_kafka_cgrp_set_join_state(rkcg,
					     RD_KAFKA_CGRP_JOIN_STATE_INIT);
	}

	rd_kafka_cgrp_try_terminate(rkcg);
}


/**
 * Checks if the current unassignment is done and if so
 * calls .._done().
 * Else does nothing.
 */
static void rd_kafka_cgrp_check_unassign_done (rd_kafka_cgrp_t *rkcg) {
	if (rkcg->rkcg_wait_unassign_cnt > 0 ||
	    rkcg->rkcg_assigned_cnt > 0 ||
	    rkcg->rkcg_flags & (RD_KAFKA_CGRP_F_WAIT_UNASSIGN|
				RD_KAFKA_CGRP_F_WAIT_COMMIT))
		return;

	rd_kafka_cgrp_unassign_done(rkcg);
}



/**
 * Remove existing assignment.
 */
static rd_kafka_resp_err_t
rd_kafka_cgrp_unassign (rd_kafka_cgrp_t *rkcg) {
        int i;

	rkcg->rkcg_flags &= ~RD_KAFKA_CGRP_F_WAIT_UNASSIGN;

        if (!rkcg->rkcg_assignment) {
		rd_kafka_cgrp_check_unassign_done(rkcg);
                return RD_KAFKA_RESP_ERR_NO_ERROR;
	}

        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "UNASSIGN",
                     "Group \"%s\": unassigning %d partition(s)",
                     rkcg->rkcg_group_id->str, rkcg->rkcg_assignment->cnt);

        rd_kafka_cgrp_set_join_state(rkcg,
                                     RD_KAFKA_CGRP_JOIN_STATE_WAIT_UNASSIGN);

        if (rkcg->rkcg_rk->rk_conf.offset_store_method ==
            RD_KAFKA_OFFSET_METHOD_BROKER &&
	    rkcg->rkcg_rk->rk_conf.enable_auto_commit) {
                /* Commit all offsets for all assigned partitions to broker */
                rd_kafka_cgrp_assigned_offsets_commit(rkcg);
        }


        for (i = 0 ; i < rkcg->rkcg_assignment->cnt ; i++) {
                rd_kafka_topic_partition_t *rktpar;
                shptr_rd_kafka_toppar_t *s_rktp;
                rd_kafka_toppar_t *rktp;

                rktpar = &rkcg->rkcg_assignment->elems[i];
                s_rktp = rktpar->_private;
                rktp = rd_kafka_toppar_s2i(s_rktp);

                if (rktp->rktp_assigned) {
                        rd_kafka_toppar_op_fetch_stop(rktp, &rkcg->rkcg_ops);
                        rkcg->rkcg_wait_unassign_cnt++;
                }

                rd_kafka_toppar_lock(rktp);
                rd_kafka_toppar_desired_del(rktp);
                rd_kafka_toppar_unlock(rktp);
        }

        rd_kafka_topic_partition_list_destroy(rkcg->rkcg_assignment);
        rkcg->rkcg_assignment = NULL;

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}


/**
 * Set new atomic partition assignment
 */
static void
rd_kafka_cgrp_assign (rd_kafka_cgrp_t *rkcg,
                      rd_kafka_topic_partition_list_t *assignment) {
        int i;

        /* FIXME: Make a diff of what partitions are removed and added. */

        /* Get toppar object for each partition */
        for (i = 0 ; assignment && i < assignment->cnt ; i++) {
                rd_kafka_topic_partition_t *rktpar;
                shptr_rd_kafka_toppar_t *s_rktp;

                rktpar = &assignment->elems[i];
                s_rktp = rd_kafka_toppar_get2(rkcg->rkcg_rk,
                                              rktpar->topic,
                                              rktpar->partition,
                                              0/*no-ua*/, 1/*create-on-miss*/);
                if (!s_rktp)
                        continue;

                rktpar->_private = s_rktp;
        }


        /* Remove existing assignment (async operation) */
	if (rkcg->rkcg_assignment)
		rd_kafka_cgrp_unassign(rkcg);

        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "ASSIGN",
                     "Group \"%s\": assigning %d partition(s) in join state %s",
                     rkcg->rkcg_group_id->str, assignment ? assignment->cnt : 0,
                     rd_kafka_cgrp_join_state_names[rkcg->rkcg_join_state]);

        if (rkcg->rkcg_wait_unassign_cnt == 0)
                rd_kafka_cgrp_set_join_state(rkcg,
                                             RD_KAFKA_CGRP_JOIN_STATE_ASSIGNED);

	if (assignment)
		rkcg->rkcg_assignment =
			rd_kafka_topic_partition_list_copy(assignment);
	else
		rkcg->rkcg_assignment = NULL;

        if (rkcg->rkcg_join_state == RD_KAFKA_CGRP_JOIN_STATE_ASSIGNED &&
	    assignment) {
                /* No existing assignment that needs to be decommissioned,
                 * start partition fetchers right away (async) */
                rd_kafka_cgrp_partitions_fetch_start(
                        rkcg, rkcg->rkcg_assignment, 0);
        }
}




/**
 * Handle a rebalance-triggered partition assignment.
 *
 * If a rebalance_cb has been registered we enqueue an op for the app
 * and let the app perform the actual assign() call.
 * Otherwise we assign() directly from here.
 *
 * This provides the most flexibility, allowing the app to perform any
 * operation it seem fit (e.g., offset writes or reads) before actually
 * updating the assign():ment.
 */
static void
rd_kafka_cgrp_handle_assignment (rd_kafka_cgrp_t *rkcg,
				 rd_kafka_topic_partition_list_t *assignment) {

	if (!rd_kafka_rebalance_op(rkcg, RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS,
				  assignment))
		rd_kafka_cgrp_assign(rkcg, assignment);
}


/**
 * Handle HeartbeatResponse errors.
 *
 * If an IllegalGeneration error code is returned in the
 * HeartbeatResponse, it indicates that the co-ordinator has
 * initiated a rebalance. The consumer then stops fetching data,
 * commits offsets and sends a JoinGroupRequest to it's co-ordinator
 * broker */
void rd_kafka_cgrp_handle_heartbeat_error (rd_kafka_cgrp_t *rkcg,
					   rd_kafka_resp_err_t err) {


	rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "HEARTBEAT",
		     "Group \"%s\" heartbeat error response in "
		     "state %s (join state %s, %d partition(s) assigned): %s",
		     rkcg->rkcg_group_id->str,
		     rd_kafka_cgrp_state_names[rkcg->rkcg_state],
		     rd_kafka_cgrp_join_state_names[rkcg->rkcg_join_state],
		     rkcg->rkcg_assignment ? rkcg->rkcg_assignment->cnt : 0,
		     rd_kafka_err2str(err));

	
	switch (err)
	{
	case RD_KAFKA_RESP_ERR_NOT_COORDINATOR_FOR_GROUP:
	case RD_KAFKA_RESP_ERR_GROUP_COORDINATOR_NOT_AVAILABLE:
	case RD_KAFKA_RESP_ERR__TRANSPORT:
		/* Remain in joined state and keep querying for coordinator */
		rd_interval_expedite(&rkcg->rkcg_coord_query_intvl, 0);
		break;

	case RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS:
	case RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION:
	case RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID:
	default:
		rkcg->rkcg_flags |= RD_KAFKA_CGRP_F_WAIT_UNASSIGN;

		/* Trigger rebalance_cb, if configured */
		if (!rd_kafka_rebalance_op(rkcg,
					   RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS,
					   rkcg->rkcg_assignment))
			rd_kafka_cgrp_unassign(rkcg);
		break;
	}
}



/**
 * Clean up any group-leader related resources.
 *
 * Locality: cgrp thread
 */
void rd_kafka_cgrp_group_leader_reset (rd_kafka_cgrp_t *rkcg){
        if (rkcg->rkcg_group_leader.protocol) {
                rd_free(rkcg->rkcg_group_leader.protocol);
                rkcg->rkcg_group_leader.protocol = NULL;
        }

        if (rkcg->rkcg_group_leader.members) {
                int i;

                for (i = 0 ; i < rkcg->rkcg_group_leader.member_cnt ; i++)
                        rd_kafka_group_member_clear(&rkcg->rkcg_group_leader.
                                                    members[i]);
                rkcg->rkcg_group_leader.member_cnt = 0;
                rd_free(rkcg->rkcg_group_leader.members);
                rkcg->rkcg_group_leader.members = NULL;
        }
}

/**
 * Remove existing topic subscription.
 */
static rd_kafka_resp_err_t
rd_kafka_cgrp_unsubscribe (rd_kafka_cgrp_t *rkcg, int leave_group) {

        if (rkcg->rkcg_subscription) {
                rd_kafka_topic_partition_list_destroy(rkcg->rkcg_subscription);
                rkcg->rkcg_subscription = NULL;
        }

        /*
         * Clean-up group leader duties, if any.
         */
        rd_kafka_cgrp_group_leader_reset(rkcg);

	if (leave_group)
		rkcg->rkcg_flags |= RD_KAFKA_CGRP_F_LEAVE_ON_UNASSIGN;

	rkcg->rkcg_flags |= RD_KAFKA_CGRP_F_WAIT_UNASSIGN;

        /* Remove assignment, if any. (async) */
	if (!rd_kafka_rebalance_op(rkcg, RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS,
				   rkcg->rkcg_assignment))
		rd_kafka_cgrp_unassign(rkcg);

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}


/**
 * Set new atomic topic subscription.
 */
static rd_kafka_resp_err_t
rd_kafka_cgrp_subscribe (rd_kafka_cgrp_t *rkcg,
                         rd_kafka_topic_partition_list_t *rktparlist) {

        if (rkcg->rkcg_rk->rk_conf.enabled_assignor_cnt == 0)
                return RD_KAFKA_RESP_ERR__INVALID_ARG;

        /* Remove existing subscription first */
        rd_kafka_cgrp_unsubscribe(rkcg, 0/* dont leave group */);

        if (!rktparlist)
                return RD_KAFKA_RESP_ERR_NO_ERROR;

        rkcg->rkcg_subscription = rktparlist;

        rd_kafka_cgrp_join(rkcg);

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}






/**
 * Same as cgrp_terminate() but called from the cgrp thread upon receiving
 * the op 'rko' from cgrp_terminate().
 *
 * NOTE: Takes ownership of 'rko'
 *
 * Locality: cgrp broker thread
 */
static void
rd_kafka_cgrp_terminate0 (rd_kafka_cgrp_t *rkcg, rd_kafka_op_t *rko) {

        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "CGRPTERM",
                     "Terminating group \"%.*s\" in state %s "
                     "with %d partition(s)",
                     RD_KAFKAP_STR_PR(rkcg->rkcg_group_id),
                     rd_kafka_cgrp_state_names[rkcg->rkcg_state],
                     rd_list_cnt(&rkcg->rkcg_toppars));

        if (unlikely(rkcg->rkcg_reply_rko != NULL)) {
                /* Already handling a previous terminate */
                rd_kafka_q_t *replyq = rko->rko_replyq;
                rko->rko_replyq = NULL;
                rd_kafka_q_op_err(replyq,
                                  RD_KAFKA_RESP_ERR__IN_PROGRESS,
                                  rkcg->rkcg_reply_rko->rko_version,
                                  "Group is busy handling %s",
                                  rd_kafka_op2str(rkcg->rkcg_reply_rko->
                                                  rko_type));
                rd_kafka_q_destroy(replyq);
                rd_kafka_op_destroy(rko);
                return;
        }

        /* Mark for stopping, the actual state transition
         * is performed when all toppars have left. */
        rkcg->rkcg_flags |= RD_KAFKA_CGRP_F_TERMINATE;
        rkcg->rkcg_reply_rko = rko;

        rd_kafka_cgrp_unsubscribe(rkcg, 1/*leave group*/);

        /* If there were no toppars attached the cgrp
         * can be terminated right away. */
        if (rd_list_empty(&rkcg->rkcg_toppars))
                rd_kafka_cgrp_terminated(rkcg);
}


/**
 * Terminate and decommission a cgrp asynchronously.
 *
 * Locality: any thread
 */
void rd_kafka_cgrp_terminate (rd_kafka_cgrp_t *rkcg, rd_kafka_q_t *replyq) {
        rd_kafka_cgrp_op(rkcg, NULL, replyq, RD_KAFKA_OP_TERMINATE, 0);
}




/**
 * Serve cgrp op queue.
 */
static void rd_kafka_cgrp_op_serve (rd_kafka_cgrp_t *rkcg,
                                    rd_kafka_broker_t *rkb) {
        rd_kafka_op_t *rko;

        while ((rko = rd_kafka_q_pop(&rkcg->rkcg_ops, RD_POLL_NOWAIT, 0))) {
                rd_kafka_op_t *rko2;
                rd_kafka_toppar_t *rktp = rko->rko_rktp ?
                        rd_kafka_toppar_s2i(rko->rko_rktp) : NULL;
                rd_kafka_resp_err_t err;
                const int silent_op = rko->rko_type == RD_KAFKA_OP_RECV_BUF;

                if (rktp && !silent_op)
                        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "CGRPOP",
                                     "Group \"%.*s\" received op %s in state %s "
                                     "(join state %s) for %.*s [%"PRId32"]",
                                     RD_KAFKAP_STR_PR(rkcg->rkcg_group_id),
                                     rd_kafka_op2str(rko->rko_type),
                                     rd_kafka_cgrp_state_names[rkcg->rkcg_state],
                                     rd_kafka_cgrp_join_state_names[rkcg->rkcg_join_state],
                                     RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                                     rktp->rktp_partition);
                else if (!silent_op)
                        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "CGRPOP",
                                     "Group \"%.*s\" received op %s in state %s "
                                     "(join state %s)",
                                     RD_KAFKAP_STR_PR(rkcg->rkcg_group_id),
                                     rd_kafka_op2str(rko->rko_type),
                                     rd_kafka_cgrp_state_names[rkcg->rkcg_state],
                                     rd_kafka_cgrp_join_state_names[rkcg->rkcg_join_state]);

                switch ((int)rko->rko_type)
                {
                case RD_KAFKA_OP_CALLBACK:
                        rd_kafka_op_call(rkcg->rkcg_rk, rko);
                        break;

		case RD_KAFKA_OP_NAME:
			/* Return the currently assigned member id. */
			rd_kafka_op_reply(rko, 0,
					  rkcg->rkcg_member_id ?
					  RD_KAFKAP_STR_DUP(rkcg->
							    rkcg_member_id) :
					  NULL, 0, rd_free);
			break;

                case RD_KAFKA_OP_OFFSET_FETCH:
                        if (rkcg->rkcg_state != RD_KAFKA_CGRP_STATE_UP ||
                            (rkcg->rkcg_flags & RD_KAFKA_CGRP_F_TERMINATE)) {
                                rd_kafka_op_handle_OffsetFetch(
                                        rkb, RD_KAFKA_RESP_ERR__WAIT_COORD,
                                        NULL, NULL, rko);
                                rko = NULL; /* rko freed by handler */
                                break;
                        }

                        rd_kafka_OffsetFetchRequest(
                                rkb, 1,
                                (rd_kafka_topic_partition_list_t *)
                                rko->rko_payload,
                                &rkcg->rkcg_ops,
                                rd_kafka_op_handle_OffsetFetch, rko);
                        rko = NULL; /* rko now owned by request */
                        break;

                case RD_KAFKA_OP_OFFSET_FETCH | RD_KAFKA_OP_REPLY:
                        /* Reply from an OffsetFetch request. */
                        if (rko->rko_err) {
				rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "OFFSET",
					     "Offset fetch error: %s",
					     rd_kafka_err2str(rko->rko_err));

                                rd_kafka_topic_partition_list_destroy(
                                        rko->rko_payload);

				rd_kafka_q_op_err(&rkcg->rkcg_q, rko->rko_err,
						  0,
						  "Failed to fetch offsets: %s",
						  rd_kafka_err2str(rko->
								   rko_err));
                                break;
                        }

                        rd_kafka_cgrp_partitions_fetch_start(
                                rkcg,
                                (rd_kafka_topic_partition_list_t *)
                                rko->rko_payload,
                                1 /* usable offsets */);
                        break;


                case RD_KAFKA_OP_PARTITION_JOIN:
                        rd_kafka_cgrp_partition_add(rkcg, rktp);

                        /* If terminating tell the partition to leave */
                        if (rkcg->rkcg_flags & RD_KAFKA_CGRP_F_TERMINATE) {
                                rko2 = rd_kafka_op_new(RD_KAFKA_OP_FETCH_STOP);
                                rko2->rko_rktp = rd_kafka_toppar_keep(rktp);
                                rko2->rko_version = rd_atomic32_add(&rktp->rktp_version, 1);
                                rd_kafka_q_enq(&rktp->rktp_ops, rko2);
                        }
                        break;

                case RD_KAFKA_OP_PARTITION_LEAVE:
                        rd_kafka_cgrp_partition_del(rkcg, rktp);
                        break;

                case RD_KAFKA_OP_FETCH_STOP:
                        /* Reply from toppar FETCH_STOP */
                        rd_kafka_assert(rkcg->rkcg_rk,
                                        rkcg->rkcg_wait_unassign_cnt > 0);
                        rkcg->rkcg_wait_unassign_cnt--;

                        rd_kafka_assert(rkcg->rkcg_rk, rktp->rktp_assigned);
			rd_kafka_assert(rkcg->rkcg_rk,
					rkcg->rkcg_assigned_cnt > 0);
                        rktp->rktp_assigned = 0;
			rkcg->rkcg_assigned_cnt--;

                        if (rkcg->rkcg_wait_unassign_cnt > 0)
                                break;

                        /* All unassigned toppars now stopped and commit done:
                         * transition to the next state. */
                        if (rkcg->rkcg_join_state ==
                            RD_KAFKA_CGRP_JOIN_STATE_WAIT_UNASSIGN)
                                rd_kafka_cgrp_check_unassign_done(rkcg);
                        break;

                case RD_KAFKA_OP_OFFSET_COMMIT:
                        /* Trigger an offsets commit.
                         * 'rko->rko_payload' is a list of offsets to commit,
                         * if NULL it will default to the current assignment. */
                        if (!rko->rko_payload && rkcg->rkcg_assignment) {
				int valid_offset_cnt;

                                rd_kafka_op_payload_set(
                                        rko,
                                        rd_kafka_topic_partition_list_copy(
                                                rkcg->rkcg_assignment),
                                        (void *)
                                        rd_kafka_topic_partition_list_destroy);

				valid_offset_cnt =
					rd_kafka_topic_partition_list_set_offsets(
						rkcg->rkcg_rk,
						rko->rko_payload, 1, 0,
						1 /* is commit */);

				if (valid_offset_cnt == 0) {
					rd_kafka_dbg(
						rkcg->rkcg_rk, CGRP,
						"COMMIT",
						"Group \"%s\": "
						"no valid offsets to commit",
						rkcg->rkcg_group_id->str);

					rd_kafka_op_handle_OffsetCommit(
						rkcg->rkcg_rkb,
						RD_KAFKA_RESP_ERR__NO_OFFSET,
						NULL, NULL, rko);
                                        rko = NULL; /* freed by op_handle */
					break;
				}
                        }

                        rd_kafka_cgrp_offsets_commit(
                                rkcg, rkb, rko,
                                (rd_kafka_topic_partition_list_t *)
                                rko->rko_payload);
                        rko = NULL; /* rko now owned by request */
                        break;

                case RD_KAFKA_OP_OFFSET_COMMIT | RD_KAFKA_OP_REPLY:
                        /* Reply for an OffsetCommitRequest */
                        rd_kafka_cgrp_handle_OffsetCommit(
                                rkcg, rko->rko_err,
                                (rd_kafka_topic_partition_list_t *)
                                rko->rko_payload);
                        /* handle_OffsetCommit() takes ownership of
                         * the offsets list. */
                        rko->rko_payload = NULL;
                        break;

                case RD_KAFKA_OP_COORD_QUERY:
                        rd_kafka_cgrp_coord_query(rkcg, rkb,
                                                  rko->rko_err ?
                                                  rd_kafka_err2str(rko->
                                                                   rko_err):
                                                  "from op");
                        break;

                case RD_KAFKA_OP_SUBSCRIBE:
                        /* New atomic subscription (may be NULL) */
                        err = rd_kafka_cgrp_subscribe(
                                rkcg,
                                (rd_kafka_topic_partition_list_t *)
                                rko->rko_payload);
                        if (!err)
                                rko->rko_payload = NULL; /* list owned by rkcg */
                        rd_kafka_op_reply(rko, err, NULL, 0, NULL);
                        break;

                case RD_KAFKA_OP_ASSIGN:
                        /* New atomic assignment (payload != NULL),
			 * or unassignment (payload == NULL) */
                        rd_kafka_cgrp_assign(rkcg,
					     (rd_kafka_topic_partition_list_t *)
					     rko->rko_payload);
                        rd_kafka_op_reply(rko, 0, NULL, 0, NULL);
                        break;

                case RD_KAFKA_OP_GET_SUBSCRIPTION:
                {
                        rd_kafka_topic_partition_list_t *rktparlist = NULL;

                        if (rkcg->rkcg_subscription)
                                rktparlist = rd_kafka_topic_partition_list_copy(rkcg->rkcg_subscription);
                        rd_kafka_op_reply(rko, 0, rktparlist, 0,
                                          (void *)rd_kafka_topic_partition_list_destroy);
                }
                break;

                case RD_KAFKA_OP_GET_ASSIGNMENT:
                {
                        rd_kafka_topic_partition_list_t *rktparlist = NULL;

                        if (rkcg->rkcg_assignment)
                                rktparlist = rd_kafka_topic_partition_list_copy(rkcg->rkcg_assignment);
                        rd_kafka_op_reply(rko, 0, rktparlist, 0,
                                          (void *)rd_kafka_topic_partition_list_destroy);
                }
                break;

                case RD_KAFKA_OP_RESTART:
                        /* On coordinator rebalance: 
                         *  - stop fetching data
                         *  - commit offsets
                         *  - send JoinGroupRequest
                         */
                        // FIXME, see handle_Heartbeat
                        break;

                case RD_KAFKA_OP_TERMINATE:
                        rd_kafka_cgrp_terminate0(rkcg, rko);
                        rko = NULL; /* terminate0() takes ownership */
                        break;

                case RD_KAFKA_OP_RECV_BUF:
                        /* Handle response */
                        rd_kafka_buf_handle_op(rko);
                        break;

                default:
                        rd_kafka_assert(rkcg->rkcg_rk, !*"unknown type");
                        break;
                }

                if (rko)
                        rd_kafka_op_destroy(rko);

                /* Bail out if managing broker changed, we must not
                 * process any more cgrp ops in this broker thread. */
                if (rkb->rkb_cgrp != rkcg)
                        break;
        }
}


/**
 * Client group's join state handling
 */
static void rd_kafka_cgrp_join_state_serve (rd_kafka_cgrp_t *rkcg,
                                            rd_kafka_broker_t *rkb) {

        if (0) // FIXME
        rd_rkb_dbg(rkb, CGRP, "JOINFSM",
                   "Group \"%s\" in join state %s with%s subscription",
                   rkcg->rkcg_group_id->str,
                   rd_kafka_cgrp_join_state_names[rkcg->rkcg_join_state],
                   rkcg->rkcg_subscription ? "" : "out");

        switch (rkcg->rkcg_join_state)
        {
        case RD_KAFKA_CGRP_JOIN_STATE_INIT:
                /* If we have a subscription start the join process. */
                if (!rkcg->rkcg_subscription)
                        break;

                if (rd_interval(&rkcg->rkcg_join_intvl, 1000*1000, 0) > 0)
                        rd_kafka_cgrp_join(rkcg);
                break;

        case RD_KAFKA_CGRP_JOIN_STATE_WAIT_JOIN:
                break;

        case RD_KAFKA_CGRP_JOIN_STATE_WAIT_METADATA:
                break;

        case RD_KAFKA_CGRP_JOIN_STATE_WAIT_SYNC:
                break;

        case RD_KAFKA_CGRP_JOIN_STATE_WAIT_REBALANCE_CB:
                break;

        case RD_KAFKA_CGRP_JOIN_STATE_WAIT_UNASSIGN:
		break;

        case RD_KAFKA_CGRP_JOIN_STATE_ASSIGNED:
                if (rd_interval(&rkcg->rkcg_heartbeat_intvl,
                                rkcg->rkcg_rk->rk_conf.
                                group_heartbeat_intvl_ms * 1000, 0) > 0)
                        rd_kafka_cgrp_heartbeat(rkcg, rkb);
                break;
        }

}
/**
 * Client group handling.
 * Called from broker thread to serve the operational aspects of a cgrp.
 */
void rd_kafka_cgrp_serve (rd_kafka_cgrp_t *rkcg, rd_kafka_broker_t *rkb) {

        if (rkb->rkb_source == RD_KAFKA_INTERNAL ||
            rkb->rkb_state < RD_KAFKA_BROKER_STATE_UP) {
                /* Broker is not up, Try reassigning management
                 * to other broker */
                if (rd_kafka_cgrp_reassign_broker(rkcg))
                        return; /* Reassignment took place, we are no longer
                                 * managing this cgrp. */
        }


        rd_kafka_cgrp_op_serve(rkcg, rkb);

        /* Bail out if we're no longer the managing broker, or terminating. */
        if (unlikely(rkb->rkb_cgrp != rkcg ||
                     rd_kafka_terminating(rkcg->rkcg_rk)))
                return;

        switch (rkcg->rkcg_state)
        {
        case RD_KAFKA_CGRP_STATE_TERM:
                break;

        case RD_KAFKA_CGRP_STATE_INIT:
                rd_kafka_cgrp_set_state(rkcg, RD_KAFKA_CGRP_STATE_QUERY_COORD);
                /* FALLTHRU */

        case RD_KAFKA_CGRP_STATE_QUERY_COORD:
                /* Query for coordinator. */
                if (rd_interval(&rkcg->rkcg_coord_query_intvl, 500*1000, 0) > 0)
                        rd_kafka_cgrp_coord_query(rkcg, rkb,
                                                  "intervaled in "
                                                  "state query-coord");
                break;

        case RD_KAFKA_CGRP_STATE_WAIT_COORD:
                /* Waiting for GroupCoordinator response */
                break;

        case RD_KAFKA_CGRP_STATE_WAIT_BROKER:
                /* See if the group should be reassigned to another broker. */
                if (rd_kafka_cgrp_reassign_broker(rkcg))
                        break;

                /* Coordinator query */
                if (rd_interval(&rkcg->rkcg_coord_query_intvl,
                                1000*1000, 0) > 0)
                        rd_kafka_cgrp_coord_query(rkcg, rkb,
                                                  "intervaled in "
                                                  "state wait-broker");
                break;

        case RD_KAFKA_CGRP_STATE_WAIT_BROKER_TRANSPORT:
                /* Waiting for broker transport to come up */
                if (rkb->rkb_state < RD_KAFKA_BROKER_STATE_UP) {
                        /* FIXME: Query another broker */
                } else
                        rd_kafka_cgrp_set_state(rkcg, RD_KAFKA_CGRP_STATE_UP);
                break;

        case RD_KAFKA_CGRP_STATE_UP:
                /* Relaxed coordinator queries. */
                if (rd_interval(&rkcg->rkcg_coord_query_intvl,
                                rkcg->rkcg_rk->rk_conf.
                                coord_query_intvl_ms * 1000, 0) > 0)
                        rd_kafka_cgrp_coord_query(rkcg, rkb,
                                                  "intervaled in state up");

                rd_kafka_cgrp_join_state_serve(rkcg, rkb);
                break;

        }
}





/**
 * Send an op to a cgrp.
 *
 * Locality: any thread
 */
void rd_kafka_cgrp_op (rd_kafka_cgrp_t *rkcg, rd_kafka_toppar_t *rktp,
                       rd_kafka_q_t *replyq, rd_kafka_op_type_t type,
                       rd_kafka_resp_err_t err) {
        rd_kafka_op_t *rko;

        rko = rd_kafka_op_new(type);
        rko->rko_err = err;
        if (replyq) {
                rko->rko_replyq = replyq;
                rd_kafka_q_keep(replyq);
        }
        if (rktp)
                rko->rko_rktp = rd_kafka_toppar_keep(rktp);

        rd_kafka_q_enq(&rkcg->rkcg_ops, rko);
}





/**
 * Removes any matching patterns
 */
rd_kafka_resp_err_t rd_kafka_cgrp_topic_pattern_del (rd_kafka_cgrp_t *rkcg,
                                                     const char *pattern) {
        int cnt = rd_kafka_pattern_list_remove(&rkcg->rkcg_whitelist, pattern);
        return cnt > 0 ? 0 : RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC;
}

/**
 * Adds topic matching pattern (regexp) to cgrp.
 */
rd_kafka_resp_err_t rd_kafka_cgrp_topic_pattern_add (rd_kafka_cgrp_t *rkcg,
                                                     const char *pattern) {
        char errstr[256];

        rd_kafka_cgrp_lock(rkcg);
        if (rd_kafka_pattern_list_append(&rkcg->rkcg_whitelist, pattern,
                                         errstr, sizeof(errstr)) == -1) {
                rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "PATTERN",
                             "Group \"%.*s\": failed to add pattern \"%s\": %s",
                             RD_KAFKAP_STR_PR(rkcg->rkcg_group_id),
                             pattern, errstr);
                rd_kafka_cgrp_unlock(rkcg);
                return RD_KAFKA_RESP_ERR__INVALID_ARG;
        }
        rd_kafka_cgrp_unlock(rkcg);

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}


/**
 * Checks if the given topic name matches a whitelisted pattern.
 * It is assumed that the global blacklist has already been checked.
 *
 * If a topic is matched it is added to the cgrp (async).
 *
 * Locality: any thread
 */
int rd_kafka_cgrp_topic_check (rd_kafka_cgrp_t *rkcg, const char *topic) {
        int matched;
        shptr_rd_kafka_itopic_t *s_rkt;
        rd_kafka_itopic_t *rkt;
        rd_kafka_resp_err_t err;
        rd_kafka_t *rk = rkcg->rkcg_rk;
        int existing;

        rd_kafka_cgrp_lock(rkcg);
        matched = rd_kafka_pattern_match(&rkcg->rkcg_whitelist, topic);
        rd_kafka_cgrp_unlock(rkcg);

        if (!matched)
                return matched;

        s_rkt = rd_kafka_topic_new0(rk, topic,
                                    rk->rk_conf.topic_conf ?
                                    rd_kafka_topic_conf_dup(rk->rk_conf.
                                                            topic_conf)
                                    : NULL, &existing, 1/*lock*/);

        if (!s_rkt) {
                rd_kafka_dbg(rkcg->rkcg_rk, TOPIC, "SUBSCRIBE",
                             "Group \"%.*s\": "
                             "Failed to create whitelist-matched topic \"%s\": "
                             "%s",
                             RD_KAFKAP_STR_PR(rkcg->rkcg_group_id), topic,
                             rd_kafka_err2str(rd_kafka_errno2err(errno)));
                return -1;
        }

        rkt = rd_kafka_topic_s2i(s_rkt);

        if (rkt->rkt_autosubscribe_rkt) {
                /* Topic is already auto-subscribed */
                rd_kafka_topic_destroy0(s_rkt);
                return 0;
        }

        if ((err = rd_kafka_subscribe_rkt(rkt))) {
                if (err != RD_KAFKA_RESP_ERR__EXISTING_SUBSCRIPTION)
                        rd_kafka_dbg(rkcg->rkcg_rk, TOPIC, "SUBSCRIBE",
                                     "Group \"%.*s\": "
                                     "failed to subscribe to "
                                     "whitelits-matched topic \"%s\": %s",
                                     RD_KAFKAP_STR_PR(rkcg->rkcg_group_id),
                                     topic, rd_kafka_err2str(err));
                rd_kafka_topic_destroy0(s_rkt);
                return -1;
        }

        rd_kafka_dbg(rkcg->rkcg_rk, TOPIC, "SUBSCRIBE",
                     "Group \"%.*s\": "
                     "subscribing to whitelits-matched topic \"%s\"",
                     RD_KAFKAP_STR_PR(rkcg->rkcg_group_id), topic);

        rkt->rkt_autosubscribe_rkt = s_rkt;

        /* Query for the topic leader (async) */
        if (!existing)
                rd_kafka_topic_leader_query(rk, rkt);



        return 0;
}



void rd_kafka_cgrp_set_member_id (rd_kafka_cgrp_t *rkcg, const char *member_id){
        if (rkcg->rkcg_member_id && member_id &&
            !rd_kafkap_str_cmp_str(rkcg->rkcg_member_id, member_id))
                return; /* No change */

        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "MEMBERID",
                     "Group \"%.*s\": updating member id \"%s\" -> \"%s\"",
                     RD_KAFKAP_STR_PR(rkcg->rkcg_group_id),
                     rkcg->rkcg_member_id ?
                     rkcg->rkcg_member_id->str : "(not-set)",
                     member_id ? member_id : "(not-set)");

        if (rkcg->rkcg_member_id) {
                rd_kafkap_str_destroy(rkcg->rkcg_member_id);
                rkcg->rkcg_member_id = NULL;
        }

        if (member_id)
                rkcg->rkcg_member_id = rd_kafkap_str_new(member_id, -1);
}



static void
rd_kafka_cgrp_assignor_run (rd_kafka_cgrp_t *rkcg,
                            const char *protocol_name,
                            rd_kafka_metadata_t *metadata,
                            rd_kafka_group_member_t *members,
                            int member_cnt) {
        rd_kafka_resp_err_t err = 0;
        char errstr[512];

        *errstr = '\0';

        /* Run assignor */
        err = rd_kafka_assignor_run(rkcg, protocol_name, metadata,
                                    members, member_cnt,
                                    errstr, sizeof(errstr));

        if (err) {
                if (!*errstr)
                        rd_snprintf(errstr, sizeof(errstr), "%s",
                                    rd_kafka_err2str(err));
                goto err;
        }

        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "ASSIGNOR",
                     "Group \"%s\": \"%s\" assignor run for %d member(s)",
                     rkcg->rkcg_group_id->str, protocol_name, member_cnt);

        /* Respond to broker with assignment set or error */
        rd_kafka_SyncGroupRequest(rkcg->rkcg_rkb,
                                  rkcg->rkcg_group_id, rkcg->rkcg_generation_id,
                                  rkcg->rkcg_member_id,
                                  members, err ? 0 : member_cnt,
                                  &rkcg->rkcg_ops,
                                  rd_kafka_handle_SyncGroup, rkcg);
        return;

err:
        rd_kafka_log(rkcg->rkcg_rk, LOG_ERR, "ASSIGNOR",
                     "Group \"%s\": failed to run assignor \"%s\" for "
                     "%d member(s): %s",
                     rkcg->rkcg_group_id->str, protocol_name,
                     member_cnt, errstr);

        rd_kafka_cgrp_set_join_state(rkcg, RD_KAFKA_CGRP_JOIN_STATE_INIT);

}


void rd_kafka_cgrp_handle_Metadata (rd_kafka_cgrp_t *rkcg,
                                    rd_kafka_resp_err_t err,
                                    rd_kafka_metadata_t *md) {

        if (rkcg->rkcg_join_state != RD_KAFKA_CGRP_JOIN_STATE_WAIT_METADATA)
                return;

        rd_kafka_cgrp_assignor_run(rkcg,
                                   rkcg->rkcg_group_leader.protocol,
                                   md,
                                   rkcg->rkcg_group_leader.members,
                                   rkcg->rkcg_group_leader.member_cnt);
}


void rd_kafka_cgrp_handle_SyncGroup (rd_kafka_cgrp_t *rkcg,
                                     rd_kafka_resp_err_t err,
                                     const rd_kafkap_bytes_t *member_state) {
        rd_kafka_buf_t *rkbuf = NULL;
        rd_kafka_topic_partition_list_t *assignment;
        const int log_decode_errors = 1;
        int16_t Version;
        int32_t TopicCnt;
        rd_kafkap_bytes_t UserData;
        rd_kafka_group_member_t rkgm;

        if (err)
                goto err;


        /* Parse assignment from MemberState */
        rkbuf = rd_kafka_buf_new_shadow(member_state->data,
                                        RD_KAFKAP_BYTES_LEN(member_state));

        rd_kafka_buf_read_i16(rkbuf, &Version);
        rd_kafka_buf_read_i32(rkbuf, &TopicCnt);

        if (TopicCnt > 10000) {
                err = RD_KAFKA_RESP_ERR__BAD_MSG;
                goto err;
        }

        assignment = rd_kafka_topic_partition_list_new(TopicCnt);
        while (TopicCnt-- > 0) {
                rd_kafkap_str_t Topic;
                int32_t PartCnt;
                rd_kafka_buf_read_str(rkbuf, &Topic);
                rd_kafka_buf_read_i32(rkbuf, &PartCnt);
                while (PartCnt-- > 0) {
                        int32_t Partition;
						char *topic_name;
						RD_KAFKAP_STR_DUPA(&topic_name, &Topic);
                        rd_kafka_buf_read_i32(rkbuf, &Partition);

                        rd_kafka_topic_partition_list_add(
                                assignment, topic_name, Partition);
                }
        }

        rd_kafka_buf_read_bytes(rkbuf, &UserData);

        memset(&rkgm, 0, sizeof(rkgm));
        rkgm.rkgm_assignment = assignment;
        rkgm.rkgm_userdata = &UserData;

        /* Set the new assignment */
	rd_kafka_cgrp_handle_assignment(rkcg, assignment);

        rd_kafka_topic_partition_list_destroy(assignment);

        rkbuf->rkbuf_buf2 = NULL; /* Avoid free of underlying memory */
        rd_kafka_buf_destroy(rkbuf);
        return;
err:
        if (rkbuf) {
                rkbuf->rkbuf_buf2 = NULL; /* Avoid free of underlying memory */
                rd_kafka_buf_destroy(rkbuf);
        }

        /* FIXME: How to handle error? Propagate to app and shut down?*/
        rd_kafka_log(rkcg->rkcg_rk, LOG_ERR, "GRPSYNC",
                     "Group \"%s\": synchronization failed: %s",
                     rkcg->rkcg_group_id->str, rd_kafka_err2str(err));
        rd_kafka_cgrp_set_join_state(rkcg, RD_KAFKA_CGRP_JOIN_STATE_INIT);
}
