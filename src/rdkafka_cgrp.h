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

#include "rdinterval.h"

#include "rdkafka_assignor.h"

/**
 * Client groups implementation
 *
 * Client groups handling for a single cgrp is assigned to a single
 * rd_kafka_broker_t object at any given time.
 * The broker will call cgrp_serve() to serve its cgrps and possibly reassign
 * the cgrp to another broker in case of coordinator or broker changes.
 *
 * This means that the cgrp itself does not need to be locked since it
 * is only ever used from a single broker thread.
 *
 *
 */




/**
 * Client group
 */
typedef struct rd_kafka_cgrp_s {
        TAILQ_ENTRY(rd_kafka_cgrp_s) rkcg_rkb_link;  /* rkb_cgrps */
        const rd_kafkap_str_t    *rkcg_group_id;
        rd_kafkap_str_t          *rkcg_member_id;  /* Last assigned MemberId */
        const rd_kafkap_str_t    *rkcg_client_id;

        enum {
                /* Init state */
                RD_KAFKA_CGRP_STATE_INIT,

                /* Cgrp has been stopped. This is a final state */
                RD_KAFKA_CGRP_STATE_TERM,

                /* Query for group coordinator */
                RD_KAFKA_CGRP_STATE_QUERY_COORD,

                /* Outstanding query, awaiting response */
                RD_KAFKA_CGRP_STATE_WAIT_COORD,

                /* Wait ack from assigned cgrp manager broker thread */
                RD_KAFKA_CGRP_STATE_WAIT_BROKER,

                /* Wait for manager broker thread to connect to broker */
                RD_KAFKA_CGRP_STATE_WAIT_BROKER_TRANSPORT,

                /* Coordinator is up and manager is assigned. */
                RD_KAFKA_CGRP_STATE_UP,
        } rkcg_state;
        rd_ts_t            rkcg_ts_statechange;     /* Timestamp of last
                                                     * state change. */


        enum {
                RD_KAFKA_CGRP_JOIN_STATE_INIT,

                /* all: JoinGroupRequest sent, awaiting response. */
                RD_KAFKA_CGRP_JOIN_STATE_WAIT_JOIN,

                /* Leader: MetadataRequest sent, awaiting response. */
                RD_KAFKA_CGRP_JOIN_STATE_WAIT_METADATA,

                /* Follower: SyncGroupRequest sent, awaiting response. */
                RD_KAFKA_CGRP_JOIN_STATE_WAIT_SYNC,

                /* all: waiting for previous assignment to decommission */
                RD_KAFKA_CGRP_JOIN_STATE_WAIT_UNASSIGN,

                /* all: waiting for application's rebalance_cb to operate */
                RD_KAFKA_CGRP_JOIN_STATE_WAIT_REBALANCE_CB,

                /* all: synchronized, assigned and operational.
                 *      may be an empty assignment. */
                RD_KAFKA_CGRP_JOIN_STATE_ASSIGNED,
        } rkcg_join_state;

        /* State when group leader */
        struct {
                char *protocol;
                rd_kafka_group_member_t *members;
                int member_cnt;
        } rkcg_group_leader;

        rd_kafka_q_t       rkcg_q;                  /* Application poll queue */
        rd_kafka_q_t       rkcg_ops;                /* Manager ops queue */

        mtx_t              rkcg_lock;

        int                rkcg_flags;
#define RD_KAFKA_CGRP_F_TERMINATE    0x1            /* Terminate cgrp (async) */
#define RD_KAFKA_CGRP_F_WAIT_COMMIT  0x2            /* Waiting for OffsetCommit
                                                     * to complete. */
#define RD_KAFKA_CGRP_F_WAIT_UNASSIGN 0x4           /* Waiting for unassign
						     * to complete */
#define RD_KAFKA_CGRP_F_LEAVE_ON_UNASSIGN 0x8       /* Send LeaveGroup when
						     * unassign is done */

        rd_interval_t      rkcg_coord_query_intvl;  /* Coordinator query intvl*/
        rd_interval_t      rkcg_heartbeat_intvl;    /* Heartbeat intvl */
        rd_interval_t      rkcg_join_intvl;         /* JoinGroup interval */

        TAILQ_HEAD(, rd_kafka_topic_s)  rkcg_topics;/* Topics subscribed to */

        rd_list_t          rkcg_toppars;            /* Toppars subscribed to*/

	int                rkcg_assigned_cnt;       /* Assigned partitions */

        int32_t            rkcg_coord_id;           /* Current coordinator id */

        int32_t            rkcg_generation_id;      /* Current generation id */

        rd_kafka_assignor_t *rkcg_assignor;         /* Selected partition
                                                     * assignor strategy. */

        rd_kafka_broker_t *rkcg_rkb;                /* Current handling broker,
                                                     * if the coordinator broker
                                                     * is not available this
                                                     * will be another broker
                                                     * that will handle the
                                                     * querying of coordinator
                                                     * etc.
                                                     * Broker in this sense
                                                     * is a broker_t object,
                                                     * not necessarily a
                                                     * real broker. */

        /* Current subscription */
        rd_kafka_topic_partition_list_t *rkcg_subscription;
        /* Current assignment */
        rd_kafka_topic_partition_list_t *rkcg_assignment;

        int rkcg_wait_unassign_cnt;                 /* Waiting for this number
                                                     * of partitions to be
                                                     * unassigned and
                                                     * decommissioned before
                                                     * transitioning to the
                                                     * next state. */

        /* Topic whitelist: topics to subscribe to */
        rd_kafka_pattern_list_t rkcg_whitelist;

        rd_kafka_resp_err_t rkcg_last_err;          /* Last error propagated to
                                                     * application.
                                                     * This is for silencing
                                                     * same errors. */

        rd_kafka_timer_t   rkcg_offset_commit_tmr;  /* Offset commit timer */

        rd_kafka_t        *rkcg_rk;

        rd_kafka_op_t     *rkcg_reply_rko;          /* Send reply for op
                                                     * (OP_TERMINATE)
                                                     * to this rko's queue. */
} rd_kafka_cgrp_t;




#define rd_kafka_cgrp_lock(rkcg)    mtx_lock(&(rkcg)->rkcg_lock)
#define rd_kafka_cgrp_unlock(rkcg)  mtx_unlock(&(rkcg)->rkcg_lock)

/* Check if broker is the coordinator */
#define RD_KAFKA_CGRP_BROKER_IS_COORD(rkcg,rkb)          \
        ((rkcg)->rkcg_coord_id != -1 &&                  \
         (rkcg)->rkcg_coord_id == (rkb)->rkb_nodeid)

extern const char *rd_kafka_cgrp_state_names[];

void rd_kafka_cgrp_destroy_final (rd_kafka_cgrp_t *rkcg);
rd_kafka_cgrp_t *rd_kafka_cgrp_new (rd_kafka_t *rk,
                                    const rd_kafkap_str_t *group_id,
                                    const rd_kafkap_str_t *client_id);
void rd_kafka_cgrp_serve (rd_kafka_cgrp_t *rkcg, rd_kafka_broker_t *rkb);
void rd_kafka_cgrp_assign_broker (rd_kafka_cgrp_t *rkcg,
                                  rd_kafka_broker_t *rkb);


void rd_kafka_cgrp_op (rd_kafka_cgrp_t *rkcg, rd_kafka_toppar_t *rktp,
                       rd_kafka_q_t *replyq, rd_kafka_op_type_t type,
                       rd_kafka_resp_err_t err);
void rd_kafka_cgrp_terminate (rd_kafka_cgrp_t *rkcg, rd_kafka_q_t *replyq);


rd_kafka_resp_err_t rd_kafka_cgrp_topic_pattern_del (rd_kafka_cgrp_t *rkcg,
                                                     const char *pattern);
rd_kafka_resp_err_t rd_kafka_cgrp_topic_pattern_add (rd_kafka_cgrp_t *rkcg,
                                                     const char *pattern);

int rd_kafka_cgrp_topic_check (rd_kafka_cgrp_t *rkcg, const char *topic);

void rd_kafka_cgrp_set_member_id (rd_kafka_cgrp_t *rkcg, const char *member_id);

void rd_kafka_cgrp_group_leader_reset (rd_kafka_cgrp_t *rkcg);

void rd_kafka_cgrp_handle_heartbeat_error (rd_kafka_cgrp_t *rkcg,
					   rd_kafka_resp_err_t err);

void rd_kafka_cgrp_handle_Metadata (rd_kafka_cgrp_t *rkcg,
                                    rd_kafka_resp_err_t err,
                                    rd_kafka_metadata_t *md);
void rd_kafka_cgrp_handle_SyncGroup (rd_kafka_cgrp_t *rkcg,
                                     rd_kafka_resp_err_t err,
                                     const rd_kafkap_bytes_t *member_state);
void rd_kafka_cgrp_set_join_state (rd_kafka_cgrp_t *rkcg, int join_state);

#define rd_kafka_cgrp_get(rk) ((rk)->rk_cgrp)
