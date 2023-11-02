/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2020-2022, Magnus Edenhill
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
 * Mocks
 *
 */

#include "rdkafka_int.h"
#include "rdbuf.h"
#include "rdkafka_mock_int.h"


static const char *rd_kafka_mock_cgrp_generic_state_names[] = {
    "Empty", "Joining", "Syncing", "Rebalancing", "Up"};


static void
rd_kafka_mock_cgrp_generic_rebalance(rd_kafka_mock_cgrp_generic_t *mcgrp,
                                     const char *reason);
static void rd_kafka_mock_cgrp_generic_member_destroy(
    rd_kafka_mock_cgrp_generic_t *mcgrp,
    rd_kafka_mock_cgrp_generic_member_t *member);

static void
rd_kafka_mock_cgrp_generic_set_state(rd_kafka_mock_cgrp_generic_t *mcgrp,
                                     unsigned int new_state,
                                     const char *reason) {
        if (mcgrp->state == new_state)
                return;

        rd_kafka_dbg(mcgrp->cluster->rk, MOCK, "MOCK",
                     "Mock consumer group %s with %d member(s) "
                     "changing state %s -> %s: %s",
                     mcgrp->id, mcgrp->member_cnt,
                     rd_kafka_mock_cgrp_generic_state_names[mcgrp->state],
                     rd_kafka_mock_cgrp_generic_state_names[new_state], reason);

        mcgrp->state = new_state;
}


/**
 * @brief Mark member as active (restart session timer)
 */
void rd_kafka_mock_cgrp_generic_member_active(
    rd_kafka_mock_cgrp_generic_t *mcgrp,
    rd_kafka_mock_cgrp_generic_member_t *member) {
        rd_kafka_dbg(mcgrp->cluster->rk, MOCK, "MOCK",
                     "Marking mock consumer group member %s as active",
                     member->id);
        member->ts_last_activity = rd_clock();
}


/**
 * @brief Verify that the protocol request is valid in the current state.
 *
 * @param member may be NULL.
 */
rd_kafka_resp_err_t rd_kafka_mock_cgrp_generic_check_state(
    rd_kafka_mock_cgrp_generic_t *mcgrp,
    rd_kafka_mock_cgrp_generic_member_t *member,
    const rd_kafka_buf_t *request,
    int32_t generation_id) {
        int16_t ApiKey              = request->rkbuf_reqhdr.ApiKey;
        rd_bool_t has_generation_id = ApiKey == RD_KAFKAP_SyncGroup ||
                                      ApiKey == RD_KAFKAP_Heartbeat ||
                                      ApiKey == RD_KAFKAP_OffsetCommit;

        if (has_generation_id && generation_id != mcgrp->generation_id)
                return RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION;

        if (ApiKey == RD_KAFKAP_OffsetCommit && !member)
                return RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID;

        switch (mcgrp->state) {
        case RD_KAFKA_MOCK_CGRP_STATE_EMPTY:
                if (ApiKey == RD_KAFKAP_JoinGroup)
                        return RD_KAFKA_RESP_ERR_NO_ERROR;
                break;

        case RD_KAFKA_MOCK_CGRP_STATE_JOINING:
                if (ApiKey == RD_KAFKAP_JoinGroup ||
                    ApiKey == RD_KAFKAP_LeaveGroup)
                        return RD_KAFKA_RESP_ERR_NO_ERROR;
                else
                        return RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS;

        case RD_KAFKA_MOCK_CGRP_STATE_SYNCING:
                if (ApiKey == RD_KAFKAP_SyncGroup ||
                    ApiKey == RD_KAFKAP_JoinGroup ||
                    ApiKey == RD_KAFKAP_LeaveGroup)
                        return RD_KAFKA_RESP_ERR_NO_ERROR;
                else
                        return RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS;

        case RD_KAFKA_MOCK_CGRP_STATE_REBALANCING:
                if (ApiKey == RD_KAFKAP_JoinGroup ||
                    ApiKey == RD_KAFKAP_LeaveGroup ||
                    ApiKey == RD_KAFKAP_OffsetCommit)
                        return RD_KAFKA_RESP_ERR_NO_ERROR;
                else
                        return RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS;

        case RD_KAFKA_MOCK_CGRP_STATE_UP:
                if (ApiKey == RD_KAFKAP_JoinGroup ||
                    ApiKey == RD_KAFKAP_LeaveGroup ||
                    ApiKey == RD_KAFKAP_Heartbeat ||
                    ApiKey == RD_KAFKAP_OffsetCommit)
                        return RD_KAFKA_RESP_ERR_NO_ERROR;
                break;
        }

        return RD_KAFKA_RESP_ERR_INVALID_REQUEST;
}


/**
 * @brief Set a member's assignment (from leader's SyncGroupRequest)
 */
void rd_kafka_mock_cgrp_generic_member_assignment_set(
    rd_kafka_mock_cgrp_generic_t *mcgrp,
    rd_kafka_mock_cgrp_generic_member_t *member,
    const rd_kafkap_bytes_t *Metadata) {
        if (member->assignment) {
                rd_assert(mcgrp->assignment_cnt > 0);
                mcgrp->assignment_cnt--;
                rd_kafkap_bytes_destroy(member->assignment);
                member->assignment = NULL;
        }

        if (Metadata) {
                mcgrp->assignment_cnt++;
                member->assignment = rd_kafkap_bytes_copy(Metadata);
        }
}


/**
 * @brief Sync done (successfully) or failed, send responses back to members.
 */
static void
rd_kafka_mock_cgrp_generic_sync_done(rd_kafka_mock_cgrp_generic_t *mcgrp,
                                     rd_kafka_resp_err_t err) {
        rd_kafka_mock_cgrp_generic_member_t *member;

        TAILQ_FOREACH(member, &mcgrp->members, link) {
                rd_kafka_buf_t *resp;

                if ((resp = member->resp)) {
                        member->resp = NULL;
                        rd_assert(resp->rkbuf_reqhdr.ApiKey ==
                                  RD_KAFKAP_SyncGroup);

                        rd_kafka_buf_write_i16(resp, err); /* ErrorCode */
                        /* MemberState */
                        rd_kafka_buf_write_kbytes(
                            resp, !err ? member->assignment : NULL);
                }

                rd_kafka_mock_cgrp_generic_member_assignment_set(mcgrp, member,
                                                                 NULL);

                if (member->conn) {
                        rd_kafka_mock_connection_set_blocking(member->conn,
                                                              rd_false);
                        if (resp)
                                rd_kafka_mock_connection_send_response(
                                    member->conn, resp);
                } else if (resp) {
                        /* Member has disconnected. */
                        rd_kafka_buf_destroy(resp);
                }
        }
}


/**
 * @brief Check if all members have sent SyncGroupRequests, if so, propagate
 *        assignment to members.
 */
static void
rd_kafka_mock_cgrp_generic_sync_check(rd_kafka_mock_cgrp_generic_t *mcgrp) {

        rd_kafka_dbg(mcgrp->cluster->rk, MOCK, "MOCK",
                     "Mock consumer group %s: awaiting %d/%d syncing members "
                     "in state %s",
                     mcgrp->id, mcgrp->assignment_cnt, mcgrp->member_cnt,
                     rd_kafka_mock_cgrp_generic_state_names[mcgrp->state]);

        if (mcgrp->assignment_cnt < mcgrp->member_cnt)
                return;

        rd_kafka_mock_cgrp_generic_sync_done(mcgrp, RD_KAFKA_RESP_ERR_NO_ERROR);
        rd_kafka_mock_cgrp_generic_set_state(mcgrp, RD_KAFKA_MOCK_CGRP_STATE_UP,
                                             "all members synced");
}


/**
 * @brief Member has sent SyncGroupRequest and is waiting for a response,
 *        which will be sent when the all group member SyncGroupRequest are
 *        received.
 */
rd_kafka_resp_err_t rd_kafka_mock_cgrp_generic_member_sync_set(
    rd_kafka_mock_cgrp_generic_t *mcgrp,
    rd_kafka_mock_cgrp_generic_member_t *member,
    rd_kafka_mock_connection_t *mconn,
    rd_kafka_buf_t *resp) {

        if (mcgrp->state != RD_KAFKA_MOCK_CGRP_STATE_SYNCING)
                return RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS; /* FIXME */

        rd_kafka_mock_cgrp_generic_member_active(mcgrp, member);

        rd_assert(!member->resp);

        member->resp = resp;
        member->conn = mconn;
        rd_kafka_mock_connection_set_blocking(member->conn, rd_true);

        /* Check if all members now have an assignment, if so, send responses */
        rd_kafka_mock_cgrp_generic_sync_check(mcgrp);

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}


/**
 * @brief Member is explicitly leaving the group (through LeaveGroupRequest)
 */
rd_kafka_resp_err_t rd_kafka_mock_cgrp_generic_member_leave(
    rd_kafka_mock_cgrp_generic_t *mcgrp,
    rd_kafka_mock_cgrp_generic_member_t *member) {

        rd_kafka_dbg(mcgrp->cluster->rk, MOCK, "MOCK",
                     "Member %s is leaving group %s", member->id, mcgrp->id);

        rd_kafka_mock_cgrp_generic_member_destroy(mcgrp, member);

        rd_kafka_mock_cgrp_generic_rebalance(mcgrp, "explicit member leave");

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}

/**
 * @brief Destroys/frees an array of protocols, including the array itself.
 */
void rd_kafka_mock_cgrp_generic_protos_destroy(
    rd_kafka_mock_cgrp_generic_proto_t *protos,
    int proto_cnt) {
        int i;

        for (i = 0; i < proto_cnt; i++) {
                rd_free(protos[i].name);
                if (protos[i].metadata)
                        rd_free(protos[i].metadata);
        }

        rd_free(protos);
}

static void rd_kafka_mock_cgrp_generic_rebalance_timer_restart(
    rd_kafka_mock_cgrp_generic_t *mcgrp,
    int timeout_ms);

/**
 * @brief Elect consumer group leader and send JoinGroup responses
 */
static void
rd_kafka_mock_cgrp_generic_elect_leader(rd_kafka_mock_cgrp_generic_t *mcgrp) {
        rd_kafka_mock_cgrp_generic_member_t *member;

        rd_assert(mcgrp->state == RD_KAFKA_MOCK_CGRP_STATE_JOINING);
        rd_assert(!TAILQ_EMPTY(&mcgrp->members));

        mcgrp->generation_id++;

        /* Elect a leader deterministically if the group.instance.id is
         * available, using the lexicographic order of group.instance.ids.
         * This is not how it's done on a real broker, which uses the first
         * member joined. But we use a determinstic method for better testing,
         * (in case we want to enforce a some consumer to be the group leader).
         * If group.instance.id is not specified for any consumer, we use the
         * first one joined, similar to the real broker. */
        mcgrp->leader = NULL;
        TAILQ_FOREACH(member, &mcgrp->members, link) {
                if (!mcgrp->leader)
                        mcgrp->leader = member;
                else if (mcgrp->leader->group_instance_id &&
                         member->group_instance_id &&
                         (rd_strcmp(mcgrp->leader->group_instance_id,
                                    member->group_instance_id) > 0))
                        mcgrp->leader = member;
        }

        rd_kafka_dbg(
            mcgrp->cluster->rk, MOCK, "MOCK",
            "Consumer group %s with %d member(s) is rebalancing: "
            "elected leader is %s (group.instance.id = %s), generation id %d",
            mcgrp->id, mcgrp->member_cnt, mcgrp->leader->id,
            mcgrp->leader->group_instance_id, mcgrp->generation_id);

        /* Find the most commonly supported protocol name among the members.
         * FIXME: For now we'll blindly use the first protocol of the leader. */
        if (mcgrp->protocol_name)
                rd_free(mcgrp->protocol_name);
        mcgrp->protocol_name = RD_KAFKAP_STR_DUP(mcgrp->leader->protos[0].name);

        /* Send JoinGroupResponses to all members */
        TAILQ_FOREACH(member, &mcgrp->members, link) {
                rd_bool_t is_leader = member == mcgrp->leader;
                int member_cnt      = is_leader ? mcgrp->member_cnt : 0;
                rd_kafka_buf_t *resp;
                rd_kafka_mock_cgrp_generic_member_t *member2;
                rd_kafka_mock_connection_t *mconn;

                /* Member connection has been closed, it will eventually
                 * reconnect or time out from the group. */
                if (!member->conn || !member->resp)
                        continue;
                mconn        = member->conn;
                member->conn = NULL;
                resp         = member->resp;
                member->resp = NULL;

                rd_assert(resp->rkbuf_reqhdr.ApiKey == RD_KAFKAP_JoinGroup);

                rd_kafka_buf_write_i16(resp, 0); /* ErrorCode */
                rd_kafka_buf_write_i32(resp, mcgrp->generation_id);
                rd_kafka_buf_write_str(resp, mcgrp->protocol_name, -1);
                rd_kafka_buf_write_str(resp, mcgrp->leader->id, -1);
                rd_kafka_buf_write_str(resp, member->id, -1);
                rd_kafka_buf_write_i32(resp, member_cnt);

                /* Send full member list to leader */
                if (member_cnt > 0) {
                        TAILQ_FOREACH(member2, &mcgrp->members, link) {
                                rd_kafka_buf_write_str(resp, member2->id, -1);
                                if (resp->rkbuf_reqhdr.ApiVersion >= 5)
                                        rd_kafka_buf_write_str(
                                            resp, member2->group_instance_id,
                                            -1);
                                /* FIXME: look up correct protocol name */
                                rd_assert(!rd_kafkap_str_cmp_str(
                                    member2->protos[0].name,
                                    mcgrp->protocol_name));

                                rd_kafka_buf_write_kbytes(
                                    resp, member2->protos[0].metadata);
                        }
                }

                /* Mark each member as active to avoid them timing out
                 * at the same time as a JoinGroup handler that blocks
                 * session.timeout.ms to elect a leader. */
                rd_kafka_mock_cgrp_generic_member_active(mcgrp, member);

                rd_kafka_mock_connection_set_blocking(mconn, rd_false);
                rd_kafka_mock_connection_send_response(mconn, resp);
        }

        mcgrp->last_member_cnt = mcgrp->member_cnt;

        rd_kafka_mock_cgrp_generic_set_state(mcgrp,
                                             RD_KAFKA_MOCK_CGRP_STATE_SYNCING,
                                             "leader elected, waiting for all "
                                             "members to sync");

        rd_kafka_mock_cgrp_generic_rebalance_timer_restart(
            mcgrp, mcgrp->session_timeout_ms);
}


/**
 * @brief Trigger group rebalance.
 */
static void
rd_kafka_mock_cgrp_generic_rebalance(rd_kafka_mock_cgrp_generic_t *mcgrp,
                                     const char *reason) {
        int timeout_ms;

        if (mcgrp->state == RD_KAFKA_MOCK_CGRP_STATE_JOINING)
                return; /* Do nothing, group is already rebalancing. */
        else if (mcgrp->state == RD_KAFKA_MOCK_CGRP_STATE_EMPTY)
                timeout_ms = 3000; /* First join, low timeout.
                                    * Same as group.initial.rebalance.delay.ms
                                    * on the broker. */
        else if (mcgrp->state == RD_KAFKA_MOCK_CGRP_STATE_REBALANCING &&
                 mcgrp->member_cnt == mcgrp->last_member_cnt)
                timeout_ms = 100; /* All members rejoined, quickly transition
                                   * to election. */
        else /* Let the rebalance delay be a bit shorter than the
              * session timeout so that we don't time out waiting members
              * who are also subject to the session timeout. */
                timeout_ms = mcgrp->session_timeout_ms > 1000
                                 ? mcgrp->session_timeout_ms - 1000
                                 : mcgrp->session_timeout_ms;

        if (mcgrp->state == RD_KAFKA_MOCK_CGRP_STATE_SYNCING)
                /* Abort current Syncing state */
                rd_kafka_mock_cgrp_generic_sync_done(
                    mcgrp, RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS);

        rd_kafka_mock_cgrp_generic_set_state(
            mcgrp, RD_KAFKA_MOCK_CGRP_STATE_JOINING, reason);
        rd_kafka_mock_cgrp_generic_rebalance_timer_restart(mcgrp, timeout_ms);
}

/**
 * @brief Consumer group state machine triggered by timer events.
 */
static void
rd_kafka_mock_cgrp_generic_fsm_timeout(rd_kafka_mock_cgrp_generic_t *mcgrp) {
        rd_kafka_dbg(mcgrp->cluster->rk, MOCK, "MOCK",
                     "Mock consumer group %s FSM timeout in state %s",
                     mcgrp->id,
                     rd_kafka_mock_cgrp_generic_state_names[mcgrp->state]);

        switch (mcgrp->state) {
        case RD_KAFKA_MOCK_CGRP_STATE_EMPTY:
                /* No members, do nothing */
                break;
        case RD_KAFKA_MOCK_CGRP_STATE_JOINING:
                /* Timed out waiting for more members, elect a leader */
                if (mcgrp->member_cnt > 0)
                        rd_kafka_mock_cgrp_generic_elect_leader(mcgrp);
                else
                        rd_kafka_mock_cgrp_generic_set_state(
                            mcgrp, RD_KAFKA_MOCK_CGRP_STATE_EMPTY,
                            "no members joined");
                break;

        case RD_KAFKA_MOCK_CGRP_STATE_SYNCING:
                /* Timed out waiting for all members to sync */

                /* Send error response to all waiting members */
                rd_kafka_mock_cgrp_generic_sync_done(
                    mcgrp, RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS /* FIXME */);

                rd_kafka_mock_cgrp_generic_set_state(
                    mcgrp, RD_KAFKA_MOCK_CGRP_STATE_REBALANCING,
                    "timed out waiting for all members to synchronize");
                break;

        case RD_KAFKA_MOCK_CGRP_STATE_REBALANCING:
                /* Timed out waiting for all members to Leave or re-Join */
                rd_kafka_mock_cgrp_generic_set_state(
                    mcgrp, RD_KAFKA_MOCK_CGRP_STATE_JOINING,
                    "timed out waiting for all "
                    "members to re-Join or Leave");
                break;

        case RD_KAFKA_MOCK_CGRP_STATE_UP:
                /* No fsm timers triggered in this state, see
                 * the session_tmr instead */
                break;
        }
}

static void rd_kafka_mcgrp_rebalance_timer_cb(rd_kafka_timers_t *rkts,
                                              void *arg) {
        rd_kafka_mock_cgrp_generic_t *mcgrp = arg;

        rd_kafka_mock_cgrp_generic_fsm_timeout(mcgrp);
}


/**
 * @brief Restart the rebalance timer, postponing leader election.
 */
static void rd_kafka_mock_cgrp_generic_rebalance_timer_restart(
    rd_kafka_mock_cgrp_generic_t *mcgrp,
    int timeout_ms) {
        rd_kafka_timer_start_oneshot(
            &mcgrp->cluster->timers, &mcgrp->rebalance_tmr, rd_true,
            timeout_ms * 1000, rd_kafka_mcgrp_rebalance_timer_cb, mcgrp);
}


static void rd_kafka_mock_cgrp_generic_member_destroy(
    rd_kafka_mock_cgrp_generic_t *mcgrp,
    rd_kafka_mock_cgrp_generic_member_t *member) {
        rd_assert(mcgrp->member_cnt > 0);
        TAILQ_REMOVE(&mcgrp->members, member, link);
        mcgrp->member_cnt--;

        rd_free(member->id);

        if (member->resp)
                rd_kafka_buf_destroy(member->resp);

        if (member->group_instance_id)
                rd_free(member->group_instance_id);

        rd_kafka_mock_cgrp_generic_member_assignment_set(mcgrp, member, NULL);

        rd_kafka_mock_cgrp_generic_protos_destroy(member->protos,
                                                  member->proto_cnt);

        rd_free(member);
}


/**
 * @brief Find member in group.
 */
rd_kafka_mock_cgrp_generic_member_t *rd_kafka_mock_cgrp_generic_member_find(
    const rd_kafka_mock_cgrp_generic_t *mcgrp,
    const rd_kafkap_str_t *MemberId) {
        const rd_kafka_mock_cgrp_generic_member_t *member;
        TAILQ_FOREACH(member, &mcgrp->members, link) {
                if (!rd_kafkap_str_cmp_str(MemberId, member->id))
                        return (rd_kafka_mock_cgrp_generic_member_t *)member;
        }

        return NULL;
}


/**
 * @brief Update or add member to consumer group
 */
rd_kafka_resp_err_t rd_kafka_mock_cgrp_generic_member_add(
    rd_kafka_mock_cgrp_generic_t *mcgrp,
    rd_kafka_mock_connection_t *mconn,
    rd_kafka_buf_t *resp,
    const rd_kafkap_str_t *MemberId,
    const rd_kafkap_str_t *ProtocolType,
    const rd_kafkap_str_t *GroupInstanceId,
    rd_kafka_mock_cgrp_generic_proto_t *protos,
    int proto_cnt,
    int session_timeout_ms) {
        rd_kafka_mock_cgrp_generic_member_t *member;
        rd_kafka_resp_err_t err;

        err = rd_kafka_mock_cgrp_generic_check_state(mcgrp, NULL, resp, -1);
        if (err)
                return err;

        /* Find member */
        member = rd_kafka_mock_cgrp_generic_member_find(mcgrp, MemberId);
        if (!member) {
                /* Not found, add member */
                member = rd_calloc(1, sizeof(*member));

                if (!RD_KAFKAP_STR_LEN(MemberId)) {
                        /* Generate a member id */
                        char memberid[32];
                        rd_snprintf(memberid, sizeof(memberid), "%p", member);
                        member->id = rd_strdup(memberid);
                } else
                        member->id = RD_KAFKAP_STR_DUP(MemberId);

                if (RD_KAFKAP_STR_LEN(GroupInstanceId))
                        member->group_instance_id =
                            RD_KAFKAP_STR_DUP(GroupInstanceId);

                TAILQ_INSERT_TAIL(&mcgrp->members, member, link);
                mcgrp->member_cnt++;
        }

        if (mcgrp->state != RD_KAFKA_MOCK_CGRP_STATE_JOINING)
                rd_kafka_mock_cgrp_generic_rebalance(mcgrp, "member join");

        mcgrp->session_timeout_ms = session_timeout_ms;

        if (member->protos)
                rd_kafka_mock_cgrp_generic_protos_destroy(member->protos,
                                                          member->proto_cnt);
        member->protos    = protos;
        member->proto_cnt = proto_cnt;

        rd_assert(!member->resp);
        member->resp = resp;
        member->conn = mconn;
        rd_kafka_mock_cgrp_generic_member_active(mcgrp, member);

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}

/**
 * @brief Check if any members have exceeded the session timeout.
 */
static void rd_kafka_mock_cgrp_generic_session_tmr_cb(rd_kafka_timers_t *rkts,
                                                      void *arg) {
        rd_kafka_mock_cgrp_generic_t *mcgrp = arg;
        rd_kafka_mock_cgrp_generic_member_t *member, *tmp;
        rd_ts_t now     = rd_clock();
        int timeout_cnt = 0;

        TAILQ_FOREACH_SAFE(member, &mcgrp->members, link, tmp) {
                if (member->ts_last_activity +
                        (mcgrp->session_timeout_ms * 1000) >
                    now)
                        continue;

                rd_kafka_dbg(mcgrp->cluster->rk, MOCK, "MOCK",
                             "Member %s session timed out for group %s",
                             member->id, mcgrp->id);

                rd_kafka_mock_cgrp_generic_member_destroy(mcgrp, member);
                timeout_cnt++;
        }

        if (timeout_cnt)
                rd_kafka_mock_cgrp_generic_rebalance(mcgrp, "member timeout");
}


void rd_kafka_mock_cgrp_generic_destroy(rd_kafka_mock_cgrp_generic_t *mcgrp) {
        rd_kafka_mock_cgrp_generic_member_t *member;

        TAILQ_REMOVE(&mcgrp->cluster->cgrps_generic, mcgrp, link);

        rd_kafka_timer_stop(&mcgrp->cluster->timers, &mcgrp->rebalance_tmr,
                            rd_true);
        rd_kafka_timer_stop(&mcgrp->cluster->timers, &mcgrp->session_tmr,
                            rd_true);
        rd_free(mcgrp->id);
        rd_free(mcgrp->protocol_type);
        if (mcgrp->protocol_name)
                rd_free(mcgrp->protocol_name);
        while ((member = TAILQ_FIRST(&mcgrp->members)))
                rd_kafka_mock_cgrp_generic_member_destroy(mcgrp, member);
        rd_free(mcgrp);
}


rd_kafka_mock_cgrp_generic_t *
rd_kafka_mock_cgrp_generic_find(rd_kafka_mock_cluster_t *mcluster,
                                const rd_kafkap_str_t *GroupId) {
        rd_kafka_mock_cgrp_generic_t *mcgrp;
        TAILQ_FOREACH(mcgrp, &mcluster->cgrps_generic, link) {
                if (!rd_kafkap_str_cmp_str(GroupId, mcgrp->id))
                        return mcgrp;
        }

        return NULL;
}


/**
 * @brief Find or create a generic consumer group
 */
rd_kafka_mock_cgrp_generic_t *
rd_kafka_mock_cgrp_generic_get(rd_kafka_mock_cluster_t *mcluster,
                               const rd_kafkap_str_t *GroupId,
                               const rd_kafkap_str_t *ProtocolType) {
        rd_kafka_mock_cgrp_generic_t *mcgrp;

        mcgrp = rd_kafka_mock_cgrp_generic_find(mcluster, GroupId);
        if (mcgrp)
                return mcgrp;

        /* FIXME: What to do with mismatching ProtocolTypes? */

        mcgrp = rd_calloc(1, sizeof(*mcgrp));

        mcgrp->cluster       = mcluster;
        mcgrp->id            = RD_KAFKAP_STR_DUP(GroupId);
        mcgrp->protocol_type = RD_KAFKAP_STR_DUP(ProtocolType);
        mcgrp->generation_id = 1;
        TAILQ_INIT(&mcgrp->members);
        rd_kafka_timer_start(&mcluster->timers, &mcgrp->session_tmr,
                             1000 * 1000 /*1s*/,
                             rd_kafka_mock_cgrp_generic_session_tmr_cb, mcgrp);

        TAILQ_INSERT_TAIL(&mcluster->cgrps_generic, mcgrp, link);

        return mcgrp;
}


/**
 * @brief A client connection closed, check if any generic cgrp has any state
 *        for this connection that needs to be cleared.
 */
void rd_kafka_mock_cgrps_generic_connection_closed(
    rd_kafka_mock_cluster_t *mcluster,
    rd_kafka_mock_connection_t *mconn) {
        rd_kafka_mock_cgrp_generic_t *mcgrp;

        TAILQ_FOREACH(mcgrp, &mcluster->cgrps_generic, link) {
                rd_kafka_mock_cgrp_generic_member_t *member, *tmp;
                TAILQ_FOREACH_SAFE(member, &mcgrp->members, link, tmp) {
                        if (member->conn == mconn) {
                                member->conn = NULL;
                                if (member->resp) {
                                        rd_kafka_buf_destroy(member->resp);
                                        member->resp = NULL;
                                }
                        }
                }
        }
}

/**
 * @brief Sets next target assignment and member epoch for \p member
 *        to a copy of partition list \p rktparlist,
 *        filling its topic ids if not provided, using \p cgrp cluster topics.
 *
 * @param mcgrp The consumer group containing the member.
 * @param member A consumer group member.
 * @param rktparlist Next target assignment.
 *
 * @locks mcluster->lock MUST be held.
 */
static void rd_kafka_mock_cgrp_consumer_member_next_assignment_set(
    rd_kafka_mock_cgrp_consumer_t *mcgrp,
    rd_kafka_mock_cgrp_consumer_member_t *member,
    const rd_kafka_topic_partition_list_t *rktpars) {
        rd_kafka_topic_partition_t *rktpar;
        if (member->next_assignment) {
                rd_kafka_topic_partition_list_destroy(member->next_assignment);
        }
        member->next_member_epoch++;
        member->next_assignment =
            rd_kafka_topic_partition_list_new(rktpars->size);

        /* If not present, fill topic ids using names */
        RD_KAFKA_TPLIST_FOREACH(rktpar, rktpars) {
                rd_kafka_Uuid_t topic_id =
                    rd_kafka_topic_partition_get_topic_id(rktpar);
                rd_kafka_mock_topic_t *mtopic =
                    rd_kafka_mock_topic_find(mcgrp->cluster, rktpar->topic);
                if (mtopic) {
                        rd_kafka_topic_partition_list_add_copy(
                            member->next_assignment, rktpar);
                        if (!rd_kafka_uuid_cmp(topic_id, RD_KAFKA_UUID_ZERO)) {
                                rd_kafka_topic_partition_t *rktpar_copy =
                                    &member->next_assignment
                                         ->elems[member->next_assignment->cnt -
                                                 1];
                                rd_kafka_topic_partition_set_topic_id(
                                    rktpar_copy, mtopic->id);
                        }
                }
        }
}

/**
 * @brief Sets \p member current assignment to a copy of
 *        \p current_assignment.
 *
 * @param member A consumer group member.
 * @param current_assignment Current assignment to set.
 *
 * @locks mcluster->lock MUST be held.
 */
static void rd_kafka_mock_cgrp_consumer_member_current_assignment_set(
    rd_kafka_mock_cgrp_consumer_member_t *member,
    const rd_kafka_topic_partition_list_t *current_assignment) {
        RD_IF_FREE(member->current_assignment,
                   rd_kafka_topic_partition_list_destroy);

        member->current_assignment =
            current_assignment
                ? rd_kafka_topic_partition_list_copy(current_assignment)
                : NULL;
}

/**
 * @brief Sets \p member returned assignment to a
 *        copy of \p returned_assignment.
 *
 * @param member A consumer group member.
 * @param returned_assignment Returned assignment to set.
 *
 * @locks mcluster->lock MUST be held.
 */
static void rd_kafka_mock_cgrp_consumer_member_returned_assignment_set(
    rd_kafka_mock_cgrp_consumer_member_t *member,
    const rd_kafka_topic_partition_list_t *returned_assignment) {
        if (member->returned_assignment) {
                rd_kafka_topic_partition_list_destroy(
                    member->returned_assignment);
        }
        member->returned_assignment =
            returned_assignment
                ? rd_kafka_topic_partition_list_copy(returned_assignment)
                : NULL;
}

/**
 * @brief Calculates next assignment and member epoch for a \p member,
 *        given \p current_assignment.
 *
 * @param member The group member.
 * @param current_assignment The assignment sent by the member, or NULL if it
 *                           didn't change. Must be NULL if *member_epoch is 0.
 * @param member_epoch Pointer to reported member epoch. Can be updated.
 *
 * @return The new assignment to return to the member.
 *
 * @remark The returned pointer ownership is transferred to the caller.
 *
 * @locks mcluster->lock MUST be held.
 */
rd_kafka_topic_partition_list_t *
rd_kafka_mock_cgrp_consumer_member_next_assignment(
    rd_kafka_mock_cgrp_consumer_member_t *member,
    rd_kafka_topic_partition_list_t *current_assignment,
    int *member_epoch) {
        rd_kafka_topic_partition_list_t *returned_assignment = NULL;

        // if (*member_epoch && !member->current_member_epoch) {
        //         /* FIXME: check what to do when member epoch is 0 */
        //         /* FENCED_MEMBER_EPOCH */
        //         *member_epoch = -1;
        //         return NULL;
        // }

        if (current_assignment) {
                /* Update current assignment to reflect what is provided
                 * by the client. */
                rd_kafka_mock_cgrp_consumer_member_current_assignment_set(
                    member, current_assignment);
        }

        /* Update current member epoch to reflect client one. */
        member->current_member_epoch = *member_epoch;

        if ((!member->target_assignment ||
             member->current_member_epoch == member->target_member_epoch) &&
            member->next_assignment) {
                RD_IF_FREE(member->target_assignment,
                           rd_kafka_topic_partition_list_destroy);

                /* Set next target assignment */
                member->target_assignment =
                    rd_kafka_topic_partition_list_copy(member->next_assignment);
                member->target_member_epoch = member->next_member_epoch;
                returned_assignment = rd_kafka_topic_partition_list_copy(
                    member->target_assignment);
        }

        if (member->target_assignment) {
                if (member->current_member_epoch <
                    member->target_member_epoch) {
                        /* Epochs are different, that means we have to
                         * check if target assignment was reached. */
                        rd_bool_t same = member->current_assignment != NULL;

                        if (member->current_assignment) {
                                /* Compare current with target assignment. */
                                same = !rd_kafka_topic_partition_list_cmp(
                                    member->current_assignment,
                                    member->target_assignment,
                                    rd_kafka_topic_partition_cmp);
                        }

                        if (same) {
                                /* Member has reached target assignment, return
                                 * its member epoch. */
                                *member_epoch = member->target_member_epoch;
                                if (member->target_member_epoch <
                                    member->next_member_epoch) {
                                        /* There's a next assignment to send */
                                        rd_bool_t same_next =
                                            !rd_kafka_topic_partition_list_cmp(
                                                member->target_assignment,
                                                member->next_assignment,
                                                rd_kafka_topic_partition_cmp);

                                        rd_kafka_topic_partition_list_destroy(
                                            member->target_assignment);
                                        member->target_assignment =
                                            rd_kafka_topic_partition_list_copy(
                                                member->next_assignment);
                                        member->target_member_epoch =
                                            member->next_member_epoch;
                                        returned_assignment =
                                            rd_kafka_topic_partition_list_copy(
                                                member->target_assignment);
                                        if (same_next) {
                                                /* Next assignment is the same
                                                 * partition list, bump new
                                                 * epoch being sent. */
                                                *member_epoch =
                                                    member->target_member_epoch;
                                        }
                                }
                        }
                }

                if (!returned_assignment && !member->returned_assignment) {
                        /* If returned assignment isn't set, and isn't available
                         * as member->returned_assignment, then it was set to
                         * NULL after a disconnection and needs to be sent
                         * again. */

                        returned_assignment =
                            rd_kafka_topic_partition_list_copy(
                                member->target_assignment);
                }
        }

        if (returned_assignment) {
                /* Compare returned_assignment with last returned_assignment.
                 * If equal return NULL, otherwise return returned_assignment
                 * and update last returned_assignment. */
                rd_bool_t same_returned_assignment =
                    member->returned_assignment &&
                    !rd_kafka_topic_partition_list_cmp(
                        member->returned_assignment, returned_assignment,
                        rd_kafka_topic_partition_by_id_cmp);
                if (same_returned_assignment) {
                        rd_kafka_topic_partition_list_destroy(
                            returned_assignment);
                        returned_assignment = NULL;
                } else {
                        rd_kafka_mock_cgrp_consumer_member_returned_assignment_set(
                            member, returned_assignment);
                }
        }
        return returned_assignment;
}

/**
 * @brief Mark member as active (restart session timer).
 *
 * @param mcgrp Member's consumer group.
 * @param member Member to set as active.
 *
 * @locks mcluster->lock MUST be held.
 */
void rd_kafka_mock_cgrp_consumer_member_active(
    rd_kafka_mock_cgrp_consumer_t *mcgrp,
    rd_kafka_mock_cgrp_consumer_member_t *member) {
        rd_kafka_dbg(mcgrp->cluster->rk, MOCK, "MOCK",
                     "Marking mock consumer group member %s as active",
                     member->id);
        member->ts_last_activity = rd_clock();
}

/**
 * @brief Finds a member in consumer group \p mcgrp by \p MemberId.
 *
 * @param mcgrp Consumer group to search.
 * @param MemberId Member id to look for.
 * @return Found member or NULL.
 *
 * @locks mcluster->lock MUST be held.
 */
rd_kafka_mock_cgrp_consumer_member_t *rd_kafka_mock_cgrp_consumer_member_find(
    const rd_kafka_mock_cgrp_consumer_t *mcgrp,
    const rd_kafkap_str_t *MemberId) {
        const rd_kafka_mock_cgrp_consumer_member_t *member;
        TAILQ_FOREACH(member, &mcgrp->members, link) {
                if (!rd_kafkap_str_cmp_str(MemberId, member->id))
                        return (rd_kafka_mock_cgrp_consumer_member_t *)member;
        }

        return NULL;
}

/**
 * @brief Adds a member to consumer group \p mcgrp. If member with same
 *        \p MemberId is already present, only updates the connection and
 *        sets it as active.
 *
 * @param mcgrp Consumer group to add the member to.
 * @param conn Member connection.
 * @param MemberId Member id.
 * @param InstanceId Group instance id (optional).
 * @param session_timeout_ms Session timeout to use.
 * @return New or existing member.
 *
 * @locks mcluster->lock MUST be held.
 */
rd_kafka_mock_cgrp_consumer_member_t *
rd_kafka_mock_cgrp_consumer_member_add(rd_kafka_mock_cgrp_consumer_t *mcgrp,
                                       struct rd_kafka_mock_connection_s *conn,
                                       const rd_kafkap_str_t *MemberId,
                                       const rd_kafkap_str_t *InstanceId,
                                       int session_timeout_ms) {
        rd_kafka_mock_cgrp_consumer_member_t *member;

        /* Find member */
        member = rd_kafka_mock_cgrp_consumer_member_find(mcgrp, MemberId);
        if (!member) {
                /* Not found, add member */
                member = rd_calloc(1, sizeof(*member));

                if (!RD_KAFKAP_STR_LEN(MemberId)) {
                        /* Generate a member id.
                         * TODO: replace with UUIDv4 generation. */
                        char memberid[32];
                        rd_snprintf(memberid, sizeof(memberid), "%p", member);
                        member->id = rd_strdup(memberid);
                } else
                        member->id = RD_KAFKAP_STR_DUP(MemberId);

                if (RD_KAFKAP_STR_LEN(InstanceId))
                        member->instance_id = RD_KAFKAP_STR_DUP(InstanceId);

                TAILQ_INSERT_TAIL(&mcgrp->members, member, link);
                mcgrp->member_cnt++;
                mcgrp->group_epoch++;
                member->next_member_epoch = mcgrp->group_epoch;
        }

        mcgrp->session_timeout_ms = session_timeout_ms;

        member->conn = conn;

        rd_kafka_mock_cgrp_consumer_member_active(mcgrp, member);

        return member;
}

/**
 * @brief Destroys a consumer group member, removing from its consumer group.
 *
 * @param mcgrp Member consumer group.
 * @param member Member to destroy.
 *
 * @locks mcluster->lock MUST be held.
 */
static void rd_kafka_mock_cgrp_consumer_member_destroy(
    rd_kafka_mock_cgrp_consumer_t *mcgrp,
    rd_kafka_mock_cgrp_consumer_member_t *member) {
        rd_assert(mcgrp->member_cnt > 0);
        TAILQ_REMOVE(&mcgrp->members, member, link);
        mcgrp->member_cnt--;
        mcgrp->group_epoch++;

        rd_free(member->id);

        if (member->instance_id)
                rd_free(member->instance_id);

        RD_IF_FREE(member->next_assignment,
                   rd_kafka_topic_partition_list_destroy);
        RD_IF_FREE(member->target_assignment,
                   rd_kafka_topic_partition_list_destroy);
        RD_IF_FREE(member->current_assignment,
                   rd_kafka_topic_partition_list_destroy);
        RD_IF_FREE(member->returned_assignment,
                   rd_kafka_topic_partition_list_destroy);

        rd_free(member);
}


/**
 * @brief Called when a member must leave a consumer group.
 *
 * @param mcgrp Consumer group to leave.
 * @param member Member that leaves.
 *
 * @locks mcluster->lock MUST be held.
 */
void rd_kafka_mock_cgrp_consumer_member_leave(
    rd_kafka_mock_cgrp_consumer_t *mcgrp,
    rd_kafka_mock_cgrp_consumer_member_t *member) {

        rd_kafka_dbg(mcgrp->cluster->rk, MOCK, "MOCK",
                     "Member %s is leaving group %s", member->id, mcgrp->id);

        rd_kafka_mock_cgrp_consumer_member_destroy(mcgrp, member);
}

/**
 * @brief Find a consumer group in cluster \p mcluster by \p GroupId.
 *
 * @param mcluster Cluster to search in.
 * @param GroupId Group id to search.
 * @return Found group or NULL.
 *
 * @locks mcluster->lock MUST be held.
 */
rd_kafka_mock_cgrp_consumer_t *
rd_kafka_mock_cgrp_consumer_find(const rd_kafka_mock_cluster_t *mcluster,
                                 const rd_kafkap_str_t *GroupId) {
        rd_kafka_mock_cgrp_consumer_t *mcgrp;
        TAILQ_FOREACH(mcgrp, &mcluster->cgrps_consumer, link) {
                if (!rd_kafkap_str_cmp_str(GroupId, mcgrp->id))
                        return mcgrp;
        }

        return NULL;
}

/**
 * @brief Check if any members have exceeded the session timeout.
 *
 * @param rkts Timers.
 * @param arg Consumer group.
 *
 * @locks mcluster->lock is acquired and released.
 */
static void rd_kafka_mock_cgrp_consumer_session_tmr_cb(rd_kafka_timers_t *rkts,
                                                       void *arg) {
        rd_kafka_mock_cgrp_consumer_t *mcgrp = arg;
        rd_kafka_mock_cgrp_consumer_member_t *member, *tmp;
        rd_ts_t now                       = rd_clock();
        rd_kafka_mock_cluster_t *mcluster = mcgrp->cluster;

        mtx_unlock(&mcluster->lock);
        TAILQ_FOREACH_SAFE(member, &mcgrp->members, link, tmp) {
                if (member->ts_last_activity +
                        (mcgrp->session_timeout_ms * 1000) >
                    now)
                        continue;

                rd_kafka_dbg(mcgrp->cluster->rk, MOCK, "MOCK",
                             "Member %s session timed out for group %s",
                             member->id, mcgrp->id);

                rd_kafka_mock_cgrp_consumer_member_destroy(mcgrp, member);
        }
        mtx_unlock(&mcluster->lock);
}


/**
 * @brief Find or create a "consumer" consumer group.
 *
 * @param mcluster Cluster to search in.
 * @param GroupId Group id to look for.
 * @return Found or new consumer group.
 *
 * @locks mcluster->lock MUST be held.
 */
rd_kafka_mock_cgrp_consumer_t *
rd_kafka_mock_cgrp_consumer_get(rd_kafka_mock_cluster_t *mcluster,
                                const rd_kafkap_str_t *GroupId) {
        rd_kafka_mock_cgrp_consumer_t *mcgrp;

        mcgrp = rd_kafka_mock_cgrp_consumer_find(mcluster, GroupId);
        if (mcgrp)
                return mcgrp;

        mcgrp              = rd_calloc(1, sizeof(*mcgrp));
        mcgrp->cluster     = mcluster;
        mcgrp->id          = RD_KAFKAP_STR_DUP(GroupId);
        mcgrp->group_epoch = 1;
        TAILQ_INIT(&mcgrp->members);
        rd_kafka_timer_start(&mcluster->timers, &mcgrp->session_tmr,
                             1000 * 1000 /*1s*/,
                             rd_kafka_mock_cgrp_consumer_session_tmr_cb, mcgrp);

        TAILQ_INSERT_TAIL(&mcluster->cgrps_consumer, mcgrp, link);

        return mcgrp;
}


void rd_kafka_mock_cgrp_consumer_target_assignment(
    rd_kafka_mock_cluster_t *mcluster,
    const char *group_id,
    const char *member_id,
    const rd_kafka_topic_partition_list_t *rktparlist) {
        rd_kafka_mock_cgrp_consumer_t *cgrp;
        rd_kafka_mock_cgrp_consumer_member_t *member;
        rd_kafkap_str_t *group_id_str =
            rd_kafkap_str_new(group_id, strlen(group_id));
        rd_kafkap_str_t *member_id_str =
            rd_kafkap_str_new(member_id, strlen(member_id));

        mtx_lock(&mcluster->lock);

        cgrp = rd_kafka_mock_cgrp_consumer_find(mcluster, group_id_str);
        if (!cgrp)
                goto destroy;

        member = rd_kafka_mock_cgrp_consumer_member_find(cgrp, member_id_str);

        if (!member)
                goto destroy;

        rd_kafka_mock_cgrp_consumer_member_next_assignment_set(cgrp, member,
                                                               rktparlist);

destroy:
        rd_kafkap_str_destroy(group_id_str);
        rd_kafkap_str_destroy(member_id_str);
        mtx_unlock(&mcluster->lock);
}

/**
 * @brief A client connection closed, check if any consumer cgrp has any state
 *        for this connection that needs to be cleared.
 *
 * @param mcluster Cluster to search in.
 * @param mconn Connection that was closed.
 *
 * @locks mcluster->lock MUST be held.
 */
void rd_kafka_mock_cgrps_consumer_connection_closed(
    rd_kafka_mock_cluster_t *mcluster,
    rd_kafka_mock_connection_t *mconn) {
        rd_kafka_mock_cgrp_consumer_t *mcgrp;

        TAILQ_FOREACH(mcgrp, &mcluster->cgrps_consumer, link) {
                rd_kafka_mock_cgrp_consumer_member_t *member, *tmp;
                TAILQ_FOREACH_SAFE(member, &mcgrp->members, link, tmp) {
                        if (member->conn == mconn) {
                                member->conn = NULL;
                                rd_kafka_mock_cgrp_consumer_member_returned_assignment_set(
                                    member, NULL);
                                rd_kafka_mock_cgrp_consumer_member_current_assignment_set(
                                    member, NULL);
                        }
                }
        }
}

/**
 * @brief Destroys consumer group \p mcgrp and all of its members.
 *
 * @param mcgrp Consumer group to destroy.
 *
 * @locks mcluster->lock MUST be held.
 */
void rd_kafka_mock_cgrp_consumer_destroy(rd_kafka_mock_cgrp_consumer_t *mcgrp) {
        rd_kafka_mock_cgrp_consumer_member_t *member;

        TAILQ_REMOVE(&mcgrp->cluster->cgrps_consumer, mcgrp, link);

        rd_kafka_timer_stop(&mcgrp->cluster->timers, &mcgrp->session_tmr,
                            rd_true);
        rd_free(mcgrp->id);
        while ((member = TAILQ_FIRST(&mcgrp->members)))
                rd_kafka_mock_cgrp_consumer_member_destroy(mcgrp, member);
        rd_free(mcgrp);
}

/**
 * @brief A client connection closed, check if any cgrp has any state
 *        for this connection that needs to be cleared.
 *
 * @param mcluster Mock cluster.
 * @param mconn Connection that was closed.
 */
void rd_kafka_mock_cgrps_connection_closed(rd_kafka_mock_cluster_t *mcluster,
                                           rd_kafka_mock_connection_t *mconn) {
        rd_kafka_mock_cgrps_generic_connection_closed(mcluster, mconn);
        rd_kafka_mock_cgrps_consumer_connection_closed(mcluster, mconn);
}
