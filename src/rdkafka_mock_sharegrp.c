/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2025, Confluent Inc.
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
#include "rdkafka_mock_group_common.h"

/**
 * @brief Share group target assignment (manual)
 */
typedef struct rd_kafka_mock_sharegroup_target_assignments_s {
        rd_list_t member_ids; /**< List of member ids (char *) */
        rd_list_t assignment; /**< List of rd_kafka_topic_partition_list_t */
} rd_kafka_mock_sharegroup_target_assignment_t;

/* Forward declarations */
static void rd_kafka_mock_sharegroup_session_tmr_cb(rd_kafka_timers_t *rkts,
                                                    void *arg);

/**
 * @brief Initializes sharegroups in mock cluster
 */
void rd_kafka_mock_sharegrps_init(rd_kafka_mock_cluster_t *mcluster) {
        TAILQ_INIT(&mcluster->sharegrps);
        mcluster->defaults.sharegroup_session_timeout_ms    = 45000;
        mcluster->defaults.sharegroup_heartbeat_interval_ms = 5000;
}

/**
 * @brief Find a share group by GroupId.
 */
rd_kafka_mock_sharegroup_t *
rd_kafka_mock_sharegroup_find(rd_kafka_mock_cluster_t *mcluster,
                              const rd_kafkap_str_t *GroupId) {
        return RD_KAFKA_MOCK_GROUP_FIND(&mcluster->sharegrps, GroupId,
                                        rd_kafka_mock_sharegroup_t);
}

/**
 * @brief Get or create a share group
 */
rd_kafka_mock_sharegroup_t *
rd_kafka_mock_sharegroup_get(rd_kafka_mock_cluster_t *mcluster,
                             const rd_kafkap_str_t *GroupID) {
        rd_kafka_mock_sharegroup_t *mshgrp;

        /* Check if the share group already exists */
        mshgrp = rd_kafka_mock_sharegroup_find(mcluster, GroupID);
        if (mshgrp)
                return mshgrp;

        /* Create new share group */
        mshgrp              = rd_calloc(1, sizeof(*mshgrp));
        mshgrp->cluster     = mcluster;
        mshgrp->id          = RD_KAFKAP_STR_DUP(GroupID);
        mshgrp->group_epoch = 1;
        mshgrp->session_timeout_ms =
            mcluster->defaults.sharegroup_session_timeout_ms;
        mshgrp->heartbeat_interval_ms =
            mcluster->defaults.sharegroup_heartbeat_interval_ms;

        TAILQ_INIT(&mshgrp->members);
        mshgrp->member_cnt = 0;

        rd_kafka_timer_start(&mcluster->timers, &mshgrp->session_tmr,
                             1000 * 1000 /* 1s */,
                             rd_kafka_mock_sharegroup_session_tmr_cb, mshgrp);

        TAILQ_INSERT_TAIL(&mcluster->sharegrps, mshgrp, link);

        return mshgrp;
}

/**
 * @brief Destroy a share group
 */
void rd_kafka_mock_sharegroup_destroy(rd_kafka_mock_sharegroup_t *mshgrp) {
        rd_kafka_mock_sharegroup_member_t *member;

        TAILQ_REMOVE(&mshgrp->cluster->sharegrps, mshgrp, link);
        rd_kafka_timer_stop(&mshgrp->cluster->timers, &mshgrp->session_tmr,
                            RD_DO_LOCK);

        /* Destroy all members */
        while ((member = TAILQ_FIRST(&mshgrp->members)))
                rd_kafka_mock_sharegroup_member_destroy(mshgrp, member);

        rd_free(mshgrp->id);
        rd_free(mshgrp);
}

/**
 * @brief Find a share group member by MemberId.
 */
rd_kafka_mock_sharegroup_member_t *
rd_kafka_mock_sharegroup_member_find(rd_kafka_mock_sharegroup_t *mshgrp,
                                     const rd_kafkap_str_t *MemberId) {
        return RD_KAFKA_MOCK_MEMBER_FIND(&mshgrp->members, MemberId,
                                         rd_kafka_mock_sharegroup_member_t);
}

/**
 * @brief Destroy a share group member.
 */
void rd_kafka_mock_sharegroup_member_destroy(
    rd_kafka_mock_sharegroup_t *mshgrp,
    rd_kafka_mock_sharegroup_member_t *member) {
        rd_assert(mshgrp->member_cnt > 0);
        TAILQ_REMOVE(&mshgrp->members, member, link);
        mshgrp->member_cnt--;
        rd_free(member->id);

        RD_IF_FREE(member->subscribed_topic_names, rd_list_destroy_free);
        RD_IF_FREE(member->assignment, rd_kafka_topic_partition_list_destroy);
        rd_free(member);
}

/**
 * @brief Mark member as active.
 */
void rd_kafka_mock_sharegroup_member_active(
    rd_kafka_mock_sharegroup_t *mshgrp,
    rd_kafka_mock_sharegroup_member_t *member) {
        rd_kafka_mock_group_member_mark_active(mshgrp->cluster->rk, "share",
                                               member->id,
                                               &member->ts_last_activity);
}

/**
 * @brief Fence a member.
 */
void rd_kafka_mock_sharegroup_member_fenced(
    rd_kafka_mock_sharegroup_t *mshgrp,
    rd_kafka_mock_sharegroup_member_t *member) {
        rd_kafka_dbg(mshgrp->cluster->rk, MOCK, "MOCK",
                     "Member %s is fenced from sharegroup %s", member->id,
                     mshgrp->id);

        rd_kafka_mock_sharegroup_member_destroy(mshgrp, member);

        /* Recalculate assignments so remaining members get the
         * freed partitions. */
        rd_kafka_mock_sharegroup_assignment_recalculate(mshgrp);
}

/**
 * @brief Check all members for inactivity and remove them if timed out.
 */
static void rd_kafka_mock_sharegroup_session_tmr_cb(rd_kafka_timers_t *rkts,
                                                    void *arg) {
        rd_kafka_mock_sharegroup_t *mshgrp = arg;
        rd_kafka_mock_sharegroup_member_t *member, *tmp;
        rd_ts_t now                       = rd_clock();
        rd_kafka_mock_cluster_t *mcluster = mshgrp->cluster;

        mtx_lock(&mcluster->lock);
        TAILQ_FOREACH_SAFE(member, &mshgrp->members, link, tmp) {
                if (member->ts_last_activity +
                        (mshgrp->session_timeout_ms * 1000) >
                    now)
                        continue;

                rd_kafka_dbg(mcluster->rk, MOCK, "MOCK",
                             "Member %s session timed out for sharegroup %s",
                             member->id, mshgrp->id);

                rd_kafka_mock_sharegroup_member_fenced(mshgrp, member);
        }
        mtx_unlock(&mcluster->lock);
}

/**
 * @brief Get or create a share group member.
 */
rd_kafka_mock_sharegroup_member_t *
rd_kafka_mock_sharegroup_member_get(rd_kafka_mock_sharegroup_t *mshgrp,
                                    const rd_kafkap_str_t *MemberID,
                                    int32_t MemberEpoch,
                                    rd_kafka_mock_connection_t *mconn) {
        rd_kafka_mock_sharegroup_member_t *member;

        /* Check if the member already exists */
        member = rd_kafka_mock_sharegroup_member_find(mshgrp, MemberID);
        if (member) {
                member->conn = mconn;
                rd_kafka_mock_sharegroup_member_active(mshgrp, member);
                return member;
        }

        /* Only create if epoch is 0 */
        if (MemberEpoch != 0)
                return NULL;

        /* Create new member */
        member               = rd_calloc(1, sizeof(*member));
        member->mshgrp       = mshgrp;
        member->id           = RD_KAFKAP_STR_DUP(MemberID);
        member->member_epoch = mshgrp->group_epoch;
        member->previous_member_epoch =
            -1; /* No previous epoch for new members */
        member->conn = mconn;

        TAILQ_INSERT_TAIL(&mshgrp->members, member, link);
        mshgrp->member_cnt++;
        rd_kafka_mock_sharegroup_member_active(mshgrp, member);

        return member;
}

/**
 * @brief Update share group member's subscribed topic names.
 */
rd_bool_t rd_kafka_mock_sharegroup_member_subscribed_topic_names_set(
    rd_kafka_mock_sharegroup_member_t *member,
    const rd_kafkap_str_t *SubscribedTopicNames,
    int32_t SubscribedTopicNamesCnt) {
        rd_bool_t changed = rd_false;
        int32_t i;

        if (!SubscribedTopicNamesCnt) {
                /* No change */
                return rd_false;
        }

        if (member->subscribed_topic_names) {
                if (rd_list_cnt(member->subscribed_topic_names) ==
                    SubscribedTopicNamesCnt) {
                        rd_bool_t same = rd_true;
                        char *topic;
                        int j;

                        RD_LIST_FOREACH(topic, member->subscribed_topic_names,
                                        j) {
                                rd_bool_t found = rd_false;
                                for (i = 0; i < SubscribedTopicNamesCnt; i++) {
                                        if (!rd_kafkap_str_cmp_str(
                                                &SubscribedTopicNames[i],
                                                topic)) {
                                                found = rd_true;
                                                break;
                                        }
                                }
                                if (!found) {
                                        same = rd_false;
                                        break;
                                }
                        }
                        if (same)
                                return rd_false;
                }
        }

        /* Subscription changed, update the list */
        changed = rd_true;
        RD_IF_FREE(member->subscribed_topic_names, rd_list_destroy);
        member->subscribed_topic_names =
            rd_list_new(SubscribedTopicNamesCnt, rd_free);

        for (i = 0; i < SubscribedTopicNamesCnt; i++) {
                rd_list_add(member->subscribed_topic_names,
                            RD_KAFKAP_STR_DUP(&SubscribedTopicNames[i]));
        }

        return changed;
}

/**
 * @brief Collect all subscribed topic names from all members.
 */
static rd_list_t *rd_kafka_mock_sharegroup_collect_subscribed_topics(
    rd_kafka_mock_sharegroup_t *mshgrp) {
        rd_kafka_mock_sharegroup_member_t *member;
        rd_list_t *all_topics;

        all_topics = rd_list_new(32, rd_free);

        TAILQ_FOREACH(member, &mshgrp->members, link) {
                const char *topic;
                int i;

                if (!member->subscribed_topic_names)
                        continue;

                RD_LIST_FOREACH(topic, member->subscribed_topic_names, i) {
                        const char *existing;
                        int j;
                        rd_bool_t found = rd_false;

                        /* Check if topic already in all_topics */
                        RD_LIST_FOREACH(existing, all_topics, j) {
                                if (!strcmp(topic, existing)) {
                                        found = rd_true;
                                        break;
                                }
                        }

                        /* Add if not found */
                        if (!found) {
                                rd_list_add(all_topics, rd_strdup(topic));
                        }
                }
        }

        return all_topics;
}

/**
 * @brief Get list of member ID's subscribed to a topic.
 */
rd_list_t *rd_kafka_mock_sharegroup_get_members_for_topic(
    rd_kafka_mock_sharegroup_t *mshgrp,
    char *topic_name) {
        rd_kafka_mock_sharegroup_member_t *member;
        rd_list_t *subscribed_members;
        int member_idx = 0;

        subscribed_members = rd_list_new(mshgrp->member_cnt, rd_free);

        TAILQ_FOREACH(member, &mshgrp->members, link) {
                char *topic;
                int i;

                if (member->subscribed_topic_names) {
                        RD_LIST_FOREACH(topic, member->subscribed_topic_names,
                                        i) {
                                if (!strcmp(topic, topic_name)) {
                                        int *idx = rd_malloc(sizeof(*idx));
                                        *idx     = member_idx;
                                        rd_list_add(subscribed_members, idx);
                                        break;
                                }
                        }
                }
                member_idx++;
        }

        return subscribed_members;
}

/**
 * @brief Assign partitions of a single topic to subscribed members.
 */
void rd_kafka_mock_sharegroup_assign_topic_partitions(
    rd_kafka_mock_sharegroup_t *mshgrp,
    rd_kafka_mock_topic_t *mtopic,
    rd_list_t *subscribed_member_indices) {
        int member_count;
        int partition_cnt;
        int partitions_per_member;
        int extra_partitions;
        int partition_idx;
        int i;

        member_count  = rd_list_cnt(subscribed_member_indices);
        partition_cnt = mtopic->partition_cnt;

        if (member_count == 0 || partition_cnt == 0)
                return;

        partitions_per_member = partition_cnt / member_count;
        extra_partitions      = partition_cnt % member_count;
        partition_idx         = 0;

        for (i = 0; i < member_count; i++) {
                int *member_idx_ptr =
                    (int *)rd_list_elem(subscribed_member_indices, i);
                rd_kafka_mock_sharegroup_member_t *member;
                int j, cnt = 0;
                int num_partitions;

                TAILQ_FOREACH(member, &mshgrp->members, link) {
                        if (cnt == *member_idx_ptr)
                                break;
                        cnt++;
                }

                if (!member)
                        continue;

                num_partitions =
                    partitions_per_member + (i < extra_partitions ? 1 : 0);

                if (!member->assignment)
                        member->assignment =
                            rd_kafka_topic_partition_list_new(num_partitions);

                for (j = 0; j < num_partitions && partition_idx < partition_cnt;
                     j++, partition_idx++) {
                        rd_kafka_topic_partition_t *rktpar;
                        rktpar = rd_kafka_topic_partition_list_add(
                            member->assignment, mtopic->name, partition_idx);
                        /* Set topic ID so the response can include it */
                        rd_kafka_topic_partition_set_topic_id(rktpar,
                                                              mtopic->id);
                }
        }
}

/**
 * @brief Recalculate assignments for all members in the share group.
 */
void rd_kafka_mock_sharegroup_assignment_recalculate(
    rd_kafka_mock_sharegroup_t *mshgrp) {
        rd_kafka_mock_sharegroup_member_t *member;
        rd_list_t *all_topics;
        char *topic_name;
        int i;

        if (mshgrp->member_cnt == 0)
                return;

        /* Skip automatic assignment if manual mode is enabled */
        if (mshgrp->manual_assignment)
                return;

        TAILQ_FOREACH(member, &mshgrp->members, link) {
                if (member->assignment) {
                        rd_kafka_topic_partition_list_destroy(
                            member->assignment);
                        member->assignment = NULL;
                }
        }

        all_topics = rd_kafka_mock_sharegroup_collect_subscribed_topics(mshgrp);

        RD_LIST_FOREACH(topic_name, all_topics, i) {
                rd_kafka_mock_topic_t *mtopic;
                rd_list_t *subscribed_members;

                mtopic = rd_kafka_mock_topic_find(mshgrp->cluster, topic_name);
                if (!mtopic)
                        continue;

                subscribed_members =
                    rd_kafka_mock_sharegroup_get_members_for_topic(mshgrp,
                                                                   topic_name);

                rd_kafka_mock_sharegroup_assign_topic_partitions(
                    mshgrp, mtopic, subscribed_members);

                rd_list_destroy(subscribed_members);
        }

        mshgrp->group_epoch++;

        TAILQ_FOREACH(member, &mshgrp->members, link) {
                /* Save the current epoch as previous before bumping.
                 * This allows the client to catch up if the response
                 * with the new epoch was lost. */
                member->previous_member_epoch = member->member_epoch;
                member->member_epoch          = mshgrp->group_epoch;
        }

        rd_list_destroy(all_topics);
}

/**
 * @brief Create a new target assignment (manual)
 */
rd_kafka_mock_sharegroup_target_assignment_t *
rd_kafka_mock_sharegroup_target_assignment_new(void) {
        rd_kafka_mock_sharegroup_target_assignment_t *target_assignment;
        target_assignment = rd_calloc(1, sizeof(*target_assignment));
        rd_list_init(&target_assignment->member_ids, 0, rd_free);
        rd_list_init(&target_assignment->assignment, 0,
                     (void *)rd_kafka_topic_partition_list_destroy);

        return target_assignment;
}

/**
 * @brief Destroy target assignment
 */
void rd_kafka_mock_sharegroup_target_assignment_destroy(
    rd_kafka_mock_sharegroup_target_assignment_t *target_assignment) {
        rd_list_destroy(&target_assignment->member_ids);
        rd_list_destroy(&target_assignment->assignment);
        rd_free(target_assignment);
}

/**
 * @brief Set the target assignment for the sharegroup.
 * This applies the manual assignment to the members.
 *
 * @locks mcluster->lock MUST be held.
 */
static void rd_kafka_mock_sharegroup_target_assignment_set(
    rd_kafka_mock_sharegroup_t *mshgrp,
    rd_kafka_mock_sharegroup_target_assignment_t *target_assignment) {
        rd_kafka_mock_sharegroup_member_t *member;
        size_t i;

        for (i = 0; i < rd_list_cnt(&target_assignment->member_ids); i++) {
                const char *member_id =
                    rd_list_elem(&target_assignment->member_ids, i);
                const rd_kafka_topic_partition_list_t *partitions =
                    rd_list_elem(&target_assignment->assignment, i);
                rd_kafkap_str_t *member_id_str;

                member_id_str = rd_kafkap_str_new(member_id, -1);
                member =
                    rd_kafka_mock_sharegroup_member_find(mshgrp, member_id_str);
                rd_kafkap_str_destroy(member_id_str);

                if (!member) {
                        rd_kafka_dbg(mshgrp->cluster->rk, MOCK, "MOCK",
                                     "Cannot set target assignment for "
                                     "non-existing member %s in sharegroup %s",
                                     member_id, mshgrp->id);
                        continue;
                }

                if (member->assignment) {
                        rd_kafka_topic_partition_list_destroy(
                            member->assignment);
                }

                member->assignment =
                    rd_kafka_topic_partition_list_copy(partitions);

                /* Set topic IDs on each partition so the heartbeat response
                 * can include them (ShareGroupHeartbeat uses topic IDs) */
                {
                        int j;
                        for (j = 0; j < member->assignment->cnt; j++) {
                                rd_kafka_topic_partition_t *rktpar =
                                    &member->assignment->elems[j];
                                rd_kafkap_str_t topic_str = {
                                    .str = rktpar->topic,
                                    .len = strlen(rktpar->topic)};
                                rd_kafka_mock_topic_t *mtopic =
                                    rd_kafka_mock_topic_find_by_kstr(
                                        mshgrp->cluster, &topic_str);
                                if (mtopic) {
                                        rd_kafka_topic_partition_set_topic_id(
                                            rktpar, mtopic->id);
                                }
                        }
                }

                rd_kafka_dbg(
                    mshgrp->cluster->rk, MOCK, "MOCK",
                    "Target assignment set for member %s: %d partition(s)",
                    member_id, member->assignment->cnt);
        }

        /* Bump the epochs */
        TAILQ_FOREACH(member, &mshgrp->members, link) {
                member->previous_member_epoch = member->member_epoch;
                member->member_epoch          = ++mshgrp->group_epoch;
        }
}

/**
 * @brief Manual target assignment interface for sharegroups.
 */
void rd_kafka_mock_sharegroup_target_assignment(
    rd_kafka_mock_cluster_t *mcluster,
    const char *group_id,
    const char **member_ids,
    rd_kafka_topic_partition_list_t **assignment,
    size_t member_cnt) {
        rd_kafka_mock_sharegroup_t *mshgrp;
        rd_kafka_mock_sharegroup_target_assignment_t *target_assignment;
        size_t i;
        rd_kafkap_str_t *group_id_str;

        mtx_lock(&mcluster->lock);
        group_id_str = rd_kafkap_str_new(group_id, -1);
        mshgrp       = rd_kafka_mock_sharegroup_find(mcluster, group_id_str);
        rd_kafkap_str_destroy(group_id_str);

        if (!mshgrp) {
                rd_kafka_log(mcluster->rk, LOG_ERR, "MOCK",
                             "Sharegroup %s not found for target assignment",
                             group_id);
                mtx_unlock(&mcluster->lock);
                return;
        }

        mshgrp->manual_assignment = rd_true;
        target_assignment = rd_kafka_mock_sharegroup_target_assignment_new();

        for (i = 0; i < member_cnt; i++) {
                rd_list_add(&target_assignment->member_ids,
                            rd_strdup(member_ids[i]));
                rd_list_add(&target_assignment->assignment,
                            rd_kafka_topic_partition_list_copy(assignment[i]));
        }
        rd_kafka_mock_sharegroup_target_assignment_set(mshgrp,
                                                       target_assignment);
        rd_kafka_mock_sharegroup_target_assignment_destroy(target_assignment);
        mtx_unlock(&mcluster->lock);
}

/**
 * @brief Set the sharegroup session timeout for the sharegroup.
 */
void rd_kafka_mock_sharegroup_set_session_timeout(
    rd_kafka_mock_cluster_t *mcluster,
    int session_timeout_ms) {
        mtx_lock(&mcluster->lock);
        mcluster->defaults.sharegroup_session_timeout_ms = session_timeout_ms;
        mtx_unlock(&mcluster->lock);
}

/**
 * @brief Set the sharegroup heartbeat interval for the sharegroup.
 */
void rd_kafka_mock_sharegroup_set_heartbeat_interval(
    rd_kafka_mock_cluster_t *mcluster,
    int heartbeat_interval_ms) {
        mtx_lock(&mcluster->lock);
        mcluster->defaults.sharegroup_heartbeat_interval_ms =
            heartbeat_interval_ms;
        mtx_unlock(&mcluster->lock);
}

/**
 * @brief A client connection closed, check if any sharegroup has any
 * state for this connection that needs to be cleared.
 *
 * @param mcluster Cluster to search in.
 * @param mconn Connection that was closed.
 *
 * @locks mcluster->lock MUST be held.
 */
void rd_kafka_mock_sharegrps_connection_closed(
    rd_kafka_mock_cluster_t *mcluster,
    rd_kafka_mock_connection_t *mconn) {
        rd_kafka_mock_sharegroup_t *mshgrp;

        TAILQ_FOREACH(mshgrp, &mcluster->sharegrps, link) {
                rd_kafka_mock_sharegroup_member_t *member, *tmp;
                TAILQ_FOREACH_SAFE(member, &mshgrp->members, link, tmp) {
                        if (member->conn == mconn) {
                                rd_kafka_mock_sharegroup_member_fenced(mshgrp,
                                                                       member);
                        }
                }
        }
}

/**
 * @brief Retrieve the member IDs from a sharegroup.
 *
 * @param mcluster Mock cluster instance.
 * @param group_id The sharegroup ID.
 * @param member_ids_out Output array of member IDs (caller must free each
 *                       string and the array itself).
 * @param member_cnt_out Output count of members.
 *
 * @returns RD_KAFKA_RESP_ERR_NO_ERROR on success,
 *          RD_KAFKA_RESP_ERR_GROUP_ID_NOT_FOUND if sharegroup not found.
 */
rd_kafka_resp_err_t rd_kafka_mock_sharegroup_get_member_ids(
    rd_kafka_mock_cluster_t *mcluster,
    const char *group_id,
    char ***member_ids_out,
    size_t *member_cnt_out) {
        rd_kafka_mock_sharegroup_t *mshgrp;
        rd_kafka_mock_sharegroup_member_t *member;
        rd_kafkap_str_t *group_id_str;
        char **member_ids;
        size_t i;

        mtx_lock(&mcluster->lock);
        group_id_str = rd_kafkap_str_new(group_id, -1);
        mshgrp       = rd_kafka_mock_sharegroup_find(mcluster, group_id_str);
        rd_kafkap_str_destroy(group_id_str);

        if (!mshgrp) {
                mtx_unlock(&mcluster->lock);
                *member_ids_out = NULL;
                *member_cnt_out = 0;
                return RD_KAFKA_RESP_ERR_GROUP_ID_NOT_FOUND;
        }

        *member_cnt_out = mshgrp->member_cnt;
        if (mshgrp->member_cnt == 0) {
                mtx_unlock(&mcluster->lock);
                *member_ids_out = NULL;
                return RD_KAFKA_RESP_ERR_NO_ERROR;
        }

        member_ids = rd_malloc(sizeof(*member_ids) * mshgrp->member_cnt);
        i          = 0;
        TAILQ_FOREACH(member, &mshgrp->members, link) {
                member_ids[i++] = rd_strdup(member->id);
        }

        mtx_unlock(&mcluster->lock);
        *member_ids_out = member_ids;
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}