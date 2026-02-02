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

/* Forward declaration - now declared in header */

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
        rd_kafka_mock_sharegroup_t *mshgrp;
        TAILQ_FOREACH(mshgrp, &mcluster->sharegrps, link) {
                if (!rd_kafkap_str_cmp_str(GroupId, mshgrp->id))
                        return mshgrp;
        }
        return NULL;
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

        TAILQ_INSERT_TAIL(&mcluster->sharegrps, mshgrp, link);

        return mshgrp;
}

/**
 * @brief Destroy a share group
 */
void rd_kafka_mock_sharegroup_destroy(rd_kafka_mock_sharegroup_t *mshgrp) {
        rd_kafka_mock_sharegroup_member_t *member;

        TAILQ_REMOVE(&mshgrp->cluster->sharegrps, mshgrp, link);

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
        rd_kafka_mock_sharegroup_member_t *member;
        TAILQ_FOREACH(member, &mshgrp->members, link) {
                if (!rd_kafkap_str_cmp_str(MemberId, member->id))
                        return member;
        }
        return NULL;
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

        /* Print all member assignments */
        printf("\n=== SHARE GROUP ASSIGNMENT (epoch %d) ===\n",
               mshgrp->group_epoch);
        TAILQ_FOREACH(member, &mshgrp->members, link) {
                printf("  %s -> ", member->id);
                if (member->assignment && member->assignment->cnt > 0) {
                        int j;
                        for (j = 0; j < member->assignment->cnt; j++) {
                                rd_kafka_topic_partition_t *p =
                                    &member->assignment->elems[j];
                                printf("%s[%d]%s", p->topic, p->partition,
                                       j < member->assignment->cnt - 1 ? ", "
                                                                       : "");
                        }
                } else {
                        printf("(none)");
                }
                printf("\n");
        }
        printf("=========================================\n\n");
        fflush(stdout);

        rd_list_destroy(all_topics);
}
