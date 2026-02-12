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
 * @file rdkafka_mock_group_common.h
 * @brief Common macros and inline functions shared between
 *        ConsumerGroupHeartbeat (KIP-848) and ShareGroupHeartbeat (KIP-932)
 *        mock broker implementations.
 */

#ifndef _RDKAFKA_MOCK_GROUP_COMMON_H_
#define _RDKAFKA_MOCK_GROUP_COMMON_H_

#include "rdkafka_int.h"

/**
 * @brief Generic group find by GroupId in a TAILQ.
 *
 * @param groups_head Pointer to TAILQ_HEAD of groups.
 * @param GroupId rd_kafkap_str_t pointer to search for.
 * @param group_type The group struct type (e.g., rd_kafka_mock_sharegroup_t).
 *
 * @returns Pointer to found group, or NULL if not found.
 */
#define RD_KAFKA_MOCK_GROUP_FIND(groups_head, GroupId, group_type)             \
        ({                                                                     \
                group_type *_grp = NULL;                                       \
                TAILQ_FOREACH(_grp, groups_head, link) {                       \
                        if (!rd_kafkap_str_cmp_str(GroupId, _grp->id))         \
                                break;                                         \
                }                                                              \
                _grp;                                                          \
        })

/**
 * @brief Generic member find by MemberId in a TAILQ.
 *
 * @param members_head Pointer to TAILQ_HEAD of members.
 * @param MemberId rd_kafkap_str_t pointer to search for.
 * @param member_type The member struct type.
 *
 * @returns Pointer to found member, or NULL if not found.
 */
#define RD_KAFKA_MOCK_MEMBER_FIND(members_head, MemberId, member_type)         \
        ({                                                                     \
                member_type *_member = NULL;                                   \
                TAILQ_FOREACH(_member, members_head, link) {                   \
                        if (!rd_kafkap_str_cmp_str(MemberId, _member->id))     \
                                break;                                         \
                }                                                              \
                _member;                                                       \
        })

/**
 * @brief Mark a group member as active by updating its last activity timestamp.
 *
 * @param rk rd_kafka_t handle for logging.
 * @param group_type String describing the group type ("consumer" or "share").
 * @param member_id Member ID string for logging.
 * @param ts_last_activity Pointer to the member's last activity timestamp.
 */
static RD_INLINE RD_UNUSED void rd_kafka_mock_group_member_mark_active(
    rd_kafka_t *rk,
    const char *group_type,
    const char *member_id,
    rd_ts_t *ts_last_activity) {
        rd_kafka_dbg(rk, MOCK, "MOCK",
                     "Marking mock %s group member %s as active", group_type,
                     member_id);
        *ts_last_activity = rd_clock();
}

#endif /* _RDKAFKA_MOCK_GROUP_COMMON_H_ */
