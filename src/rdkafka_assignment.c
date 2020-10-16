/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2020 Magnus Edenhill
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
 * @name Consumer assignment state.
 *
 * Responsible for managing the state of assigned partitions.
 *
 *
 ******************************************************************************
 * rd_kafka_assignment_serve()
 * ---------------------------
 *
 * It is important to call rd_kafka_assignment_serve() after each change
 * to the assignment through assignment_add, assignment_subtract or
 * assignment_clear as those functions only modify the assignment but does
 * not take any action to transition partitions to or from the assignment
 * states.
 *
 * The reason assignment_serve() is not automatically called from these
 * functions is for the caller to be able to set the current state before
 * the side-effects of serve() kick in, such as the call to
 * rd_kafka_cgrp_assignment_done() that in turn will set the cgrp state.
 *
 *
 *
 ******************************************************************************
 * Querying for committed offsets (.queried list)
 * ----------------------------------------------
 *
 * We only allow one outstanding query (fetch committed offset), this avoids
 * complex handling of partitions that are assigned, unassigned and reassigned
 * all within the window of a OffsetFetch request.
 * Consider the following case:
 *
 *  1. tp1 and tp2 are incrementally assigned.
 *  2. An OffsetFetchRequest is sent for tp1 and tp2
 *  3. tp2 is incremental unassigned.
 *  4. Broker sends OffsetFetchResponse with offsets tp1=10, tp2=20.
 *  4. Some other consumer commits offsets 30 for tp2.
 *  5. tp2 is incrementally assigned again.
 *  6. The OffsetFetchResponse is received.
 *
 * Without extra handling the consumer would start fetching tp1 at offset 10
 * (which is correct) and tp2 at offset 20 (which is incorrect, the last
 *  committed offset is now 30).
 *
 * To alleviate this situation we remove unassigned partitions from the
 * .queried list, and in the OffsetFetch response handler we only use offsets
 * for partitions that are on the .queried list.
 *
 * To make sure the tp1 offset is used and not re-queried we only allow
 * one outstanding OffsetFetch request at the time, meaning that at step 5
 * a new OffsetFetch request will not be sent and tp2 will remain in the
 * .pending list until the outstanding OffsetFetch response is received in
 * step 6. At this point tp2 will transition to .queried and a new
 * OffsetFetch request will be sent.
 *
 * This explanation is more verbose than the code involved.
 *
 ******************************************************************************
 *
 *
 * @remark Try to keep any cgrp state out of this file.
 *
 * FIXME: There are some pretty obvious optimizations that needs to be done here
 *        with regards to partition_list_t lookups. But we can do that when
 *        we know the current implementation works correctly.
 */

#include "rdkafka_int.h"
#include "rdkafka_assignment.h"
#include "rdkafka_cgrp.h"
#include "rdkafka_offset.h"
#include "rdkafka_request.h"


static void rd_kafka_assignment_clear_lost (struct rd_kafka_cgrp_s *rkcg,
                                            char *fmt, ...)
        RD_FORMAT(printf, 2, 3);

static void rd_kafka_assignment_dump (rd_kafka_cgrp_t *rkcg) {
        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "DUMP",
                     "Assignment dump (started_cnt=%d, wait_stop_cnt=%d, "
                     "lost=%s)",
                     rkcg->rkcg_assignment.started_cnt,
                     rkcg->rkcg_assignment.wait_stop_cnt,
                     RD_STR_ToF(rd_kafka_assignment_is_lost(rkcg)));

        rd_kafka_topic_partition_list_log(
                rkcg->rkcg_rk, "DUMP_ALL", RD_KAFKA_DBG_CGRP,
                rkcg->rkcg_assignment.all);

        rd_kafka_topic_partition_list_log(
                rkcg->rkcg_rk, "DUMP_PND", RD_KAFKA_DBG_CGRP,
                rkcg->rkcg_assignment.pending);

        rd_kafka_topic_partition_list_log(
                rkcg->rkcg_rk, "DUMP_QRY", RD_KAFKA_DBG_CGRP,
                rkcg->rkcg_assignment.queried);

        rd_kafka_topic_partition_list_log(
                rkcg->rkcg_rk, "DUMP_REM", RD_KAFKA_DBG_CGRP,
                rkcg->rkcg_assignment.removed);
}

/**
 * @brief Apply the fetched committed offsets to the current assignment's
 *        queried partitions.
 *
 * @param err is the request-level error, if any. The caller is responsible
 *            for raising this error to the application. It is only used here
 *            to avoid taking actions.
 *
 * Called from the FetchOffsets response handler below.
 */
static void
rd_kafka_assignment_apply_offsets (rd_kafka_cgrp_t *rkcg,
                                   rd_kafka_topic_partition_list_t *offsets,
                                   rd_kafka_resp_err_t err) {
        rd_kafka_topic_partition_t *rktpar;

        RD_KAFKA_TPLIST_FOREACH(rktpar, offsets) {
                rd_kafka_toppar_t *rktp = rktpar->_private; /* May be NULL */

                if (!rd_kafka_topic_partition_list_del(
                            rkcg->rkcg_assignment.queried,
                            rktpar->topic, rktpar->partition)) {
                        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "OFFSETFETCH",
                                     "Group \"%s\": Ignoring OffsetFetch "
                                     "response for %s [%"PRId32"] which is no "
                                     "longer in the queried list "
                                     "(possibly unassigned?)",
                                     rkcg->rkcg_group_id->str,
                                     rktpar->topic, rktpar->partition);
                        continue;
                }

                if (rktpar->err) {
                        /* Partition-level error */
                        rd_kafka_consumer_err(
                                rkcg->rkcg_q, RD_KAFKA_NODEID_UA,
                                rktpar->err, 0,
                                rktpar->topic, rktp,
                                RD_KAFKA_OFFSET_INVALID,
                                "Failed to fetch committed offset for "
                                "group \"%s\" topic %s [%"PRId32"]: %s",
                                rkcg->rkcg_group_id->str,
                                rktpar->topic, rktpar->partition,
                                rd_kafka_err2str(rktpar->err));

                        /* The partition will not be added back to .pending
                         * and thus only reside on .all until the application
                         * unassigns it and possible re-assigns it. */

                } else if (!err) {
                        /* If rktpar->offset is RD_KAFKA_OFFSET_INVALID it means
                         * there was no committed offset for this partition.
                         * serve_pending() will now start this partition
                         * since the offset is set to INVALID (rather than
                         * STORED) and the partition fetcher will employ
                         * auto.offset.reset to know what to do. */

                        /* Add partition to pending list where serve()
                         * will start the fetcher. */
                        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "OFFSETFETCH",
                                     "Adding %s [%"PRId32"] back to pending "
                                     "list with offset %s",
                                     rktpar->topic,
                                     rktpar->partition,
                                     rd_kafka_offset2str(rktpar->offset));

                        rd_kafka_topic_partition_list_add_copy(
                                rkcg->rkcg_assignment.pending, rktpar);
                }
                /* Do nothing for request-level errors (err is set). */

        }

        if (offsets->cnt > 0)
                rd_kafka_assignment_serve(rkcg);
}



/**
 * @brief Reply handler for OffsetFetch queries from the assignment code.
 */
static void
rd_kafka_cgrp_assignment_handle_OffsetFetch (rd_kafka_t *rk,
                                             rd_kafka_broker_t *rkb,
                                             rd_kafka_resp_err_t err,
                                             rd_kafka_buf_t *reply,
                                             rd_kafka_buf_t *request,
                                             void *opaque) {
        rd_kafka_cgrp_t *rkcg;
        rd_kafka_topic_partition_list_t *offsets = NULL;

        if (err == RD_KAFKA_RESP_ERR__DESTROY) {
                /* Termination, quick cleanup. */
                return;
        }

        rkcg = rd_kafka_cgrp_get(rk);

        /* If all partitions already had usable offsets then there
         * was no request sent and thus no reply (NULL), the offsets list is
         * good to go. */
        if (reply) {
                err = rd_kafka_handle_OffsetFetch(rk, rkb, err,
                                                  reply, request, &offsets,
                                                  rd_true/* Update toppars */,
                                                  rd_true/* Add parts */);
                if (err == RD_KAFKA_RESP_ERR__IN_PROGRESS)
                        return; /* retrying */
        }

        if (err) {
                rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "OFFSET",
                             "Offset fetch error for %d partition(s): %s",
                             offsets->cnt,
                             rd_kafka_err2str(err));
                rd_kafka_consumer_err(rkcg->rkcg_q,
                                      rd_kafka_broker_id(rkb),
                                      err, 0, NULL, NULL,
                                      RD_KAFKA_OFFSET_INVALID,
                                      "Failed to fetch committed offsets for "
                                      "%d partition(s) in group \"%s\": %s",
                                      offsets->cnt,
                                      rkcg->rkcg_group_id->str,
                                      rd_kafka_err2str(err));

        }

        /* Apply the fetched offsets to the assignment */
        rd_kafka_assignment_apply_offsets(rkcg, offsets, err);

        rd_kafka_topic_partition_list_destroy(offsets);
}


/**
 * @brief Decommission all partitions in the removed list.
 *
 * @returns >0 if there are removal operations in progress, else 0.
 */
static int rd_kafka_assignment_serve_removals (rd_kafka_cgrp_t *rkcg) {
        rd_kafka_topic_partition_t *rktpar;
        int valid_offsets = 0;

        RD_KAFKA_TPLIST_FOREACH(rktpar, rkcg->rkcg_assignment.removed) {
                rd_kafka_toppar_t *rktp = rktpar->_private; /* Borrow ref */
                int was_pending, was_queried;

                /* Remove partition from pending and querying lists,
                 * if it happens to be there.
                 * Outstanding query results will be dropped since a version
                 * barrier is pushed on each assignment subtraction/clear. */
                was_pending = rd_kafka_topic_partition_list_del(
                        rkcg->rkcg_assignment.pending,
                        rktpar->topic, rktpar->partition);
                was_queried = rd_kafka_topic_partition_list_del(
                        rkcg->rkcg_assignment.queried,
                        rktpar->topic, rktpar->partition);

                if (rktp->rktp_started) {
                        /* Partition was started, stop the fetcher. */
                        rd_assert(rkcg->rkcg_assignment.started_cnt > 0);

                        rd_kafka_toppar_op_fetch_stop(
                                rktp, RD_KAFKA_REPLYQ(rkcg->rkcg_ops, 0));
                        rkcg->rkcg_assignment.wait_stop_cnt++;
                }

                /* Reset the (lib) pause flag which may have been set by
                 * the cgrp when scheduling the rebalance callback. */
                rd_kafka_toppar_op_pause_resume(rktp,
                                                rd_false/*resume*/,
                                                RD_KAFKA_TOPPAR_F_LIB_PAUSE,
                                                RD_KAFKA_NO_REPLYQ);

                rd_kafka_toppar_lock(rktp);

                /* Save the currently stored offset on .removed
                 * so it will be committed below. */
                rktpar->offset = rktp->rktp_stored_offset;
                valid_offsets += !RD_KAFKA_OFFSET_IS_LOGICAL(rktpar->offset);

                /* Reset the stored offset to invalid so that
                 * a manual offset-less commit() or the auto-committer
                 * will not commit a stored offset from a previous
                 * assignment (issue #2782). */
                rd_kafka_offset_store0(rktp, RD_KAFKA_OFFSET_INVALID,
                                       RD_DONT_LOCK);

                /* Partition is no longer desired */
                rd_kafka_toppar_desired_del(rktp);
                rd_kafka_toppar_unlock(rktp);

                rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "REMOVE",
                             "Removing %s [%"PRId32"] from assignment "
                             "(started=%s, pending=%s, queried=%s, "
                             "stored offset=%s)",
                             rktpar->topic, rktpar->partition,
                             RD_STR_ToF(rktp->rktp_started),
                             RD_STR_ToF(was_pending),
                             RD_STR_ToF(was_queried),
                             rd_kafka_offset2str(rktpar->offset));
        }

        rd_kafka_dbg(rkcg->rkcg_rk, CONSUMER|RD_KAFKA_DBG_CGRP, "REMOVE",
                     "Group \"%s\": served %d removed partition(s), with "
                     "%d offset(s) to commit",
                     rkcg->rkcg_group_id->str,
                     rkcg->rkcg_assignment.removed->cnt,
                     valid_offsets);

        /* If enable.auto.commit=true:
         * Commit final offsets to broker for the removed partitions,
         * unless this is a consumer destruction with a close() call. */
        if (valid_offsets > 0 &&
            rkcg->rkcg_rk->rk_conf.offset_store_method ==
            RD_KAFKA_OFFSET_METHOD_BROKER &&
            rkcg->rkcg_rk->rk_conf.enable_auto_commit &&
            !rd_kafka_destroy_flags_no_consumer_close(rkcg->rkcg_rk))
                rd_kafka_cgrp_assigned_offsets_commit(
                        rkcg,
                        rkcg->rkcg_assignment.removed,
                        rd_false /* use offsets from .removed */,
                        "unassigned partitions");

        rd_kafka_topic_partition_list_clear(rkcg->rkcg_assignment.removed);

        return rkcg->rkcg_assignment.wait_stop_cnt +
                rkcg->rkcg_wait_commit_cnt;
}


/**
 * @brief Serve all partitions in the pending list.
 *
 * This either (asynchronously) queries the partition's committed offset, or
 * if the start offset is known, starts the partition fetcher.
 *
 * @returns >0 if there are pending operations in progress, else 0.
 */
static int rd_kafka_assignment_serve_pending (rd_kafka_cgrp_t *rkcg) {
        rd_kafka_topic_partition_list_t *partitions_to_query = NULL;
        /* We can query committed offsets only if all of the following are true:
         *  - We have a coordinator
         *  - There are no outstanding commits (since we might need to
         *    read back those commits as our starting position).
         *  - There are no outstanding queries already (since we want to
         *    avoid using a earlier queries response for a partition that
         *    is unassigned and then assigned again).
         */
        rd_bool_t can_query_offsets =
                rkcg->rkcg_state == RD_KAFKA_CGRP_STATE_UP &&
                rkcg->rkcg_wait_commit_cnt == 0 &&
                rkcg->rkcg_assignment.queried->cnt == 0;
        int i;

        if (can_query_offsets)
                partitions_to_query = rd_kafka_topic_partition_list_new(
                        rkcg->rkcg_assignment.pending->cnt);

        /* Scan the list backwards so removals are cheap (no array shuffle) */
        for (i = rkcg->rkcg_assignment.pending->cnt - 1 ; i >= 0 ; i--) {
                rd_kafka_topic_partition_t *rktpar =
                        &rkcg->rkcg_assignment.pending->elems[i];
                rd_kafka_toppar_t *rktp = rktpar->_private; /* Borrow ref */

                rd_assert(!rktp->rktp_started);

                if (!RD_KAFKA_OFFSET_IS_LOGICAL(rktpar->offset) ||
                    rktpar->offset == RD_KAFKA_OFFSET_BEGINNING ||
                    rktpar->offset == RD_KAFKA_OFFSET_END ||
                    rktpar->offset == RD_KAFKA_OFFSET_INVALID ||
                    rktpar->offset <= RD_KAFKA_OFFSET_TAIL_BASE) {
                        /* The partition fetcher can handle absolute
                         * as well as beginning/end/tail start offsets, so we're
                         * ready to start the fetcher now.
                         * The INVALID offset means there was no committed
                         * offset and the partition fetcher will employ
                         * auto.offset.reset.
                         *
                         * Start fetcher for partition and forward partition's
                         * fetchq to consumer group's queue. */

                        /* Reset the (lib) pause flag which may have been set by
                         * the cgrp when scheduling the rebalance callback. */

                        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "SRVPEND",
                                     "Starting pending assigned partition "
                                     "%s [%"PRId32"] at offset %s",
                                     rktpar->topic, rktpar->partition,
                                     rd_kafka_offset2str(rktpar->offset));

                        rd_kafka_toppar_op_pause_resume(
                                rktp,
                                rd_false/*resume*/,
                                RD_KAFKA_TOPPAR_F_LIB_PAUSE,
                                RD_KAFKA_NO_REPLYQ);

                        /* Start the fetcher */
                        rktp->rktp_started = rd_true;
                        rkcg->rkcg_assignment.started_cnt++;

                        rd_kafka_toppar_op_fetch_start(
                                rktp, rktpar->offset,
                                rkcg->rkcg_q, RD_KAFKA_NO_REPLYQ);


                } else if (can_query_offsets) {
                        /* Else use the last committed offset for partition.
                         * We can't rely on any internal cached committed offset
                         * so we'll accumulate a list of partitions that need
                         * to be queried and then send FetchOffsetsRequest
                         * to the group coordinator. */

                        rd_dassert(!rd_kafka_topic_partition_list_find(
                                           rkcg->rkcg_assignment.queried,
                                           rktpar->topic, rktpar->partition));

                        rd_kafka_topic_partition_list_add_copy(
                                partitions_to_query, rktpar);

                        rd_kafka_topic_partition_list_add_copy(
                                rkcg->rkcg_assignment.queried, rktpar);

                        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "SRVPEND",
                                     "Querying committed offset for pending "
                                     "assigned partition %s [%"PRId32"]",
                                     rktpar->topic, rktpar->partition);


                } else {
                        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "SRVPEND",
                                     "Pending assignment partition "
                                     "%s [%"PRId32"] can't fetch committed "
                                     "offset yet "
                                     "(cgrp state %s, awaiting %d commits, "
                                     "%d partition(s) already being queried)",
                                     rktpar->topic, rktpar->partition,
                                     rd_kafka_cgrp_state_names[rkcg->
                                                               rkcg_state],
                                     rkcg->rkcg_wait_commit_cnt,
                                     rkcg->rkcg_assignment.queried->cnt);

                        continue; /* Keep rktpar on pending list */
                }

                /* Remove rktpar from the pending list */
                rd_kafka_topic_partition_list_del_by_idx(
                        rkcg->rkcg_assignment.pending, i);
        }


        if (!can_query_offsets)
                return rkcg->rkcg_assignment.pending->cnt +
                        rkcg->rkcg_assignment.queried->cnt;


        if (partitions_to_query->cnt > 0) {
                rd_assert(rkcg->rkcg_coord);

                rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "OFFSETFETCH",
                             "Fetching committed offsets for "
                             "%d pending partition(s) in assignment",
                             partitions_to_query->cnt);

                rd_kafka_OffsetFetchRequest(
                        rkcg->rkcg_coord, 1,
                        partitions_to_query,
                        RD_KAFKA_REPLYQ(rkcg->rkcg_ops, 0),
                        rd_kafka_cgrp_assignment_handle_OffsetFetch,
                        NULL);
        }

        rd_kafka_topic_partition_list_destroy(partitions_to_query);

        return rkcg->rkcg_assignment.pending->cnt +
                rkcg->rkcg_assignment.queried->cnt;
}



/**
 * @brief Serve updates to the assignment.
 *
 * Call on:
 * - assignment changes
 * - rkcg_wait_commit_cnt reaches 0
 * - partition fetcher is stopped
 */
void rd_kafka_assignment_serve (rd_kafka_cgrp_t *rkcg) {
        int inp_removals = 0;
        int inp_pending = 0;

        rd_kafka_assignment_dump(rkcg);

        /* Serve any partitions that should be removed */
        if (rkcg->rkcg_assignment.removed->cnt > 0)
                inp_removals = rd_kafka_assignment_serve_removals(rkcg);

        /* Serve any partitions in the pending list that need further action,
         * unless we're waiting for a previous assignment change (an unassign
         * in some form) to propagate, or outstanding offset commits
         * to finish (since we might need the committed offsets as start
         * offsets). */
        if (rkcg->rkcg_assignment.wait_stop_cnt == 0 &&
            rkcg->rkcg_wait_commit_cnt == 0 &&
            inp_removals == 0 &&
            rkcg->rkcg_assignment.pending->cnt > 0)
                inp_pending = rd_kafka_assignment_serve_pending(rkcg);

        if (inp_removals + inp_pending + rkcg->rkcg_assignment.queried->cnt +
            rkcg->rkcg_assignment.wait_stop_cnt +
            rkcg->rkcg_wait_commit_cnt == 0) {
                /* No assignment operations in progress,
                 * signal assignment done back to cgrp to let it
                 * transition to its next state if necessary.
                 * We may emit this signalling more than necessary and it is
                 * up to the cgrp to only take action if needed, based on its
                 * state. */
                rd_kafka_cgrp_assignment_done(rkcg);
        } else {
                rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "ASSIGNMENT",
                             "Current assignment of %d partition(s) "
                             "with %d pending adds, %d offset queries, "
                             "%d partitions awaiting stop and "
                             "%d offset commits in progress",
                             rkcg->rkcg_assignment.all->cnt,
                             inp_pending,
                             rkcg->rkcg_assignment.queried->cnt,
                             rkcg->rkcg_assignment.wait_stop_cnt,
                             rkcg->rkcg_wait_commit_cnt);
        }
}


/**
 * @returns true if the current or previous assignment has operations in
 *          progress, such as waiting for partition fetchers to stop.
 */
rd_bool_t rd_kafka_assignment_in_progress (rd_kafka_cgrp_t *rkcg) {
        return rkcg->rkcg_wait_commit_cnt > 0 ||
                rkcg->rkcg_assignment.wait_stop_cnt > 0 ||
                rkcg->rkcg_assignment.pending->cnt > 0 ||
                rkcg->rkcg_assignment.queried->cnt > 0 ||
                rkcg->rkcg_assignment.removed->cnt > 0;
}


/**
 * @brief Clear the current assignment.
 *
 * @remark Make sure to call rd_kafka_assignment_serve() after successful
 *         return from this function.
 */
void rd_kafka_assignment_clear (rd_kafka_cgrp_t *rkcg) {

        /* Any change to the assignment marks the current assignment
         * as not lost. */
        rd_kafka_assignment_clear_lost(rkcg, "assignment removed");

        if (rkcg->rkcg_assignment.all->cnt == 0) {
                rd_kafka_dbg(rkcg->rkcg_rk, CONSUMER|RD_KAFKA_DBG_CGRP,
                             "CLEARASSIGN",
                             "Group \"%s\": no current assignment to clear",
                             rkcg->rkcg_group_id->str);
                return;
        }

        rd_kafka_dbg(rkcg->rkcg_rk, CONSUMER|RD_KAFKA_DBG_CGRP, "CLEARASSIGN",
                     "Group \"%s\": clearing current assignment of "
                     "%d partition(s)",
                     rkcg->rkcg_group_id->str, rkcg->rkcg_assignment.all->cnt);

        rd_kafka_topic_partition_list_clear(rkcg->rkcg_assignment.pending);
        rd_kafka_topic_partition_list_clear(rkcg->rkcg_assignment.queried);

        rd_kafka_topic_partition_list_add_list(rkcg->rkcg_assignment.removed,
                                               rkcg->rkcg_assignment.all);
        rd_kafka_topic_partition_list_clear(rkcg->rkcg_assignment.all);

        rd_kafka_wrlock(rkcg->rkcg_rk);
        rkcg->rkcg_c.assignment_size = 0;
        rd_kafka_wrunlock(rkcg->rkcg_rk);
}


/**
 * @brief Adds \p partitions to the current assignment.
 *
 * Will return error if trying to add a partition that is already in the
 * assignment.
 *
 * @remark Make sure to call rd_kafka_assignment_serve() after successful
 *         return from this function.
 */
rd_kafka_error_t *
rd_kafka_assignment_add (rd_kafka_cgrp_t *rkcg,
                         rd_kafka_topic_partition_list_t *partitions) {
        rd_bool_t was_empty = rkcg->rkcg_assignment.all->cnt == 0;
        int i;

        /* Make sure there are no duplicates, invalid partitions, or
         * invalid offsets in the input partitions. */
        rd_kafka_topic_partition_list_sort(partitions, NULL, NULL);

        for (i = 0 ; i < partitions->cnt ; i++) {
                rd_kafka_topic_partition_t *rktpar = &partitions->elems[i];
                const rd_kafka_topic_partition_t *prev =
                        i > 0 ? &partitions->elems[i-1] : NULL;

                if (RD_KAFKA_OFFSET_IS_LOGICAL(rktpar->offset) &&
                    rktpar->offset != RD_KAFKA_OFFSET_BEGINNING &&
                    rktpar->offset != RD_KAFKA_OFFSET_END &&
                    rktpar->offset != RD_KAFKA_OFFSET_STORED &&
                    rktpar->offset != RD_KAFKA_OFFSET_INVALID &&
                    rktpar->offset > RD_KAFKA_OFFSET_TAIL_BASE)
                        return rd_kafka_error_new(
                                RD_KAFKA_RESP_ERR__INVALID_ARG,
                                "%s [%"PRId32"] has invalid start offset %"
                                PRId64,
                                rktpar->topic, rktpar->partition,
                                rktpar->offset);

                if (prev && !rd_kafka_topic_partition_cmp(rktpar, prev))
                        return rd_kafka_error_new(
                                RD_KAFKA_RESP_ERR__INVALID_ARG,
                                "Duplicate %s [%"PRId32"] in input list",
                                rktpar->topic, rktpar->partition);

                if (rd_kafka_topic_partition_list_find(
                            rkcg->rkcg_assignment.all,
                            rktpar->topic, rktpar->partition))
                        return rd_kafka_error_new(
                                RD_KAFKA_RESP_ERR__CONFLICT,
                                "%s [%"PRId32"] is already part of the "
                                "current assignment",
                                rktpar->topic, rktpar->partition);

                /* Translate RD_KAFKA_OFFSET_INVALID to RD_KAFKA_OFFSET_STORED,
                 * i.e., read from committed offset, since we use INVALID
                 * internally to differentiate between querying for
                 * committed offset (STORED) and no committed offset (INVALID).
                 */
                if (rktpar->offset == RD_KAFKA_OFFSET_INVALID)
                        rktpar->offset = RD_KAFKA_OFFSET_STORED;

                /* Get toppar object for each partition.
                 * This is to make sure the rktp stays alive while unassigning
                 * any previous assignment in the call to
                 * assignment_clear() below. */
                rd_kafka_topic_partition_ensure_toppar(rkcg->rkcg_rk, rktpar,
                                                       rd_true);

                /* FIXME: old cgrp_assign() marks rktp as desired, should we? */
        }

        /* Add the new list of partitions to the current assignment.
         * Only need to sort the final assignment if it was non-empty
         * to begin with since \p partitions is sorted above. */
        rd_kafka_topic_partition_list_add_list(rkcg->rkcg_assignment.all,
                                               partitions);
        if (!was_empty)
                rd_kafka_topic_partition_list_sort(rkcg->rkcg_assignment.all,
                                                   NULL, NULL);

        /* And add to .pending for serve_pending() to handle. */
        rd_kafka_topic_partition_list_add_list(rkcg->rkcg_assignment.pending,
                                               partitions);


        rd_kafka_dbg(rkcg->rkcg_rk, CONSUMER|RD_KAFKA_DBG_CGRP, "ASSIGNMENT",
                     "Group \"%s\": added %d partition(s) to assignment which "
                     "now consists of %d partition(s) where of %d are in "
                     "pending state and %d are being queried",
                     rkcg->rkcg_group_id->str,
                     partitions->cnt,
                     rkcg->rkcg_assignment.all->cnt,
                     rkcg->rkcg_assignment.pending->cnt,
                     rkcg->rkcg_assignment.queried->cnt);

        rd_kafka_wrlock(rkcg->rkcg_rk);
        rkcg->rkcg_c.assignment_size = rkcg->rkcg_assignment.all->cnt;
        rd_kafka_wrunlock(rkcg->rkcg_rk);

        /* Any change to the assignment marks the current assignment
         * as not lost. */
        rd_kafka_assignment_clear_lost(rkcg, "assignment updated");

        return NULL;
}


/**
 * @brief Remove \p partitions from the current assignment.
 *
 * Will return error if trying to remove a partition that is not in the
 * assignment.
 *
 * The cgrp version barrier will be bumped to invalidate any outstanding
 * partition queries.
 *
 * @remark Make sure to call rd_kafka_assignment_serve() after successful
 *         return from this function.
 */
rd_kafka_error_t *
rd_kafka_assignment_subtract (rd_kafka_cgrp_t *rkcg,
                              rd_kafka_topic_partition_list_t *partitions) {
        int i;
        int matched_queried_partitions = 0;
        int assignment_pre_cnt;

        if (rkcg->rkcg_assignment.all->cnt == 0 && partitions->cnt > 0)
                return rd_kafka_error_new(
                        RD_KAFKA_RESP_ERR__INVALID_ARG,
                        "Can't subtract from empty assignment");

        /* Verify that all partitions in \p partitions are in the assignment
         * before starting to modify the assignment. */
        rd_kafka_topic_partition_list_sort(partitions, NULL, NULL);

        for (i = 0 ; i < partitions->cnt ; i++) {
                rd_kafka_topic_partition_t *rktpar = &partitions->elems[i];

                if (!rd_kafka_topic_partition_list_find(
                            rkcg->rkcg_assignment.all,
                            rktpar->topic, rktpar->partition))
                        return rd_kafka_error_new(
                                RD_KAFKA_RESP_ERR__INVALID_ARG,
                                "%s [%"PRId32"] can't be unassigned since "
                                "it is not in the current assignment",
                                rktpar->topic, rktpar->partition);

                rd_kafka_topic_partition_ensure_toppar(rkcg->rkcg_rk, rktpar,
                                                       rd_true);
        }


        assignment_pre_cnt = rkcg->rkcg_assignment.all->cnt;

        /* Remove partitions in reverse order to avoid excessive
         * array shuffling of .all.
         * Add the removed partitions to .pending for serve() to handle. */
        for (i = partitions->cnt-1 ; i >= 0 ; i--) {
                const rd_kafka_topic_partition_t *rktpar =
                        &partitions->elems[i];

                if (!rd_kafka_topic_partition_list_del(
                            rkcg->rkcg_assignment.all,
                            rktpar->topic, rktpar->partition))
                        RD_BUG("Removed partition %s [%"PRId32"] not found "
                               "in assignment.all",
                               rktpar->topic, rktpar->partition);

                if (rd_kafka_topic_partition_list_del(
                            rkcg->rkcg_assignment.queried,
                            rktpar->topic, rktpar->partition))
                        matched_queried_partitions++;
                else
                        rd_kafka_topic_partition_list_del(
                                rkcg->rkcg_assignment.pending,
                                rktpar->topic, rktpar->partition);

                /* Add to .removed list which will be served by
                 * serve_removals(). */
                rd_kafka_topic_partition_list_add_copy(
                        rkcg->rkcg_assignment.removed, rktpar);
        }

        rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "REMOVEASSIGN",
                     "Group \"%s\": removed %d partition(s) "
                     "(%d with outstanding offset queries) from assignment "
                     "of %d partition(s)",
                     rkcg->rkcg_group_id->str, partitions->cnt,
                     matched_queried_partitions, assignment_pre_cnt);

        if (rkcg->rkcg_assignment.all->cnt == 0) {
                /* Some safe checking */
                rd_assert(rkcg->rkcg_assignment.pending->cnt == 0);
                rd_assert(rkcg->rkcg_assignment.queried->cnt == 0);
        }

        rd_kafka_wrlock(rkcg->rkcg_rk);
        rkcg->rkcg_c.assignment_size = rkcg->rkcg_assignment.all->cnt;
        rd_kafka_wrunlock(rkcg->rkcg_rk);

        /* Any change to the assignment marks the current assignment
         * as not lost. */
        rd_kafka_assignment_clear_lost(rkcg, "assignment subtracted");

        return NULL;
}


/**
 * @brief Call when partition fetcher has stopped.
 */
void rd_kafka_assignment_partition_stopped (rd_kafka_cgrp_t *rkcg,
                                            rd_kafka_toppar_t *rktp) {
        rd_assert(rkcg->rkcg_assignment.wait_stop_cnt > 0);
        rkcg->rkcg_assignment.wait_stop_cnt--;

        rd_assert(rktp->rktp_started);
        rktp->rktp_started = rd_false;

        rd_assert(rkcg->rkcg_assignment.started_cnt > 0);
        rkcg->rkcg_assignment.started_cnt--;

        /* If this was the last partition we awaited stop for, serve the
         * assignment to transition any existing assignment to the next state */
        if (rkcg->rkcg_assignment.wait_stop_cnt == 0) {
                rd_kafka_dbg(rkcg->rkcg_rk, CGRP, "STOPSERVE",
                             "Group \"%s\": all partitions awaiting stop now "
                             "stopped: serving assignment",
                             rkcg->rkcg_group_id->str);
                rd_kafka_assignment_serve(rkcg);
        }
}


/**
 * @returns true if the current assignment is lost.
 */
rd_bool_t rd_kafka_assignment_is_lost (rd_kafka_cgrp_t *rkcg) {
        return rd_atomic32_get(&rkcg->rkcg_assignment.lost) != 0;
}


/**
 * @brief Call when the current assignment has been lost, with a
 *        human-readable reason.
 */
void rd_kafka_assignment_set_lost (rd_kafka_cgrp_t *rkcg,
                                   char *fmt, ...) {
        va_list ap;
        char reason[256];

        if (rkcg->rkcg_assignment.all->cnt == 0)
                return;

        va_start(ap, fmt);
        rd_vsnprintf(reason, sizeof(reason), fmt, ap);
        va_end(ap);

        rd_kafka_dbg(rkcg->rkcg_rk, CONSUMER|RD_KAFKA_DBG_CGRP, "LOST",
                     "Group \"%s\": "
                     "current assignment of %d partition(s) lost: %s",
                     rkcg->rkcg_group_id->str,
                     rkcg->rkcg_assignment.all->cnt,
                     reason);

        rd_atomic32_set(&rkcg->rkcg_assignment.lost, rd_true);
}


/**
 * @brief Call when the current assignment is no longer considered lost, with a
 *        human-readable reason.
 */
static void rd_kafka_assignment_clear_lost (rd_kafka_cgrp_t *rkcg,
                                            char *fmt, ...) {
        va_list ap;
        char reason[256];

        if (!rd_atomic32_get(&rkcg->rkcg_assignment.lost))
                return;

        va_start(ap, fmt);
        rd_vsnprintf(reason, sizeof(reason), fmt, ap);
        va_end(ap);

        rd_kafka_dbg(rkcg->rkcg_rk, CONSUMER|RD_KAFKA_DBG_CGRP, "LOST",
                     "Group \"%s\": "
                     "current assignment no longer considered lost: %s",
                     rkcg->rkcg_group_id->str, reason);

        rd_atomic32_set(&rkcg->rkcg_assignment.lost, rd_false);
}


/**
 * @brief Destroy assignment state (but not \p assignment itself)
 */
void rd_kafka_assignment_destroy (rd_kafka_assignment_t *assignment) {
        rd_kafka_topic_partition_list_destroy(assignment->all);
        rd_kafka_topic_partition_list_destroy(assignment->pending);
        rd_kafka_topic_partition_list_destroy(assignment->queried);
        rd_kafka_topic_partition_list_destroy(assignment->removed);
}


/**
 * @brief Initialize the assignment struct.
 */
void rd_kafka_assignment_init (rd_kafka_assignment_t *assignment) {
        memset(assignment, 0, sizeof(*assignment));
        assignment->all     = rd_kafka_topic_partition_list_new(100);
        assignment->pending = rd_kafka_topic_partition_list_new(100);
        assignment->queried = rd_kafka_topic_partition_list_new(100);
        assignment->removed = rd_kafka_topic_partition_list_new(100);
        rd_atomic32_init(&assignment->lost, rd_false);
}
