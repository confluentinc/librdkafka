/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2022 Magnus Edenhill
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

#ifndef _RDKAFKA_METRIC_H_
#define _RDKAFKA_METRIC_H_

/**
 * @name Metrics
 *
 */


typedef enum {
        RD_KAFKA_METRIC_TYPE_NONE,
        RD_KAFKA_METRIC_TYPE_GAUGE,
        RD_KAFKA_METRIC_TYPE_SUM,
        RD_KAFKA_METRIC_TYPE_INT_HISTOGRAM,
        RD_KAFKA_METRIC_TYPE_HISTOGRAM,
        RD_KAFKA_METRIC_TYPE__NUM,
} rd_kafka_metric_type_t;

static RD_UNUSED const char *
rd_kafka_metric_type2str (rd_kafka_metric_type_t mtype) {
        static const char *names[] = { "None", "Gauge", "Sum",
                                       "IntHistogram", "Histogram" };
        rd_assert((int)mtype >= 0 && mtype < RD_KAFKA_METRIC_TYPE__NUM);
        return names[mtype];
}


/**
 * @name Metric subscription pruning trie.
 *
 * This word-based prefix trie contains pointers to all available metrics.
 * A "word" in this case is a .-delimited part of the current metric name, e.g:
 *
 * "producer.partition.queue.bytes" yields four words:
 *   producer, partition, queue, and bytes.
 *
 * Each node is comprised of:
 *  - the metric word
 *  - a list of the current metrics with this full name
 *  - the number of matching metrics subscriptions at this and lower levels
 *
 * The matching metrics counter allows quick pruning of sub-trees when there
 * are no matching subscriptions in that part of the tree.
 *
 *
 *                 [root, matching:2]
 *                   /             \
 *                  /               \
 *     ["producer",m:2]            ["consumer",m:0]
 *      /            \                       \
 *     /              \                       \
 * ["partition",m:2] ["record",m:0]         ["fetch",m:0]
 *   /          \                \                   \
 *  /            \                \                   \...
 * ["queue",m:1] ["latency":m1]   ["bytes",m:0]
 *     /          |                          |
 *    /           +--> rd_kafka_metric_t*    +--> rd_kafka_metric_t*
 *   /            +--> rd_kafka_metric_t*
 *  ["count",m:1]
 *   |
 *   +-- rd_kafka_metric_t*  .. different attributes for each partition
 *   +-- rd_kafka_metric_t*  ..
 *   +-- rd_kafka_metric_t*  ..
 *
 * As can be seen in the diagram above, the entire "consumer" sub-tree can be
 * skipped when collecting metrics since the matching count is zero.
 *
 * Whenever the subscription is updated the longest match is found and
 * the matching counter is updated accordingly in all parent nodes up to the
 * root.
 */

typedef struct rd_kafka_metric_trie_node_s {
        TAILQ_ENTRY(rd_kafka_metric_trie_node_s) link;

        struct rd_kafka_metric_trie_node_s *parent;

        /** Metric type for all elements in \c .metrics */
        rd_kafka_metric_type_t mtype;

        /** Full metric name (NULL for intermediary nodes) */
        const char *name;

        /** Sub name (one metric word) of this node.
         *  E.g., "partition" for the second node in "producer.partition.queue"
         */
        rd_chariov_t subname;

        /** Sorted list of children */
        TAILQ_HEAD(, rd_kafka_metric_trie_node_s) children;

        /** Indicates whether there is a metrics subscription for
         *  this node. */
        rd_bool_t enabled;

        /** Number of enabled/matching subscriptions under and including
         *  this node. */
        int matchcnt;

        /** List of metrics attached to this name. */
        TAILQ_HEAD(, rd_kafka_metric_s) metrics;
        int metriccnt;

} rd_kafka_metric_trie_node_t;


typedef struct rd_kafka_metric_trie_s {
        /** Root node of the trie */
        rd_kafka_metric_trie_node_t *root;
        /** Mutex for protecting trie_node_t.metrics accesses. */
        mtx_t mtx;
} rd_kafka_metric_trie_t;


typedef struct rd_kafka_metric_s {
        TAILQ_ENTRY(rd_kafka_metric_s) link;
        rd_kafka_metric_trie_node_t *node;
        //attributes_t **attributes;
} rd_kafka_metric_t;

typedef struct rd_kafka_metric_sum_s {
        rd_kafka_metric_t metric;
        rd_atomic64_t sum;
} rd_kafka_metric_sum_t;

typedef struct rd_kafka_metric_gauge_s {
        rd_kafka_metric_t metric;
        rd_atomic64_t gauge;
} rd_kafka_metric_gauge_t;


typedef struct rd_kafka_metric_avg_s {
        rd_kafka_metric_t metric;
        rd_avg_t avg;
} rd_kafka_metric_avg_t;


#define rd_kafka_metric_gauge_incr(METRIC,VAL)  \
        rd_atomic64_add(&(METRIC)->gauge, (VAL))

#define rd_kafka_metric_gauge_decr(METRIC,VAL) (                        \
                rd_dassert(rd_atomic64_get(&(METRIC)->gauge) >= (VAL));   \
                rd_atomic64_add(&(METRIC)->gauge, (VAL)))                 \

#define rd_kafka_metric_sum_add(METRIC,VAL)     \
        rd_atomic64_add(&(METRIC)->sum, (VAL))

#define rd_kafka_metric_avg_add(METRIC,VAL)     \
        rd_avg_add(&(METRIC)->avg, (VAL))


/**
 *@}
 */


#endif /* _RDKAFKA_METRIC_H_ */
