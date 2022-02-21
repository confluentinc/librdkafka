/*
 * librdkafka - The Apache Kafka C/C++ library
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

#include "rdkafka_int.h"
#include "rdkafka_metric.h"

//#include "protobuf/opentelemetry/proto/metrics/v1/metrics.pb-c.h"


rd_kafka_metric_trie_node_t root;

#if 0
static int rd_kafka_metric_trie_node_cmp (const void *_a, const void *_b) {
        const rd_kafka_metric_trie_node_t *a = _a, *b = _b;
        return rd_chariov_cmp(&a->subname, &b->subname);
}
#endif


static rd_chariov_t next_word (const char **sp) {
        const char *s = *sp;
        rd_chariov_t ch = { (char *)s, strcspn(s, "./") };

        if (ch.ptr[ch.size] != '\0')
                *sp = ch.ptr + ch.size + 1;
        else
                *sp = ch.ptr + ch.size;

        return ch;
}

static rd_kafka_metric_trie_node_t *
rd_kafka_metric_trie_find (const rd_kafka_metric_trie_node_t *root,
                           const char *name) {
        const rd_kafka_metric_trie_node_t *node = NULL, *parent = root;
        const char *s = name;

        rd_assert(*s);

        while (*s) {
                const rd_chariov_t ch = next_word(&s);

                TAILQ_FOREACH(node, &parent->children, link) {
                        int r = rd_chariov_cmp(&ch, &node->subname);
                        if (r == 0)
                                break;
                        else if (r < 0) /* no more possible matches */
                                return NULL;
                }

                if (!node)
                        return NULL;

                parent = node;
        }

        return (rd_kafka_metric_trie_node_t *)node;
}


rd_kafka_metric_trie_node_t *
rd_kafka_metric_trie_insert (rd_kafka_metric_trie_node_t *root,
                             rd_kafka_metric_type_t mtype, const char *name) {
        rd_kafka_metric_trie_node_t *node = NULL, *parent = root;
        const char *s = name;

        rd_assert(*s);

        printf(">> %s\n", name);
        while (*s) {
                const rd_chariov_t ch = next_word(&s);
                rd_bool_t is_last = *s == '\0';
                rd_kafka_metric_trie_node_t *insert_pos = NULL;
                int r = -1;

                TAILQ_FOREACH(node, &parent->children, link) {
                        r = rd_chariov_cmp(&ch, &node->subname);
                        if (r <= 0) {
                                /* Match (0) or no more possible matches (<0).
                                 * If an existing node, make sure the
                                 * metric type matches. */
                                if (r == 0 && is_last)
                                        rd_assert(node->mtype == mtype);
                                break;
                        }

                        insert_pos = node;
                }


                if (!node || r != 0) {
                        /* Add node for this word */
                        node = rd_calloc(1, sizeof(*node));
                        node->subname = ch;
                        node->parent = parent;
                        if (is_last)
                                node->mtype = mtype;
                        else
                                node->mtype = RD_KAFKA_METRIC_TYPE_NONE;

                        TAILQ_INIT(&node->metrics);
                        TAILQ_INIT(&node->children);

                        /* Sorted insert in parent's children list */
                        if (insert_pos)
                                TAILQ_INSERT_AFTER(&parent->children,
                                                   insert_pos, node, link);
                        else
                                TAILQ_INSERT_TAIL(&parent->children, node,
                                                  link);
                }

                parent = node;
        }

        rd_assert(node != NULL);

        return node;
}


/**
 * @brief Destroy trie node and all its children.
 *
 * @remark The node must be unlinked from its parent prior to this call.
 */
#if 0
static void
rd_kafka_metric_trie_node_destroy (rd_kafka_metric_trie_node_t *node) {
        rd_kafka_metric_trie_node_t *child, *tmp;

        TAILQ_FOREACH_SAFE(child, &node->children, link, tmp)
                rd_kafka_metric_trie_node_destroy(child);

        rd_free(node);
}
#endif


void rd_kafka_metric_destroy (rd_kafka_metric_t *metric) {
        rd_assert(metric->node->metriccnt > 0);
        metric->node->metriccnt--;
        TAILQ_REMOVE(&metric->node->metrics, metric, link);
        metric->node = NULL;
}

static void rd_kafka_metric_init (rd_kafka_metric_t *metric,
                                  rd_kafka_metric_trie_node_t *node) {
        node->metriccnt++;
        TAILQ_INSERT_TAIL(&node->metrics, metric, link);
        metric->node = node;
}

void rd_kafka_metric_sum_init (rd_kafka_metric_sum_t *msum,
                               rd_kafka_metric_trie_node_t *node) {
        rd_atomic64_init(&msum->sum, 0);
        rd_kafka_metric_init(&msum->metric, node);
}

#if 0
struct rd_kafka_metric_s {
        Opentelemetry__Proto__Metrics__V1__Metric pb;
};


void rd_kafka_metric_init (rd_kafka_metric_t *metric,
                           // FIXME: add gcc require_const ..
                           const char *name,
                           const char *unit,
                           rd_kafka_metric_type_t metric_type,
                           const char **attributes,
                           size_t attribute_cnt) {
        static const Opentelemetry__Proto__Metrics__V1__Metric__DataCase
                metric_type2data_case[] = {
                [RD_KAFKA_METRIC_TYPE_GAUGE] =
                OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_GAUGE,
                [RD_KAFKA_METRIC_TYPE_SUM] =
                OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_SUM,
                [RD_KAFKA_METRIC_TYPE_INT_HISTOGRAM] =
                OPENTELEMETRY__PROTO__METRICS__V1__METRIC__INT_HISTOGRAM,
                [RD_KAFKA_METRIC_TYPE_HISTOGRAM] =
                OPENTELEMETRY__PROTO__METRICS__V1__METRIC__HISTOGRAM,
        };

        opentelemetry__proto_metrics__v1__metric__init(&metric->pb);
        metric->pb.name = name;
        metric->pb.unit = unit;
        metric->pb.data_case = metric_type2data_case[metric_type];


        if (attribute_cnt > 0) {
                size_t i;
                Opentelemetry__Proto__Common__V1__KeyValue **pb_attrs;

                pb_attrs = rd_malloc(sizeof(*pb_attrs) * (attribute_cnt / 2));
                for (i = 0 ; i < attribute_cnt ; i += 2) {

                }
        }
}
#endif


void rd_kafka_metrics_trie_dump (FILE *fp,
                                 const rd_kafka_metric_trie_node_t *parent,
                                 rd_bool_t prune) {
        const rd_kafka_metric_trie_node_t *node;
        const char *parname = parent->subname.ptr;
        size_t parsize = parent->subname.size;

        if (!parsize) {
                parname = "[root]";
                parsize = 6;
        }

        fprintf(fp, " x%p [label=\"%.*s, %s, m:%d%s\"];\n",
                parent, (int)parsize, parname,
                rd_kafka_metric_type2str(parent->mtype), parent->matchcnt,
                parent->enabled ? ", *" : "");

        if (parent->metriccnt > 0) {
                const rd_kafka_metric_t *metric;
                fprintf(fp,
                        " subgraph {\n"
                        "  node [shape=record];\n");

                TAILQ_FOREACH(metric, &parent->metrics, link) {
                        fprintf(fp, " %s m%p",
                                metric == TAILQ_FIRST(&parent->metrics) ?
                                "" : " -> ",
                                metric);
                }
                fprintf(fp,
                        " [color=grey arrowhead=none];\n"
                        " }\n"
                        " x%p -> m%p;\n",
                        parent, TAILQ_FIRST(&parent->metrics));
        }

        TAILQ_FOREACH(node, &parent->children, link) {
                if (!node->matchcnt && prune)
                        continue;
                fprintf(fp, "  x%p -> x%p;\n", parent, node);
                rd_kafka_metrics_trie_dump(fp, node, prune);
        }
}


static void
rd_kafka_metrics_trie_set_enable (rd_kafka_metric_trie_node_t *parent,
                                  rd_bool_t enable) {
        rd_kafka_metric_trie_node_t *node;

        if (enable)
                parent->matchcnt++;
        else
                parent->matchcnt = 0;
        parent->enabled = enable;

        TAILQ_FOREACH(node, &parent->children, link) {
                rd_kafka_metrics_trie_set_enable(node, enable);
        }
}


static void
rd_kafka_metrics_set_subscription (rd_kafka_metric_trie_node_t *root,
                                   const char **pfxs, size_t pfxcnt) {
        size_t i;
        rd_kafka_metric_trie_node_t *node;

        /* Reset all matches to begin with */
        rd_kafka_metrics_trie_set_enable(root, rd_false);

        for (i = 0 ; i < pfxcnt ; i++) {

                node = rd_kafka_metric_trie_find(root, pfxs[i]);
                if (!node) {
                        printf("** Unmatched prefix: \"%s\": ignored\n",
                               pfxs[i]);
                        continue;
                }

                rd_kafka_metrics_trie_set_enable(node, rd_true);

                while ((node = node->parent))
                        node->matchcnt++;

        }
}

static void rd_kafka_metric_trie_init (rd_kafka_metric_trie_t *mtrie) {
        rd_kafka_metric_trie_node_t *node;

        memset(mtrie, 0, sizeof(*mtrie));
        mtx_init(&mtrie->mtx, mtx_plain);

        mtrie->root = rd_calloc(1, sizeof(*mtrie->root));
        mtrie->root->subname.ptr = "";
        TAILQ_INIT(&mtrie->root->metrics);
        TAILQ_INIT(&mtrie->root->children);

        rd_kafka_metric_trie_insert(mtrie->root, RD_KAFKA_METRIC_TYPE_GAUGE, "a.b.c");
        rd_kafka_metric_trie_insert(mtrie->root, RD_KAFKA_METRIC_TYPE_SUM, "a.b.c.d");
        rd_kafka_metric_trie_insert(mtrie->root, RD_KAFKA_METRIC_TYPE_SUM, "a.b.x");
        rd_kafka_metric_trie_insert(mtrie->root, RD_KAFKA_METRIC_TYPE_INT_HISTOGRAM,
                                    "a.b.x.y");
        rd_kafka_metric_trie_insert(mtrie->root, RD_KAFKA_METRIC_TYPE_HISTOGRAM,
                                    "a.b.x.z");
        rd_kafka_metric_trie_insert(mtrie->root, RD_KAFKA_METRIC_TYPE_SUM, "org.apache.kafka/a.bb.ccc.dddd.eeee");
        rd_kafka_metric_trie_insert(mtrie->root, RD_KAFKA_METRIC_TYPE_SUM, "org.apache.kafka/a.bb.ccc.dddd.f");
        rd_kafka_metric_trie_insert(mtrie->root, RD_KAFKA_METRIC_TYPE_SUM, "org.apache.kafka/a.bb.ccc.xx.yy");

        rd_kafka_metric_sum_t yy, yy2;
        rd_kafka_metric_sum_init(&yy, mtrie,
                                 "a.bb.ccc.xx.yy");
        rd_kafka_metric_sum_add(&yy, 15);
        rd_kafka_metric_sum_init(&yy2, mtrie, "a.bb.ccc.xx.z.poppy");
        rd_kafka_metric_sum_add(&yy2, 1232356);

        rd_kafka_metric_trie_insert(mtrie->root, RD_KAFKA_METRIC_TYPE_SUM, "org.apache.kafka/a.x.y.z");
        rd_kafka_metric_trie_insert(mtrie->root, RD_KAFKA_METRIC_TYPE_SUM, "edenhill.librdkafka/frayed.ends.of.sanity");
        rd_kafka_metric_trie_insert(mtrie->root, RD_KAFKA_METRIC_TYPE_SUM, "edenhill.librdkafka/frayed.ends.of.lunacy");

        rd_kafka_metrics_set_subscription(
                mtrie->root,
                (const char *[]){
                        "org.apache.kafka/a.bb",
                                "edenhill.librdkafka/frayed",
                                "a.b.x.z",
                                "a.bb"
                                },
                4);

        FILE *fp = fopen("trie.dot", "w");
        fprintf(fp, "digraph {\n");
        rd_kafka_metrics_trie_dump(fp, mtrie->root, rd_false);
        fprintf(fp, "}\n");
        fclose(fp);
}

void rd_kafka_metrics_init (rd_kafka_t *rk) {
        rd_kafka_metric_trie_t mtrie;

        rd_kafka_metric_trie_init(&mtrie);
}
