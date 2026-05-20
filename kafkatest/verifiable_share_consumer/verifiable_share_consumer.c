/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2026, Confluent Inc.
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
#define _POSIX_C_SOURCE 200809L

#include <getopt.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "common.h"
#include "rdkafka.h"

#define POLL_TIMEOUT_MS   5000
#define BATCH_CAPACITY    1024
#define COMMIT_TIMEOUT_MS 60000
#define MAX_TOPIC_LEN     256
#define MAX_PARTITIONS    16

/* These modes only control whether/how we commit; we leave
 * share.acknowledgement.mode unset (matches Java's verifiable consumer)
 * so the consumer uses librdkafka's default (implicit).
 *   AUTO  - no explicit commit; implicit commit fires on the next poll
 *   SYNC  - call rd_kafka_share_commit_sync each batch
 *   ASYNC - call rd_kafka_share_commit_async each batch */
enum ack_mode { ACK_AUTO, ACK_SYNC, ACK_ASYNC };

struct partition_data {
        char topic[MAX_TOPIC_LEN];
        int32_t partition;
        int64_t offsets[BATCH_CAPACITY];
        int offsets_cnt;
};

/* Per-batch accumulator indexed by topic+partition. */
struct partition_bucket {
        struct partition_data items[MAX_PARTITIONS];
        int cnt;
};

/* Module-level handle so cleanup_and_exit can be called from anywhere
 * (including deep helpers and the ack callback). NULL until consumer is
 * created. */
static rd_kafka_share_t *rkshare;

/* Running total of successfully-acknowledged offsets. Read by the main
 * loop's stop condition, written by share_ack_cb. */
static int64_t total_acknowledged;

/* Close + destroy the consumer, emit shutdown_complete, and exit(rc).
 * Safe to call from anywhere after the consumer has been created;
 * matches the cleanup path in main(). */
static void cleanup_and_exit(int rc) {
        if (rkshare) {
                rd_kafka_share_consumer_close(rkshare);
                rd_kafka_share_destroy(rkshare);
                rkshare = NULL;
        }
        emit_event("shutdown_complete");
        exit(rc);
}

static void pd_append(struct partition_data *pd, int64_t offset) {
        if (pd->offsets_cnt >= BATCH_CAPACITY) {
                fprintf(stderr, "offsets array full (%d) for %s[%" PRId32 "]\n",
                        BATCH_CAPACITY, pd->topic, pd->partition);
                cleanup_and_exit(1);
        }
        pd->offsets[pd->offsets_cnt++] = offset;
}

static void pb_reset(struct partition_bucket *pb) {
        for (int i = 0; i < pb->cnt; i++)
                pb->items[i].offsets_cnt = 0;
        pb->cnt = 0;
}

static struct partition_data *pb_find_or_add(struct partition_bucket *pb,
                                             const char *topic,
                                             int32_t partition) {
        struct partition_data *pd;
        for (int i = 0; i < pb->cnt; i++) {
                if (pb->items[i].partition == partition &&
                    strcmp(pb->items[i].topic, topic) == 0)
                        return &pb->items[i];
        }
        if (pb->cnt >= MAX_PARTITIONS) {
                fprintf(stderr,
                        "partition bucket full (%d); cannot add %s[%" PRId32
                        "]; terminating\n",
                        MAX_PARTITIONS, topic, partition);
                cleanup_and_exit(1);
        }
        pd = &pb->items[pb->cnt++];
        snprintf(pd->topic, sizeof(pd->topic), "%s", topic);
        pd->partition   = partition;
        pd->offsets_cnt = 0;
        return pd;
}

/* -------------------- Event emission -------------------- */

static void emit_partitions_array(const struct partition_bucket *pb,
                                  int64_t total_count) {
        const struct partition_data *pd;
        fprintf(stdout, ",\"count\":%" PRId64, total_count);
        fputs(",\"partitions\":[", stdout);
        for (int i = 0; i < pb->cnt; i++) {
                pd = &pb->items[i];
                if (i > 0)
                        fputc(',', stdout);
                fputs("{\"topic\":", stdout);
                json_write_string(stdout, pd->topic);
                fprintf(stdout, ",\"partition\":%" PRId32, pd->partition);
                fprintf(stdout, ",\"count\":%d,\"offsets\":[", pd->offsets_cnt);
                for (int j = 0; j < pd->offsets_cnt; j++) {
                        if (j > 0)
                                fputc(',', stdout);
                        fprintf(stdout, "%" PRId64, pd->offsets[j]);
                }
                fputs("]}", stdout);
        }
        fputs("]", stdout);
}

static void emit_records_consumed(const struct partition_bucket *pb,
                                  int64_t total_count) {
        stdout_lock();
        fputs("{\"name\":\"records_consumed\"", stdout);
        fprintf(stdout, ",\"timestamp\":%" PRId64, now_ms());
        emit_partitions_array(pb, total_count);
        fputs("}\n", stdout);
        fflush(stdout);
        stdout_unlock();
}

static void emit_offsets_acknowledged(const struct partition_bucket *pb,
                                      int64_t total_count,
                                      int success,
                                      const char *errmsg) {
        stdout_lock();
        fputs("{\"name\":\"offsets_acknowledged\"", stdout);
        fprintf(stdout, ",\"timestamp\":%" PRId64, now_ms());
        emit_partitions_array(pb, total_count);
        fputs(",\"success\":", stdout);
        fputs(success ? "true" : "false", stdout);
        if (errmsg && *errmsg) {
                fputs(",\"error\":", stdout);
                json_write_string(stdout, errmsg);
        }
        fputs("}\n", stdout);
        fflush(stdout);
        stdout_unlock();
}

static void emit_offset_reset_strategy_set(const char *strategy) {
        stdout_lock();
        fputs("{\"name\":\"offset_reset_strategy_set\"", stdout);
        fprintf(stdout, ",\"timestamp\":%" PRId64, now_ms());
        fputs(",\"offsetResetStrategy\":", stdout);
        json_write_string(stdout, strategy);
        fputs("}\n", stdout);
        fflush(stdout);
        stdout_unlock();
}

static void emit_record_data(const rd_kafka_message_t *rkm) {
        char *k, *v;
        stdout_lock();
        fputs("{\"name\":\"record_data\"", stdout);
        fprintf(stdout, ",\"timestamp\":%" PRId64, now_ms());
        fputs(",\"key\":", stdout);
        if (rkm->key) {
                k = malloc(rkm->key_len + 1);
                memcpy(k, rkm->key, rkm->key_len);
                k[rkm->key_len] = '\0';
                json_write_string(stdout, k);
                free(k);
        } else {
                fputs("null", stdout);
        }
        fputs(",\"value\":", stdout);
        v = malloc(rkm->len + 1);
        memcpy(v, rkm->payload, rkm->len);
        v[rkm->len] = '\0';
        json_write_string(stdout, v);
        free(v);
        fputs(",\"topic\":", stdout);
        json_write_string(stdout, rd_kafka_topic_name(rkm->rkt));
        fprintf(stdout, ",\"partition\":%" PRId32, rkm->partition);
        fprintf(stdout, ",\"offset\":%" PRId64, rkm->offset);
        fputs("}\n", stdout);
        fflush(stdout);
        stdout_unlock();
}

/* -------------------- Share group config via admin API -------------------- */

/* Set `share.auto.offset.reset` on the share group via IncrementalAlterConfigs.
 * Required because share groups default to reset=latest, so a fresh group
 * started after messages were already produced would consume nothing.
 *
 * Returns 0 on success, -1 on failure. */
static int set_share_group_offset_reset(const char *bootstrap,
                                        const char *group_id,
                                        const char *reset_value,
                                        char *errbuf,
                                        size_t errbuf_size) {
        char errstr[512];
        rd_kafka_conf_t *admin_conf;
        rd_kafka_t *admin;
        rd_kafka_queue_t *q;
        rd_kafka_ConfigResource_t *res;
        rd_kafka_error_t *e;
        rd_kafka_event_t *ev;
        rd_kafka_resp_err_t err;
        int rc = 0;

        admin_conf = rd_kafka_conf_new();
        if (rd_kafka_conf_set(admin_conf, "bootstrap.servers", bootstrap,
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                snprintf(errbuf, errbuf_size, "admin bootstrap.servers: %s",
                         errstr);
                rd_kafka_conf_destroy(admin_conf);
                return -1;
        }

        admin =
            rd_kafka_new(RD_KAFKA_PRODUCER, admin_conf, errstr, sizeof(errstr));
        if (!admin) {
                snprintf(errbuf, errbuf_size, "admin client: %s", errstr);
                rd_kafka_conf_destroy(admin_conf);
                return -1;
        }

        q   = rd_kafka_queue_new(admin);
        res = rd_kafka_ConfigResource_new(RD_KAFKA_RESOURCE_GROUP, group_id);

        e = rd_kafka_ConfigResource_add_incremental_config(
            res, "share.auto.offset.reset", RD_KAFKA_ALTER_CONFIG_OP_TYPE_SET,
            reset_value);
        if (e) {
                snprintf(errbuf, errbuf_size, "add_incremental_config: %s",
                         rd_kafka_error_string(e));
                rd_kafka_error_destroy(e);
                rd_kafka_ConfigResource_destroy(res);
                rd_kafka_queue_destroy(q);
                rd_kafka_destroy(admin);
                return -1;
        }

        rd_kafka_IncrementalAlterConfigs(admin, &res, 1, NULL, q);
        rd_kafka_ConfigResource_destroy(res);

        ev = rd_kafka_queue_poll(q, 30000);
        if (!ev) {
                snprintf(errbuf, errbuf_size,
                         "IncrementalAlterConfigs: timeout");
                rc = -1;
        } else {
                err = rd_kafka_event_error(ev);
                if (err) {
                        snprintf(errbuf, errbuf_size,
                                 "IncrementalAlterConfigs: %s",
                                 rd_kafka_event_error_string(ev));
                        rc = -1;
                }
                rd_kafka_event_destroy(ev);
        }

        rd_kafka_queue_destroy(q);
        rd_kafka_destroy(admin);
        return rc;
}

/* Acknowledgement-commit callback
 *
 * For async commits, this callback
 * is the only source of offsets_acknowledged events.
 */
static void share_ack_cb(rd_kafka_share_t *rkshare,
                         rd_kafka_share_partition_offsets_list_t *partitions,
                         rd_kafka_resp_err_t err,
                         void *opaque) {
        struct partition_bucket pb = {0};
        const rd_kafka_share_partition_offsets_t *entry;
        const rd_kafka_topic_partition_t *tp;
        const int64_t *offsets;
        struct partition_data *pd;
        const char *errmsg;
        size_t offsets_cnt, i;
        int success;

        /* Each callback invocation carries exactly one partition with one
         * error. */
        entry = rd_kafka_share_partition_offsets_list_get(partitions, 0);
        if (!entry)
                return;

        tp          = rd_kafka_share_partition_offsets_partition(entry);
        offsets     = rd_kafka_share_partition_offsets_offsets(entry);
        offsets_cnt = rd_kafka_share_partition_offsets_offsets_cnt(entry);

        pd = pb_find_or_add(&pb, tp->topic, tp->partition);
        for (i = 0; i < offsets_cnt; i++)
                pd_append(pd, offsets[i]);

        if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                success = 1;
                errmsg  = "";
        } else {
                success = 0;
                errmsg  = rd_kafka_err2str(err);
        }

        emit_offsets_acknowledged(&pb, (int64_t)offsets_cnt, success, errmsg);

        if (success)
                total_acknowledged += (int64_t)offsets_cnt;
}

/* Run a synchronous commit. Acknowledgement events are emitted by
 * share_ack_cb (which fires for sync mode too), so this only inspects
 * the result for stderr logging. Matches Java's verifiable consumer. */
static void handle_commit_sync(rd_kafka_share_t *rkshare) {
        rd_kafka_topic_partition_list_t *result = NULL;
        rd_kafka_error_t *commit_error;
        int i;

        commit_error =
            rd_kafka_share_commit_sync(rkshare, COMMIT_TIMEOUT_MS, &result);

        if (commit_error) {
                fprintf(stderr, "commit_sync: %s\n",
                        rd_kafka_error_string(commit_error));
                rd_kafka_error_destroy(commit_error);
                if (result)
                        rd_kafka_topic_partition_list_destroy(result);
                return;
        }

        if (result) {
                for (i = 0; i < result->cnt; i++) {
                        if (result->elems[i].err != RD_KAFKA_RESP_ERR_NO_ERROR)
                                fprintf(stderr,
                                        "commit_sync %s[%" PRId32 "]: %s\n",
                                        result->elems[i].topic,
                                        result->elems[i].partition,
                                        rd_kafka_err2str(result->elems[i].err));
                }
                rd_kafka_topic_partition_list_destroy(result);
        }
}

/* -------------------- CLI -------------------- */

static void usage(const char *prog) {
        fprintf(stderr,
                "Usage: %s --topic <t> --group-id <g> --bootstrap-server <s> "
                "[options]\n"
                "\nOptions:\n"
                "  --topic <t>                    Topic (required)\n"
                "  --group-id <g>                 Share group id (required)\n"
                "  --bootstrap-server <s>         Broker list (required; "
                "also --broker-list)\n"
                "  --max-messages <n>             Default: -1 (infinite)\n"
                "  --acknowledgement-mode <m>     auto|sync|async "
                "(default: auto)\n"
                "  --offset-reset-strategy <s>    earliest|latest; sets "
                "share.auto.offset.reset on the group via AlterConfigs\n"
                "  --verbose                      Emit record_data per msg\n"
                "  --command-config <file>        Properties file\n"
                "  --debug <flags>                librdkafka debug flags\n"
                "  -X, --property <k=v,..>        Raw librdkafka properties\n",
                prog);
}

int main(int argc, char **argv) {
        const char *topic                 = NULL;
        const char *group_id              = NULL;
        const char *bootstrap             = NULL;
        int64_t max_messages              = -1;
        enum ack_mode ack_mode            = ACK_AUTO;
        int verbose                       = 0;
        const char *offset_reset_strategy = NULL;
        const char *config_file           = NULL;
        const char *debug_flags           = NULL;
        const char *x_props               = NULL;
        int opt;
        char errstr[512];
        char admin_errstr[512];
        rd_kafka_conf_t *conf;
        rd_kafka_topic_partition_list_t *subscription;
        rd_kafka_resp_err_t err;
        rd_kafka_error_t *error;
        rd_kafka_error_t *commit_error;
        rd_kafka_message_t *batch[BATCH_CAPACITY];
        rd_kafka_message_t *rkm;
        const char *t;
        struct partition_data *pd_c;
        size_t i, rcvd;
        int64_t consumed_in_batch;
        int fatal, retriable;
        static struct partition_bucket consumed;

        static struct option long_opts[] = {
            {"topic", required_argument, 0, 't'},
            {"group-id", required_argument, 0, 'g'},
            {"bootstrap-server", required_argument, 0, 'b'},
            {"broker-list", required_argument, 0, 'b'},
            {"max-messages", required_argument, 0, 'm'},
            {"acknowledgement-mode", required_argument, 0, 'A'},
            {"offset-reset-strategy", required_argument, 0, 'O'},
            {"verbose", no_argument, 0, 'V'},
            {"command-config", required_argument, 0, 'C'},
            {"debug", required_argument, 0, 'd'},
            {"property", required_argument, 0, 'X'},
            {"help", no_argument, 0, 'h'},
            {0, 0, 0, 0}};

        while ((opt = getopt_long(argc, argv, "X:h", long_opts, NULL)) != -1) {
                switch (opt) {
                case 't':
                        topic = optarg;
                        break;
                case 'g':
                        group_id = optarg;
                        break;
                case 'b':
                        bootstrap = optarg;
                        break;
                case 'm':
                        max_messages = strtoll(optarg, NULL, 10);
                        break;
                case 'A':
                        if (!strcmp(optarg, "auto"))
                                ack_mode = ACK_AUTO;
                        else if (!strcmp(optarg, "sync"))
                                ack_mode = ACK_SYNC;
                        else if (!strcmp(optarg, "async"))
                                ack_mode = ACK_ASYNC;
                        else {
                                fprintf(stderr,
                                        "--acknowledgement-mode must be "
                                        "auto|sync|async\n");
                                return 1;
                        }
                        break;
                case 'O':
                        if (strcmp(optarg, "earliest") != 0 &&
                            strcmp(optarg, "latest") != 0) {
                                fprintf(stderr,
                                        "--offset-reset-strategy must be "
                                        "earliest|latest\n");
                                return 1;
                        }
                        offset_reset_strategy = optarg;
                        break;
                case 'V':
                        verbose = 1;
                        break;
                case 'C':
                        config_file = optarg;
                        break;
                case 'd':
                        debug_flags = optarg;
                        break;
                case 'X':
                        x_props = optarg;
                        break;
                case 'h':
                        usage(argv[0]);
                        return 0;
                default:
                        usage(argv[0]);
                        return 1;
                }
        }

        if (!topic || !group_id || !bootstrap) {
                fprintf(stderr,
                        "--topic, --group-id, and --bootstrap-server are "
                        "required\n");
                usage(argv[0]);
                return 1;
        }

        install_signals();

        conf = rd_kafka_conf_new();

        if (conf_set(conf, "bootstrap.servers", bootstrap) == -1)
                return 1;
        if (conf_set(conf, "group.id", group_id) == -1)
                return 1;
        if (debug_flags && conf_set(conf, "debug", debug_flags) == -1)
                return 1;

        rd_kafka_conf_set_share_acknowledgement_commit_cb(conf, share_ack_cb);

        if (config_file && load_properties_file(config_file, conf, errstr,
                                                sizeof(errstr)) == -1) {
                fprintf(stderr, "%s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return 1;
        }
        if (x_props &&
            apply_x_properties(x_props, conf, errstr, sizeof(errstr)) == -1) {
                fprintf(stderr, "%s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return 1;
        }

        rkshare = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        if (!rkshare) {
                fprintf(stderr, "Failed to create share consumer: %s\n",
                        errstr);
                rd_kafka_conf_destroy(conf);
                return 1;
        }

        emit_event("startup_complete");

        /* Post-consumer-create errors fall through to `cleanup` so the
         * consumer is closed cleanly and shutdown_complete is emitted
         * regardless of exit path (matches Java's finally block). */

        /* If requested, set share.auto.offset.reset on the group before
         * subscribing. */
        if (offset_reset_strategy) {
                if (set_share_group_offset_reset(
                        bootstrap, group_id, offset_reset_strategy,
                        admin_errstr, sizeof(admin_errstr)) != 0) {
                        fprintf(stderr, "%s\n", admin_errstr);
                        cleanup_and_exit(1);
                }
                emit_offset_reset_strategy_set(offset_reset_strategy);
        }

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);
        err = rd_kafka_share_subscribe(rkshare, subscription);
        rd_kafka_topic_partition_list_destroy(subscription);
        if (err) {
                fprintf(stderr, "subscribe: %s\n", rd_kafka_err2str(err));
                cleanup_and_exit(1);
        }

        while (run && (max_messages < 0 || total_acknowledged < max_messages)) {
                rcvd  = 0;
                error = rd_kafka_share_consume_batch(rkshare, POLL_TIMEOUT_MS,
                                                     batch, &rcvd);
                if (error) {
                        fatal     = rd_kafka_error_is_fatal(error);
                        retriable = rd_kafka_error_is_retriable(error);
                        fprintf(stderr,
                                "consume_batch: %s (fatal=%d, retriable=%d)\n",
                                rd_kafka_error_string(error), fatal, retriable);
                        rd_kafka_error_destroy(error);
                        if (fatal || !retriable)
                                cleanup_and_exit(1);
                        continue;
                }
                if (rcvd == 0)
                        continue;

                pb_reset(&consumed);
                consumed_in_batch = 0;

                for (i = 0; i < rcvd; i++) {
                        rkm = batch[i];
                        if (rkm->err) {
                                fprintf(stderr, "share msg err: %s\n",
                                        rd_kafka_message_errstr(rkm));
                                rd_kafka_message_destroy(rkm);
                                continue;
                        }

                        t    = rd_kafka_topic_name(rkm->rkt);
                        pd_c = pb_find_or_add(&consumed, t, rkm->partition);
                        pd_append(pd_c, rkm->offset);
                        consumed_in_batch++;

                        if (verbose)
                                emit_record_data(rkm);

                        rd_kafka_message_destroy(rkm);
                }

                if (consumed_in_batch > 0)
                        emit_records_consumed(&consumed, consumed_in_batch);

                /* Commit behavior per mode:
                 *
                 *   sync: call commit_sync, inspect per-partition
                 *         result, emit offsets_acknowledged with
                 *         per-partition success/error split.
                 *
                 *   async: fire-and-forget commit_async. The
                 *          acknowledgement-commit callback emits
                 *          offsets_acknowledged and updates
                 *          total_acknowledged.
                 *
                 *   auto (implicit): no explicit commit; librdkafka
                 *         drives implicit commits on the next poll.
                 *         The callback emits offsets_acknowledged and
                 *         updates total_acknowledged. */

                if (ack_mode == ACK_SYNC) {
                        handle_commit_sync(rkshare);
                } else if (ack_mode == ACK_ASYNC) {
                        commit_error = rd_kafka_share_commit_async(rkshare);
                        if (commit_error) {
                                fprintf(stderr, "commit_async: %s\n",
                                        rd_kafka_error_string(commit_error));
                                rd_kafka_error_destroy(commit_error);
                        }
                }
        }

        cleanup_and_exit(0);
        return 0; /* unreachable; satisfies compiler */
}