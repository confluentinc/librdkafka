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

enum ack_mode {
        ACK_AUTO, /* share.acknowledgement.mode=implicit; implicit ack via
                     commit */
        ACK_SYNC, /* explicit ack + rd_kafka_share_commit_sync */
        ACK_ASYNC /* explicit ack + rd_kafka_share_commit_async */
};

#define MAX_TOPIC_LEN  256
#define MAX_PARTITIONS 16

struct partition_data {
        char topic[MAX_TOPIC_LEN];
        int32_t partition;
        int64_t offsets[BATCH_CAPACITY];
        int offsets_cnt;
};

static void pd_append(struct partition_data *pd, int64_t offset) {
        if (pd->offsets_cnt >= BATCH_CAPACITY) {
                fprintf(stderr, "offsets array full (%d) for %s[%" PRId32 "]\n",
                        BATCH_CAPACITY, pd->topic, pd->partition);
                return;
        }
        pd->offsets[pd->offsets_cnt++] = offset;
}

/* Per-batch accumulator indexed by topic+partition. */
struct partition_bucket {
        struct partition_data items[MAX_PARTITIONS];
        int cnt;
};

static void pb_reset(struct partition_bucket *pb) {
        for (int i = 0; i < pb->cnt; i++)
                pb->items[i].offsets_cnt = 0;
        pb->cnt = 0;
}

static struct partition_data *pb_find_or_add(struct partition_bucket *pb,
                                             const char *topic,
                                             int32_t partition) {
        for (int i = 0; i < pb->cnt; i++) {
                if (pb->items[i].partition == partition &&
                    strcmp(pb->items[i].topic, topic) == 0)
                        return &pb->items[i];
        }
        if (pb->cnt >= MAX_PARTITIONS) {
                fprintf(stderr,
                        "partition bucket full (%d); dropping %s[%" PRId32
                        "]\n",
                        MAX_PARTITIONS, topic, partition);
                return &pb->items[pb->cnt - 1];
        }
        struct partition_data *pd = &pb->items[pb->cnt++];
        snprintf(pd->topic, sizeof(pd->topic), "%s", topic);
        pd->partition   = partition;
        pd->offsets_cnt = 0;
        return pd;
}

/* -------------------- Event emission -------------------- */

static void emit_partitions_array(const struct partition_bucket *pb,
                                  int64_t total_count) {
        fprintf(stdout, ",\"count\":%" PRId64, total_count);
        fputs(",\"partitions\":[", stdout);
        for (int i = 0; i < pb->cnt; i++) {
                const struct partition_data *pd = &pb->items[i];
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
        stdout_lock();
        fputs("{\"name\":\"record_data\"", stdout);
        fprintf(stdout, ",\"timestamp\":%" PRId64, now_ms());
        fputs(",\"key\":", stdout);
        if (rkm->key) {
                char *k = malloc(rkm->key_len + 1);
                memcpy(k, rkm->key, rkm->key_len);
                k[rkm->key_len] = '\0';
                json_write_string(stdout, k);
                free(k);
        } else {
                fputs("null", stdout);
        }
        fputs(",\"value\":", stdout);
        char *v = malloc(rkm->len + 1);
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
        rd_kafka_conf_t *admin_conf = rd_kafka_conf_new();

        if (rd_kafka_conf_set(admin_conf, "bootstrap.servers", bootstrap,
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                snprintf(errbuf, errbuf_size, "admin bootstrap.servers: %s",
                         errstr);
                rd_kafka_conf_destroy(admin_conf);
                return -1;
        }

        rd_kafka_t *admin =
            rd_kafka_new(RD_KAFKA_PRODUCER, admin_conf, errstr, sizeof(errstr));
        if (!admin) {
                snprintf(errbuf, errbuf_size, "admin client: %s", errstr);
                rd_kafka_conf_destroy(admin_conf);
                return -1;
        }

        rd_kafka_queue_t *q = rd_kafka_queue_new(admin);
        rd_kafka_ConfigResource_t *res =
            rd_kafka_ConfigResource_new(RD_KAFKA_RESOURCE_GROUP, group_id);

        rd_kafka_error_t *e = rd_kafka_ConfigResource_add_incremental_config(
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

        rd_kafka_event_t *ev = rd_kafka_queue_poll(q, 30000);
        int rc               = 0;
        if (!ev) {
                snprintf(errbuf, errbuf_size,
                         "IncrementalAlterConfigs: timeout");
                rc = -1;
        } else {
                rd_kafka_resp_err_t err = rd_kafka_event_error(ev);
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

/* Look up the per-partition error in a commit_sync result list.
 * Returns RD_KAFKA_RESP_ERR_NO_ERROR if not present (commit_sync omits
 * partitions that had nothing pending). */
static rd_kafka_resp_err_t
lookup_partition_err(const rd_kafka_topic_partition_list_t *result,
                     const char *topic,
                     int32_t partition) {
        for (int j = 0; j < result->cnt; j++) {
                if (result->elems[j].partition == partition &&
                    strcmp(result->elems[j].topic, topic) == 0)
                        return result->elems[j].err;
        }
        return RD_KAFKA_RESP_ERR_UNKNOWN;
}

/* Run a synchronous commit and emit offsets_acknowledged events.
 * Returns the number of offsets that committed successfully. */
static int64_t handle_commit_sync(rd_kafka_share_t *rkshare,
                                  struct partition_bucket *acked,
                                  struct partition_bucket *acked_ok,
                                  struct partition_bucket *acked_err,
                                  int64_t acked_in_batch) {
        rd_kafka_topic_partition_list_t *result = NULL;
        rd_kafka_error_t *commit_error =
            rd_kafka_share_commit_sync(rkshare, COMMIT_TIMEOUT_MS, &result);

        /* Top-level commit_sync failure: no per-partition result to
         * inspect. Emit the whole batch as failed. */
        if (commit_error) {
                char errmsg[256];
                snprintf(errmsg, sizeof(errmsg), "%s",
                         rd_kafka_error_string(commit_error));
                rd_kafka_error_destroy(commit_error);
                if (result)
                        rd_kafka_topic_partition_list_destroy(result);
                if (acked_in_batch > 0)
                        emit_offsets_acknowledged(acked, acked_in_batch, 0,
                                                  errmsg);
                return 0;
        }

        /* Split `acked` into per-partition-result buckets. A commit may
         * partially succeed: some partitions OK, others failed. Emit one
         * offsets_acknowledged per outcome so the harness records them
         * correctly. */
        pb_reset(acked_ok);
        pb_reset(acked_err);
        int64_t ok_count  = 0;
        int64_t err_count = 0;
        char errmsg[256]  = "";

        for (int i = 0; i < acked->cnt; i++) {
                struct partition_data *src = &acked->items[i];
                rd_kafka_resp_err_t perr =
                    lookup_partition_err(result, src->topic, src->partition);

                struct partition_bucket *dst =
                    perr == RD_KAFKA_RESP_ERR_NO_ERROR ? acked_ok : acked_err;
                struct partition_data *pd_dst =
                    pb_find_or_add(dst, src->topic, src->partition);
                for (int k = 0; k < src->offsets_cnt; k++)
                        pd_append(pd_dst, src->offsets[k]);

                if (perr == RD_KAFKA_RESP_ERR_NO_ERROR) {
                        ok_count += src->offsets_cnt;
                } else {
                        err_count += src->offsets_cnt;
                        if (!*errmsg)
                                snprintf(errmsg, sizeof(errmsg), "%s",
                                         rd_kafka_err2str(perr));
                }
        }

        if (result)
                rd_kafka_topic_partition_list_destroy(result);

        if (ok_count > 0)
                emit_offsets_acknowledged(acked_ok, ok_count, 1, "");
        if (err_count > 0)
                emit_offsets_acknowledged(acked_err, err_count, 0, errmsg);

        return ok_count;
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

        int opt;
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

        char errstr[512];
        rd_kafka_conf_t *conf = rd_kafka_conf_new();

        if (conf_set(conf, "bootstrap.servers", bootstrap) == -1)
                return 1;
        if (conf_set(conf, "group.id", group_id) == -1)
                return 1;
        if (conf_set(conf, "share.acknowledgement.mode",
                     ack_mode == ACK_AUTO ? "implicit" : "explicit") == -1)
                return 1;
        if (debug_flags && conf_set(conf, "debug", debug_flags) == -1)
                return 1;

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

        rd_kafka_share_t *rkshare =
            rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        if (!rkshare) {
                fprintf(stderr, "Failed to create share consumer: %s\n",
                        errstr);
                rd_kafka_conf_destroy(conf);
                return 1;
        }

        emit_event("startup_complete");

        /* If requested, set share.auto.offset.reset on the group before
         * subscribing. */
        if (offset_reset_strategy) {
                char admin_errstr[512];
                if (set_share_group_offset_reset(
                        bootstrap, group_id, offset_reset_strategy,
                        admin_errstr, sizeof(admin_errstr)) != 0) {
                        fprintf(stderr, "%s\n", admin_errstr);
                        rd_kafka_share_destroy(rkshare);
                        return 1;
                }
                emit_offset_reset_strategy_set(offset_reset_strategy);
        }

        rd_kafka_topic_partition_list_t *subscription =
            rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);
        rd_kafka_resp_err_t err =
            rd_kafka_share_subscribe(rkshare, subscription);
        rd_kafka_topic_partition_list_destroy(subscription);
        if (err) {
                fprintf(stderr, "subscribe: %s\n", rd_kafka_err2str(err));
                rd_kafka_share_destroy(rkshare);
                return 1;
        }

        static struct partition_bucket consumed, acked, acked_ok, acked_err;
        rd_kafka_message_t *batch[BATCH_CAPACITY];
        int64_t total_acknowledged = 0;

        while (run && (max_messages < 0 || total_acknowledged < max_messages)) {
                size_t rcvd             = 0;
                rd_kafka_error_t *error = rd_kafka_share_consume_batch(
                    rkshare, POLL_TIMEOUT_MS, batch, &rcvd);
                if (error) {
                        fprintf(stderr, "consume_batch: %s\n",
                                rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                        continue;
                }
                if (rcvd == 0)
                        continue;

                pb_reset(&consumed);
                pb_reset(&acked);
                int64_t consumed_in_batch = 0;
                int64_t acked_in_batch    = 0;

                for (size_t i = 0; i < rcvd; i++) {
                        rd_kafka_message_t *rkm = batch[i];
                        if (rkm->err) {
                                fprintf(stderr, "share msg err: %s\n",
                                        rd_kafka_message_errstr(rkm));
                                rd_kafka_message_destroy(rkm);
                                continue;
                        }

                        const char *t = rd_kafka_topic_name(rkm->rkt);
                        struct partition_data *pd_c =
                            pb_find_or_add(&consumed, t, rkm->partition);
                        pd_append(pd_c, rkm->offset);
                        consumed_in_batch++;

                        if (verbose)
                                emit_record_data(rkm);

                        /* In explicit (sync/async) mode we ACCEPT each
                         * record. */
                        if (ack_mode != ACK_AUTO) {
                                rd_kafka_resp_err_t ack_err =
                                    rd_kafka_share_acknowledge_type(
                                        rkshare, rkm,
                                        RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT);
                                if (ack_err)
                                        fprintf(stderr, "acknowledge: %s\n",
                                                rd_kafka_err2str(ack_err));
                        }

                        /* Track what we are about to commit. */
                        struct partition_data *pd_a =
                            pb_find_or_add(&acked, t, rkm->partition);
                        pd_append(pd_a, rkm->offset);
                        acked_in_batch++;

                        rd_kafka_message_destroy(rkm);
                }

                if (consumed_in_batch > 0)
                        emit_records_consumed(&consumed, consumed_in_batch);

                /* Commit behavior per mode:
                 *
                 *   auto  (implicit): share.acknowledgement.mode=implicit
                 *                     means librdkafka accepts on the
                 *                     next poll. We do not issue a
                 *                     commit and cannot emit
                 *                     offsets_acknowledged.
                 *                     TODO once librdkafka
                 *                     adds the callback
                 *
                 *   sync: call commit_sync, inspect per-partition
                 *         result, emit offsets_acknowledged with
                 *         success=true iff every partition err is 0.
                 *
                 *   async: fire-and-forget commit_async. No callback
                 *          available, so do NOT emit
                 *          offsets_acknowledged. TODO once librdkafka
                 *          adds the callback. */

                if (ack_mode == ACK_SYNC) {
                        total_acknowledged +=
                            handle_commit_sync(rkshare, &acked, &acked_ok,
                                               &acked_err, acked_in_batch);
                } else if (ack_mode == ACK_ASYNC) {
                        rd_kafka_error_t *commit_error =
                            rd_kafka_share_commit_async(rkshare);
                        if (commit_error) {
                                fprintf(stderr, "commit_async: %s\n",
                                        rd_kafka_error_string(commit_error));
                                rd_kafka_error_destroy(commit_error);
                        }
                        /* Optimistically count as acked for the
                         * max-messages guard; do NOT emit an event.
                         * TODO once librdkafka adds callback */
                        total_acknowledged += acked_in_batch;
                } else {
                        /* auto/implicit: no commit, no event. Count
                         * consumed-as-acked for the guard.
                         * TODO once librdkafka adds callback
                         */
                        total_acknowledged += consumed_in_batch;
                }
        }

        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);

        emit_event("shutdown_complete");
        return 0;
}