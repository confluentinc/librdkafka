/*
 * librdkafka share-consumer verification helper.
 *
 * Subscribes to one or more topics as a KIP-932 share consumer in
 * EXPLICIT acknowledgement mode. For every record it emits one JSON
 * event to stdout for the consume side and one JSON event per record
 * for the ack side. The orchestrator (tests/chaos/chaos.py) consumes
 * these events to build a per-(topic, partition, offset) -> delivery
 * count map and detect records that were consumed but never acked
 * (potential data loss) or acked without being consumed (book-keeping
 * inconsistency).
 *
 * Event lines (one JSON object per line, stdout):
 *
 *   {"e":"consumed","t":"<topic>","id":"<topic_id_b64>","p":<part>,
 *      "o":<offset>,"dc":<deliv_cnt>}
 *   {"e":"acked","t":"<topic>","id":"<topic_id_b64>","p":<part>,
 *      "o":<offset>,"err":<resp_err_int>}
 *
 * `id` is a base64-encoded snapshot of the broker-side topic_id at
 * the time of the consume. Under --topic-chaos, the OLD and NEW
 * topic generations both restart from offset 0, so the consumer
 * bookkeeping must key on (topic, id, partition, offset) — name +
 * id together — to avoid collisions.
 *
 * stderr is reserved for librdkafka debug logs.
 *
 * Usage:
 *   share_consume_verify [-d <debug>] [-X k=v]... \
 *                         <bootstrap> <group.id> <topic1> [topic2 ..]
 *
 *   -d <debug>   librdkafka debug contexts (e.g.
 *                broker,fetch,cgrp,protocol,topic).
 *   -X k=v       Set arbitrary rdkafka conf property (repeatable).
 *                Mirrors rdkafka_performance -X. Default-set:
 *                  share.acknowledgement.mode=explicit
 *                Override by passing it explicitly.
 *
 * SIGINT triggers a clean shutdown.
 */

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "rdkafka.h"

static volatile sig_atomic_t run = 1;

static void stop(int sig) {
        (void)sig;
        run = 0;
}

#define MAX_BATCH 10001

/* Track which offsets we explicitly acked in this batch so we can
 * emit the matching "acked" events tagged with the per-partition err
 * from commit_sync. Lives on the stack of the main loop; cleared per
 * batch. */
struct acked_offset {
        const char *topic;
        int32_t partition;
        int64_t offset;
        /* base64-encoded topic_id captured at consume time, reused
         * for the matching "acked" event so the orchestrator can
         * key (topic, topic_id, partition, offset) — necessary
         * under --topic-chaos where OLD and NEW topic generations
         * both restart from offset 0. ~22 chars + NUL; 32 is
         * comfortable. */
        char topic_id_str[32];
};

static void usage(const char *prog) {
        fprintf(stderr,
                "Usage: %s [-d <debug>] [-X k=v]... "
                "<broker> <group.id> <topic1> [topic2 ..]\n",
                prog);
}

/* Apply one "key=value" string to the conf. Returns 0 on success,
 * -1 on parse / set error (errstr populated by librdkafka). */
static int apply_conf_kv(rd_kafka_conf_t *conf,
                         const char *kv,
                         char *errstr,
                         size_t errstr_size) {
        const char *eq = strchr(kv, '=');
        char key[256];
        size_t klen;

        if (!eq || eq == kv) {
                snprintf(errstr, errstr_size, "expected key=value, got '%s'",
                         kv);
                return -1;
        }
        klen = (size_t)(eq - kv);
        if (klen >= sizeof(key)) {
                snprintf(errstr, errstr_size, "conf key too long: '%s'", kv);
                return -1;
        }
        memcpy(key, kv, klen);
        key[klen] = '\0';

        if (rd_kafka_conf_set(conf, key, eq + 1, errstr, errstr_size) !=
            RD_KAFKA_CONF_OK)
                return -1;
        return 0;
}

int main(int argc, char **argv) {
        rd_kafka_share_t *rkshare;
        rd_kafka_conf_t *conf;
        rd_kafka_resp_err_t err;
        char errstr[512];
        const char *brokers;
        const char *groupid;
        char **topics;
        int topic_cnt;
        const char *debug = NULL;
        rd_kafka_topic_partition_list_t *subscription;
        rd_kafka_message_t *rkmessages[MAX_BATCH];
        struct acked_offset acked[MAX_BATCH];
        int i;
        int opt;

        conf = rd_kafka_conf_new();

        /* share.acknowledgement.mode is the only conf this binary
         * really requires (explicit-ack drives the per-record
         * bookkeeping). Set it as the default; user can still
         * override via -X share.acknowledgement.mode=implicit. */
        if (rd_kafka_conf_set(conf, "share.acknowledgement.mode", "explicit",
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%% conf: %s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return 1;
        }

        while ((opt = getopt(argc, argv, "d:X:h")) != -1) {
                switch (opt) {
                case 'd':
                        debug = optarg;
                        break;
                case 'X':
                        if (apply_conf_kv(conf, optarg, errstr,
                                          sizeof(errstr)) == -1) {
                                fprintf(stderr, "%% -X %s: %s\n", optarg,
                                        errstr);
                                rd_kafka_conf_destroy(conf);
                                return 1;
                        }
                        break;
                case 'h':
                        usage(argv[0]);
                        rd_kafka_conf_destroy(conf);
                        return 0;
                default:
                        usage(argv[0]);
                        rd_kafka_conf_destroy(conf);
                        return 1;
                }
        }

        if (argc - optind < 3) {
                usage(argv[0]);
                rd_kafka_conf_destroy(conf);
                return 1;
        }

        brokers   = argv[optind];
        groupid   = argv[optind + 1];
        topics    = &argv[optind + 2];
        topic_cnt = argc - optind - 2;

        if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK ||
            rd_kafka_conf_set(conf, "group.id", groupid, errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%% conf: %s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return 1;
        }

        if (debug && rd_kafka_conf_set(conf, "debug", debug, errstr,
                                       sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%% -d %s: %s\n", debug, errstr);
                rd_kafka_conf_destroy(conf);
                return 1;
        }

        rkshare = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        if (!rkshare) {
                fprintf(stderr, "%% Failed to create share consumer: %s\n",
                        errstr);
                return 1;
        }
        conf = NULL;

        subscription = rd_kafka_topic_partition_list_new(topic_cnt);
        for (i = 0; i < topic_cnt; i++)
                rd_kafka_topic_partition_list_add(subscription, topics[i],
                                                  RD_KAFKA_PARTITION_UA);

        err = rd_kafka_share_subscribe(rkshare, subscription);
        rd_kafka_topic_partition_list_destroy(subscription);
        if (err) {
                fprintf(stderr, "%% subscribe failed: %s\n",
                        rd_kafka_err2str(err));
                rd_kafka_share_destroy(rkshare);
                return 1;
        }

        /* Stdout is the event channel. Line-buffer so each JSON
         * object becomes visible to the orchestrator immediately. */
        setvbuf(stdout, NULL, _IOLBF, 0);

        fprintf(stderr, "%% verify: subscribed to %d topic(s)\n", topic_cnt);

        signal(SIGINT, stop);

        while (run) {
                size_t rcvd   = 0;
                int acked_cnt = 0;
                rd_kafka_error_t *error;
                rd_kafka_topic_partition_list_t *results = NULL;

                error = rd_kafka_share_consume_batch(rkshare, 1000, rkmessages,
                                                     &rcvd);
                if (error) {
                        fprintf(stderr, "%% consume_batch error: %s\n",
                                rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                        continue;
                }

                if (rcvd == 0)
                        continue;

                for (i = 0; i < (int)rcvd; i++) {
                        rd_kafka_message_t *rkm = rkmessages[i];
                        rd_kafka_Uuid_t *id;
                        const char *id_str;

                        if (rkm->err) {
                                fprintf(stderr,
                                        "%% rkm err on %s [%" PRId32 "]: %s\n",
                                        rkm->rkt ? rd_kafka_topic_name(rkm->rkt)
                                                 : "?",
                                        rkm->partition,
                                        rd_kafka_message_errstr(rkm));
                                rd_kafka_message_destroy(rkm);
                                continue;
                        }

                        /* Capture the topic_id at consume time; the
                         * same string is reused for the matching
                         * "acked" event so consumed+acked share the
                         * same (topic, topic_id, partition, offset)
                         * key on the orchestrator side. */
                        id     = rd_kafka_topic_id(rkm->rkt);
                        id_str = rd_kafka_Uuid_base64str(id);

                        /* "consumed" event */
                        printf(
                            "{\"e\":\"consumed\",\"t\":\"%s\","
                            "\"id\":\"%s\","
                            "\"p\":%" PRId32 ",\"o\":%" PRId64
                            ",\"dc\":%" PRId16 "}\n",
                            rd_kafka_topic_name(rkm->rkt), id_str,
                            rkm->partition, rkm->offset,
                            rd_kafka_message_delivery_count(rkm));

                        err = rd_kafka_share_acknowledge_type(
                            rkshare, rkm,
                            RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT);
                        if (err) {
                                fprintf(stderr,
                                        "%% acknowledge_type err for "
                                        "%s [%" PRId32 "] @ %" PRId64 ": %s\n",
                                        rd_kafka_topic_name(rkm->rkt),
                                        rkm->partition, rkm->offset,
                                        rd_kafka_err2str(err));
                                rd_kafka_Uuid_destroy(id);
                                rd_kafka_message_destroy(rkm);
                                continue;
                        }

                        /* Remember offsets so we can pair the per-record
                         * "acked" event with the per-partition err from
                         * commit_sync. The topic string lives in the
                         * rkt and is destroyed below, so copy via the
                         * topic_name (interned in rkt's parent rkt
                         * structure — stable across the message
                         * destroy). */
                        acked[acked_cnt].topic = rd_kafka_topic_name(rkm->rkt);
                        acked[acked_cnt].partition = rkm->partition;
                        acked[acked_cnt].offset    = rkm->offset;
                        snprintf(acked[acked_cnt].topic_id_str,
                                 sizeof(acked[acked_cnt].topic_id_str), "%s",
                                 id_str);
                        acked_cnt++;

                        rd_kafka_Uuid_destroy(id);
                        rd_kafka_message_destroy(rkm);
                }

                if (acked_cnt == 0)
                        continue;

                error = rd_kafka_share_commit_sync(rkshare, 30000, &results);
                if (error) {
                        rd_kafka_resp_err_t top_err =
                            rd_kafka_error_code(error);
                        fprintf(stderr, "%% commit_sync error: %s\n",
                                rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);

                        /* Surface the top-level err to the orchestrator
                         * for every record we just acked, since we
                         * can't tell partition-level outcomes. */
                        for (i = 0; i < acked_cnt; i++)
                                printf(
                                    "{\"e\":\"acked\",\"t\":\"%s\","
                                    "\"id\":\"%s\","
                                    "\"p\":%" PRId32 ",\"o\":%" PRId64
                                    ",\"err\":%d}\n",
                                    acked[i].topic, acked[i].topic_id_str,
                                    acked[i].partition, acked[i].offset,
                                    (int)top_err);
                        if (results)
                                rd_kafka_topic_partition_list_destroy(results);
                        continue;
                }

                /* commit_sync returned a per-partition result list.
                 * Look up each acked record's (topic, partition) and
                 * emit "acked" with the matching err. */
                for (i = 0; i < acked_cnt; i++) {
                        rd_kafka_topic_partition_t *rktpar = NULL;
                        rd_kafka_resp_err_t part_err =
                            RD_KAFKA_RESP_ERR_NO_ERROR;

                        if (results)
                                rktpar = rd_kafka_topic_partition_list_find(
                                    results, acked[i].topic,
                                    acked[i].partition);
                        if (rktpar)
                                part_err = rktpar->err;

                        printf(
                            "{\"e\":\"acked\",\"t\":\"%s\","
                            "\"id\":\"%s\","
                            "\"p\":%" PRId32 ",\"o\":%" PRId64 ",\"err\":%d}\n",
                            acked[i].topic, acked[i].topic_id_str,
                            acked[i].partition, acked[i].offset, (int)part_err);
                }

                if (results)
                        rd_kafka_topic_partition_list_destroy(results);
        }

        fprintf(stderr, "%% verify: shutting down\n");
        rd_kafka_share_consumer_close(rkshare);
        rd_kafka_share_destroy(rkshare);
        return 0;
}
