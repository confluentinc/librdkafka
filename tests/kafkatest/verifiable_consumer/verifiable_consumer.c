#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "common.h"
#include "rdkafka.h"

/* Flush the records_consumed buffer after this many ms or this many
 * messages, whichever comes first. */
#define FLUSH_INTERVAL_MS  100
#define FLUSH_MAX_MESSAGES 500

struct partition_state {
        char *topic;
        int32_t partition;
        int64_t count;
        int64_t min_offset;
        int64_t max_offset;
};

/* A topic+partition pair, used as a key in the assigned-set. */
struct tp_key {
        char *topic;
        int32_t partition;
};

struct consumer_state {
        int verbose;
        int enable_autocommit;

        /* Per-partition counters for records_consumed batching. */
        struct partition_state *buf;
        int buf_cnt;
        int buf_cap;
        int64_t buf_total;
        int64_t last_flush_ms;

        /* Totals (used for max-messages check). */
        int64_t consumed_total;

        /* Empty-assignment workaround state (see stats_cb). */
        int is_consumer_protocol;
        int last_assignment_reported;

        /* Set of currently-assigned (topic, partition) pairs. Updated
         * by rebalance_cb (full-replace on eager, incremental for
         * cooperative). record_message() drops records for partitions
         * not in this set — necessary because, around a rebalance,
         * librdkafka may deliver in-flight records for partitions that
         * have just been revoked. Without filtering, those records get
         * counted here AND re-counted by the new owner that fetches
         * from the last commit, breaking
         *   total_consumed == current_position
         * in the harness. Mirrors the Go POC's `currentAssignment`
         * filter at the top of onRecordsReceived. */
        struct tp_key *assigned;
        int assigned_cnt;
        int assigned_cap;

        /* Serializes access to buf, assigned, and related counters
         * between the main poll thread and the rebalance callback
         * (which runs on a librdkafka thread). */
        pthread_mutex_t mtx;
};

static struct consumer_state state;

/* -------------------- Event emission -------------------- */

static void emit_partition_list_event(const char *name,
                                      const rd_kafka_topic_partition_list_t *p) {
        stdout_lock();
        fputs("{\"name\":", stdout);
        json_write_string(stdout, name);
        fprintf(stdout, ",\"timestamp\":%" PRId64, now_ms());
        fputs(",\"partitions\":[", stdout);
        if (p) {
                for (int i = 0; i < p->cnt; i++) {
                        if (i > 0)
                                fputc(',', stdout);
                        fputs("{\"topic\":", stdout);
                        json_write_string(stdout, p->elems[i].topic);
                        fprintf(stdout, ",\"partition\":%" PRId32 "}",
                                p->elems[i].partition);
                }
        }
        fputs("]}\n", stdout);
        fflush(stdout);
        stdout_unlock();
}

static void emit_record_data(const rd_kafka_message_t *rkm) {
        stdout_lock();
        fputs("{\"name\":\"record_data\"", stdout);
        fprintf(stdout, ",\"timestamp\":%" PRId64, now_ms());
        fputs(",\"topic\":", stdout);
        json_write_string(stdout, rd_kafka_topic_name(rkm->rkt));
        fprintf(stdout, ",\"partition\":%" PRId32, rkm->partition);
        fprintf(stdout, ",\"offset\":%" PRId64, rkm->offset);
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
        fputs("}\n", stdout);
        fflush(stdout);
        stdout_unlock();
}

/* -------------------- Batched records_consumed + commit -------------------- */

/* Drain the accumulated per-partition state: emit records_consumed,
 * store offsets, and clear the buffer. Returns an rd_kafka_topic_partition_list_t
 * containing next-to-read offsets for each partition, or NULL if the
 * buffer was empty. Caller must destroy the returned list.
 *
 * This function is safe to call from inside librdkafka callbacks — it
 * only emits JSON and calls rd_kafka_offsets_store (non-blocking). It
 * does NOT call rd_kafka_commit.
 *
 * Must be called with state.mtx held. */
static rd_kafka_topic_partition_list_t *
drain_batch_locked(rd_kafka_t *rk) {
        if (state.buf_cnt == 0)
                return NULL;

        /* Emit records_consumed event. */
        stdout_lock();
        fputs("{\"name\":\"records_consumed\"", stdout);
        fprintf(stdout, ",\"timestamp\":%" PRId64, now_ms());
        fprintf(stdout, ",\"count\":%" PRId64, state.buf_total);
        fputs(",\"partitions\":[", stdout);
        for (int i = 0; i < state.buf_cnt; i++) {
                struct partition_state *ps = &state.buf[i];
                if (i > 0)
                        fputc(',', stdout);
                fputs("{\"topic\":", stdout);
                json_write_string(stdout, ps->topic);
                fprintf(stdout, ",\"partition\":%" PRId32, ps->partition);
                fprintf(stdout, ",\"count\":%" PRId64, ps->count);
                fprintf(stdout, ",\"minOffset\":%" PRId64, ps->min_offset);
                fprintf(stdout, ",\"maxOffset\":%" PRId64 "}", ps->max_offset);
        }
        fputs("]}\n", stdout);
        fflush(stdout);
        stdout_unlock();

        /* Build next-to-read offsets list and store (non-blocking). */
        rd_kafka_topic_partition_list_t *offsets =
            rd_kafka_topic_partition_list_new(state.buf_cnt);
        for (int i = 0; i < state.buf_cnt; i++) {
                rd_kafka_topic_partition_t *tp =
                    rd_kafka_topic_partition_list_add(offsets, state.buf[i].topic,
                                                      state.buf[i].partition);
                tp->offset = state.buf[i].max_offset + 1;
        }
        rd_kafka_offsets_store(rk, offsets);

        /* Reset buffer. */
        for (int i = 0; i < state.buf_cnt; i++)
                free(state.buf[i].topic);
        state.buf_cnt       = 0;
        state.buf_total     = 0;
        state.last_flush_ms = now_ms();

        return offsets;
}

/* Full flush: drain the buffer AND sync-commit offsets. Emits
 * offsets_committed. MUST NOT be called from inside a librdkafka
 * callback — sync commit from a callback can deadlock the op queue.
 *
 * Must be called with state.mtx held. */
static void flush_batch_locked(rd_kafka_t *rk) {
        rd_kafka_topic_partition_list_t *offsets = drain_batch_locked(rk);
        if (!offsets)
                return;

        if (!state.enable_autocommit) {
                rd_kafka_resp_err_t err = rd_kafka_commit(rk, offsets, 0);

                int commit_success = err == RD_KAFKA_RESP_ERR_NO_ERROR;
                const char *errmsg = commit_success ? "" : rd_kafka_err2str(err);

                stdout_lock();
                fputs("{\"name\":\"offsets_committed\"", stdout);
                fprintf(stdout, ",\"timestamp\":%" PRId64, now_ms());
                fputs(",\"offsets\":[", stdout);
                for (int i = 0; i < offsets->cnt; i++) {
                        if (i > 0)
                                fputc(',', stdout);
                        fputs("{\"topic\":", stdout);
                        json_write_string(stdout, offsets->elems[i].topic);
                        fprintf(stdout, ",\"partition\":%" PRId32,
                                offsets->elems[i].partition);
                        fprintf(stdout, ",\"offset\":%" PRId64 "}",
                                offsets->elems[i].offset);
                }
                fputs("],\"success\":", stdout);
                fputs(commit_success ? "true" : "false", stdout);
                fputs(",\"error\":", stdout);
                json_write_string(stdout, errmsg);
                fputs("}\n", stdout);
                fflush(stdout);
                stdout_unlock();
        }

        rd_kafka_topic_partition_list_destroy(offsets);
}

/* Must be called with state.mtx held. */
static struct partition_state *find_or_add_partition_locked(const char *topic,
                                                            int32_t partition) {
        for (int i = 0; i < state.buf_cnt; i++) {
                if (state.buf[i].partition == partition &&
                    strcmp(state.buf[i].topic, topic) == 0)
                        return &state.buf[i];
        }

        if (state.buf_cnt == state.buf_cap) {
                int new_cap  = state.buf_cap ? state.buf_cap * 2 : 8;
                state.buf    = realloc(state.buf,
                                       (size_t)new_cap * sizeof(*state.buf));
                state.buf_cap = new_cap;
        }

        struct partition_state *ps = &state.buf[state.buf_cnt++];
        ps->topic      = strdup(topic);
        ps->partition  = partition;
        ps->count      = 0;
        ps->min_offset = INT64_MAX;
        ps->max_offset = INT64_MIN;
        return ps;
}

/* Assigned-set helpers. All assume state.mtx is held by the caller. */

static int assigned_contains_locked(const char *topic, int32_t partition) {
        for (int i = 0; i < state.assigned_cnt; i++) {
                if (state.assigned[i].partition == partition &&
                    strcmp(state.assigned[i].topic, topic) == 0)
                        return 1;
        }
        return 0;
}

static void assigned_add_locked(const char *topic, int32_t partition) {
        if (assigned_contains_locked(topic, partition))
                return;
        if (state.assigned_cnt == state.assigned_cap) {
                int new_cap = state.assigned_cap ? state.assigned_cap * 2 : 8;
                state.assigned = realloc(state.assigned,
                                         (size_t)new_cap *
                                             sizeof(*state.assigned));
                state.assigned_cap = new_cap;
        }
        state.assigned[state.assigned_cnt].topic     = strdup(topic);
        state.assigned[state.assigned_cnt].partition = partition;
        state.assigned_cnt++;
}

static void assigned_remove_locked(const char *topic, int32_t partition) {
        for (int i = 0; i < state.assigned_cnt; i++) {
                if (state.assigned[i].partition == partition &&
                    strcmp(state.assigned[i].topic, topic) == 0) {
                        free(state.assigned[i].topic);
                        state.assigned[i] =
                            state.assigned[--state.assigned_cnt];
                        return;
                }
        }
}

static void assigned_clear_locked(void) {
        for (int i = 0; i < state.assigned_cnt; i++)
                free(state.assigned[i].topic);
        state.assigned_cnt = 0;
}

/* Replace the entire assigned set with `parts` (eager rebalance:
 * full-list semantics on ASSIGN). */
static void assigned_replace_locked(
    const rd_kafka_topic_partition_list_t *parts) {
        assigned_clear_locked();
        if (!parts)
                return;
        for (int i = 0; i < parts->cnt; i++)
                assigned_add_locked(parts->elems[i].topic,
                                    parts->elems[i].partition);
}

/* Add `parts` to the assigned set (cooperative ASSIGN: incremental). */
static void assigned_add_list_locked(
    const rd_kafka_topic_partition_list_t *parts) {
        if (!parts)
                return;
        for (int i = 0; i < parts->cnt; i++)
                assigned_add_locked(parts->elems[i].topic,
                                    parts->elems[i].partition);
}

/* Remove `parts` from the assigned set (cooperative REVOKE: incremental). */
static void assigned_remove_list_locked(
    const rd_kafka_topic_partition_list_t *parts) {
        if (!parts)
                return;
        for (int i = 0; i < parts->cnt; i++)
                assigned_remove_locked(parts->elems[i].topic,
                                       parts->elems[i].partition);
}

static void record_message(rd_kafka_t *rk, const rd_kafka_message_t *rkm) {
        const char *topic = rd_kafka_topic_name(rkm->rkt);

        pthread_mutex_lock(&state.mtx);

        /* Drop records for partitions we no longer own. librdkafka
         * may deliver in-flight records after a partition has been
         * revoked. Counting those would cause the harness to see
         * total_consumed > current_position because the new owner
         * will re-fetch and re-count the same offsets. */
        if (!assigned_contains_locked(topic, rkm->partition)) {
                pthread_mutex_unlock(&state.mtx);
                return;
        }

        struct partition_state *ps =
            find_or_add_partition_locked(topic, rkm->partition);
        ps->count++;
        if (rkm->offset < ps->min_offset)
                ps->min_offset = rkm->offset;
        if (rkm->offset > ps->max_offset)
                ps->max_offset = rkm->offset;
        state.buf_total++;
        state.consumed_total++;

        int should_flush =
            state.buf_total >= FLUSH_MAX_MESSAGES ||
            (now_ms() - state.last_flush_ms) >= FLUSH_INTERVAL_MS;
        if (should_flush)
                flush_batch_locked(rk);
        pthread_mutex_unlock(&state.mtx);
}

/* -------------------- Rebalance callback -------------------- */

static void rebalance_cb(rd_kafka_t *rk,
                         rd_kafka_resp_err_t err,
                         rd_kafka_topic_partition_list_t *parts,
                         void *opaque) {
        (void)opaque;
        const char *proto = rd_kafka_rebalance_protocol(rk);
        int cooperative   = proto && strcmp(proto, "COOPERATIVE") == 0;

        if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
                emit_partition_list_event("partitions_assigned", parts);
                if (cooperative) {
                        rd_kafka_error_t *e =
                            rd_kafka_incremental_assign(rk, parts);
                        if (e)
                                rd_kafka_error_destroy(e);
                } else {
                        rd_kafka_assign(rk, parts);
                }
                pthread_mutex_lock(&state.mtx);
                /* Update assigned-set so record_message can filter
                 * post-revoke in-flight records. Eager replaces the
                 * full set; cooperative is incremental-add. */
                if (cooperative)
                        assigned_add_list_locked(parts);
                else
                        assigned_replace_locked(parts);
                state.last_assignment_reported = 1;
                pthread_mutex_unlock(&state.mtx);
        } else if (err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
                /* Drain any buffered records BEFORE emitting
                 * partitions_revoked. If we don't, the next main-loop
                 * flush would emit `records_consumed` for partitions
                 * that the Python harness has already removed from the
                 * consumer's assignment, and it would assert.
                 *
                 * We use drain_batch_locked() (not flush_batch_locked)
                 * because we must NOT call rd_kafka_commit() from inside
                 * the rebalance callback — sync commit needs the same
                 * op queue the callback is running on → deadlock.
                 * librdkafka will commit any stored offsets as part of
                 * the unassign. */
                pthread_mutex_lock(&state.mtx);
                rd_kafka_topic_partition_list_t *drained =
                    drain_batch_locked(rk);
                /* Update assigned-set immediately so any subsequent
                 * record_message() call drops in-flight records for
                 * the now-revoked partitions. Eager fully clears;
                 * cooperative is incremental-remove. */
                if (cooperative)
                        assigned_remove_list_locked(parts);
                else
                        assigned_clear_locked();
                state.last_assignment_reported = 0;
                pthread_mutex_unlock(&state.mtx);
                if (drained)
                        rd_kafka_topic_partition_list_destroy(drained);

                emit_partition_list_event("partitions_revoked", parts);
                if (cooperative) {
                        rd_kafka_error_t *e =
                            rd_kafka_incremental_unassign(rk, parts);
                        if (e)
                                rd_kafka_error_destroy(e);
                } else {
                        rd_kafka_assign(rk, NULL);
                }
        } else {
                fprintf(stderr, "rebalance error: %s\n", rd_kafka_err2str(err));
                rd_kafka_assign(rk, NULL);
        }
}

/* -------------------- Empty-assignment workaround -------------------- */

/*
 * Workaround for a librdkafka KIP-848 (group.protocol=consumer) bug.
 *
 * When a consumer joins a group and receives an *empty* assignment,
 * rdkafka_cgrp.c:rd_kafka_cgrp_consumer_is_new_assignment_different()
 * short-circuits reconciliation because "current == target" (two empty
 * lists), so the rebalance callback is never invoked. The Java client
 * always invokes its callback, even on empty assignments, and the
 * Apache Kafka system-test harness relies on that event to know every
 * consumer has joined.
 *
 * This workaround runs via stats_cb (statistics.interval.ms=1000). When
 * we see the consumer reach cgrp.state=up with an empty assignment and
 * no prior partitions_assigned has fired, we emit a synthetic one.
 *
 * Threshold rationale for stateage < 2000 ms: the stats callback fires
 * at most once per statistics.interval.ms (1000 ms). A 2000 ms window =
 * 2x the stats interval, giving one "missed callback" of slack while
 * still narrow enough that a long-lived consumer sitting in `up` with a
 * legitimately empty assignment won't re-trigger after the latch
 * (last_assignment_reported) is cleared by a revoke. Tightening below
 * ~1500 ms risks missing the detection window on systems where stats
 * callbacks slip slightly. Matches the Go POC value.
 *
 * Scoped to group.protocol=consumer only: the classic protocol does
 * not have this bug.
 */
static int stats_cb(rd_kafka_t *rk,
                    char *json,
                    size_t json_len,
                    void *opaque) {
        (void)json_len;
        (void)opaque;

        if (!state.is_consumer_protocol)
                return 0;

        pthread_mutex_lock(&state.mtx);
        int already_reported = state.last_assignment_reported;
        pthread_mutex_unlock(&state.mtx);
        if (already_reported)
                return 0;

        /* Substring-match the stats JSON. Format (from rdkafka.c):
         *   "cgrp": { "state": "<s>", "stateage": <n>, ...,
         *              "assignment_size": <n> }
         * Note the spaces after colons. We avoid pulling in a JSON
         * parser for this tiny need. */
        const char *cgrp = strstr(json, "\"cgrp\": {");
        if (!cgrp)
                return 0;

        const char *state_key = strstr(cgrp, "\"state\": \"");
        if (!state_key)
                return 0;
        state_key += strlen("\"state\": \"");
        if (strncmp(state_key, "up\"", 3) != 0)
                return 0;

        const char *age_key = strstr(cgrp, "\"stateage\": ");
        if (!age_key)
                return 0;
        age_key += strlen("\"stateage\": ");
        long long stateage_ms = strtoll(age_key, NULL, 10);
        if (stateage_ms >= 2000)
                return 0;

        const char *asz_key = strstr(cgrp, "\"assignment_size\": ");
        if (!asz_key)
                return 0;
        asz_key += strlen("\"assignment_size\": ");
        long long assignment_size = strtoll(asz_key, NULL, 10);
        if (assignment_size != 0)
                return 0;

        /* Empty assignment, state=up, freshly transitioned, and no
         * rebalance_cb has fired. Synthesize the missing event. */
        rd_kafka_topic_partition_list_t *empty =
            rd_kafka_topic_partition_list_new(0);
        emit_partition_list_event("partitions_assigned", empty);
        rd_kafka_topic_partition_list_destroy(empty);

        pthread_mutex_lock(&state.mtx);
        state.last_assignment_reported = 1;
        pthread_mutex_unlock(&state.mtx);
        return 0;
}

/* -------------------- Assignment-strategy translation -------------------- */

/* Map a single Java assignor class name like
 * "org.apache.kafka.clients.consumer.CooperativeStickyAssignor" to the
 * librdkafka strategy name "cooperative-sticky". Returns a static
 * string or NULL if unrecognized. */
static const char *translate_one_strategy(const char *java_class) {
        const char *simple = strrchr(java_class, '.');
        simple = simple ? simple + 1 : java_class;

        if (!strcmp(simple, "RangeAssignor"))
                return "range";
        if (!strcmp(simple, "RoundRobinAssignor"))
                return "roundrobin";
        if (!strcmp(simple, "StickyAssignor"))
                return "cooperative-sticky";
        if (!strcmp(simple, "CooperativeStickyAssignor"))
                return "cooperative-sticky";
        return NULL;
}

/* Translate a possibly comma-separated list of Java class names into a
 * comma-separated list of librdkafka strategy names. Tests like
 * `consumer_rolling_upgrade_test` pass multiple strategies to switch
 * preference across rolling restarts. Returns a malloc'd string the
 * caller must free, or NULL if any element is unrecognized. */
static char *translate_assignment_strategy(const char *java_classes) {
        char *input = strdup(java_classes);
        if (!input)
                return NULL;
        char *result = malloc(strlen(java_classes) + 64);
        if (!result) {
                free(input);
                return NULL;
        }
        result[0] = '\0';

        int first = 1;
        char *saveptr = NULL;
        for (char *tok = strtok_r(input, ",", &saveptr); tok;
             tok = strtok_r(NULL, ",", &saveptr)) {
                /* Trim leading whitespace. */
                while (*tok == ' ' || *tok == '\t')
                        tok++;
                /* Trim trailing whitespace. */
                size_t len = strlen(tok);
                while (len > 0 &&
                       (tok[len - 1] == ' ' || tok[len - 1] == '\t')) {
                        tok[--len] = '\0';
                }
                const char *mapped = translate_one_strategy(tok);
                if (!mapped) {
                        free(input);
                        free(result);
                        return NULL;
                }
                if (!first)
                        strcat(result, ",");
                strcat(result, mapped);
                first = 0;
        }
        free(input);
        return result;
}

/* -------------------- CLI -------------------- */

static void usage(const char *prog) {
        fprintf(stderr,
                "Usage: %s --topic <t> --group-id <g> --bootstrap-server <s> "
                "[options]\n"
                "\nOptions:\n"
                "  --topic <t>                      Topic (required)\n"
                "  --group-id <g>                   Group id (required)\n"
                "  --bootstrap-server <s>           Broker list (required; "
                "also --broker-list)\n"
                "  --group-protocol <classic|consumer>  Default: classic\n"
                "  --group-remote-assignor <name>   Consumer-protocol only\n"
                "  --group-instance-id <id>         Static membership\n"
                "  --assignment-strategy <java.Class>   Classic only\n"
                "  --max-messages <n>               Default: -1 (infinite)\n"
                "  --session-timeout <ms>           Default: broker\n"
                "  --verbose                        Emit record_data per msg\n"
                "  --enable-autocommit              Default: off\n"
                "  --reset-policy <earliest|latest|none>  Default: earliest\n"
                "  --consumer.config <file>         Properties file "
                "(deprecated)\n"
                "  --command-config <file>          Properties file\n"
                "  --debug <flags>                  librdkafka debug flags\n"
                "  -X, --property <k=v,..>          Raw librdkafka "
                "properties\n",
                prog);
}

int main(int argc, char **argv) {
        const char *topic               = NULL;
        const char *group_id            = NULL;
        const char *bootstrap           = NULL;
        const char *group_protocol      = "classic";
        const char *group_remote_assign = NULL;
        const char *group_instance_id   = NULL;
        const char *assignment_strategy = NULL;
        int64_t max_messages            = -1;
        int session_timeout_ms          = -1;
        int verbose                     = 0;
        int enable_autocommit           = 0;
        const char *reset_policy        = "earliest";
        const char *config_file         = NULL;
        const char *debug_flags         = NULL;
        const char *x_props             = NULL;

        static struct option long_opts[] = {
            {"topic", required_argument, 0, 't'},
            {"group-id", required_argument, 0, 'g'},
            {"bootstrap-server", required_argument, 0, 'b'},
            {"broker-list", required_argument, 0, 'b'},
            {"group-protocol", required_argument, 0, 'P'},
            {"group-remote-assignor", required_argument, 0, 'R'},
            {"group-instance-id", required_argument, 0, 'I'},
            {"assignment-strategy", required_argument, 0, 'A'},
            {"max-messages", required_argument, 0, 'm'},
            {"session-timeout", required_argument, 0, 's'},
            {"verbose", no_argument, 0, 'V'},
            {"enable-autocommit", no_argument, 0, 'a'},
            {"reset-policy", required_argument, 0, 'r'},
            {"consumer.config", required_argument, 0, 'c'},
            {"command-config", required_argument, 0, 'C'},
            {"debug", required_argument, 0, 'd'},
            {"property", required_argument, 0, 'X'},
            {"help", no_argument, 0, 'h'},
            {0, 0, 0, 0}};

        int opt;
        while ((opt = getopt_long(argc, argv, "X:h", long_opts, NULL)) != -1) {
                switch (opt) {
                case 't': topic = optarg; break;
                case 'g': group_id = optarg; break;
                case 'b': bootstrap = optarg; break;
                case 'P': group_protocol = optarg; break;
                case 'R': group_remote_assign = optarg; break;
                case 'I': group_instance_id = optarg; break;
                case 'A': assignment_strategy = optarg; break;
                case 'm': max_messages = strtoll(optarg, NULL, 10); break;
                case 's': session_timeout_ms = atoi(optarg); break;
                case 'V': verbose = 1; break;
                case 'a': enable_autocommit = 1; break;
                case 'r': reset_policy = optarg; break;
                case 'c':
                case 'C': config_file = optarg; break;
                case 'd': debug_flags = optarg; break;
                case 'X': x_props = optarg; break;
                case 'h': usage(argv[0]); return 0;
                default: usage(argv[0]); return 1;
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

        state.verbose              = verbose;
        state.enable_autocommit    = enable_autocommit;
        state.is_consumer_protocol = strcmp(group_protocol, "consumer") == 0;
        state.last_flush_ms        = now_ms();
        pthread_mutex_init(&state.mtx, NULL);

        char errstr[512];
        rd_kafka_conf_t *conf = rd_kafka_conf_new();

#define CONF_SET(k, v)                                                       \
        do {                                                                 \
                if (rd_kafka_conf_set(conf, k, v, errstr, sizeof(errstr)) != \
                    RD_KAFKA_CONF_OK) {                                      \
                        fprintf(stderr, "%s: %s\n", k, errstr);              \
                        rd_kafka_conf_destroy(conf);                         \
                        return 1;                                            \
                }                                                            \
        } while (0)

        CONF_SET("bootstrap.servers", bootstrap);
        CONF_SET("group.id", group_id);
        CONF_SET("group.protocol", group_protocol);
        CONF_SET("auto.offset.reset", reset_policy);
        CONF_SET("enable.auto.commit", enable_autocommit ? "true" : "false");
        /* Commit only what we've explicitly stored after processing,
         * matching Java's "only commit processed messages" semantics. */
        CONF_SET("enable.auto.offset.store", "false");
        /* For the empty-assignment workaround. */
        CONF_SET("statistics.interval.ms", "1000");

        if (group_remote_assign)
                CONF_SET("group.remote.assignor", group_remote_assign);
        if (group_instance_id)
                CONF_SET("group.instance.id", group_instance_id);
        if (assignment_strategy && !state.is_consumer_protocol) {
                char *rd_strategy =
                    translate_assignment_strategy(assignment_strategy);
                if (!rd_strategy) {
                        fprintf(stderr,
                                "Unknown --assignment-strategy: %s\n",
                                assignment_strategy);
                        rd_kafka_conf_destroy(conf);
                        return 1;
                }
                fprintf(stderr,
                        "%% Mapped assignment strategy %s -> %s\n",
                        assignment_strategy, rd_strategy);
                CONF_SET("partition.assignment.strategy", rd_strategy);
                free(rd_strategy);
        }
        if (session_timeout_ms > 0) {
                char buf[32];
                snprintf(buf, sizeof(buf), "%d", session_timeout_ms);
                CONF_SET("session.timeout.ms", buf);
        }
        if (debug_flags)
                CONF_SET("debug", debug_flags);

        if (config_file &&
            load_properties_file(config_file, conf, errstr, sizeof(errstr)) ==
                -1) {
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

#undef CONF_SET

        rd_kafka_conf_set_rebalance_cb(conf, rebalance_cb);
        rd_kafka_conf_set_stats_cb(conf, stats_cb);

        rd_kafka_t *rk =
            rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        if (!rk) {
                fprintf(stderr, "Failed to create consumer: %s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return 1;
        }

        rd_kafka_poll_set_consumer(rk);

        rd_kafka_topic_partition_list_t *subscription =
            rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);
        rd_kafka_resp_err_t err = rd_kafka_subscribe(rk, subscription);
        rd_kafka_topic_partition_list_destroy(subscription);
        if (err) {
                fprintf(stderr, "subscribe: %s\n", rd_kafka_err2str(err));
                rd_kafka_destroy(rk);
                return 1;
        }

        emit_event("startup_complete");

        while (run && (max_messages < 0 || state.consumed_total < max_messages)) {
                rd_kafka_message_t *rkm = rd_kafka_consumer_poll(rk, 100);
                if (!rkm) {
                        /* Timed flush in case the message rate is slow. */
                        pthread_mutex_lock(&state.mtx);
                        if (state.buf_total > 0 &&
                            (now_ms() - state.last_flush_ms) >=
                                FLUSH_INTERVAL_MS)
                                flush_batch_locked(rk);
                        pthread_mutex_unlock(&state.mtx);
                        continue;
                }

                if (rkm->err) {
                        if (rkm->err != RD_KAFKA_RESP_ERR__PARTITION_EOF)
                                fprintf(stderr, "consumer poll: %s\n",
                                        rd_kafka_message_errstr(rkm));
                        rd_kafka_message_destroy(rkm);

                        /* Detect fatal errors (e.g., FENCED_INSTANCE_ID
                         * when another consumer joins with the same
                         * group.instance.id). librdkafka latches a
                         * fatal error on the rd_kafka_t; once set, the
                         * consumer can never recover. We must terminate
                         * and emit shutdown_complete so the harness
                         * sees the consumer as dead. Mirrors the Go
                         * POC's IsFatal() check. */
                        char ferrstr[512];
                        rd_kafka_resp_err_t ferr =
                            rd_kafka_fatal_error(rk, ferrstr, sizeof(ferrstr));
                        if (ferr) {
                                fprintf(stderr,
                                        "%% Fatal error %s: %s — "
                                        "terminating immediately\n",
                                        rd_kafka_err2name(ferr), ferrstr);
                                /* Emit shutdown_complete and exit hard.
                                 * Don't fall through to consumer_close()
                                 * — closing a fenced consumer may hang
                                 * trying to leave the group. Mirrors the
                                 * Go POC's behavior. */
                                emit_event("shutdown_complete");
                                fflush(stdout);
                                _exit(1);
                        }
                        continue;
                }

                if (verbose)
                        emit_record_data(rkm);

                record_message(rk, rkm);
                rd_kafka_message_destroy(rkm);
        }

        /* Final flush + commit before close. */
        pthread_mutex_lock(&state.mtx);
        flush_batch_locked(rk);
        pthread_mutex_unlock(&state.mtx);

        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);

        /* Release buf + assigned-set memory. */
        for (int i = 0; i < state.buf_cnt; i++)
                free(state.buf[i].topic);
        free(state.buf);
        for (int i = 0; i < state.assigned_cnt; i++)
                free(state.assigned[i].topic);
        free(state.assigned);

        pthread_mutex_destroy(&state.mtx);

        emit_event("shutdown_complete");
        return 0;
}
