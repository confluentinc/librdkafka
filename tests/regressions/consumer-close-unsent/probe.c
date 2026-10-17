/* Linux-only native regression: all brokers live inside this owned process.
 * Exercise the same commit-response handler used by manual and automatic
 * commits. */
#include "rdkafka.h"
#include "rdkafka_mock.h"
#include <dlfcn.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* The held-send mode owns one restoration thread and joins it before
 * destruction. */
static void (*hold_commits)(int);
static int (*commits_held)(void);
static uint64_t (*held_sends)(void);

/* Unlike C assert, these checks remain active in every build configuration. */
static void require(int condition, const char *message) {
        if (!condition) {
                fprintf(stderr, "Native consumer-close regression: %s\n",
                        message);
                exit(2);
        }
}

/* Measure deadlines independently of wall-clock corrections. */
static double seconds(void) {
        struct timespec now;
        require(clock_gettime(CLOCK_MONOTONIC, &now) == 0,
                "read monotonic clock");
        return (double)now.tv_sec + (double)now.tv_nsec / 1e9;
}

/* Reject unsupported configuration rather than silently testing defaults. */
static void
setting(rd_kafka_conf_t *config, const char *name, const char *value) {
        char error[512];
        if (rd_kafka_conf_set(config, name, value, error, sizeof(error)) !=
            RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s: %s\n", name, error);
                exit(2);
        }
}

/* Preserve bounded diagnostic transitions; probe messages contain no
 * application data. */
static void log_event(const rd_kafka_t *client,
                      int level,
                      const char *facility,
                      const char *message) {
        (void)client;
        (void)level;
        if (!strcmp(facility, "COMMIT") || !strcmp(facility, "CGRPTERM"))
                fprintf(stderr, "%s %s\n", facility, message);
}

/* Serve the native consumer queue while waiting for an observed state. */
static void poll_consumer(rd_kafka_t *consumer) {
        rd_kafka_message_t *message = rd_kafka_consumer_poll(consumer, 100);
        if (message)
                rd_kafka_message_destroy(message);
}

/* Deliberately hold the injected network fault longer than the permitted close.
 * This interval defines the fault, not an estimate of readiness. */
static void *release_send_fault(void *unused) {
        (void)unused;
        struct timespec remaining = {25, 0};
        while (nanosleep(&remaining, &remaining) != 0)
                require(errno == EINTR, "wait for fault release");
        hold_commits(0);
        return NULL;
}

/* Own the mock cluster, consumer, commit and optional release thread through
 * teardown. */
int main(int argc, char **argv) {
        require(argc == 2,
                "select normal, held-send, fatal-immediate or fatal-late");
        const int held      = !strcmp(argv[1], "held-send");
        const int immediate = !strcmp(argv[1], "fatal-immediate");
        const int late      = !strcmp(argv[1], "fatal-late");
        const int normal    = !strcmp(argv[1], "normal");
        require(held || immediate || late || normal, "unknown probe mode");
        hold_commits = dlsym(RTLD_DEFAULT, "hold_offset_commits");
        commits_held = dlsym(RTLD_DEFAULT, "offset_commits_held");
        held_sends   = dlsym(RTLD_DEFAULT, "held_offset_commit_sends");
        require(hold_commits && commits_held && held_sends,
                "load the send fault injector");

        char error[512];
        rd_kafka_t *owner = rd_kafka_new(RD_KAFKA_PRODUCER, rd_kafka_conf_new(),
                                         error, sizeof(error));
        require(owner != NULL, "create mock-cluster owner");
        rd_kafka_mock_cluster_t *cluster = rd_kafka_mock_cluster_new(owner, 3);
        require(cluster != NULL, "create private mock cluster");
        require(rd_kafka_mock_topic_create(cluster, "close-probe", 1, 3) == 0,
                "create probe topic");
        require(rd_kafka_mock_coordinator_set(cluster, "group",
                                              "close-probe-group", 1) == 0,
                "select group coordinator");

        rd_kafka_conf_t *config = rd_kafka_conf_new();
        setting(config, "bootstrap.servers",
                rd_kafka_mock_cluster_bootstraps(cluster));
        setting(config, "group.id", "close-probe-group");
        setting(config, "session.timeout.ms", "3000");
        setting(config, "heartbeat.interval.ms", "1000");
        setting(config, "socket.timeout.ms", late ? "30000" : "1000");
        setting(config, "auto.commit.interval.ms", "1000");
        setting(config, "debug", "cgrp");
        rd_kafka_conf_set_log_cb(config, log_event);
        rd_kafka_t *consumer =
            rd_kafka_new(RD_KAFKA_CONSUMER, config, error, sizeof(error));
        require(consumer != NULL, "create consumer");
        require(rd_kafka_poll_set_consumer(consumer) == 0,
                "route consumer callbacks");
        rd_kafka_topic_partition_list_t *topics =
            rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(topics, "close-probe",
                                          RD_KAFKA_PARTITION_UA);
        require(rd_kafka_subscribe(consumer, topics) == 0,
                "subscribe to probe topic");
        rd_kafka_topic_partition_list_destroy(topics);

        rd_kafka_topic_partition_list_t *assignment = NULL;
        const double assignment_deadline            = seconds() + 15;
        while (seconds() < assignment_deadline) {
                poll_consumer(consumer);
                require(rd_kafka_assignment(consumer, &assignment) == 0,
                        "read assignment");
                if (assignment->cnt)
                        break;
                rd_kafka_topic_partition_list_destroy(assignment);
                assignment = NULL;
        }
        require(assignment && assignment->cnt == 1,
                "observe one assigned partition");
        assignment->elems[0].offset = 1;
        if (held)
                hold_commits(1);
        if (immediate || late) {
                rd_kafka_mock_push_request_errors(
                    cluster, 8, 1, RD_KAFKA_RESP_ERR_FENCED_INSTANCE_ID);
                if (late)
                        require(rd_kafka_mock_broker_set_rtt(cluster, -1,
                                                             6000) == 0,
                                "delay fatal response beyond close budget");
        }
        require(rd_kafka_commit(consumer, assignment, 1) == 0,
                "submit offset commit");

        if (normal) {
                const double deadline = seconds() + 5;
                do {
                        poll_consumer(consumer);
                        require(
                            rd_kafka_committed(consumer, assignment, 1000) == 0,
                            "read committed offset");
                } while (assignment->elems[0].offset != 1 &&
                         seconds() < deadline);
                require(assignment->elems[0].offset == 1,
                        "preserve normal offset persistence");
        } else if (held || immediate) {
                const double deadline = seconds() + 5;
                while (seconds() < deadline) {
                        poll_consumer(consumer);
                        if (held ? held_sends() > 0
                                 : rd_kafka_fatal_error(consumer, error,
                                                        sizeof(error)) != 0)
                                break;
                }
                require(held ? held_sends() > 0
                             : rd_kafka_fatal_error(consumer, error,
                                                    sizeof(error)) ==
                                   RD_KAFKA_RESP_ERR_FENCED_INSTANCE_ID,
                        "observe the injected send/fatal condition");
        }
        rd_kafka_topic_partition_list_destroy(assignment);

        pthread_t restoration;
        if (held)
                require(pthread_create(&restoration, NULL, release_send_fault,
                                       NULL) == 0,
                        "own the fault release thread");
        const double close_started = seconds();
        const rd_kafka_resp_err_t close_error =
            rd_kafka_consumer_close(consumer);
        const double close_seconds = seconds() - close_started;
        const rd_kafka_resp_err_t fatal =
            rd_kafka_fatal_error(consumer, error, sizeof(error));
        if (normal || held)
                require(close_error == 0 && fatal == 0,
                        "complete ordinary consumer close");
        else
                require(fatal == RD_KAFKA_RESP_ERR_FENCED_INSTANCE_ID,
                        "preserve fatal fencing after close starts");
        if (held) {
                require(commits_held() && close_seconds < 20,
                        "close before the held send is released");
                require(pthread_join(restoration, NULL) == 0,
                        "join the owned fault release thread");
        }
        /* Each mode explicitly qualified close above; avoid a second implicit
         * close, particularly when the immediate fatal control correctly
         * returned ERR__FATAL. */
        rd_kafka_destroy_flags(consumer, RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);
        rd_kafka_mock_cluster_destroy(cluster);
        rd_kafka_destroy(owner);
        require(rd_kafka_wait_destroyed(10000) == 0,
                "join all native client threads");
        printf("{\"mode\":\"%s\",\"close_seconds\":%.3f,\"fatal_error\":%d}\n",
               argv[1], close_seconds, fatal);
        return 0;
}
