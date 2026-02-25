/*
 * rdkafka_benchmark_cpp - librdkafka C++ API benchmark tool with JSON output
 *
 * Copyright (c) 2012-2022, Magnus Edenhill
 *               2023, Confluent Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
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
 * @brief librdkafka C++ API benchmark with structured JSON output.
 *
 * Designed to be orchestrated by tools/benchmark/ Python framework.
 * Uses the same stdout protocol as rdkafka_benchmark.c:
 *
 * STDOUT protocol:
 *   Consumer mode: prints "BENCHMARK_READY\n" once partition assignment fires,
 *                  then a JSON result object when done.
 *   Producer mode: prints a single JSON result object when done.
 *
 * All diagnostic/progress output goes to STDERR.
 *
 * Build (local librdkafka):
 *   cd <repo_root> && ./configure && make
 *   (rdkafka_benchmark_cpp is built as part of examples/)
 *
 * Build (system librdkafka):
 *   make -C examples rdkafka_benchmark_cpp_sys
 */

#include <algorithm>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <inttypes.h>
#include <string>
#include <vector>

#ifndef _WIN32
#include <sys/time.h>
#include <unistd.h>
#else
#include "../win32/wingetopt.h"
#include <windows.h>
#endif

#include "rdkafkacpp.h"

/* -----------------------------------------------------------------------
 * Globals */

static volatile sig_atomic_t g_run          = 1;
static volatile sig_atomic_t g_consumer_rdy = 0;

static void stop(int) {
        g_run = 0;
}

/* -----------------------------------------------------------------------
 * Wall-clock helpers (microseconds) */

static int64_t wall_clock_us(void) {
#ifdef _WIN32
        FILETIME ft;
        GetSystemTimeAsFileTime(&ft);
        return ((int64_t)ft.dwHighDateTime << 32 | ft.dwLowDateTime) / 10 -
               11644473600000000LL;
#else
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        return (int64_t)tv.tv_sec * 1000000LL + (int64_t)tv.tv_usec;
#endif
}

static double wall_clock_sec(void) {
        return (double)wall_clock_us() / 1e6;
}

/* -----------------------------------------------------------------------
 * Latency sample helpers */

#define LAT_MAX_SAMPLES 10000000

static double lat_avg(const std::vector<int64_t> &v) {
        if (v.empty())
                return 0.0;
        int64_t sum = 0;
        for (auto x : v)
                sum += x;
        return (double)sum / (double)v.size();
}

/* p in [0, 100] */
static double lat_pct(const std::vector<int64_t> &v, double p) {
        if (v.empty())
                return 0.0;
        size_t idx = (size_t)(p / 100.0 * (double)(v.size() - 1));
        if (idx >= v.size())
                idx = v.size() - 1;
        return (double)v[idx];
}

static double lat_min(const std::vector<int64_t> &v) {
        return v.empty() ? 0.0 : (double)v.front();
}

static double lat_max(const std::vector<int64_t> &v) {
        return v.empty() ? 0.0 : (double)v.back();
}

/* -----------------------------------------------------------------------
 * JSON output macros (stdout only) */

#define JSTR(k, v, c)  printf("\"" k "\":\"%s\"%s", (v), (c) ? "," : "")
#define JI64(k, v, c)  printf("\"" k "\":%" PRId64 "%s", (int64_t)(v), (c) ? "," : "")
#define JDBL(k, v, c)  printf("\"" k "\":%.2f%s", (double)(v), (c) ? "," : "")
#define JNULL(k, c)    printf("\"" k "\":null%s", (c) ? "," : "")

/* -----------------------------------------------------------------------
 * Config helper */

static void set_conf(RdKafka::Conf *conf,
                     const std::string &key,
                     const std::string &val) {
        std::string errstr;
        if (conf->set(key, val, errstr) != RdKafka::Conf::CONF_OK) {
                fprintf(stderr, "%% Config: %s=%s: %s\n",
                        key.c_str(), val.c_str(), errstr.c_str());
                exit(1);
        }
}

/* -----------------------------------------------------------------------
 * Delivery report callback (producer) */

class BenchmarkDRCb : public RdKafka::DeliveryReportCb {
       public:
        int64_t              msgs_delivered  = 0;
        int64_t              msgs_failed     = 0;
        int64_t              bytes_delivered = 0;
        std::vector<int64_t> dr_lat_us; /* DR latency samples in microseconds */

        void dr_cb(RdKafka::Message &msg) override {
                if (msg.err()) {
                        msgs_failed++;
                        fprintf(stderr, "%% Delivery error: %s\n",
                                msg.errstr().c_str());
                        return;
                }

                msgs_delivered++;
                bytes_delivered += (int64_t)msg.len();

                /* Extract LATENCY:<us> timestamp from payload */
                if (msg.len() >= 10 && msg.payload()) {
                        int64_t src_us;
                        if (sscanf((const char *)msg.payload(),
                                   "LATENCY:%" SCNd64, &src_us) == 1) {
                                int64_t lat = wall_clock_us() - src_us;
                                if (lat > 0 &&
                                    lat < (int64_t)(5 * 60 * 1000000LL) &&
                                    dr_lat_us.size() < LAT_MAX_SAMPLES)
                                        dr_lat_us.push_back(lat);
                        }
                }
        }
};

/* -----------------------------------------------------------------------
 * Rebalance callback (consumer) */

class BenchmarkRebalanceCb : public RdKafka::RebalanceCb {
       public:
        void rebalance_cb(RdKafka::KafkaConsumer *consumer,
                          RdKafka::ErrorCode err,
                          std::vector<RdKafka::TopicPartition *> &partitions)
            override {
                if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
                        consumer->assign(partitions);
                        if (!g_consumer_rdy) {
                                g_consumer_rdy = 1;
                                /* Signal Python runner: group joined */
                                printf("BENCHMARK_READY\n");
                                fflush(stdout);
                        }
                } else {
                        consumer->unassign();
                }
                /* NOTE: do NOT call TopicPartition::destroy(partitions) —
                 * the partitions vector is owned by librdkafka and freed
                 * internally after the callback returns. */
        }
};

/* -----------------------------------------------------------------------
 * Producer */

static int run_producer(const std::string &brokers,
                        const std::string &topic,
                        int msg_size,
                        int64_t msg_count,
                        int duration_sec,
                        RdKafka::Conf *conf) {
        std::string  errstr;
        BenchmarkDRCb dr_cb;

        set_conf(conf, "bootstrap.servers", brokers);
        conf->set("dr_cb", &dr_cb, errstr);

        RdKafka::Producer *producer =
            RdKafka::Producer::create(conf, errstr);
        if (!producer) {
                fprintf(stderr, "%% Failed to create producer: %s\n",
                        errstr.c_str());
                return 1;
        }
        conf = nullptr; /* owned by producer */

        /* Build payload buffer */
        std::string payload(msg_size, 'x');

        double  t_start  = wall_clock_sec();
        double  deadline = (duration_sec > 0) ? (t_start + duration_sec) : 0.0;
        int64_t max_msgs = (msg_count > 0) ? msg_count : INT64_MAX;
        int64_t msgs_produced = 0;

        fprintf(stderr,
                "%% Producer: topic=%s size=%d count=%" PRId64 " duration=%ds\n",
                topic.c_str(), msg_size, msg_count, duration_sec);

        while (g_run && msgs_produced < max_msgs) {
                if (deadline > 0.0 && wall_clock_sec() >= deadline)
                        break;

                /* Embed wall-clock timestamp for latency measurement */
                if (msg_size >= 10) {
                        int ts_len = snprintf(&payload[0], msg_size,
                                              "LATENCY:%" PRId64,
                                              wall_clock_us());
                        if (ts_len > 0 && ts_len < msg_size)
                                memset(&payload[ts_len], 'x', msg_size - ts_len);
                }

                RdKafka::ErrorCode err;
                while (g_run) {
                        err = producer->produce(
                            topic, RdKafka::Topic::PARTITION_UA,
                            RdKafka::Producer::RK_MSG_COPY,
                            (void *)payload.data(), (size_t)msg_size,
                            nullptr, 0, 0, nullptr, nullptr);

                        if (err == RdKafka::ERR_NO_ERROR)
                                break;
                        if (err == RdKafka::ERR__QUEUE_FULL) {
                                producer->poll(10);
                                continue;
                        }
                        fprintf(stderr, "%% produce error: %s\n",
                                RdKafka::err2str(err).c_str());
                        g_run = 0;
                        break;
                }

                msgs_produced++;
                if (msgs_produced % 10000 == 0)
                        producer->poll(0);
        }

        double t_end_send = wall_clock_sec();
        fprintf(stderr, "%% Flushing %d in-flight messages...\n",
                producer->outq_len());
        producer->flush(30000);

        double elapsed = t_end_send - t_start;
        if (elapsed < 1e-9)
                elapsed = 1e-9;

        std::sort(dr_cb.dr_lat_us.begin(), dr_cb.dr_lat_us.end());

        /* --- JSON to STDOUT --- */
        printf("{");
        JSTR("mode", "producer", 1);
        JI64("msgs_produced", msgs_produced, 1);
        JI64("msgs_delivered", dr_cb.msgs_delivered, 1);
        JI64("msgs_failed", dr_cb.msgs_failed, 1);
        JI64("bytes_delivered", dr_cb.bytes_delivered, 1);
        JDBL("elapsed_sec", elapsed, 1);
        JDBL("msgs_per_sec", dr_cb.msgs_delivered / elapsed, 1);
        JDBL("mb_per_sec",
             (double)dr_cb.bytes_delivered / elapsed / 1048576.0, 1);

        if (!dr_cb.dr_lat_us.empty()) {
                JDBL("dr_latency_avg_us", lat_avg(dr_cb.dr_lat_us), 1);
                JDBL("dr_latency_p50_us",
                     lat_pct(dr_cb.dr_lat_us, 50.0), 1);
                JDBL("dr_latency_p95_us",
                     lat_pct(dr_cb.dr_lat_us, 95.0), 1);
                JDBL("dr_latency_p99_us",
                     lat_pct(dr_cb.dr_lat_us, 99.0), 1);
                JDBL("dr_latency_min_us", lat_min(dr_cb.dr_lat_us), 1);
                JDBL("dr_latency_max_us", lat_max(dr_cb.dr_lat_us), 1);
        } else {
                JNULL("dr_latency_avg_us", 1);
                JNULL("dr_latency_p50_us", 1);
                JNULL("dr_latency_p95_us", 1);
                JNULL("dr_latency_p99_us", 1);
                JNULL("dr_latency_min_us", 1);
                JNULL("dr_latency_max_us", 1);
        }

        JSTR("librdkafka_version", RdKafka::version_str().c_str(), 0);
        printf("}\n");
        fflush(stdout);

        delete producer;
        return dr_cb.msgs_failed > 0 ? 1 : 0;
}

/* -----------------------------------------------------------------------
 * Consumer */

static int run_consumer(const std::string &brokers,
                        const std::string &topic,
                        const std::string &group_id,
                        int64_t msg_count,
                        RdKafka::Conf *conf) {
        std::string          errstr;
        BenchmarkRebalanceCb rebalance_cb;
        std::vector<int64_t> e2e_lat_us;

        set_conf(conf, "bootstrap.servers", brokers);
        set_conf(conf, "group.id", group_id);
        set_conf(conf, "enable.partition.eof", "true");
        conf->set("rebalance_cb", &rebalance_cb, errstr);

        RdKafka::KafkaConsumer *consumer =
            RdKafka::KafkaConsumer::create(conf, errstr);
        if (!consumer) {
                fprintf(stderr, "%% Failed to create consumer: %s\n",
                        errstr.c_str());
                return 1;
        }
        conf = nullptr;

        std::vector<std::string> topics = {topic};
        RdKafka::ErrorCode err = consumer->subscribe(topics);
        if (err) {
                fprintf(stderr, "%% Subscribe failed: %s\n",
                        RdKafka::err2str(err).c_str());
                delete consumer;
                return 1;
        }

        fprintf(stderr,
                "%% Consumer: topic=%s group=%s count=%" PRId64 "\n",
                topic.c_str(), group_id.c_str(), msg_count);

        int64_t msgs_consumed  = 0;
        int64_t bytes_consumed = 0;
        double  t_start        = 0.0;
        double  t_end          = 0.0;
        int64_t max_msgs = (msg_count > 0) ? msg_count : INT64_MAX;

        while (g_run && msgs_consumed < max_msgs) {
                RdKafka::Message *msg = consumer->consume(1000);
                if (!msg)
                        continue;

                if (msg->err()) {
                        /* Save error code and string BEFORE deleting msg
                         * to avoid use-after-free. */
                        RdKafka::ErrorCode ec  = msg->err();
                        std::string        es  = msg->errstr();
                        delete msg;

                        if (ec == RdKafka::ERR__PARTITION_EOF)
                                continue;

                        fprintf(stderr, "%% Consumer error: %s\n", es.c_str());
                        if (ec == RdKafka::ERR__UNKNOWN_TOPIC ||
                            ec == RdKafka::ERR__UNKNOWN_PARTITION)
                                break;
                        continue;
                }

                double now = wall_clock_sec();
                if (t_start == 0.0)
                        t_start = now;
                t_end = now;

                msgs_consumed++;
                bytes_consumed += (int64_t)msg->len();

                /* E2E latency from LATENCY:<us> payload prefix */
                if (msg->len() >= 10 && msg->payload()) {
                        int64_t src_us;
                        if (sscanf((const char *)msg->payload(),
                                   "LATENCY:%" SCNd64, &src_us) == 1) {
                                int64_t lat = wall_clock_us() - src_us;
                                if (lat > 0 &&
                                    lat < (int64_t)(5 * 60 * 1000000LL) &&
                                    e2e_lat_us.size() < LAT_MAX_SAMPLES)
                                        e2e_lat_us.push_back(lat);
                        }
                }

                delete msg;
        }

        /* Safety: emit BENCHMARK_READY if not yet done (edge case) */
        if (!g_consumer_rdy) {
                g_consumer_rdy = 1;
                printf("BENCHMARK_READY\n");
                fflush(stdout);
        }

        consumer->close();
        delete consumer;

        double elapsed = (t_end > t_start) ? (t_end - t_start) : 1e-9;
        std::sort(e2e_lat_us.begin(), e2e_lat_us.end());

        /* --- JSON to STDOUT --- */
        printf("{");
        JSTR("mode", "consumer", 1);
        JI64("msgs_consumed", msgs_consumed, 1);
        JI64("bytes_consumed", bytes_consumed, 1);
        JDBL("elapsed_sec", elapsed, 1);
        JDBL("msgs_per_sec", (double)msgs_consumed / elapsed, 1);
        JDBL("mb_per_sec",
             (double)bytes_consumed / elapsed / 1048576.0, 1);

        if (!e2e_lat_us.empty()) {
                /* Convert µs → ms for E2E */
                JDBL("e2e_latency_avg_ms",
                     lat_avg(e2e_lat_us) / 1000.0, 1);
                JDBL("e2e_latency_p50_ms",
                     lat_pct(e2e_lat_us, 50.0) / 1000.0, 1);
                JDBL("e2e_latency_p95_ms",
                     lat_pct(e2e_lat_us, 95.0) / 1000.0, 1);
                JDBL("e2e_latency_p99_ms",
                     lat_pct(e2e_lat_us, 99.0) / 1000.0, 1);
                JDBL("e2e_latency_min_ms",
                     lat_min(e2e_lat_us) / 1000.0, 1);
                JDBL("e2e_latency_max_ms",
                     lat_max(e2e_lat_us) / 1000.0, 1);
        } else {
                JNULL("e2e_latency_avg_ms", 1);
                JNULL("e2e_latency_p50_ms", 1);
                JNULL("e2e_latency_p95_ms", 1);
                JNULL("e2e_latency_p99_ms", 1);
                JNULL("e2e_latency_min_ms", 1);
                JNULL("e2e_latency_max_ms", 1);
        }

        JSTR("librdkafka_version", RdKafka::version_str().c_str(), 0);
        printf("}\n");
        fflush(stdout);

        return 0;
}

/* -----------------------------------------------------------------------
 * main */

int main(int argc, char **argv) {
        char        mode         = '\0';
        std::string brokers      = "localhost:9092";
        std::string topic;
        std::string group_id     = "rdkafka-benchmark-cpp-cg";
        int         msg_size     = 1024;
        int64_t     msg_count    = 0; /* 0 = unlimited */
        int         duration_sec = 0; /* 0 = count-driven */
        int         opt;

        RdKafka::Conf *conf =
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

        signal(SIGINT, stop);
        signal(SIGTERM, stop);

        while ((opt = getopt(argc, argv, "PCb:t:G:s:c:d:X:")) != -1) {
                switch (opt) {
                case 'P':
                        mode = 'P';
                        break;
                case 'C':
                        mode = 'C';
                        break;
                case 'b':
                        brokers = optarg;
                        break;
                case 't':
                        topic = optarg;
                        break;
                case 'G':
                        group_id = optarg;
                        break;
                case 's':
                        msg_size = atoi(optarg);
                        if (msg_size < 1)
                                msg_size = 1;
                        break;
                case 'c':
                        msg_count = atoll(optarg);
                        break;
                case 'd':
                        duration_sec = atoi(optarg);
                        break;
                case 'X': {
                        std::string kv  = optarg;
                        size_t      pos = kv.find('=');
                        if (pos == std::string::npos) {
                                fprintf(stderr,
                                        "%% -X: expected key=value, got: %s\n",
                                        optarg);
                                exit(1);
                        }
                        std::string errstr;
                        if (conf->set(kv.substr(0, pos), kv.substr(pos + 1),
                                      errstr) != RdKafka::Conf::CONF_OK) {
                                fprintf(stderr, "%% -X %s: %s\n", optarg,
                                        errstr.c_str());
                                exit(1);
                        }
                        break;
                }
                default:
                        fprintf(stderr,
                                "Usage: %s -P|-C -b <brokers> -t <topic> "
                                "[options]\n"
                                "  -P             Producer mode\n"
                                "  -C             Consumer mode\n"
                                "  -b <brokers>   Bootstrap broker(s)\n"
                                "  -t <topic>     Topic name (required)\n"
                                "  -G <group>     Consumer group ID\n"
                                "  -s <size>      Message size bytes "
                                "(default: 1024)\n"
                                "  -c <count>     Message count; 0=unlimited\n"
                                "  -d <seconds>   Duration; 0=count-driven\n"
                                "  -X key=value   librdkafka config property\n",
                                argv[0]);
                        exit(1);
                }
        }

        if (topic.empty()) {
                fprintf(stderr, "%% -t <topic> is required\n");
                exit(1);
        }
        if (mode == '\0') {
                fprintf(stderr, "%% -P (producer) or -C (consumer) required\n");
                exit(1);
        }

        int rc;
        if (mode == 'P')
                rc = run_producer(brokers, topic, msg_size, msg_count,
                                  duration_sec, conf);
        else
                rc = run_consumer(brokers, topic, group_id, msg_count, conf);

        RdKafka::wait_destroyed(2000);
        return rc;
}
