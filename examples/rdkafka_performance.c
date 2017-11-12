/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012, Magnus Edenhill
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
 * Apache Kafka consumer & producer performance tester
 * using the Kafka driver from librdkafka
 * (https://github.com/edenhill/librdkafka)
 */

#ifdef _MSC_VER
#define  _CRT_SECURE_NO_WARNINGS /* Silence nonsense on MSVC */
#endif

#include "../src/rd.h"

#define _GNU_SOURCE /* for strndup() */
#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <errno.h>

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"  /* for Kafka driver */
/* Do not include these defines from your program, they will not be
 * provided by librdkafka. */
#include "rd.h"
#include "rdtime.h"

#ifdef _MSC_VER
#include "../win32/wingetopt.h"
#include "../win32/wintime.h"
#endif


static int run = 1;
static int forever = 1;
static rd_ts_t dispintvl = 1000;
//static int do_seq = 0;
static int exit_after = 0;
static int exit_eof = 0;
static FILE *stats_fp;
static int dr_disp_div;
static int verbosity = 1;
//static int latency_mode = 0;
static int report_offset = 0;
static FILE *latency_fp = NULL;
static int msgcnt = -1;
static int incremental_mode = 0;
static int partition_cnt = 0;
static int with_dr = 1;

static void stop (int sig) {
        if (!run)
                exit(0);
	run = 0;
}

static long int msgs_wait_cnt = 0;
static long int msgs_wait_produce_cnt = 0;
static rd_ts_t t_end;
static rd_kafka_t *global_rk;

struct avg {
        int64_t  val;
        int      cnt;
        uint64_t ts_start;
};

static struct {
	rd_ts_t  t_start;
	rd_ts_t  t_end;
	rd_ts_t  t_end_send;
	uint64_t msgs;
	uint64_t msgs_last;
        uint64_t msgs_dr_ok;
        uint64_t msgs_dr_err;
        uint64_t bytes_dr_ok;
	uint64_t bytes;
	uint64_t bytes_last;
	uint64_t tx;
	uint64_t tx_err;
        uint64_t avg_rtt;
        uint64_t offset;
	rd_ts_t  t_fetch_latency;
	rd_ts_t  t_last;
        rd_ts_t  t_enobufs_last;
	rd_ts_t  t_total;
        rd_ts_t  latency_last;
        rd_ts_t  latency_lo;
        rd_ts_t  latency_hi;
        rd_ts_t  latency_sum;
        int      latency_cnt;
        int64_t  last_offset;
} statsCollector;


uint64_t wall_clock (void) {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        return ((uint64_t)tv.tv_sec * 1000000LLU) +
		((uint64_t)tv.tv_usec);
}

static void err_cb (rd_kafka_t *rk, int err, const char *reason, void *opaque) {
	printf("%% ERROR CALLBACK: %s: %s: %s\n",
	       rd_kafka_name(rk), rd_kafka_err2str(err), reason);
}

static void throttle_cb (rd_kafka_t *rk, const char *broker_name,
			 int32_t broker_id, int throttle_time_ms,
			 void *opaque) {
	printf("%% THROTTLED %dms by %s (%"PRId32")\n", throttle_time_ms,
	       broker_name, broker_id);
}

static void offset_commit_cb (rd_kafka_t *rk, rd_kafka_resp_err_t err,
                              rd_kafka_topic_partition_list_t *offsets,
                              void *opaque) {
        int i;

        if (err || verbosity >= 2)
                printf("%% Offset commit of %d partition(s): %s\n",
                       offsets->cnt, rd_kafka_err2str(err));

        for (i = 0 ; i < offsets->cnt ; i++) {
                rd_kafka_topic_partition_t *rktpar = &offsets->elems[i];
                if (rktpar->err || verbosity >= 2)
                        printf("%%  %s [%"PRId32"] @ %"PRId64": %s\n",
                               rktpar->topic, rktpar->partition,
                               rktpar->offset, rd_kafka_err2str(err));
        }
}

static void msg_delivered (rd_kafka_t *rk,
                           const rd_kafka_message_t *rkmessage, void *opaque) {
	static rd_ts_t last;
	rd_ts_t now = rd_clock();
	static int msgs;

	msgs++;

	msgs_wait_cnt--;

	if (rkmessage->err)
                statsCollector.msgs_dr_err++;
        else {
                statsCollector.msgs_dr_ok++;
                statsCollector.bytes_dr_ok += rkmessage->len;
        }


	if ((rkmessage->err &&
	     (statsCollector.msgs_dr_err < 50 ||
              !(statsCollector.msgs_dr_err % (dispintvl / 1000)))) ||
	    !last || msgs_wait_cnt < 5 ||
	    !(msgs_wait_cnt % dr_disp_div) ||
	    (now - last) >= dispintvl * 1000 ||
            verbosity >= 3) {
		if (rkmessage->err && verbosity >= 2)
			printf("%% Message delivery failed: %s [%"PRId32"]: "
			       "%s (%li remain)\n",
			       rd_kafka_topic_name(rkmessage->rkt),
			       rkmessage->partition,
			       rd_kafka_err2str(rkmessage->err),
			       msgs_wait_cnt);
		else if (verbosity > 2)
			printf("%% Message delivered (offset %"PRId64"): "
                               "%li remain\n",
                               rkmessage->offset, msgs_wait_cnt);

		last = now;
	}

        if (report_offset)
                statsCollector.last_offset = rkmessage->offset;

	if (msgs_wait_produce_cnt == 0 && msgs_wait_cnt == 0 && !forever) {
		if (verbosity >= 2)
			printf("All messages delivered!\n");
		t_end = rd_clock();
		run = 0;
	}

	if (exit_after && exit_after <= msgs) {
		printf("%% Hard exit after %i messages, as requested\n",
		       exit_after);
		exit(0);
	}
}

static int stats_cb (rd_kafka_t *rk, char *json, size_t json_len,
		     void *opaque) {

        /* Extract values for our own stats */
        json_parse_stats(json);

        if (stats_fp)
                fprintf(stats_fp, "%s\n", json);
	return 0;
}

#define _OTYPE_TAB      0x1  /* tabular format */
#define _OTYPE_SUMMARY  0x2  /* summary format */
#define _OTYPE_FORCE    0x4  /* force output regardless of interval timing */
static void print_stats (rd_kafka_t *rk,
                         int mode, int otype, const char *compression) {
	rd_ts_t now = rd_clock();
	rd_ts_t t_total;
        static int rows_written = 0;
        int print_header;
        double latency_avg = 0.0f;
        char extra[512];
        int extra_of = 0;
        *extra = '\0';

	if (!(otype & _OTYPE_FORCE) &&
            (((otype & _OTYPE_SUMMARY) && verbosity == 0) ||
             statsCollector.t_last + dispintvl > now))
		return;

        print_header = !rows_written ||(verbosity > 0 && !(rows_written % 20));

	if (statsCollector.t_end_send)
		t_total = statsCollector.t_end_send - statsCollector.t_start;
	else if (statsCollector.t_end)
		t_total = statsCollector.t_end - statsCollector.t_start;
	else if (statsCollector.t_start)
		t_total = now - statsCollector.t_start;
	else
		t_total = 1;

        if (mode == 'P') {

                if (otype & _OTYPE_TAB) {
#define ROW_START()        do {} while (0)
#define COL_HDR(NAME)      printf("| %10.10s ", (NAME))
#define COL_PR64(NAME,VAL) printf("| %10"PRIu64" ", (VAL))
#define COL_PRF(NAME,VAL)  printf("| %10.2f ", (VAL))
#define ROW_END()          do {                 \
                                printf("\n");   \
                                rows_written++; \
                        } while (0)

                        if (print_header) {
                                /* First time, print header */
                                ROW_START();
                                COL_HDR("elapsed");
                                COL_HDR("msgs");
                                COL_HDR("bytes");
                                COL_HDR("rtt");
                                COL_HDR("dr");
                                COL_HDR("dr_m/s");
                                COL_HDR("dr_MB/s");
                                COL_HDR("dr_err");
                                COL_HDR("tx_err");
                                COL_HDR("outq");
                                if (report_offset)
                                        COL_HDR("offset");
                                ROW_END();
                        }

                        ROW_START();
                        COL_PR64("elapsed", t_total / 1000);
                        COL_PR64("msgs", statsCollector.msgs);
                        COL_PR64("bytes", statsCollector.bytes);
                        COL_PR64("rtt", statsCollector.avg_rtt / 1000);
                        COL_PR64("dr", statsCollector.msgs_dr_ok);
                        COL_PR64("dr_m/s",
                                 ((statsCollector.msgs_dr_ok * 1000000) / t_total));
                        COL_PRF("dr_MB/s",
                                (float)((statsCollector.bytes_dr_ok) / (float)t_total));
                        COL_PR64("dr_err", statsCollector.msgs_dr_err);
                        COL_PR64("tx_err", statsCollector.tx_err);
                        COL_PR64("outq",
                                 rk ? (uint64_t)rd_kafka_outq_len(rk) : 0);
                        if (report_offset)
                                COL_PR64("offset", (uint64_t)statsCollector.last_offset);
                        ROW_END();
                }

                if (otype & _OTYPE_SUMMARY) {
                        printf("%% %"PRIu64" messages produced "
                               "(%"PRIu64" bytes), "
                               "%"PRIu64" delivered "
                               "(offset %"PRId64", %"PRIu64" failed) "
                               "in %"PRIu64"ms: %"PRIu64" msgs/s and "
                               "%.02f MB/s, "
                               "%"PRIu64" produce failures, %i in queue, "
                               "%s compression\n",
                               statsCollector.msgs, statsCollector.bytes,
                               statsCollector.msgs_dr_ok, statsCollector.last_offset, statsCollector.msgs_dr_err,
                               t_total / 1000,
                               ((statsCollector.msgs_dr_ok * 1000000) / t_total),
                               (float)((statsCollector.bytes_dr_ok) / (float)t_total),
                               statsCollector.tx_err,
                               rk ? rd_kafka_outq_len(rk) : 0,
                               compression);
                }

        } else {

                if (otype & _OTYPE_TAB) {
                        if (print_header) {
                                /* First time, print header */
                                ROW_START();
                                COL_HDR("elapsed");
                                COL_HDR("msgs");
                                COL_HDR("bytes");
                                COL_HDR("rtt");
                                COL_HDR("m/s");
                                COL_HDR("MB/s");
                                COL_HDR("rx_err");
                                COL_HDR("offset");

                                ROW_END();
                        }

                        ROW_START();
                        COL_PR64("elapsed", t_total / 1000);
                        COL_PR64("msgs", statsCollector.msgs);
                        COL_PR64("bytes", statsCollector.bytes);
                        COL_PR64("rtt", statsCollector.avg_rtt / 1000);
                        COL_PR64("m/s",
                                 ((statsCollector.msgs * 1000000) / t_total));
                        COL_PRF("MB/s",
                                (float)((statsCollector.bytes) / (float)t_total));
                        COL_PR64("rx_err", statsCollector.msgs_dr_err);
                        COL_PR64("offset", statsCollector.offset);

                        ROW_END();

                }

                if (otype & _OTYPE_SUMMARY) {
                        if (latency_avg >= 1.0f)
                                extra_of += rd_snprintf(extra+extra_of,
                                                     sizeof(extra)-extra_of,
                                                     ", latency "
                                                     "curr/avg/lo/hi "
                                                     "%.2f/%.2f/%.2f/%.2fms",
                                                     statsCollector.latency_last / 1000.0f,
                                                     latency_avg  / 1000.0f,
                                                     statsCollector.latency_lo / 1000.0f,
                                                     statsCollector.latency_hi / 1000.0f)
;
                        printf("%% %"PRIu64" messages (%"PRIu64" bytes) "
                               "consumed in %"PRIu64"ms: %"PRIu64" msgs/s "
                               "(%.02f MB/s)"
                               "%s\n",
                               statsCollector.msgs, statsCollector.bytes,
                               t_total / 1000,
                               ((statsCollector.msgs * 1000000) / t_total),
                               (float)((statsCollector.bytes) / (float)t_total),
                               extra);
                }

                if (incremental_mode && now > statsCollector.t_last) {
                        uint64_t i_msgs = statsCollector.msgs - statsCollector.msgs_last;
                        uint64_t i_bytes = statsCollector.bytes - statsCollector.bytes_last;
                        uint64_t i_time = statsCollector.t_last ? now - statsCollector.t_last : 0;

                        printf("%% INTERVAL: %"PRIu64" messages "
                               "(%"PRIu64" bytes) "
                               "consumed in %"PRIu64"ms: %"PRIu64" msgs/s "
                               "(%.02f MB/s)"
                               "%s\n",
                               i_msgs, i_bytes,
                               i_time / 1000,
                               ((i_msgs * 1000000) / i_time),
                               (float)((i_bytes) / (float)i_time),
                               extra);

                }
        }

	statsCollector.t_last = now;
	statsCollector.msgs_last = statsCollector.msgs;
	statsCollector.bytes_last = statsCollector.bytes;
}


static void sig_usr1 (int sig) {
	rd_kafka_dump(stdout, global_rk);
}


int main (int argc, char **argv) {
	char *brokers = NULL;
	char mode = 'C';
	char *topic = NULL;
	const char *key = NULL;
        int *partitions = NULL;
	int opt;
	int sendflags = 0;
	char *msgpattern = "librdkafka_performance testing!";
	int msgsize = (int)strlen(msgpattern);
	const char *debug = NULL;
	rd_ts_t now;
	char errstr[512];
	uint64_t seq = 0;
	int seed = (int)time(NULL);
        rd_kafka_t *rk;
	rd_kafka_topic_t *rkt;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	rd_kafka_queue_t *rkqu = NULL;
	const char *compression = "no";
	int64_t start_offset = 0;
	int batch_size = 0;
	int idle = 0;
        const char *stats_cmd = NULL;
        char *stats_intvlstr = NULL;
        char tmp[128];
        char *tmp2;
        int otype = _OTYPE_SUMMARY;
        double dtmp;
        int rate_sleep = 0;
	rd_kafka_topic_partition_list_t *topics;
        int exitcode = 0;

	/* Kafka configuration */
	conf = rd_kafka_conf_new();
	rd_kafka_conf_set_error_cb(conf, err_cb);
	rd_kafka_conf_set_throttle_cb(conf, throttle_cb);
        rd_kafka_conf_set_offset_commit_cb(conf, offset_commit_cb);

#ifdef SIGIO
        /* Quick termination */
	rd_snprintf(tmp, sizeof(tmp), "%i", SIGIO);
	rd_kafka_conf_set(conf, "internal.termination.signal", tmp, NULL, 0);
#endif

	/* Producer config */
	rd_kafka_conf_set(conf, "queue.buffering.max.messages", "5000000",
			  NULL, 0);
	rd_kafka_conf_set(conf, "message.send.max.retries", "3", NULL, 0);
	rd_kafka_conf_set(conf, "retry.backoff.ms", "500", NULL, 0);

	/* Consumer config */
	/* Tell rdkafka to (try to) maintain 1M messages
	 * in its internal receive buffers. This is to avoid
	 * application -> rdkafka -> broker  per-message ping-pong
	 * latency.
	 * The larger the local queue, the higher the performance.
	 * Try other values with: ... -X queued.min.messages=1000
	 */
	rd_kafka_conf_set(conf, "queued.min.messages", "1000", NULL, 0);
	rd_kafka_conf_set(conf, "session.timeout.ms", "6000", NULL, 0);

	/* Kafka topic configuration */
	topic_conf = rd_kafka_topic_conf_new();
	rd_kafka_topic_conf_set(topic_conf, "auto.offset.reset", "earliest",
				NULL, 0);

	topics = rd_kafka_topic_partition_list_new(1);

	while ((opt =
		getopt(argc, argv,
		       "PCG:t:p:b:s:k:c:fi:MDd:m:S:x:"
                       "R:a:z:o:X:B:eT:Y:qvIur:lA:OwN")) != -1) {
		switch (opt) {
		case 'G':
			if (rd_kafka_conf_set(conf, "group.id", optarg,
					      errstr, sizeof(errstr)) !=
			    RD_KAFKA_CONF_OK) {
				fprintf(stderr, "%% %s\n", errstr);
				exit(1);
			}
			/* no break */
			/* FALLTHRU */
		case 'P':
		case 'C':
//			***** REMOVED *****
			fprintf(stderr, "-o option disabled\n");
			break;
		case 't':
			rd_kafka_topic_partition_list_add(topics, optarg,
							  RD_KAFKA_PARTITION_UA);
			break;
		case 'p':
                        partition_cnt++;
			partitions = realloc(partitions, sizeof(*partitions) * partition_cnt);
			partitions[partition_cnt-1] = atoi(optarg);
			break;

		case 'b':
			brokers = optarg;
			break;
		case 's':
			msgsize = atoi(optarg);
			break;
		case 'k':
			key = optarg;
			break;
		case 'c':
			msgcnt = atoi(optarg);
			break;
		case 'D':
			sendflags |= RD_KAFKA_MSG_F_FREE;
			break;
		case 'i':
			dispintvl = atoi(optarg);
			break;
		case 'm':
			msgpattern = optarg;
			break;
		case 'S':
			//			***** REMOVED *****
			fprintf(stderr, "-S option disabled\n");
			break;
		case 'x':
			exit_after = atoi(optarg);
			break;
		case 'R':
			seed = atoi(optarg);
			break;
		case 'a':
			if (rd_kafka_topic_conf_set(topic_conf,
						    "request.required.acks",
						    optarg,
						    errstr, sizeof(errstr)) !=
			    RD_KAFKA_CONF_OK) {
				fprintf(stderr, "%% %s\n", errstr);
				exit(1);
			}
			break;
		case 'B':
			batch_size = atoi(optarg);
			break;
		case 'z':
			if (rd_kafka_conf_set(conf, "compression.codec",
					      optarg,
					      errstr, sizeof(errstr)) !=
			    RD_KAFKA_CONF_OK) {
				fprintf(stderr, "%% %s\n", errstr);
				exit(1);
			}
			compression = optarg;
			break;
		case 'o':
//			***** REMOVED *****
			fprintf(stderr, "-o option disabled\n");
			break;
		case 'e':
				exit_eof = 1;
			break;
		case 'd':
				debug = optarg;
			break;
		case 'X':
//			***** REMOVED *****
			fprintf(stderr, "-X option disabled\n");
			break;

		case 'T':
            stats_intvlstr = optarg;
            break;
		case 'Y':
//			***** REMOVED *****
			fprintf(stderr, "-Y option disabled\n");
			break;

		case 'q':
                        verbosity--;
			break;

		case 'v':
                        verbosity++;
			break;

		case 'I':
			idle = 1;
			break;

		case 'u':
				otype = _OTYPE_TAB;
				verbosity--; /* remove some fluff */
				break;

		case 'r':
//			***** REMOVED *****
			fprintf(stderr, "-r option disabled\n");
				break;

		case 'l':
//			***** REMOVED *****
			fprintf(stderr, "-l option disabled\n");
			break;

		case 'A':
//			***** REMOVED *****
			fprintf(stderr, "-A option disabled\n");
			break;

		case 'M':
			incremental_mode = 1;
			break;

		case 'N':
			with_dr = 0;
			break;

		default:
                        fprintf(stderr, "Unknown option: %c\n", opt);
			goto usage;
		}
	}

	if (topics->cnt == 0 || optind != argc) {
                if (optind < argc)
                        fprintf(stderr, "Unknown argument: %s\n", argv[optind]);
	usage:
		fprintf(stderr,
			"Usage: %s [-C|-P] -t <topic> "
			"[-p <partition>] [-b <broker,broker..>] [options..]\n"
			"\n"
			"librdkafka version %s (0x%08x)\n"
			"\n"
			" Options:\n"
			"  -C | -P |    Consumer or Producer mode\n"
			"  -G <groupid> High-level Kafka Consumer mode\n"
			"  -t <topic>   Topic to consume / produce\n"
			"  -p <num>     Partition (defaults to random). "
			"Multiple partitions are allowed in -C consumer mode.\n"
			"  -M           Print consumer interval stats\n"
			"  -b <brokers> Broker address list (host[:port],..)\n"
			"  -s <size>    Message size (producer)\n"
			"  -k <key>     Message key (producer)\n"
			"  -c <cnt>     Messages to transmit/receive\n"
			"  -x <cnt>     Hard exit after transmitting <cnt> messages (producer)\n"
			"  -D           Copy/Duplicate data buffer (producer)\n"
			"  -i <ms>      Display interval\n"
			"  -m <msg>     Message payload pattern\n"
			"  -S <start>   Send a sequence number starting at "
			"<start> as payload\n"
			"  -R <seed>    Random seed value (defaults to time)\n"
			"  -a <acks>    Required acks (producer): "
			"-1, 0, 1, >1\n"
			"  -B <size>    Consume batch size (# of msgs)\n"
			"  -z <codec>   Enable compression:\n"
			"               none|gzip|snappy\n"
			"  -o <offset>  Start offset (consumer)\n"
			"               beginning, end, NNNNN or -NNNNN\n"
			"  -d [facs..]  Enable debugging contexts:\n"
			"               %s\n"
			"  -X <prop=name> Set arbitrary librdkafka "
			"configuration property\n"
			"               Properties prefixed with \"topic.\" "
			"will be set on topic object.\n"
			"               Use '-X list' to see the full list\n"
			"               of supported properties.\n"
                        "  -X file=<path> Read config from file.\n"
			"  -T <intvl>   Enable statistics from librdkafka at "
			"specified interval (ms)\n"
                        "  -Y <command> Pipe statistics to <command>\n"
			"  -I           Idle: dont produce any messages\n"
			"  -q           Decrease verbosity\n"
                        "  -v           Increase verbosity (default 1)\n"
                        "  -u           Output stats in table format\n"
                        "  -r <rate>    Producer msg/s limit\n"
                        "  -l           Latency measurement.\n"
                        "               Needs two matching instances, one\n"
                        "               consumer and one producer, both\n"
                        "               running with the -l switch.\n"
                        "  -l           Producer: per-message latency stats\n"
			"  -A <file>    Write per-message latency stats to "
			"<file>. Requires -l\n"
                        "  -O           Report produced offset (producer)\n"
			"  -N           No delivery reports (producer)\n"
			"\n"
			" In Consumer mode:\n"
			"  consumes messages and prints thruput\n"
			"  If -B <..> is supplied the batch consumer\n"
			"  mode is used, else the callback mode is used.\n"
			"\n"
			" In Producer mode:\n"
			"  writes messages of size -s <..> and prints thruput\n"
			"\n",
			argv[0],
			rd_kafka_version_str(), rd_kafka_version(),
			RD_KAFKA_DEBUG_CONTEXTS);
		exit(1);
	}


	dispintvl *= 1000; /* us */

        if (verbosity > 1)
                printf("%% Using random seed %i, verbosity level %i\n",
                       seed, verbosity);
	srand(seed);
	signal(SIGINT, stop);
#ifdef SIGUSR1
	signal(SIGUSR1, sig_usr1);
#endif


        /* Always enable stats (for RTT extraction), and if user supplied
         * the -T <intvl> option we let her take part of the stats aswell. */
//        rd_kafka_conf_set_stats_cb(conf, stats_cb);

        if (!stats_intvlstr) {
                /* if no user-desired stats, adjust stats interval
                 * to the display interval. */
                rd_snprintf(tmp, sizeof(tmp), "%"PRId64, dispintvl / 1000);
        }

//        if (rd_kafka_conf_set(conf, "statistics.interval.ms",
//                              stats_intvlstr ? stats_intvlstr : tmp,
//                              errstr, sizeof(errstr)) !=
//            RD_KAFKA_CONF_OK) {
//                fprintf(stderr, "%% %s\n", errstr);
//                exit(1);
//        }


        if (stats_intvlstr) {
                /* User enabled stats (-T) */

#ifndef _MSC_VER
                if (stats_cmd) {
                        if (!(stats_fp = popen(stats_cmd, "we"))) {
                                fprintf(stderr,
                                        "%% Failed to start stats command: "
                                        "%s: %s", stats_cmd, strerror(errno));
                                exit(1);
                        }
                } else
#endif
                        stats_fp = stdout;
        }

	if (msgcnt != -1)
		forever = 0;

	topic = topics->elems[0].topic;

	if (mode == 'P') {
		/*
		 * Producer
		 */
		char *sbuf;
		char *pbuf;
		int outq;
		int keylen = key ? (int)strlen(key) : 0;
		off_t rof = 0;
		size_t plen = strlen(msgpattern);
		int partition = partitions ? partitions[0] : RD_KAFKA_PARTITION_UA;

		sbuf = malloc(msgsize);

		/* Copy payload content to new buffer */
		while (rof < msgsize) {
			size_t xlen = RD_MIN((size_t)msgsize-rof, plen);
			memcpy(sbuf+rof, msgpattern, xlen);
			rof += (off_t)xlen;
		}

		if (msgcnt == -1)
			printf("%% Sending messages of size %i bytes\n",
			       msgsize);
		else
			printf("%% Sending %i messages of size %i bytes\n",
			       msgcnt, msgsize);

		if (with_dr)
			rd_kafka_conf_set_dr_msg_cb(conf, msg_delivered);

		/* Create Kafka handle */
		if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
 					errstr, sizeof(errstr)))) {
			fprintf(stderr,
				"%% Failed to create Kafka producer: %s\n",
				errstr);
			exit(1);
		}

                global_rk = rk;

		/* Add broker(s) */
		if (brokers && rd_kafka_brokers_add(rk, brokers) < 1) {
			fprintf(stderr, "%% No valid brokers specified\n");
			exit(1);
		}

		/* Explicitly create topic to avoid per-msg lookups. */
		rkt = rd_kafka_topic_new(rk, topic, topic_conf);



                dr_disp_div = msgcnt / 50;
                if (dr_disp_div == 0)
                        dr_disp_div = 10;

		statsCollector.t_start = statsCollector.t_last = rd_clock();

		msgs_wait_produce_cnt = msgcnt;

		while (run && (msgcnt == -1 || (int)statsCollector.msgs < msgcnt)) {
			/* Send/Produce message. */

			if (idle) {
				rd_kafka_poll(rk, 1000);
				continue;
			}

			if (sendflags & RD_KAFKA_MSG_F_FREE) {
				/* Duplicate memory */
				pbuf = malloc(msgsize);
				memcpy(pbuf, sbuf, msgsize);
			} else {
				pbuf = sbuf;
			}
			if (msgsize == 0){
					pbuf = NULL;
			}
			statsCollector.tx++;
			while (run &&
			       rd_kafka_produce(rkt, partition,
						sendflags, pbuf, msgsize,
						key, keylen, NULL) == -1) {
				rd_kafka_resp_err_t err = rd_kafka_last_error();
				if (err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION)
					printf("%% No such partition: "
						   "%"PRId32"\n", partition);
				else if (verbosity >= 3 ||
					(err != RD_KAFKA_RESP_ERR__QUEUE_FULL && verbosity >= 1))
					printf("%% produce error: %s%s\n",
						   rd_kafka_err2str(err),
						   err == RD_KAFKA_RESP_ERR__QUEUE_FULL ?
						   " (backpressure)" : "");

				statsCollector.tx_err++;
				if (err != RD_KAFKA_RESP_ERR__QUEUE_FULL) {
					run = 0;
					break;
				}

				/* Poll to handle delivery reports */
				rd_kafka_poll(rk, 10);

                                print_stats(rk, mode, otype, compression);
			}

			msgs_wait_cnt++;
			if (msgs_wait_produce_cnt != -1)
				msgs_wait_produce_cnt--;
			statsCollector.msgs++;
			statsCollector.bytes += msgsize;

			/* Must poll to handle delivery reports */
			rd_kafka_poll(rk, 0);

			print_stats(rk, mode, otype, compression);
		}

		forever = 0;

		/* Wait for messages to be delivered */
		while (run && rd_kafka_poll(rk, 1000) != -1){
			print_stats(rk, mode, otype, compression);
		}


		outq = rd_kafka_outq_len(rk);
                if (verbosity >= 2)
                        printf("%% %i messages in outq\n", outq);
		statsCollector.msgs -= outq;
		statsCollector.bytes -= msgsize * outq;

		statsCollector.t_end = t_end;

		if (statsCollector.tx_err > 0)
			printf("%% %"PRIu64" backpressures for %"PRIu64
			       " produce calls: %.3f%% backpressure rate\n",
			       statsCollector.tx_err, statsCollector.tx,
			       ((double)statsCollector.tx_err / (double)statsCollector.tx) * 100.0);

		stats_cb();
		/* Destroy topic */
		rd_kafka_topic_destroy(rkt);

		/* Destroy the handle */
		rd_kafka_destroy(rk);
                global_rk = rk = NULL;

		free(sbuf);

                exitcode = statsCollector.msgs == statsCollector.msgs_dr_ok ? 0 : 1;

	}

	print_stats(NULL, mode, otype|_OTYPE_FORCE, compression);

	if (statsCollector.t_fetch_latency && statsCollector.msgs)
		printf("%% Average application fetch latency: %"PRIu64"us\n",
		       statsCollector.t_fetch_latency / statsCollector.msgs);

	if (latency_fp)
		fclose(latency_fp);

        if (stats_fp) {
#ifndef _MSC_VER
                pclose(stats_fp);
#endif
                stats_fp = NULL;
        }

        if (partitions)
                free(partitions);

	rd_kafka_topic_partition_list_destroy(topics);

	/* Let background threads clean up and terminate cleanly. */
	rd_kafka_wait_destroyed(2000);

	return exitcode;
}

