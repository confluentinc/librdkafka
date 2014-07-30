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

static int run = 1;
static int forever = 1;
static int dispintvl = 1000;
static int do_seq = 0;
static int exit_after = 0;
static int exit_eof = 0;
static FILE *stats_fp;
static int dr_disp_div;
static int verbosity = 1;

static void stop (int sig) {
        if (!run)
                exit(0);
	run = 0;
}

static long int msgs_wait_cnt = 0;
static rd_ts_t t_end;
static rd_kafka_t *rk;


static struct {
	rd_ts_t  t_start;
	rd_ts_t  t_end;
	rd_ts_t  t_end_send;
	uint64_t msgs;
        uint64_t msgs_dr_ok;
        uint64_t msgs_dr_err;
        uint64_t bytes_dr_ok;
	uint64_t bytes;
	uint64_t tx;
	uint64_t tx_err;
        uint64_t avg_rtt;
        uint64_t offset;
	rd_ts_t  t_latency;
	rd_ts_t  t_last;
        rd_ts_t  t_enobufs_last;
	rd_ts_t  t_total;
} cnt = {};


static void err_cb (rd_kafka_t *rk, int err, const char *reason, void *opaque) {
	printf("%% ERROR CALLBACK: %s: %s: %s\n",
	       rd_kafka_name(rk), rd_kafka_err2str(err), reason);
}


static void msg_delivered (rd_kafka_t *rk,
			   void *payload, size_t len,
			   int error_code,
			   void *opaque, void *msg_opaque) {
	static rd_ts_t last;
	rd_ts_t now = rd_clock();
	static int msgs;

	msgs++;

	msgs_wait_cnt--;

	if (error_code)
                cnt.msgs_dr_err++;
        else {
                cnt.msgs_dr_ok++;
                cnt.bytes_dr_ok += len;
        }


	if ((error_code &&
	     (cnt.msgs_dr_err < 50 ||
              !(cnt.msgs_dr_err % (dispintvl / 1000)))) ||
	    !last || msgs_wait_cnt < 5 ||
	    !(msgs_wait_cnt % dr_disp_div) || 
	    (now - last) >= dispintvl * 1000 ||
            verbosity >= 3) {
		if (error_code && verbosity >= 2)
			printf("%% Message delivery failed: %s (%li remain)\n",
			       rd_kafka_err2str(error_code),
			       msgs_wait_cnt);
		else if (verbosity >= 2)
			printf("%% Message delivered: %li remain\n",
			       msgs_wait_cnt);
		if (verbosity >= 3 && do_seq)
			printf(" --> \"%.*s\"\n", (int)len, (char *)payload);
		last = now;
	}

	if (msgs_wait_cnt == 0 && !forever) {
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


static void msg_consume (rd_kafka_message_t *rkmessage, void *opaque) {

	if (rkmessage->err) {
		if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                        cnt.offset = rkmessage->offset;

                        if (verbosity >= 1)
                                printf("%% Consumer reached end of "
                                       "%s [%"PRId32"] "
                                       "message queue at offset %"PRId64"\n",
                                       rd_kafka_topic_name(rkmessage->rkt),
                                       rkmessage->partition, rkmessage->offset);

			if (exit_eof)
				run = 0;

			return;
		}

		printf("%% Consume error for topic \"%s\" [%"PRId32"] "
		       "offset %"PRId64": %s\n",
		       rd_kafka_topic_name(rkmessage->rkt),
		       rkmessage->partition,
		       rkmessage->offset,
		       rd_kafka_message_errstr(rkmessage));

                cnt.msgs_dr_err++;
		return;
	}

        cnt.offset = rkmessage->offset;
	cnt.msgs++;
	cnt.bytes += rkmessage->len;

	if (verbosity >= 2 && !(cnt.msgs % 1000000))
		printf("@%"PRId64": %.*s\n",
		       rkmessage->offset,
		       (int)rkmessage->len, (char *)rkmessage->payload);
		
#if 0 /* Future API */
	/* We store offset when we're done processing
	 * the current message. */
	rd_kafka_offset_store(rkmessage->rkt, rkmessage->partition,
			      rd_kafka_offset_next(rkmessage));
#endif

}


/**
 * Find and extract single value from a two-level search.
 * First find 'field1', then find 'field2' and extract its value.
 * Returns 0 on miss else the value.
 */
static uint64_t json_parse_fields (const char *json, const char **end,
                                   const char *field1, const char *field2) {
        const char *t = json;
        const char *t2;
        int len1 = strlen(field1);
        int len2 = strlen(field2);

        while ((t2 = strstr(t, field1))) {
                uint64_t v;

                t = t2;
                t += len1;

                /* Find field */
                if (!(t2 = strstr(t, field2)))
                        continue;
                t2 += len2;

                while (isspace((int)*t2))
                        t2++;

                v = strtoull(t2, (char **)&t, 10);
                if (t2 == t)
                        continue;

                *end = t;
                return v;
        }

        *end = t + strlen(t);
        return 0;
}

/**
 * Parse various values from rdkafka stats
 */
static void json_parse_stats (const char *json) {
        const char *t;
        const int max_avgs = 100; /* max number of brokers to scan for rtt */
        uint64_t avg_rtt[max_avgs+1];
        int avg_rtt_i     = 0;

        /* Store totals at end of array */
        avg_rtt[max_avgs]     = 0;

        /* Extract all broker RTTs */
        t = json;
        while (avg_rtt_i < max_avgs && *t) {
                avg_rtt[avg_rtt_i] = json_parse_fields(t, &t,
                                                       "\"rtt\":",
                                                       "\"avg\":");

                /* Skip low RTT values, means no messages are passing */
                if (avg_rtt[avg_rtt_i] < 100 /*0.1ms*/)
                        continue;


                avg_rtt[max_avgs] += avg_rtt[avg_rtt_i];
                avg_rtt_i++;
        }

        if (avg_rtt_i > 0)
                avg_rtt[max_avgs] /= avg_rtt_i;

        cnt.avg_rtt = avg_rtt[max_avgs];
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
static void print_stats (int mode, int otype, const char *compression) {
	rd_ts_t now = rd_clock();
	rd_ts_t t_total;
        static int rows_written = 0;

	if (!(otype & _OTYPE_FORCE) &&
            (verbosity == 0 ||
             cnt.t_last + dispintvl > now))
		return;

	if (cnt.t_end_send)
		t_total = cnt.t_end_send - cnt.t_start;
	else if (cnt.t_end)
		t_total = cnt.t_end - cnt.t_start;
	else
		t_total = now - cnt.t_start;

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

                        if (!(rows_written % 20)) {
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
                                ROW_END();
                        }

                        ROW_START();
                        COL_PR64("elapsed", t_total / 1000);
                        COL_PR64("msgs", cnt.msgs);
                        COL_PR64("bytes", cnt.bytes);
                        COL_PR64("rtt", cnt.avg_rtt / 1000);
                        COL_PR64("dr", cnt.msgs_dr_ok);
                        COL_PR64("dr_m/s",
                                 ((cnt.msgs_dr_ok * 1000000) / t_total));
                        COL_PRF("dr_MB/s",
                                (float)((cnt.bytes_dr_ok) / (float)t_total));
                        COL_PR64("dr_err", cnt.msgs_dr_err);
                        COL_PR64("tx_err", cnt.tx_err);
                        COL_PR64("outq", (uint64_t)rd_kafka_outq_len(rk));
                        ROW_END();
                }

                if (otype & _OTYPE_SUMMARY) {
                        printf("%% %"PRIu64" messages produced "
                               "(%"PRIu64" bytes), "
                               "%"PRIu64" delivered (%"PRIu64" failed) "
                               "in %"PRIu64"ms: %"PRIu64" msgs/s and "
                               "%.02f Mb/s, "
                               "%"PRIu64" produce failures, %i in queue, "
                               "%s compression\n",
                               cnt.msgs, cnt.bytes,
                               cnt.msgs_dr_ok, cnt.msgs_dr_err,
                               t_total / 1000,
                               ((cnt.msgs_dr_ok * 1000000) / t_total),
                               (float)((cnt.bytes_dr_ok) / (float)t_total),
                               cnt.tx_err, rd_kafka_outq_len(rk), compression);
                }

        } else {
                if (otype & _OTYPE_TAB) {
                        if (!(rows_written % 20)) {
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
                        COL_PR64("msgs", cnt.msgs);
                        COL_PR64("bytes", cnt.bytes);
                        COL_PR64("rtt", cnt.avg_rtt / 1000);
                        COL_PR64("m/s",
                                 ((cnt.msgs * 1000000) / t_total));
                        COL_PRF("MB/s",
                                (float)((cnt.bytes) / (float)t_total));
                        COL_PR64("rx_err", cnt.msgs_dr_err);
                        COL_PR64("offset", cnt.offset);
        
                        ROW_END();

                }

                if (otype & _OTYPE_SUMMARY) {
                        printf("%% %"PRIu64" messages and %"PRIu64" bytes "
                               "%s in %"PRIu64"ms: %"PRIu64" msgs/s and "
                               "%.02f Mb/s, "
                               "%s compression\n",
                               cnt.msgs, cnt.bytes,
                               mode == 'P' ? "produced" : "consumed",
                               t_total / 1000,
                               ((cnt.msgs * 1000000) / t_total),
                               (float)((cnt.bytes) / (float)t_total),
                               compression);
                }
        }

	cnt.t_last = now;
}


static void sig_usr1 (int sig) {
	rd_kafka_dump(stdout, rk);
}

int main (int argc, char **argv) {
	char *brokers = "localhost";
	char mode = 'C';
	char *topic = NULL;
	const char *key = NULL;
	int partition = RD_KAFKA_PARTITION_UA; /* random */
	int opt;
	int msgcnt = -1;
	int sendflags = 0;
	char *msgpattern = "librdkafka_performance testing!";
	int msgsize = strlen(msgpattern);
	const char *debug = NULL;
	rd_ts_t now;
	char errstr[512];
	uint64_t seq = 0;
	int seed = time(NULL);
	rd_kafka_topic_t *rkt;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
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

	/* Kafka configuration */
	conf = rd_kafka_conf_new();
	rd_kafka_conf_set_error_cb(conf, err_cb);
	rd_kafka_conf_set_dr_cb(conf, msg_delivered);

	/* Producer config */
	rd_kafka_conf_set(conf, "queue.buffering.max.messages", "500000",
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
	rd_kafka_conf_set(conf, "queued.min.messages", "1000000", NULL, 0);


	/* Kafka topic configuration */
	topic_conf = rd_kafka_topic_conf_new();

	while ((opt =
		getopt(argc, argv,
		       "PCt:p:b:s:k:c:fi:Dd:m:S:x:"
                       "R:a:z:o:X:B:eT:G:qvIur:")) != -1) {
		switch (opt) {
		case 'P':
		case 'C':
			mode = opt;
			break;
		case 't':
			topic = optarg;
			break;
		case 'p':
			partition = atoi(optarg);
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
			seq = strtoull(optarg, NULL, 10);
			do_seq = 1;
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
			if (!strcmp(optarg, "end"))
				start_offset = RD_KAFKA_OFFSET_END;
			else if (!strcmp(optarg, "beginning"))
				start_offset = RD_KAFKA_OFFSET_BEGINNING;
			else if (!strcmp(optarg, "stored"))
				start_offset = RD_KAFKA_OFFSET_STORED;
			else {
				start_offset = strtoll(optarg, NULL, 10);

				if (start_offset < 0)
					start_offset = RD_KAFKA_OFFSET_TAIL(-start_offset);
			}

			break;
		case 'e':
			exit_eof = 1;
			break;
		case 'd':
			debug = optarg;
			break;
		case 'X':
		{
			char *name, *val;
			rd_kafka_conf_res_t res;

			if (!strcmp(optarg, "list") ||
			    !strcmp(optarg, "help")) {
				rd_kafka_conf_properties_show(stdout);
				exit(0);
			}

			name = optarg;
			if (!(val = strchr(name, '='))) {
				fprintf(stderr, "%% Expected "
					"-X property=value, not %s\n", name);
				exit(1);
			}

			*val = '\0';
			val++;

			res = RD_KAFKA_CONF_UNKNOWN;
			/* Try "topic." prefixed properties on topic
			 * conf first, and then fall through to global if
			 * it didnt match a topic configuration property. */
			if (!strncmp(name, "topic.", strlen("topic.")))
				res = rd_kafka_topic_conf_set(topic_conf,
							      name+
							      strlen("topic."),
							      val,
							      errstr,
							      sizeof(errstr));

			if (res == RD_KAFKA_CONF_UNKNOWN)
				res = rd_kafka_conf_set(conf, name, val,
							errstr, sizeof(errstr));

			if (res != RD_KAFKA_CONF_OK) {
				fprintf(stderr, "%% %s\n", errstr);
				exit(1);
			}
		}
		break;

		case 'T':
                        stats_intvlstr = optarg;
			break;
                case 'G':
                        stats_cmd = optarg;
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
                        break;

                case 'r':
                        dtmp = strtod(optarg, &tmp2);
                        if (tmp2 == optarg ||
                            (dtmp >= -0.001 && dtmp <= 0.001)) {
                                fprintf(stderr, "%% Invalid rate: %s\n",
                                        optarg);
                                exit(1);
                        }

                        rate_sleep = (int)(1000000.0 / dtmp);
                        break;

		default:
			goto usage;
		}
	}

	if (!topic || optind != argc) {
	usage:
		fprintf(stderr,
			"Usage: %s [-C|-P] -t <topic> "
			"[-p <partition>] [-b <broker,broker..>] [options..]\n"
			"\n"
			"librdkafka version %s (0x%08x)\n"
			"\n"
			" Options:\n"
			"  -C | -P      Consumer or Producer mode\n"
			"  -t <topic>   Topic to fetch / produce\n"
			"  -p <num>     Partition (defaults to random)\n"
			"  -b <brokers> Broker address list (host[:port],..)\n"
			"  -s <size>    Message size (producer)\n"
			"  -k <key>     Message key (producer)\n"
			"  -c <cnt>     Messages to transmit/receive\n"
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
			"  -T <intvl>   Enable statistics from librdkafka at "
			"specified interval (ms)\n"
                        "  -G <command> Pipe statistics to <command>\n"
			"  -I           Idle: dont produce any messages\n"
			"  -q           Decrease verbosity\n"
                        "  -v           Increase verbosity (default 1)\n"
                        "  -u           Output stats in table format\n"
                        "  -r <rate>    Producer msg/s limit\n"
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

	printf("%% Using random seed %i, verbosity level %i\n",
               seed, verbosity);
	srand(seed);
	signal(SIGINT, stop);
	signal(SIGUSR1, sig_usr1);


	if (debug &&
	    rd_kafka_conf_set(conf, "debug", debug, errstr, sizeof(errstr)) !=
	    RD_KAFKA_CONF_OK) {
		printf("%% Debug configuration failed: %s: %s\n",
		       errstr, debug);
		exit(1);
	}

        /* Always enable stats (for RTT extraction), and if user supplied
         * the -T <intvl> option we let her take part of the stats aswell. */
        rd_kafka_conf_set_stats_cb(conf, stats_cb);

        if (!stats_intvlstr) {
                /* if no user-desired stats, adjust stats interval
                 * to the display interval. */
                snprintf(tmp, sizeof(tmp), "%i", dispintvl / 1000);
        }

        if (rd_kafka_conf_set(conf, "statistics.interval.ms",
                              stats_intvlstr ? : tmp,
                              errstr, sizeof(errstr)) !=
            RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%% %s\n", errstr);
                exit(1);
        }

        if (stats_intvlstr) {
                /* User enabled stats (-T) */

                if (stats_cmd) {
                        if (!(stats_fp = popen(stats_cmd, "we"))) {
                                fprintf(stderr,
                                        "%% Failed to start stats command: "
                                        "%s: %s", stats_cmd, strerror(errno));
                                exit(1);
                        }
                } else
                        stats_fp = stdout;
        }

	if (msgcnt != -1)
		forever = 0;

	if (mode == 'P') {
		/*
		 * Producer
		 */
		char *sbuf;
		char *pbuf;
		int outq;
		int keylen = key ? strlen(key) : 0;
		off_t rof = 0;
		size_t plen = strlen(msgpattern);

		if (do_seq) {
			if (msgsize < strlen("18446744073709551615: ")+1)
				msgsize = strlen("18446744073709551615: ")+1;
			/* Force duplication of payload */
			sendflags |= RD_KAFKA_MSG_F_FREE;
		}

		sbuf = malloc(msgsize);

		/* Copy payload content to new buffer */
		while (rof < msgsize) {
			size_t xlen = RD_MIN(msgsize-rof, plen);
			memcpy(sbuf+rof, msgpattern, xlen);
			rof += xlen;
		}

		if (msgcnt == -1)
			printf("%% Sending messages of size %i bytes\n",
			       msgsize);
		else
			printf("%% Sending %i messages of size %i bytes\n",
			       msgcnt, msgsize);

		/* Create Kafka handle */
		if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
 					errstr, sizeof(errstr)))) {
			fprintf(stderr,
				"%% Failed to create Kafka producer: %s\n",
				errstr);
			exit(1);
		}

		if (debug)
			rd_kafka_set_log_level(rk, 7);

		/* Add broker(s) */
		if (rd_kafka_brokers_add(rk, brokers) < 1) {
			fprintf(stderr, "%% No valid brokers specified\n");
			exit(1);
		}

		/* Explicitly create topic to avoid per-msg lookups. */
		rkt = rd_kafka_topic_new(rk, topic, topic_conf);


                if (rate_sleep && verbosity >= 2)
                        fprintf(stderr,
                                "%% Inter message rate limiter sleep %ius\n",
                                rate_sleep);

                dr_disp_div = msgcnt / 50;
                if (dr_disp_div == 0)
                        dr_disp_div = 10;

		cnt.t_start = rd_clock();

		while (run && (msgcnt == -1 || cnt.msgs < msgcnt)) {
			/* Send/Produce message. */

			if (idle) {
				rd_kafka_poll(rk, 1000);
				continue;
			}

			if (do_seq) {
				snprintf(sbuf, msgsize-1, "%"PRIu64": ", seq);
				seq++;
			}

			if (sendflags & RD_KAFKA_MSG_F_FREE) {
				/* Duplicate memory */
				pbuf = malloc(msgsize);
				memcpy(pbuf, sbuf, msgsize);
			} else
				pbuf = sbuf;

			cnt.tx++;
			while (run &&
			       rd_kafka_produce(rkt, partition,
						sendflags, pbuf, msgsize,
						key, keylen, NULL) == -1) {
				if (errno == ESRCH)
					printf("%% No such partition: "
                                               "%"PRId32"\n", partition);
				else if ((errno != ENOBUFS && verbosity >= 1) ||
                                         verbosity >= 3)
					printf("%% produce error: %s%s\n",
					       rd_kafka_err2str(
						       rd_kafka_errno2err(
							       errno)),
					       errno == ENOBUFS ?
					       " (backpressure)":"");

				cnt.tx_err++;
				if (errno != ENOBUFS) {
					run = 0;
					break;
				}
				now = rd_clock();
				if (verbosity >= 2 &&
                                    cnt.t_enobufs_last + dispintvl <= now) {
					printf("%% Backpressure %i "
					       "(tx %"PRIu64", "
					       "txerr %"PRIu64")\n",
					       rd_kafka_outq_len(rk),
					       cnt.tx, cnt.tx_err);
					cnt.t_enobufs_last = now;
				}

				/* Poll to handle delivery reports */
				rd_kafka_poll(rk, 10);

                                print_stats(mode, otype, compression);
			}

			msgs_wait_cnt++;
			cnt.msgs++;
			cnt.bytes += msgsize;

                        if (rate_sleep)
                                usleep(rate_sleep);

			/* Must poll to handle delivery reports */
			rd_kafka_poll(rk, 0);

			print_stats(mode, otype, compression);
		}

		forever = 0;
                if (verbosity >= 2)
                        printf("%% All messages produced, "
                               "now waiting for %li deliveries\n",
                               msgs_wait_cnt);
		if (debug)
			rd_kafka_dump(stdout, rk);

		/* Wait for messages to be delivered */
                while (run && rd_kafka_poll(rk, 1000) != -1)
			print_stats(mode, otype, compression);


		outq = rd_kafka_outq_len(rk);
                if (verbosity >= 2)
                        printf("%% %i messages in outq\n", outq);
		cnt.msgs -= outq;
		cnt.bytes -= msgsize * outq;

		cnt.t_end = t_end;

		if (cnt.tx_err > 0)
			printf("%% %"PRIu64" backpressures for %"PRIu64
			       " produce calls: %.3f%% backpressure rate\n",
			       cnt.tx_err, cnt.tx,
			       ((double)cnt.tx_err / (double)cnt.tx) * 100.0);

		if (debug)
			rd_kafka_dump(stdout, rk);

		/* Destroy topic */
		rd_kafka_topic_destroy(rkt);

		/* Destroy the handle */
		rd_kafka_destroy(rk);

	} else if (mode == 'C') {
		/*
		 * Consumer
		 */

		rd_kafka_message_t **rkmessages = NULL;

#if 0 /* Future API */
		/* The offset storage file is optional but its presence
		 * avoids starting all over from offset 0 again when
		 * the program restarts.
		 * ZooKeeper functionality will be implemented in future
		 * versions and then the offset will be stored there instead. */
		conf.consumer.offset_file = "."; /* current directory */

		/* Indicate to rdkafka that the application is responsible
		 * for storing the offset. This allows the application to
		 * successfully handle a message before storing the offset.
		 * If this flag is not set rdkafka will store the offset
		 * just prior to returning the message from rd_kafka_consume().
		 */
		conf.flags |= RD_KAFKA_CONF_F_APP_OFFSET_STORE;
#endif

		/* Create Kafka handle */
		if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf,
					errstr, sizeof(errstr)))) {
			fprintf(stderr,
				"%% Failed to create Kafka consumer: %s\n",
				errstr);
			exit(1);
		}

		if (debug)
			rd_kafka_set_log_level(rk, 7);

		/* Add broker(s) */
		if (rd_kafka_brokers_add(rk, brokers) < 1) {
			fprintf(stderr, "%% No valid brokers specified\n");
			exit(1);
		}

		/* Create topic to consume from */
		rkt = rd_kafka_topic_new(rk, topic, topic_conf);

		/* Batch consumer */
		if (batch_size)
			rkmessages = malloc(sizeof(*rkmessages) * batch_size);

		/* Start consuming */
		if (rd_kafka_consume_start(rkt, partition, start_offset) == -1){
			fprintf(stderr, "%% Failed to start consuming: %s\n",
				rd_kafka_err2str(rd_kafka_errno2err(errno)));
			exit(1);
		}
		
		cnt.t_start = rd_clock();
		while (run && (msgcnt == -1 || msgcnt > cnt.msgs)) {
			/* Consume messages.
			 * A message may either be a real message, or
			 * an error signaling (if rkmessage->err is set).
			 */
			uint64_t latency;
			int r;

			latency = rd_clock();
			
			if (batch_size) {
				int i;

				/* Batch fetch mode */
				r = rd_kafka_consume_batch(rkt, partition,
							   1000,
							   rkmessages,
							   batch_size);
				if (r != -1) {
					for (i = 0 ; i < r ; i++) {
						msg_consume(rkmessages[i],NULL);
						rd_kafka_message_destroy(
							rkmessages[i]);
					}
				}
			} else {
				/* Callback mode */
				r = rd_kafka_consume_callback(rkt, partition,
							      1000/*timeout*/,
							      msg_consume,
							      NULL);
			}

			cnt.t_latency += rd_clock() - latency;
			
			if (r == -1)
				fprintf(stderr, "%% Error: %s\n",
					rd_kafka_err2str(
						rd_kafka_errno2err(errno)));

			print_stats(mode, otype, compression);

			/* Poll to handle stats callbacks */
			rd_kafka_poll(rk, 0);
		}
		cnt.t_end = rd_clock();

		/* Stop consuming */
		rd_kafka_consume_stop(rkt, partition);

		/* Destroy topic */
		rd_kafka_topic_destroy(rkt);

		if (batch_size)
			free(rkmessages);

		/* Destroy the handle */
		rd_kafka_destroy(rk);

	}

	print_stats(mode, otype|_OTYPE_FORCE, compression);

	if (cnt.t_latency && cnt.msgs)
		printf("%% Average application fetch latency: %"PRIu64"us\n",
		       cnt.t_latency / cnt.msgs);

        if (stats_cmd) {
                pclose(stats_fp);
                stats_fp = NULL;
        }

	/* Let background threads clean up and terminate cleanly. */
	rd_kafka_wait_destroyed(2000);

	return 0;
}
