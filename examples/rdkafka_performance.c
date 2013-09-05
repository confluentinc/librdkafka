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

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"  /* for Kafka driver */
/* Do not include these defines from your program, they will not be
 * provided by librdkafka. */
#include "rd.h"
#include "rdtime.h"

static int run = 1;
static int dispintvl = 1000;
static int do_seq = 0;
static int exit_after = 0;

static void stop (int sig) {
	run = 0;
}

static long int msgs_wait_cnt = 0;
static int msgs_failed = 0;

static rd_ts_t t_end;

static void err_cb (rd_kafka_t *rk, int err, const char *reason, void *opaque) {
	printf("ERROR CALLBACK: %s: %s: %s\n",
	       rd_kafka_name(rk), rd_kafka_err2str(rk, err), reason);
}


static void msg_delivered (rd_kafka_t *rk,
			   void *payload, size_t len,
			   int error_code,
			   void *opaque, void *msg_opaque) {
	long int msgid = (long int)msg_opaque;
	static rd_ts_t last;
	rd_ts_t now = rd_clock();
	static int msgs;

	msgs++;

	msgs_wait_cnt--;

	if (error_code)
		msgs_failed++;


	if ((error_code &&
	     (msgs_failed < 50 || !(msgs_failed % (dispintvl / 1000)))) ||
	    !last || msgs_wait_cnt < 5 ||
	    !(msgs_wait_cnt % (dispintvl / 1000)) || 
	    (now - last) >= dispintvl * 1000) {
		if (error_code)
			printf("Message %ld delivered failed: %s (%li remain)",
			       msgid, rd_kafka_err2str(rk, error_code),
			       msgs_wait_cnt);
		else
			printf("Message %ld delivered: %li remain\n",
			       msgid, msgs_wait_cnt);
		if (do_seq)
			printf(" --> \"%.*s\"\n", (int)len, (char *)payload);
		last = now;
	}

	if (msgs_wait_cnt == 0) {
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

rd_kafka_t *rk;

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
	struct {
		rd_ts_t  t_start;
		rd_ts_t  t_end;
		rd_ts_t  t_end_send;
		uint64_t msgs;
		uint64_t bytes;
		uint64_t tx;
		uint64_t tx_err;
		rd_ts_t  t_latency;
		rd_ts_t  t_last;
		rd_ts_t  t_total;
	} cnt = {};
	rd_ts_t now;
	char *dirstr = "";
	char errstr[512];
	uint64_t seq = 0;
	int seed = time(NULL);
	rd_kafka_conf_t conf;
	rd_kafka_topic_conf_t topic_conf;
	static const char *compression_names[] = {
		"no",
		"gzip",
		"snappy"
	};

	/* Kafka configuration
	 * Base configuration on the default config. */
	rd_kafka_defaultconf_set(&conf);
	conf.error_cb                  = err_cb;
	conf.opaque                    = NULL;
	conf.producer.dr_cb            = msg_delivered;
	conf.producer.max_messages     = 500000;
	conf.producer.max_retries      = 3;
	conf.producer.retry_backoff_ms = 2000;

	/* Topic configuration
	 * Base topic configuration on the default topic config. */
	rd_kafka_topic_defaultconf_set(&topic_conf);
	topic_conf.message_timeout_ms  = 5000;

	while ((opt = getopt(argc, argv,
			     "PCt:p:b:s:k:c:fi:Dd:m:S:x:R:a:z:")) != -1) {
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
			topic_conf.required_acks = atoi(optarg);
			break;
		case 'z':
			if (rd_kafka_conf_set(&conf, "compression.codec",
					      optarg,
					      errstr, sizeof(errstr)) !=
			    RD_KAFKA_CONF_OK) {
				fprintf(stderr, "%% %s\n", errstr);
				exit(1);
			}
			break;
		case 'd':
			debug = optarg;
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
			"  -z <codec>   Enable compression:\n"
			"               none|gzip|snappy\n"
			"  -d [facs..]  Enable debugging contexts:\n"
			"               %s\n"
			"\n"
			" In Consumer mode:\n"
			"  consumes messages and prints thruput\n"
			" In Producer mode:\n"
			"  writes messages of size -s <..> and prints thruput\n"
			"\n",
			argv[0],
			RD_KAFKA_DEBUG_CONTEXTS);
		exit(1);
	}

	dispintvl *= 1000; /* us */

	printf("%% Using random seed %i\n", seed);
	srand(seed);
	signal(SIGINT, stop);
	signal(SIGUSR1, sig_usr1);


	if (debug &&
	    rd_kafka_conf_set(&conf, "debug", debug, errstr, sizeof(errstr)) !=
	    RD_KAFKA_CONF_OK) {
		printf("%% Debug configuration failed: %s: %s\n",
		       errstr, debug);
		exit(1);
	}

	/* Socket hangups are gracefully handled in librdkafka on socket error
	 * without the use of signals, so SIGPIPE should be ignored by the
	 * calling program. */
	signal(SIGPIPE, SIG_IGN);

	if (mode == 'P') {
		/*
		 * Producer
		 */
		char *sbuf;
		char *pbuf;
		int outq;
		int i;
		rd_kafka_topic_t *rkt;
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
		if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, &conf,
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
		rkt = rd_kafka_topic_new(rk, topic, &topic_conf);

		cnt.t_start = rd_clock();

		msgs_wait_cnt = msgcnt;
		
		while (run && (msgcnt == -1 || cnt.msgs < msgcnt)) {
			/* Send/Produce message. */

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
			while (rd_kafka_produce(rkt, partition,
						sendflags, pbuf, msgsize,
						key, keylen,
						(void *)cnt.msgs) == -1) {
				printf("produce error: %s%s\n",
				       strerror(errno),
				       errno == ENOBUFS ? " (backpressure)":"");
				cnt.tx_err++;
				now = rd_clock();
				if (cnt.t_last + dispintvl <= now) {
					printf("%% Backpressure %i/%i "
					       "(tx %"PRIu64", "
					       "txerr %"PRIu64")\n",
					       rd_kafka_outq_len(rk),
					       conf.producer.max_messages,
					       cnt.tx, cnt.tx_err);
					cnt.t_last = now;
				}
				/* Poll to handle delivery reports */
				rd_kafka_poll(rk, 10);
			}

			cnt.msgs++;
			cnt.bytes += msgsize;
			
			now = rd_clock();
			if (cnt.t_last + dispintvl <= now &&
			    now - cnt.t_start > 0) {
				printf("%% %"PRIu64" messages and %"PRIu64 
				       "bytes: %"PRIu64" msgs/s and "
				       "%.2f Mb/s\n",
				       cnt.msgs, cnt.bytes,
				       ((cnt.msgs * 1000000000) /
					(now - cnt.t_start)),
				       (float)(cnt.bytes /
					       (now - cnt.t_start)));
				cnt.t_last = now;
			}

			/* Must poll to handle delivery reports */
			rd_kafka_poll(rk, 0);
			
		}

		printf("All messages produced, "
		       "now waiting for %li deliveries\n",
		       msgs_wait_cnt);
		rd_kafka_dump(stdout, rk);

		// cnt.t_end_send = rd_clock();

		/* Wait for messages to be delivered */
		i = 0;
		while (run && rd_kafka_poll(rk, 1000) != -1) {
			if (!(i++ % (dispintvl/1000)))
				printf("%% Waiting for %li, "
				       "%i messages in outq "
				       "to be sent. Abort with Ctrl-c\n",
				       msgs_wait_cnt,
				       rd_kafka_outq_len(rk));
		}


		outq = rd_kafka_outq_len(rk);
		printf("%% %i messages in outq\n", outq);
		cnt.msgs -= outq;
		cnt.bytes -= msgsize * outq;

		cnt.t_end = t_end;

		if (cnt.tx_err > 0)
			printf("%% %"PRIu64" backpressures for %"PRIu64
			       " produce calls: %.3f%% backpressure rate\n",
			       cnt.tx_err, cnt.tx,
			       ((double)cnt.tx_err / (double)cnt.tx) * 100.0);

		rd_kafka_dump(stdout, rk);

		/* Destroy the handle */
		rd_kafka_destroy(rk);

		dirstr = "sent";

	} else if (mode == 'C') {
#if 0
		/*
		 * Consumer
		 */
		rd_kafka_op_t *rko;
		/* Base our configuration on the default config. */
		rd_kafka_conf_t conf = rd_kafka_defaultconf;

		/* The offset storage file is optional but its presence
		 * avoids starting all over from offset 0 again when
		 * the program restarts.
		 * ZooKeeper functionality will be implemented in future
		 * versions and then the offset will be stored there instead. */
		conf.consumer.offset_file = "."; /* current directory */

		/* Indicate to rdkafka that the application is responsible
		 * for storing the offset. This allows the application to
		 * succesfully handle a message before storing the offset.
		 * If this flag is not set rdkafka will store the offset
		 * just prior to returning the message from rd_kafka_consume().
		 */
		conf.flags |= RD_KAFKA_CONF_F_APP_OFFSET_STORE;


		/* Tell rdkafka to (try to) maintain 10000 messages
		 * in its internal receive buffers. This is to avoid
		 * application -> rdkafka -> broker  per-message ping-pong
		 * latency. */
		conf.consumer.replyq_low_thres = 100000;

		/* Use the consumer convenience function
		 * to create a Kafka handle. */
		if (!(rk = rd_kafka_new_consumer(broker, topic,
						 (uint32_t)partition,
						 0, &conf))) {
			perror("kafka_new_consumer");
			exit(1);
		}
		
		cnt.t_start = rd_clock();
		while (run && (msgcnt == -1 || msgcnt > cnt.msgs)) {
			/* Fetch an "op" which is one of:
			 *  - a kafka message (if rko_len>0 && rko_err==0)
			 *  - an error (if rko_err)
			 */
			uint64_t latency;

			latency = rd_clock();
			if (!(rko = rd_kafka_consume(rk, 1000/*timeout ms*/)))
				continue;
			cnt.t_latency += rd_clock() - latency;
			
			if (rko->rko_err)
				fprintf(stderr, "%% Error: %.*s\n",
					rko->rko_len, rko->rko_payload);
			else if (rko->rko_len) {
				cnt.msgs++;
				cnt.bytes += rko->rko_len;
			}

			/* rko_offset contains the offset of the _next_
			 * message. We store it when we're done processing
			 * the current message. */
			if (rko->rko_offset)
				rd_kafka_offset_store(rk, rko->rko_offset);

			/* Destroy the op */
			rd_kafka_op_destroy(rk, rko);

			now = rd_clock();
			if (cnt.t_last + dispintvl <= now &&
				cnt.t_start + 1000000 < now) {
				printf("%% %"PRIu64" messages and %"PRIu64 
				       " bytes: %"PRIu64" msgs/s and "
				       "%.2f Mb/s\n",
				       cnt.msgs, cnt.bytes,
				       (cnt.msgs / ((now - cnt.t_start)/1000))
				       * 1000,
				       (float)(cnt.bytes /
					       ((now - cnt.t_start) / 1000)));
				cnt.t_last = now;
			}

		}
		cnt.t_end = rd_clock();

		/* Destroy the handle */
		rd_kafka_destroy(rk);
#endif
		dirstr = "received";
	}

	if (cnt.t_end_send)
		cnt.t_total = cnt.t_end_send - cnt.t_start;
	else
		cnt.t_total = cnt.t_end - cnt.t_start;

	printf("Result:\n");
	if (cnt.t_total > 0) {
		printf("%% %"PRIu64" messages and %"PRIu64" bytes "
		       "%s in %"PRIu64"ms: %"PRIu64" msgs/s and %.02f Mb/s, "
		       "%i messages failed, %s compression\n",
		       cnt.msgs, cnt.bytes,
		       dirstr,
		       cnt.t_total / 1000,
		       ((cnt.msgs * 1000000) / cnt.t_total),
		       (float)((cnt.bytes) / (float)cnt.t_total),
		       msgs_failed,
		       compression_names[conf.producer.compression_codec]);
	}

	if (cnt.t_latency)
		printf("%% Average application fetch latency: %"PRIu64"us\n",
		       cnt.t_latency / cnt.msgs);


	return 0;
}
