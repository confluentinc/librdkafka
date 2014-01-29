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
 * Apache Kafka consumer & producer example programs
 * using the Kafka driver from librdkafka
 * (https://github.com/edenhill/librdkafka)
 */

#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <syslog.h>
#include <sys/time.h>
#include <errno.h>

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is builtin from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"  /* for Kafka driver */


static int run = 1;
static rd_kafka_t *rk;
static int exit_eof = 0;
static int quiet = 0;
static 	enum {
	OUTPUT_HEXDUMP,
	OUTPUT_RAW,
} output = OUTPUT_HEXDUMP;

static void stop (int sig) {
	run = 0;
	fclose(stdin); /* abort fgets() */
}


static void hexdump (FILE *fp, const char *name, const void *ptr, size_t len) {
	const char *p = (const char *)ptr;
	int of = 0;


	if (name)
		fprintf(fp, "%s hexdump (%zd bytes):\n", name, len);

	for (of = 0 ; of < len ; of += 16) {
		char hexen[16*3+1];
		char charen[16+1];
		int hof = 0;

		int cof = 0;
		int i;

		for (i = of ; i < of + 16 && i < len ; i++) {
			hof += sprintf(hexen+hof, "%02x ", p[i] & 0xff);
			cof += sprintf(charen+cof, "%c",
				       isprint((int)p[i]) ? p[i] : '.');
		}
		fprintf(fp, "%08x: %-48s %-16s\n",
			of, hexen, charen);
	}
}

/**
 * Kafka logger callback (optional)
 */
static void logger (const rd_kafka_t *rk, int level,
		    const char *fac, const char *buf) {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	fprintf(stderr, "%u.%03u RDKAFKA-%i-%s: %s: %s\n",
		(int)tv.tv_sec, (int)(tv.tv_usec / 1000),
		level, fac, rd_kafka_name(rk), buf);
}

/**
 * Message delivery report callback.
 * Called once for each message.
 * See rdkafka.h for more information.
 */
static void msg_delivered (rd_kafka_t *rk,
			   void *payload, size_t len,
			   int error_code,
			   void *opaque, void *msg_opaque) {

	if (error_code)
		fprintf(stderr, "%% Message delivery failed: %s\n",
			rd_kafka_err2str(error_code));
	else if (!quiet)
		fprintf(stderr, "%% Message delivered (%zd bytes)\n", len);
}


static void msg_consume (rd_kafka_message_t *rkmessage,
			 void *opaque) {
	if (rkmessage->err) {
		if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
			fprintf(stderr,
				"%% Consumer reached end of %s [%"PRId32"] "
			       "message queue at offset %"PRId64"\n",
			       rd_kafka_topic_name(rkmessage->rkt),
			       rkmessage->partition, rkmessage->offset);

			if (exit_eof)
				run = 0;

			return;
		}

		fprintf(stderr, "%% Consume error for topic \"%s\" [%"PRId32"] "
		       "offset %"PRId64": %s\n",
		       rd_kafka_topic_name(rkmessage->rkt),
		       rkmessage->partition,
		       rkmessage->offset,
		       rd_kafka_message_errstr(rkmessage));
		return;
	}

	if (!quiet)
		fprintf(stdout, "%% Message (offset %"PRId64", %zd bytes):\n",
			rkmessage->offset, rkmessage->len);

	if (rkmessage->key_len) {
		if (output == OUTPUT_HEXDUMP)
			hexdump(stdout, "Message Key",
				rkmessage->key, rkmessage->key_len);
		else
			printf("Key: %.*s\n",
			       (int)rkmessage->key_len, (char *)rkmessage->key);
	}

	if (output == OUTPUT_HEXDUMP)
		hexdump(stdout, "Message Payload",
			rkmessage->payload, rkmessage->len);
	else
		printf("%.*s\n",
		       (int)rkmessage->len, (char *)rkmessage->payload);
}


static void sig_usr1 (int sig) {
	rd_kafka_dump(stdout, rk);
}

int main (int argc, char **argv) {
	rd_kafka_topic_t *rkt;
	char *brokers = "localhost:9092";
	char mode = 'C';
	char *topic = NULL;
	int partition = RD_KAFKA_PARTITION_UA;
	int opt;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	char errstr[512];
	const char *debug = NULL;
	int64_t start_offset = 0;
	int do_conf_dump = 0;

	quiet = !isatty(STDIN_FILENO);

	/* Kafka configuration */
	conf = rd_kafka_conf_new();

	/* Topic configuration */
	topic_conf = rd_kafka_topic_conf_new();

	while ((opt = getopt(argc, argv, "PCt:p:b:z:qd:o:eX:A")) != -1) {
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
		case 'z':
			if (rd_kafka_conf_set(conf, "compression.codec",
					      optarg,
					      errstr, sizeof(errstr)) !=
			    RD_KAFKA_CONF_OK) {
				fprintf(stderr, "%% %s\n", errstr);
				exit(1);
			}
			break;
		case 'o':
			if (!strcmp(optarg, "end"))
				start_offset = RD_KAFKA_OFFSET_END;
			else if (!strcmp(optarg, "beginning"))
				start_offset = RD_KAFKA_OFFSET_BEGINNING;
			else if (!strcmp(optarg, "stored"))
				start_offset = RD_KAFKA_OFFSET_STORED;
			else
				start_offset = strtoll(optarg, NULL, 10);
			break;
		case 'e':
			exit_eof = 1;
			break;
		case 'd':
			debug = optarg;
			break;
		case 'q':
			quiet = 1;
			break;
		case 'A':
			output = OUTPUT_RAW;
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

			if (!strcmp(optarg, "dump")) {
				do_conf_dump = 1;
				continue;
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

		default:
			goto usage;
		}
	}


	if (do_conf_dump) {
		const char **arr;
		size_t cnt;
		int pass;

		for (pass = 0 ; pass < 2 ; pass++) {
			int i;

			if (pass == 0) {
				arr = rd_kafka_conf_dump(conf, &cnt);
				printf("# Global config\n");
			} else {
				printf("# Topic config\n");
				arr = rd_kafka_topic_conf_dump(topic_conf,
							       &cnt);
			}

			for (i = 0 ; i < cnt ; i += 2)
				printf("%s = %s\n",
				       arr[i], arr[i+1]);

			printf("\n");

			rd_kafka_conf_dump_free(arr, cnt);
		}

		exit(0);
	}


	if (!topic || optind != argc) {
	usage:
		fprintf(stderr,
			"Usage: %s [-C|-P] -t <topic> "
			"[-p <partition>] [-b <host1:port1,host2:port2,..>]\n"
			"\n"
			"librdkafka version %s (0x%08x)\n"
			"\n"
			" Options:\n"
			"  -C | -P         Consumer or Producer mode\n"
			"  -t <topic>      Topic to fetch / produce\n"
			"  -p <num>        Partition (random partitioner)\n"
			"  -b <brokers>    Broker address (localhost:9092)\n"
			"  -z <codec>      Enable compression:\n"
			"                  none|gzip|snappy\n"
			"  -o <offset>     Start offset (consumer)\n"
			"  -e              Exit consumer when last message\n"
			"                  in partition has been received.\n"
			"  -d [facs..]     Enable debugging contexts:\n"
			"  -q              Be quiet\n"
			"  -A              Raw payload output (consumer)\n"
			"                  %s\n"
			"  -X <prop=name> Set arbitrary librdkafka "
			"configuration property\n"
			"               Properties prefixed with \"topic.\" "
			"will be set on topic object.\n"
			"               Use '-X list' to see the full list\n"
			"               of supported properties.\n"
			"\n"
			" In Consumer mode:\n"
			"  writes fetched messages to stdout\n"
			" In Producer mode:\n"
			"  reads messages from stdin and sends to broker\n"
			"\n"
			"\n"
			"\n",
			argv[0],
			rd_kafka_version_str(), rd_kafka_version(),
			RD_KAFKA_DEBUG_CONTEXTS);
		exit(1);
	}


	signal(SIGINT, stop);
	signal(SIGUSR1, sig_usr1);

	if (debug &&
	    rd_kafka_conf_set(conf, "debug", debug, errstr, sizeof(errstr)) !=
	    RD_KAFKA_CONF_OK) {
		fprintf(stderr, "%% Debug configuration failed: %s: %s\n",
			errstr, debug);
		exit(1);
	}

	if (mode == 'P') {
		/*
		 * Producer
		 */
		char buf[2048];
		int sendcnt = 0;

		/* Set up a message delivery report callback.
		 * It will be called once for each message, either on successful
		 * delivery to broker, or upon failure to deliver to broker. */
		rd_kafka_conf_set_dr_cb(conf, msg_delivered);

		/* Create Kafka handle */
		if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
					errstr, sizeof(errstr)))) {
			fprintf(stderr,
				"%% Failed to create new producer: %s\n",
				errstr);
			exit(1);
		}

		/* Set logger */
		rd_kafka_set_logger(rk, logger);
		rd_kafka_set_log_level(rk, LOG_DEBUG);

		/* Add brokers */
		if (rd_kafka_brokers_add(rk, brokers) == 0) {
			fprintf(stderr, "%% No valid brokers specified\n");
			exit(1);
		}

		/* Create topic */
		rkt = rd_kafka_topic_new(rk, topic, topic_conf);

		if (!quiet)
			fprintf(stderr,
				"%% Type stuff and hit enter to send\n");

		while (run && fgets(buf, sizeof(buf), stdin)) {
			size_t len = strlen(buf);
			if (buf[len-1] == '\n')
				buf[--len] = '\0';

			/* Send/Produce message. */
			if (rd_kafka_produce(rkt, partition,
					     RD_KAFKA_MSG_F_COPY,
					     /* Payload and length */
					     buf, len,
					     /* Optional key and its length */
					     NULL, 0,
					     /* Message opaque, provided in
					      * delivery report callback as
					      * msg_opaque. */
					     NULL) == -1) {
				fprintf(stderr,
					"%% Failed to produce to topic %s "
					"partition %i: %s\n",
					rd_kafka_topic_name(rkt), partition,
					rd_kafka_err2str(
						rd_kafka_errno2err(errno)));
				/* Poll to handle delivery reports */
				rd_kafka_poll(rk, 0);
				continue;
			}

			if (!quiet)
				fprintf(stderr, "%% Sent %zd bytes to topic "
					"%s partition %i\n",
				len, rd_kafka_topic_name(rkt), partition);
			sendcnt++;
			/* Poll to handle delivery reports */
			rd_kafka_poll(rk, 0);
		}

		/* Poll to handle delivery reports */
		rd_kafka_poll(rk, 0);

		/* Wait for messages to be delivered */
		while (run && rd_kafka_outq_len(rk) > 0)
			rd_kafka_poll(rk, 100);

		/* Destroy the handle */
		rd_kafka_destroy(rk);

	} else if (mode == 'C') {
		/*
		 * Consumer
		 */

		/* Create Kafka handle */
		if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf,
					errstr, sizeof(errstr)))) {
			fprintf(stderr,
				"%% Failed to create new consumer: %s\n",
				errstr);
			exit(1);
		}

		/* Set logger */
		rd_kafka_set_logger(rk, logger);
		rd_kafka_set_log_level(rk, LOG_DEBUG);

		/* Add brokers */
		if (rd_kafka_brokers_add(rk, brokers) == 0) {
			fprintf(stderr, "%% No valid brokers specified\n");
			exit(1);
		}

		/* Create topic */
		rkt = rd_kafka_topic_new(rk, topic, topic_conf);

		/* Start consuming */
		if (rd_kafka_consume_start(rkt, partition, start_offset) == -1){
			fprintf(stderr, "%% Failed to start consuming: %s\n",
				rd_kafka_err2str(rd_kafka_errno2err(errno)));
			exit(1);
		}

		while (run) {
			rd_kafka_message_t *rkmessage;

			/* Consume single message.
			 * See rdkafka_performance.c for high speed
			 * consuming of messages. */
			rkmessage = rd_kafka_consume(rkt, partition, 1000);
			if (!rkmessage) /* timeout */
				continue;

			msg_consume(rkmessage, NULL);

			/* Return message to rdkafka */
			rd_kafka_message_destroy(rkmessage);
		}

		/* Stop consuming */
		rd_kafka_consume_stop(rkt, partition);

		rd_kafka_topic_destroy(rkt);

		rd_kafka_destroy(rk);
	}

	/* Let background threads clean up and terminate cleanly. */
	rd_kafka_wait_destroyed(2000);

	return 0;
}
